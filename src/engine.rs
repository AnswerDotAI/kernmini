use crate::language::{
    CompleteRequest, ContextMessage, ExecuteRequest, ExecutionContext, ExecutionInterrupt, InspectRequest, Language, LanguageEvent, LanguageMessage,
    LanguageSession, SessionCommand,
};
use crate::transport::{Inbound, Iopub, RouterPeers, serve_heartbeat, serve_router};
use crate::wire::{Message, Session};
use crate::{ConnectionInfo, Error, ErrorKind};
use bytes::Bytes;
use serde_json::{Value, json};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::future::{Future, poll_fn};
use std::path::Path;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};

#[cfg(unix)]
async fn termination_signal() {
    let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("install SIGTERM handler");
    signal.recv().await;
}

#[cfg(not(unix))]
async fn termination_signal() { std::future::pending().await }

#[derive(Clone, Default)]
pub struct KernelInterrupter { notify: Arc<Notify> }

impl KernelInterrupter {
    pub fn interrupt(&self) { self.notify.notify_one() }
    async fn notified(&self) { self.notify.notified().await }
}

#[derive(Clone, Copy)]
struct KernelConfig { iopub_capacity: usize, hold_timeout: Duration }

impl KernelConfig {
    fn from_env() -> Self {
        let iopub_capacity = std::env::var("KERNMINI_IOPUB_QMAX").ok().and_then(|value| value.parse().ok()).unwrap_or(10_000).max(1);
        let hold_seconds = std::env::var("KERNMINI_HOLD_TIMEOUT")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 0.0)
            .unwrap_or(3600.0);
        Self { iopub_capacity, hold_timeout: Duration::from_secs_f64(hold_seconds) }
    }
}

async fn send_iopub(iopub: &Iopub, session: &Session, parent: &Message, msg_type: &str, content: Value) -> crate::Result<()> {
    iopub.publish(session.message(msg_type, content, Some(parent))).await
}

async fn send_reply(reply: &crate::transport::ReplySink, session: &Session, request: &Message, msg_type: &str, content: Value) -> crate::Result<()> {
    if let Err(error) = reply.send(session.encode(&session.reply(request, msg_type, content))?).await {
        if error.kind() != ErrorKind::Closed { eprintln!("reply to disconnected client failed: {error}"); }
    }
    Ok(())
}

async fn status(iopub: &Iopub, session: &Session, parent: &Message, state: &str) -> crate::Result<()> {
    send_iopub(iopub, session, parent, "status", json!({"execution_state": state})).await
}

fn event_channel(capacity: usize) -> (mpsc::Sender<ContextMessage>, mpsc::Receiver<ContextMessage>) { mpsc::channel(capacity) }

#[derive(Clone)]
struct Stdin { peers: RouterPeers, pending: Arc<Mutex<HashMap<String, PendingInput>>>, session: Session }

struct PendingInput { identity: Bytes, complete: oneshot::Sender<crate::Result<String>> }

impl Stdin {
    fn new(listener: TcpListener, session: Session, tasks: &mut JoinSet<crate::Result<()>>) -> Self {
        let peers = RouterPeers::default();
        let pending = Arc::new(Mutex::new(HashMap::<String, PendingInput>::new()));
        let (send, mut incoming) = mpsc::channel(64);
        tasks.spawn(serve_router(listener, session.clone(), send, Some(peers.clone())));
        let replies = pending.clone();
        tasks.spawn(async move {
            while let Some(inbound) = incoming.recv().await {
                if inbound.message.msg_type() != "input_reply" { continue; }
                let parent = inbound.message.parent_header.get("msg_id").and_then(Value::as_str).unwrap_or("");
                let value = inbound.message.content.get("value").and_then(Value::as_str).unwrap_or("").to_owned();
                let mut replies = replies.lock().await;
                let key = if replies.contains_key(parent) { Some(parent.to_owned()) } else { replies.iter().find(|(_, pending)| pending.identity == inbound.identity).map(|(key, _)| key.clone()) };
                if let Some(pending) = key.and_then(|key| replies.remove(&key)) { let _ = pending.complete.send(Ok(value)); }
            }
            Err(Error::closed("stdin router"))
        });
        Self { peers, pending, session }
    }

    async fn request(&self, identity: &Bytes, parent: &Message, prompt: String, password: bool, interrupt: &ExecutionInterrupt) -> crate::Result<String> {
        let message = self.session.message("input_request", json!({"prompt": prompt, "password": password}), Some(parent));
        let msg_id = message.msg_id().to_owned();
        let (complete, result) = oneshot::channel();
        self.pending.lock().await.insert(msg_id.clone(), PendingInput { identity: identity.clone(), complete });
        let result = tokio::select! {
            biased;
            _ = interrupt.cancelled() => Err(Error::interrupted()),
            result = async {
                let peer = self.peers.wait(identity).await;
                tokio::select! {
                    biased;
                    error = peer.closed() => Err(error.context("waiting for stdin")),
                    result = async {
                        peer.send(self.session.encode(&message)?).await?;
                        result.await?
                    } => result,
                }
            } => result,
        };
        self.pending.lock().await.remove(&msg_id);
        result
    }
}

#[derive(Clone)]
struct ShellServices<L> { language: L, shared: KernelServices }

impl KernelServices {
    fn output_context(&self, request: &Message, identity: Option<Bytes>, silent: bool, interrupt: ExecutionInterrupt, execution_count: u64) -> ExecutionContext {
        let (events, mut output) = event_channel(self.config.iopub_capacity);
        let execution = identity.is_some();
        let client_session = request.header.get("session").and_then(Value::as_str).unwrap_or("").to_owned();
        let subshells = execution.then(|| (client_session, self.subshells.clone()));
        let parent = json!({
            "header": request.header, "parent_header": request.parent_header,
            "metadata": request.metadata, "content": request.content,
        });
        let allow_stdin = execution && request.content.get("allow_stdin").and_then(Value::as_bool).unwrap_or(true);
        let context = ExecutionContext::new(events, interrupt.clone(), subshells, parent, allow_stdin, execution_count);
        let iopub = self.iopub.clone();
        let session = self.session.clone();
        let request = request.clone();
        let stdin = identity.map(|identity| (self.stdin.clone(), identity));
        let failures = self.failures.clone();
        tokio::spawn(async move {
            let pump = async {
                let mut batch = vec![];
                let capacity = output.max_capacity();
                while output.recv_many(&mut batch, capacity).await != 0 {
                    let mut messages = batch.drain(..).peekable();
                    while let Some(mut message) = messages.next() {
                        if let ContextMessage::Event(LanguageEvent::Stream { name, text }) = &mut message {
                            while matches!(messages.peek(), Some(ContextMessage::Event(LanguageEvent::Stream { name: next, .. })) if next == name) {
                                if let Some(ContextMessage::Event(LanguageEvent::Stream { text: next, .. })) = messages.next() { text.push_str(&next); }
                            }
                        }
                        match message {
                            ContextMessage::Event(event) => {
                                publish_event(&iopub, &session, &request, event, silent).await?;
                            }
                            ContextMessage::Flush(complete) => {
                                let _ = complete.send(());
                            }
                            ContextMessage::Input { prompt, password, complete } => {
                                let result = if let Some((stdin, identity)) = &stdin {
                                    stdin.request(identity, &request, prompt, password, &interrupt).await
                                } else { Err(Error::new(ErrorKind::Unavailable, "input is unavailable outside execution")) };
                                let _ = complete.send(result);
                            }
                        }
                    }
                }
                Ok::<_, Error>(())
            };
            tokio::select! {
                result = pump => if let Err(error) = result { let _ = failures.send(error); },
                _ = failures.closed() => {},
            }
        });
        context
    }
}

async fn publish_event(iopub: &Iopub, session: &Session, request: &Message, event: LanguageEvent, silent: bool) -> crate::Result<()> {
    match event {
        LanguageEvent::Stream { name, text } if !silent => {
            send_iopub(iopub, session, request, "stream", json!({"name": name, "text": text})).await?;
        }
        LanguageEvent::Display { event, buffers } if !silent => {
            let (msg_type, content) = if event.get("type").and_then(Value::as_str) == Some("clear_output") {
                ("clear_output", json!({"wait": event.get("wait").and_then(Value::as_bool).unwrap_or(false)}))
            } else {
                let msg_type = if event.get("update").and_then(Value::as_bool).unwrap_or(false) { "update_display_data" } else { "display_data" };
                (
                    msg_type,
                    json!({
                        "data": event.get("data").cloned().unwrap_or_else(|| json!({})),
                        "metadata": event.get("metadata").cloned().unwrap_or_else(|| json!({})),
                        "transient": event.get("transient").cloned().unwrap_or_else(|| json!({})),
                    }),
                )
            };
            let mut message = session.message(msg_type, content, Some(request));
            message.buffers = buffers.into_iter().map(Bytes::from).collect();
            iopub.publish(message).await?;
        }
        LanguageEvent::Message { msg_type, content, metadata, identity, buffers } if !silent => {
            let mut message = session.message(&msg_type, content, Some(request));
            message.metadata = metadata.as_object().cloned().unwrap_or_default();
            if let Some(identity) = identity { message.identities.push(Bytes::from(identity)) }
            message.buffers = buffers.into_iter().map(Bytes::from).collect();
            iopub.publish(message).await?;
        }
        _ => {}
    }
    Ok(())
}

fn missing_fields(request: &Message) -> Vec<&'static str> {
    let required: &[&str] = match request.msg_type() {
        "execute_request" | "is_complete_request" => &["code"],
        "complete_request" | "inspect_request" => &["code", "cursor_pos"],
        "history_request" => &["hist_access_type"],
        _ => &[],
    };
    required.iter().copied().filter(|key| request.content.get(*key).is_none()).collect()
}

async fn reply_missing(
    execution_count: u64,
    iopub: &Iopub,
    session: &Session,
    request: &Message,
    reply: &crate::transport::ReplySink,
    missing: &[&str],
) -> crate::Result<()> {
    let content = error_content(request, execution_count, "MissingField", format!("missing required fields: {}", missing.join(", ")));
    reply_error(iopub, session, request, reply, content).await
}

async fn reply_error(
    iopub: &Iopub, session: &Session, request: &Message, reply: &crate::transport::ReplySink, content: Value,
) -> crate::Result<()> {
    if request.msg_type() == "execute_request" {
        status(iopub, session, request, "busy").await?;
        send_iopub(iopub, session, request, "error", content.clone()).await?;
    }
    let reply_type = request.msg_type().replace("_request", "_reply");
    send_reply(reply, session, request, &reply_type, content).await?;
    if request.msg_type() == "execute_request" { status(iopub, session, request, "idle").await?; }
    Ok(())
}

fn error_content(request: &Message, execution_count: u64, ename: &str, evalue: String) -> Value {
    let mut content = json!({"status": "error", "ename": ename, "evalue": evalue, "traceback": []});
    if request.msg_type() == "execute_request" {
        content["execution_count"] = json!(execution_count);
        content["user_expressions"] = json!({});
        content["payload"] = json!([]);
    }
    else if request.msg_type() == "complete_request" {
        content["matches"] = json!([]);
        content["cursor_start"] = json!(0);
        content["cursor_end"] = json!(0);
        content["metadata"] = json!({});
    }
    else if request.msg_type() == "inspect_request" {
        content["found"] = json!(false);
        content["data"] = json!({});
        content["metadata"] = json!({});
    }
    else if request.msg_type() == "history_request" { content["history"] = json!([]); }
    else if request.msg_type() == "is_complete_request" { content["indent"] = json!(""); }
    content
}

/// The execute request in `content`, with the protocol's defaults for missing fields.
fn execute_request(content: &Value) -> ExecuteRequest {
    ExecuteRequest {
        code: content.get("code").and_then(Value::as_str).unwrap_or("").to_owned(),
        silent: content.get("silent").and_then(Value::as_bool).unwrap_or(false),
        store_history: content.get("store_history").and_then(Value::as_bool).unwrap_or(true),
        user_expressions: content.get("user_expressions").cloned().unwrap_or_else(|| json!({})),
        allow_stdin: content.get("allow_stdin").and_then(Value::as_bool).unwrap_or(true),
    }
}
async fn execute(
    services: &ShellServices<impl LanguageSession>,
    identity: Bytes,
    request: &Message,
    execute: ExecuteRequest,
    execution_count: u64,
    interrupt: ExecutionInterrupt,
) -> crate::Result<(Value, bool)> {
    let language = &services.language;
    let iopub = &services.shared.iopub;
    let session = &services.shared.session;
    status(iopub, session, request, "busy").await?;
    if !execute.silent {
        send_iopub(iopub, session, request, "execute_input", json!({"code": execute.code, "execution_count": execution_count})).await?;
    }

    let silent = execute.silent;
    let output = services.shared.output_context(request, Some(identity), silent, interrupt, execution_count);
    let outcome = language.execute(execute, output.clone()).await;
    let flushed = output.flush().await;
    let outcome = outcome?;
    flushed?;

    let failed = outcome.error.is_some();
    let content = if let Some(error) = outcome.error {
        let error = json!({"ename": error.ename, "evalue": error.evalue, "traceback": error.traceback});
        send_iopub(iopub, session, request, "error", error.clone()).await?;
        json!({
            "status": "error", "execution_count": execution_count,
            "ename": error["ename"], "evalue": error["evalue"], "traceback": error["traceback"],
        })
    } else {
        if !silent && let Some(result) = outcome.result {
            send_iopub(
                iopub,
                session,
                request,
                "execute_result",
                json!({
                    "execution_count": execution_count,
                    "data": result, "metadata": outcome.result_metadata,
                }),
            )
            .await?;
        }
        json!({
            "status": "ok", "execution_count": execution_count,
            "user_expressions": outcome.user_expressions, "payload": outcome.payload,
        })
    };
    Ok((content, failed))
}

struct QueueItem { priority: f64, order: u64, inbound: Inbound }

impl PartialEq for QueueItem { fn eq(&self, other: &Self) -> bool { self.priority == other.priority && self.order == other.order } }

impl Eq for QueueItem {}

impl PartialOrd for QueueItem { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }

impl Ord for QueueItem {
    fn cmp(&self, other: &Self) -> Ordering { self.priority.partial_cmp(&other.priority).unwrap_or(Ordering::Equal).then_with(|| other.order.cmp(&self.order)) }
}

enum ShellOutcome { Continue, Shutdown }

struct ExecutionDone { failed: bool, stop_on_error: bool }

struct ActiveExecution { task: JoinHandle<crate::Result<ExecutionDone>>, interrupt: ExecutionInterrupt }

impl Drop for ActiveExecution { fn drop(&mut self) { self.task.abort(); } }

async fn wait_execution(active: &mut Option<ActiveExecution>) -> crate::Result<ExecutionDone> {
    if let Some(active) = active { (&mut active.task).await? } else { std::future::pending().await }
}

enum ShellControl { Release { msg_id: String, status: String, complete: oneshot::Sender<bool> }, Interrupt, Stop }

async fn handle_shell(services: &ShellServices<impl LanguageSession>, inbound: Inbound, execution_count: u64) -> crate::Result<ShellOutcome> {
    let language = &services.language;
    let iopub = &services.shared.iopub;
    let session = &services.shared.session;
    let request = inbound.message;
    let reply = inbound.reply;
    let missing = missing_fields(&request);
    if !missing.is_empty() {
        reply_missing(execution_count, iopub, session, &request, &reply, &missing).await?;
        return Ok(ShellOutcome::Continue);
    }
    match request.msg_type() {
        "shutdown_request" => {
            let content = json!({"status": "ok", "restart": request.content.get("restart").and_then(Value::as_bool).unwrap_or(false)});
            send_reply(&reply, session, &request, "shutdown_reply", content).await?;
            Ok(ShellOutcome::Shutdown)
        }
        "kernel_info_request" | "complete_request" | "inspect_request" | "is_complete_request" | "history_request" => {
            status(iopub, session, &request, "busy").await?;
            let content = match request.msg_type() {
                "kernel_info_request" => {
                    let info = language.kernel_info()?;
                    let debugger = language.supports_debugger();
                    let mut features = vec![];
                    if services.shared.supports_subshells { features.push("kernel subshells"); }
                    if debugger { features.push("debugger"); }
                    json!({
                        "status": "ok", "protocol_version": "5.3",
                        "implementation": info.implementation,
                        "implementation_version": info.implementation_version,
                        "banner": info.banner, "language_info": info.language_info,
                        "help_links": [], "debugger": debugger, "supported_features": features,
                    })
                },
                "complete_request" => language.complete(CompleteRequest {
                    code: request.content.get("code").and_then(Value::as_str).unwrap_or("").to_owned(),
                    cursor_pos: request.content.get("cursor_pos").and_then(Value::as_u64).unwrap_or(0),
                }).await?,
                "inspect_request" => language.inspect(InspectRequest {
                    code: request.content.get("code").and_then(Value::as_str).unwrap_or("").to_owned(),
                    cursor_pos: request.content.get("cursor_pos").and_then(Value::as_u64).unwrap_or(0),
                    detail_level: request.content.get("detail_level").and_then(Value::as_u64).unwrap_or(0),
                }).await?,
                "is_complete_request" => language.is_complete(request.content.get("code").and_then(Value::as_str).unwrap_or("").to_owned()).await?,
                "history_request" => language.history(request.content.clone()).await?,
                _ => unreachable!(),
            };
            send_reply(&reply, session, &request, &request.msg_type().replace("_request", "_reply"), content).await?;
            status(iopub, session, &request, "idle").await?;
            Ok(ShellOutcome::Continue)
        }
        "comm_info_request" => {
            let content = language.comm_info(request.content.clone()).await?;
            send_reply(&reply, session, &request, "comm_info_reply", content).await?;
            Ok(ShellOutcome::Continue)
        }
        "connect_request" => {
            send_reply(&reply, session, &request, "connect_reply", services.shared.connection.as_ref().clone()).await?;
            Ok(ShellOutcome::Continue)
        }
        "comm_open" | "comm_msg" | "comm_close" => {
            let output = services.shared.output_context(&request, None, false, ExecutionInterrupt::default(), execution_count);
            language
                .message(
                    LanguageMessage {
                        msg_type: request.msg_type().to_owned(),
                        content: request.content.clone(),
                        buffers: request.buffers.iter().map(|buffer| buffer.to_vec()).collect(),
                    },
                    output.clone(),
                )
                .await?;
            output.flush().await?;
            Ok(ShellOutcome::Continue)
        }
        msg_type if msg_type.ends_with("_request") => {
            let reply_type = msg_type.replace("_request", "_reply");
            send_reply(&reply, session, &request, &reply_type, json!({"status": "ok"})).await?;
            Ok(ShellOutcome::Continue)
        }
        _ => Ok(ShellOutcome::Continue),
    }
}

async fn run_execution(
    services: ShellServices<impl LanguageSession>,
    inbound: Inbound,
    execution: ExecuteRequest,
    execution_count: u64,
    interrupt: ExecutionInterrupt,
) -> crate::Result<ExecutionDone> {
    let request = inbound.message;
    let stop_on_error = request.content.get("stop_on_error").and_then(Value::as_bool).unwrap_or(true);
    let (content, failed) = match execute(&services, inbound.identity, &request, execution, execution_count, interrupt).await {
        Ok(result) => result,
        Err(error) => {
            report_failure(&services, &request, &inbound.reply, &error, execution_count).await?;
            if error.kind() == ErrorKind::Closed { return Err(error); }
            return Ok(ExecutionDone { failed: true, stop_on_error });
        }
    };
    send_reply(&inbound.reply, &services.shared.session, &request, "execute_reply", content).await?;
    status(&services.shared.iopub, &services.shared.session, &request, "idle").await?;
    Ok(ExecutionDone { failed, stop_on_error })
}

async fn report_failure(
    services: &ShellServices<impl LanguageSession>,
    request: &Message,
    reply: &crate::transport::ReplySink,
    error: &Error,
    execution_count: u64,
) -> crate::Result<()> {
    let ename = if error.kind() == ErrorKind::Interrupted { "KeyboardInterrupt" } else { "KernelError" };
    let content = error_content(request, execution_count, ename, error.to_string());
    if request.msg_type() == "execute_request" { send_iopub(&services.shared.iopub, &services.shared.session, request, "error", content.clone()).await?; }
    if request.msg_type().ends_with("_request") {
        send_reply(reply, &services.shared.session, request, &request.msg_type().replace("_request", "_reply"), content).await?;
    }
    else { eprintln!("{} failed: {error}", request.msg_type()); }
    status(&services.shared.iopub, &services.shared.session, request, "idle").await
}

async fn abort_execute(execution_count: u64, iopub: &Iopub, session: &Session, inbound: Inbound) -> crate::Result<()> {
    let request = inbound.message;
    status(iopub, session, &request, "busy").await?;
    let content = json!({
        "status": "aborted", "execution_count": execution_count,
        "user_expressions": {}, "payload": [],
    });
    send_reply(&inbound.reply, session, &request, "execute_reply", content).await?;
    status(iopub, session, &request, "idle").await
}

async fn interrupt_execute(execution_count: u64, iopub: &Iopub, session: &Session, inbound: Inbound) -> crate::Result<()> {
    let request = inbound.message;
    status(iopub, session, &request, "busy").await?;
    let content = json!({
        "status": "error", "execution_count": execution_count,
        "ename": "KeyboardInterrupt", "evalue": "", "traceback": [],
        "user_expressions": {}, "payload": [],
    });
    send_iopub(iopub, session, &request, "error", json!({"ename": "KeyboardInterrupt", "evalue": "", "traceback": []})).await?;
    send_reply(&inbound.reply, session, &request, "execute_reply", content).await?;
    status(iopub, session, &request, "idle").await
}

async fn begin_hold(execution_count: u64, iopub: &Iopub, session: &Session, item: &QueueItem) -> crate::Result<()> {
    let request = &item.inbound.message;
    status(iopub, session, request, "busy").await?;
    if !request.content.get("silent").and_then(Value::as_bool).unwrap_or(false) {
        let code = request.content.get("code").and_then(Value::as_str).unwrap_or("");
        send_iopub(iopub, session, request, "execute_input", json!({"code": code, "execution_count": execution_count})).await?;
    }
    Ok(())
}

async fn finish_hold(execution_count: u64, iopub: &Iopub, session: &Session, item: QueueItem, release_status: &str) -> crate::Result<bool> {
    let request = item.inbound.message;
    let error = match release_status {
        "error" => Some(json!({"ename": "HoldError", "evalue": "released with status error", "traceback": []})),
        "interrupt" => Some(json!({"ename": "KeyboardInterrupt", "evalue": "", "traceback": []})),
        "timeout" => Some(json!({"ename": "HoldTimeout", "evalue": "held execution timed out", "traceback": []})),
        _ => None,
    };
    let failed = error.is_some();
    let content = if let Some(error) = error {
        send_iopub(iopub, session, &request, "error", error.clone()).await?;
        json!({
            "status": "error", "execution_count": execution_count,
            "ename": error["ename"], "evalue": error["evalue"], "traceback": error["traceback"],
            "user_expressions": {}, "payload": [],
        })
    } else { json!({"status": "ok", "execution_count": null, "user_expressions": {}, "payload": []}) };
    send_reply(&item.inbound.reply, session, &request, "execute_reply", content).await?;
    status(iopub, session, &request, "idle").await?;
    Ok(failed)
}

struct Held { item: QueueItem, deadline: tokio::time::Instant }

async fn wait_hold(deadline: Option<tokio::time::Instant>) {
    if let Some(deadline) = deadline { tokio::time::sleep_until(deadline).await } else { std::future::pending().await }
}

fn pop_runnable(queue: &mut BinaryHeap<QueueItem>, execution_active: bool, held: Option<&Held>) -> Option<QueueItem> {
    let mut parked = vec![];
    let runnable = loop {
        let Some(item) = queue.pop() else { break None };
        let execute = item.inbound.message.msg_type() == "execute_request";
        let above_hold = held.is_none_or(|hold| item.priority > hold.item.priority);
        if !execute || (!execution_active && above_hold) { break Some(item); }
        parked.push(item);
    };
    queue.extend(parked);
    runnable
}

struct Shell<L: LanguageSession> {
    services: ShellServices<L>,
    incoming: mpsc::Receiver<Inbound>,
    controls: mpsc::Receiver<ShellControl>,
    queue: BinaryHeap<QueueItem>,
    order: u64,
    held: Option<Held>,
    active: Option<ActiveExecution>,
    stopping: bool,
    interrupting: bool,
    /// The count of the next execution that stores history. As in IPython, it starts at 1, and every other request reports it without
    /// advancing it.
    execution_count: u64,
}

impl<L: LanguageSession> Shell<L> {
    fn enqueue(&mut self, inbound: Inbound) {
        let priority = inbound.message.metadata.get("priority").and_then(Value::as_f64).unwrap_or(0.0);
        self.queue.push(QueueItem { priority, order: self.order, inbound });
        self.order += 1;
    }

    /// The execution count for `execute`. An execution that stores history takes the count and advances it.
    fn count_execution(&mut self, execute: &ExecuteRequest) -> u64 {
        let count = self.execution_count;
        if execute.store_history && !execute.silent { self.execution_count += 1 }
        count
    }

    async fn abort_pending(&mut self) -> crate::Result<()> {
        while let Ok(inbound) = self.incoming.try_recv() { self.enqueue(inbound) }
        let mut keep = vec![];
        while let Some(item) = self.queue.pop() {
            if item.inbound.message.msg_type() == "execute_request" {
                abort_execute(self.execution_count, &self.services.shared.iopub, &self.services.shared.session, item.inbound).await?;
            } else { keep.push(item); }
        }
        self.queue.extend(keep);
        Ok(())
    }

    async fn apply_control(&mut self, control: ShellControl) -> crate::Result<()> {
        match control {
            ShellControl::Release { msg_id, status: release_status, complete } => {
                let found = self.held.as_ref().is_some_and(|held| held.item.inbound.message.msg_id() == msg_id);
                if found {
                    let failed = finish_hold(
                        self.execution_count,
                        &self.services.shared.iopub,
                        &self.services.shared.session,
                        self.held.take().unwrap().item,
                        &release_status,
                    )
                    .await?;
                    if failed { self.abort_pending().await? }
                }
                let _ = complete.send(found);
            }
            ShellControl::Interrupt => {
                self.interrupting = true;
                if let Some(active) = &self.active { active.interrupt.request()?; }
                if let Some(held) = self.held.take() {
                    finish_hold(self.execution_count, &self.services.shared.iopub, &self.services.shared.session, held.item, "interrupt").await?;
                }
                self.abort_pending().await?;
            }
            ShellControl::Stop => self.stopping = true,
        }
        Ok(())
    }

    async fn execution_done(&mut self, done: ExecutionDone) -> crate::Result<()> {
        self.active.take();
        if done.failed && done.stop_on_error && !self.interrupting { self.abort_pending().await? }
        Ok(())
    }

    async fn handle_item(&mut self, item: QueueItem) -> crate::Result<bool> {
        if item.inbound.message.msg_type() != "execute_request" {
            let request = item.inbound.message.clone();
            let reply = item.inbound.reply.clone();
            return match handle_shell(&self.services, item.inbound, self.execution_count).await {
                Ok(outcome) => Ok(matches!(outcome, ShellOutcome::Shutdown)),
                Err(error) => {
                    report_failure(&self.services, &request, &reply, &error, self.execution_count).await?;
                    if error.kind() == ErrorKind::Closed { Err(error) } else { Ok(false) }
                }
            };
        }
        if self.interrupting {
            interrupt_execute(self.execution_count, &self.services.shared.iopub, &self.services.shared.session, item.inbound).await?;
            return Ok(false);
        }
        let missing = missing_fields(&item.inbound.message);
        if !missing.is_empty() {
            reply_missing(
                self.execution_count,
                &self.services.shared.iopub,
                &self.services.shared.session,
                &item.inbound.message,
                &item.inbound.reply,
                &missing,
            )
            .await?;
            self.abort_pending().await?;
        }
        else if item.inbound.message.metadata.get("hold").and_then(Value::as_bool).unwrap_or(false) {
            begin_hold(self.execution_count, &self.services.shared.iopub, &self.services.shared.session, &item).await?;
            self.held = Some(Held { item, deadline: tokio::time::Instant::now() + self.services.shared.config.hold_timeout });
        }
        else {
            let interrupt = ExecutionInterrupt::default();
            let execution = execute_request(&item.inbound.message.content);
            let execution_count = self.count_execution(&execution);
            let task = tokio::spawn(run_execution(self.services.clone(), item.inbound, execution, execution_count, interrupt.clone()));
            self.active = Some(ActiveExecution { task, interrupt });
        }
        Ok(false)
    }

    async fn run(mut self) -> crate::Result<()> {
        loop {
            while let Ok(inbound) = self.incoming.try_recv() { self.enqueue(inbound) }
            while let Ok(control) = self.controls.try_recv() { self.apply_control(control).await? }
            if self.active.as_ref().is_some_and(|active| active.task.is_finished()) {
                let done = wait_execution(&mut self.active).await?;
                self.execution_done(done).await?;
            }
            if self.interrupting && self.active.is_none() { self.interrupting = false }
            if self.stopping && self.queue.is_empty() && self.active.is_none() && self.held.is_none() {
                self.services.language.shutdown().await?;
                return Ok(());
            }

            if let Some(item) = pop_runnable(&mut self.queue, self.active.is_some(), self.held.as_ref()) {
                if self.handle_item(item).await? { return self.services.language.shutdown().await; }
                continue;
            }

            tokio::select! {
                message = self.incoming.recv() => self.enqueue(message.ok_or_else(|| Error::closed("shell service"))?),
                control = self.controls.recv() => self.apply_control(control.ok_or_else(|| Error::closed("shell control"))?).await?,
                result = wait_execution(&mut self.active) => self.execution_done(result?).await?,
                _ = wait_hold(self.held.as_ref().map(|held| held.deadline)), if self.held.is_some() => {
                    finish_hold(self.execution_count, &self.services.shared.iopub, &self.services.shared.session,
                        self.held.take().unwrap().item, "timeout").await?;
                    self.abort_pending().await?;
                }
            }
        }
    }
}

struct ShellHandle { incoming: mpsc::Sender<Inbound>, controls: mpsc::Sender<ShellControl>, task: JoinHandle<crate::Result<()>> }

#[derive(Clone)]
struct KernelServices {
    iopub: Iopub,
    stdin: Stdin,
    session: Session,
    connection: Arc<Value>,
    supports_subshells: bool,
    config: KernelConfig,
    subshells: mpsc::UnboundedSender<SessionCommand>,
    failures: mpsc::UnboundedSender<Error>,
}

impl KernelServices {
    fn spawn_shell(&self, language: impl LanguageSession) -> ShellHandle {
        let (incoming, requests) = mpsc::channel(256);
        let (controls, shell_controls) = mpsc::channel(64);
        let services = ShellServices { language, shared: self.clone() };
        let shell = Shell {
            services,
            incoming: requests,
            controls: shell_controls,
            queue: BinaryHeap::new(),
            order: 0,
            held: None,
            active: None,
            stopping: false,
            interrupting: false,
            execution_count: 1,
        };
        ShellHandle { incoming, controls, task: tokio::spawn(shell.run()) }
    }
}

fn subshell_id(request: &Message) -> &str { request.header.get("subshell_id").and_then(Value::as_str).filter(|id| !id.is_empty()).unwrap_or("") }

async fn reply_subshell_error(iopub: &Iopub, session: &Session, inbound: Inbound, ename: &str, evalue: String) -> crate::Result<()> {
    let request = inbound.message;
    if !request.msg_type().ends_with("_request") { return Ok(()); }
    let content = error_content(&request, 0, ename, evalue);
    reply_error(iopub, session, &request, &inbound.reply, content).await
}

async fn reply_subshell_not_found(iopub: &Iopub, session: &Session, inbound: Inbound) -> crate::Result<()> {
    let id = subshell_id(&inbound.message).to_owned();
    reply_subshell_error(iopub, session, inbound, "SubshellNotFound", format!("Unknown subshell_id {id:?}")).await
}

async fn route_shell<L: Language>(
    inbound: Inbound,
    language: &L,
    services: &KernelServices,
    shells: &mut HashMap<String, ShellHandle>,
    route_overrides: &HashMap<String, String>,
) -> crate::Result<()> {
    let (iopub, session) = (&services.iopub, &services.session);
    let explicit = subshell_id(&inbound.message);
    let client_session = inbound.message.header.get("session").and_then(Value::as_str).unwrap_or("");
    let id = if !explicit.is_empty() { explicit.to_owned() } else if inbound.message.msg_type() == "execute_request" { route_overrides.get(client_session).cloned().unwrap_or_default() } else { String::new() };
    if !explicit.is_empty() && !shells.contains_key(&id) {
        if let Err(error) = create_subshell(language, services, shells, Some(id.clone())).await {
            return reply_subshell_error(iopub, session, inbound, "SubshellCreationError", error.to_string()).await;
        }
    }
    if let Some(target) = shells.get(&id) {
        if let Err(error) = target.incoming.send(inbound).await { reply_subshell_not_found(iopub, session, error.0).await? }
    }
    else { reply_subshell_not_found(iopub, session, inbound).await?; }
    Ok(())
}

async fn route_pending_shells<L: Language>(
    shell: &mut mpsc::Receiver<Inbound>,
    language: &L,
    services: &KernelServices,
    shells: &mut HashMap<String, ShellHandle>,
    route_overrides: &HashMap<String, String>,
) -> crate::Result<()> {
    while let Ok(inbound) = shell.try_recv() { route_shell(inbound, language, services, shells, route_overrides).await? }
    Ok(())
}

async fn stop_shell(shell: ShellHandle, interrupt: bool) -> crate::Result<()> {
    if interrupt { let _ = shell.controls.send(ShellControl::Interrupt).await; }
    let _ = shell.controls.send(ShellControl::Stop).await;
    shell.task.await?
}

async fn interrupt_shells(shells: &HashMap<String, ShellHandle>) -> crate::Result<()> {
    for shell in shells.values() { shell.controls.send(ShellControl::Interrupt).await?; }
    Ok(())
}

async fn create_subshell(
    language: &impl Language,
    services: &KernelServices,
    shells: &mut HashMap<String, ShellHandle>,
    requested_id: Option<String>,
) -> crate::Result<String> {
    let id = requested_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    if id.is_empty() { return Err(Error::new(ErrorKind::InvalidInput, "subshell_id cannot be empty")); }
    if !language.supports_children() { return Err(Error::new(ErrorKind::Unavailable, "subshells are not supported")); }
    if shells.contains_key(&id) { return Ok(id); }
    let child = language.create_child().await?;
    shells.insert(id.clone(), services.spawn_shell(child));
    Ok(id)
}

pub async fn run_kernel(connection_file: impl AsRef<Path>, language: impl Language) -> crate::Result<()> {
    let interrupt = KernelInterrupter::default();
    let signal_interrupt = interrupt.clone();
    let signals = tokio::spawn(async move { while tokio::signal::ctrl_c().await.is_ok() { signal_interrupt.interrupt() } });
    let result = run_kernel_with_interrupter(connection_file, language, interrupt).await;
    signals.abort();
    result
}

/// Runs `run_kernel` on a new multi-threaded Tokio runtime, for a program that has none. The runtime awaits `language` first, so
/// starting the language can use it, as `ThreadWorker::start` does.
pub fn run_kernel_blocking<L: Language>(connection_file: impl AsRef<Path>, language: impl Future<Output = crate::Result<L>>) -> crate::Result<()> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async { run_kernel(connection_file, language.await?).await })
}

pub async fn run_kernel_with_interrupter(connection_file: impl AsRef<Path>, language: impl Language, interrupt: KernelInterrupter) -> crate::Result<()> {
    let config = KernelConfig::from_env();
    let connection = ConnectionInfo::read(connection_file)?;
    let connection_content = Arc::new(json!({
        "shell_port": connection.shell_port, "iopub_port": connection.iopub_port,
        "stdin_port": connection.stdin_port, "control_port": connection.control_port,
        "hb_port": connection.hb_port,
    }));
    let session = Session::new(connection.key.as_bytes().to_vec(), "kernel");
    let shell_listener = TcpListener::bind(connection.address(connection.shell_port)?).await?;
    let control_listener = TcpListener::bind(connection.address(connection.control_port)?).await?;
    let iopub_listener = TcpListener::bind(connection.address(connection.iopub_port)?).await?;
    let heartbeat_listener = TcpListener::bind(connection.address(connection.hb_port)?).await?;
    let stdin_listener = TcpListener::bind(connection.address(connection.stdin_port)?).await?;

    let (shell_send, mut shell) = mpsc::channel(256);
    let (control_send, mut control) = mpsc::channel(64);
    let mut background = JoinSet::new();
    background.spawn(serve_router(shell_listener, session.clone(), shell_send, None));
    background.spawn(serve_router(control_listener, session.clone(), control_send, None));
    let stdin = Stdin::new(stdin_listener, session.clone(), &mut background);
    let iopub = Iopub::new(session.clone(), config.iopub_capacity);
    background.spawn(iopub.clone().serve(iopub_listener));
    background.spawn(serve_heartbeat(heartbeat_listener));
    let supports_children = language.supports_children();
    let (subshell_send, mut subshell_commands) = mpsc::unbounded_channel();
    let (failures, mut failed) = mpsc::unbounded_channel();
    let kernel_services = KernelServices {
        iopub: iopub.clone(),
        stdin: stdin.clone(),
        session: session.clone(),
        connection: connection_content.clone(),
        supports_subshells: supports_children,
        config,
        subshells: subshell_send.clone(),
        failures,
    };
    let mut shells = HashMap::new();
    let mut route_overrides = HashMap::<String, String>::new();
    let mut terminate = Box::pin(termination_signal());
    let parent = language.parent();
    let control_language = parent.clone();
    let debug_iopub = iopub.clone();
    let debug_session = session.clone();
    let runtime = tokio::runtime::Handle::current();
    let debug_failures = kernel_services.failures.clone();
    parent.set_debug_sender(Arc::new(move |event| {
        let iopub = debug_iopub.clone();
        let session = debug_session.clone();
        let failures = debug_failures.clone();
        runtime.spawn(async move { if let Err(error) = iopub.publish(session.message("debug_event", event, None)).await { let _ = failures.send(error); } });
    }))?;
    shells.insert(String::new(), kernel_services.spawn_shell(parent));
    let result = async {
        loop {
            let inbound = tokio::select! {
                result = background.join_next() => {
                    result.expect("kernel services are running")??;
                    return Err(Error::closed("kernel service"));
                }
                error = failed.recv() => return Err(error.unwrap_or_else(|| Error::closed("kernel output"))),
                (id, result) = poll_fn(|cx| {
                    for (id, shell) in &mut shells {
                        if let Poll::Ready(result) = std::pin::Pin::new(&mut shell.task).poll(cx) { return Poll::Ready((id.clone(), result)); }
                    }
                    Poll::Pending
                }) => {
                    shells.remove(&id);
                    result??;
                    if id.is_empty() { return Ok(()); }
                    route_overrides.retain(|_, routed| routed != &id);
                    continue;
                }
                _ = &mut terminate => {
                    return Ok(());
                }
                _ = interrupt.notified() => {
                    interrupt_shells(&shells).await?;
                    continue;
                }
                message = shell.recv() => {
                    let inbound = message.ok_or_else(|| Error::closed("shell service"))?;
                    route_shell(inbound, &language, &kernel_services, &mut shells, &route_overrides).await?;
                    continue;
                }
                command = subshell_commands.recv() => {
                    match command.ok_or_else(|| Error::closed("subshell command service"))? {
                        SessionCommand::Open { client_session, subshell_id, complete } => {
                            let result = if route_overrides.contains_key(&client_session) {
                                Err(Error::new(ErrorKind::InvalidInput, "this client session already has a subshell route"))
                            } else {
                                create_subshell(&language, &kernel_services, &mut shells, subshell_id).await.map(|id| {
                                    route_overrides.insert(client_session, id.clone());
                                    id
                                })
                            };
                            let _ = complete.send(result);
                        }
                        SessionCommand::Close { client_session, subshell_id, delete, complete } => {
                            let result = if route_overrides.get(&client_session) == Some(&subshell_id) {
                                route_overrides.remove(&client_session);
                                if delete {
                                    if let Some(shell) = shells.remove(&subshell_id) { stop_shell(shell, false).await?; }
                                }
                                Ok(())
                            } else {
                                Err(Error::new(ErrorKind::InvalidInput, "subshell route is not active"))
                            };
                            let _ = complete.send(result);
                        }
                    }
                    continue;
                }
                message = control.recv() => message.ok_or_else(|| Error::closed("control service"))?,
            };
            let request = inbound.message;
            let reply = inbound.reply;
            match request.msg_type() {
                "shutdown_request" => {
                    let content = json!({"status": "ok", "restart": request.content.get("restart").and_then(Value::as_bool).unwrap_or(false)});
                    send_reply(&reply, &session, &request, "shutdown_reply", content).await?;
                    return Ok(());
                }
                "interrupt_request" => {
                    route_pending_shells(&mut shell, &language, &kernel_services, &mut shells, &route_overrides).await?;
                    interrupt_shells(&shells).await?;
                    send_reply(&reply, &session, &request, "interrupt_reply", json!({"status": "ok"})).await?;
                }
                "create_subshell_request" => {
                    if !supports_children {
                        send_reply(
                            &reply,
                            &session,
                            &request,
                            "create_subshell_reply",
                            json!({
                                "status": "error", "ename": "SubshellsNotSupported", "evalue": "kernel subshells are not supported", "traceback": [],
                            }),
                        )
                        .await?;
                        continue;
                    }
                    let requested_id = request.content.get("subshell_id").and_then(Value::as_str).map(str::to_owned);
                    let content = match create_subshell(&language, &kernel_services, &mut shells, requested_id).await {
                        Ok(id) => json!({"status": "ok", "subshell_id": id}),
                        Err(error) => json!({"status": "error", "ename": "SubshellCreationError", "evalue": error.to_string(), "traceback": []}),
                    };
                    send_reply(&reply, &session, &request, "create_subshell_reply", content).await?;
                }
                "list_subshell_request" => {
                    let ids = shells.keys().filter(|id| !id.is_empty()).cloned().collect::<Vec<_>>();
                    send_reply(&reply, &session, &request, "list_subshell_reply", json!({"status": "ok", "subshell_id": ids})).await?;
                }
                "delete_subshell_request" => {
                    let id = request.content.get("subshell_id").and_then(Value::as_str).unwrap_or("");
                    let content = if let Some(shell) = shells.remove(id) {
                        route_overrides.retain(|_, routed| routed != id);
                        stop_shell(shell, true).await?;
                        json!({"status": "ok"})
                    } else { json!({"status": "error", "ename": "SubshellNotFound", "evalue": format!("Unknown subshell_id {id:?}"), "traceback": []}) };
                    send_reply(&reply, &session, &request, "delete_subshell_reply", content).await?;
                }
                "release_request" => {
                    route_pending_shells(&mut shell, &language, &kernel_services, &mut shells, &route_overrides).await?;
                    let msg_id = request.content.get("msg_id").and_then(Value::as_str).unwrap_or("").to_owned();
                    let release_status = request.content.get("status").and_then(Value::as_str).unwrap_or("ok").to_owned();
                    let mut found = false;
                    for shell in shells.values() {
                        let (complete, result) = oneshot::channel();
                        shell.controls.send(ShellControl::Release { msg_id: msg_id.clone(), status: release_status.clone(), complete }).await?;
                        found |= result.await?;
                    }
                    let content = json!({"status": "ok", "found": found});
                    send_reply(&reply, &session, &request, "release_reply", content).await?;
                }
                "debug_request" => {
                    status(&iopub, &session, &request, "busy").await?;
                    let result = control_language.debug(request.content.clone()).await.unwrap_or_else(|error| {
                        json!({"response": {
                            "type": "response", "request_seq": request.content["seq"], "command": request.content["command"],
                            "success": false, "message": error.to_string(),
                        }})
                    });
                    let response = result.get("response").cloned().unwrap_or_else(|| json!({}));
                    send_reply(&reply, &session, &request, "debug_reply", response).await?;
                    if let Some(events) = result.get("events").and_then(Value::as_array) {
                        for event in events { send_iopub(&iopub, &session, &request, "debug_event", event.clone()).await?; }
                    }
                    status(&iopub, &session, &request, "idle").await?;
                }
                msg_type if msg_type.ends_with("_request") => {
                    let reply_type = msg_type.replace("_request", "_reply");
                    send_reply(&reply, &session, &request, &reply_type, json!({"status": "ok"})).await?;
                }
                _ => {}
            }
        }
    }
    .await;
    let mut result = result;
    for (_, shell) in shells.drain() {
        if let Err(error) = stop_shell(shell, true).await { if result.is_ok() { result = Err(error); } else { eprintln!("kernel shutdown failed: {error}"); } }
    }
    background.shutdown().await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pending_input_lifetime() -> crate::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let session = Session::new(b"test".to_vec(), "test");
        let parent = session.message("execute_request", json!({"code": "input()"}), None);
        let mut tasks = JoinSet::new();
        let stdin = Stdin::new(listener, session.clone(), &mut tasks);
        let missing = Bytes::from_static(b"missing");
        let interrupt = ExecutionInterrupt::default();
        let read = stdin.request(&missing, &parent, "".into(), false, &interrupt);
        tokio::pin!(read);
        poll_fn(|cx| { assert!(read.as_mut().poll(cx).is_pending()); Poll::Ready(()) })
        .await;
        interrupt.request()?;
        assert_eq!(read.await.unwrap_err().kind(), ErrorKind::Interrupted);
        assert!(stdin.pending.lock().await.is_empty());

        for scenario in ["interrupt", "disconnect", "reply"] {
            let mut peer = zmtpmini::Dealer::connect(address, Some(scenario.as_bytes())).await?;
            let token = ExecutionInterrupt::default();
            let (input, parent, interrupt) = (stdin.clone(), parent.clone(), token.clone());
            let read = tokio::spawn(async move { input.request(&Bytes::from(scenario), &parent, "prompt".into(), false, &interrupt).await });
            let prompt = session.decode(tokio::time::timeout(Duration::from_secs(2), peer.recv()).await.unwrap()?)?;
            assert_eq!(prompt.msg_type(), "input_request");
            match scenario {
                "interrupt" => {
                    token.request()?;
                }
                "disconnect" => drop(peer),
                _ => {
                    let reply = session.message("input_reply", json!({"value": ""}), Some(&prompt));
                    peer.send(session.encode(&reply)?).await?;
                }
            }
            let result = tokio::time::timeout(Duration::from_secs(2), read).await.unwrap()?;
            match scenario {
                "interrupt" => assert_eq!(result.unwrap_err().kind(), ErrorKind::Interrupted),
                "disconnect" => assert_eq!(result.unwrap_err().kind(), ErrorKind::Closed),
                _ => assert_eq!(result?, ""),
            }
            assert!(stdin.pending.lock().await.is_empty());
        }
        Ok(())
    }
}
