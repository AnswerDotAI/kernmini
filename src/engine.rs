use crate::ConnectionInfo;
use crate::language::{
    CompleteRequest, ContextMessage, ExecuteRequest, ExecutionContext, ExecutionInterrupt, InspectRequest, Language, LanguageEvent, LanguageMessage,
    LanguageSession, SessionCommand,
};
use crate::transport::{Inbound, Iopub, RouterPeers, serve_heartbeat, serve_router};
use crate::wire::{Message, Session};
use bytes::Bytes;
use serde_json::{Value, json};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, hash_map::Entry};
use std::path::Path;
use std::sync::Arc;
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

async fn send_iopub(iopub: &Iopub, session: &Session, parent: &Message, msg_type: &str, content: Value) -> anyhow::Result<()> {
    iopub.publish(session.message(msg_type, content, Some(parent))).await
}

async fn send_reply(reply: &crate::transport::ReplySink, session: &Session, request: &Message, msg_type: &str, content: Value) -> anyhow::Result<()> {
    reply.send(session.encode(&session.reply(request, msg_type, content))?).await
}

async fn status(iopub: &Iopub, session: &Session, parent: &Message, state: &str) -> anyhow::Result<()> {
    send_iopub(iopub, session, parent, "status", json!({"execution_state": state})).await
}

fn event_channel(capacity: usize) -> (mpsc::Sender<ContextMessage>, mpsc::Receiver<ContextMessage>) { mpsc::channel(capacity) }

#[derive(Clone)]
struct Stdin { peers: RouterPeers, pending: Arc<Mutex<HashMap<String, PendingInput>>>, session: Session }

struct PendingInput { identity: Bytes, complete: oneshot::Sender<anyhow::Result<String>> }

impl Stdin {
    fn new(listener: TcpListener, session: Session) -> Self {
        let peers = RouterPeers::default();
        let pending = Arc::new(Mutex::new(HashMap::<String, PendingInput>::new()));
        let (send, mut incoming) = mpsc::channel(64);
        tokio::spawn(serve_router(listener, session.clone(), send, Some(peers.clone())));
        let replies = pending.clone();
        tokio::spawn(async move {
            while let Some(inbound) = incoming.recv().await {
                if inbound.message.msg_type() != "input_reply" { continue; }
                let parent = inbound.message.parent_header.get("msg_id").and_then(Value::as_str).unwrap_or("");
                let value = inbound.message.content.get("value").and_then(Value::as_str).unwrap_or("").to_owned();
                let mut replies = replies.lock().await;
                let key = if replies.contains_key(parent) { Some(parent.to_owned()) } else { replies.iter().find(|(_, pending)| pending.identity == inbound.identity).map(|(key, _)| key.clone()) };
                if let Some(pending) = key.and_then(|key| replies.remove(&key)) { let _ = pending.complete.send(Ok(value)); }
            }
        });
        Self { peers, pending, session }
    }

    async fn request(&self, identity: &Bytes, parent: &Message, prompt: String, password: bool) -> anyhow::Result<String> {
        let message = self.session.message("input_request", json!({"prompt": prompt, "password": password}), Some(parent));
        let msg_id = message.msg_id().to_owned();
        let (complete, result) = oneshot::channel();
        self.pending.lock().await.insert(msg_id.clone(), PendingInput { identity: identity.clone(), complete });
        let peer = self.peers.wait(identity).await;
        if let Err(error) = peer.send(self.session.encode(&message)?).await {
            self.pending.lock().await.remove(&msg_id);
            return Err(error);
        }
        result.await?
    }

    async fn interrupt(&self) {
        for (_, pending) in self.pending.lock().await.drain() { let _ = pending.complete.send(Err(anyhow::anyhow!("KeyboardInterrupt"))); }
    }
}

#[derive(Clone)]
struct ShellServices<L> {
    language: L,
    iopub: Iopub,
    stdin: Stdin,
    session: Session,
    connection: Arc<Value>,
    supports_subshells: bool,
    config: KernelConfig,
    unlocks: mpsc::UnboundedSender<String>,
    subshells: mpsc::UnboundedSender<SessionCommand>,
}

impl<L: LanguageSession> ShellServices<L> {
    fn output_context(&self, request: &Message, identity: Option<Bytes>, silent: bool, interrupt: ExecutionInterrupt) -> ExecutionContext {
        let (events, mut output) = event_channel(self.config.iopub_capacity);
        let execution = identity.is_some();
        let unlock = execution.then(|| (request.msg_id().to_owned(), self.unlocks.clone()));
        let client_session = request.header.get("session").and_then(Value::as_str).unwrap_or("").to_owned();
        let subshells = execution.then(|| (client_session, self.subshells.clone()));
        let parent = json!({
            "header": request.header, "parent_header": request.parent_header,
            "metadata": request.metadata, "content": request.content,
        });
        let context = ExecutionContext::new(events, interrupt, unlock, subshells, parent);
        let iopub = self.iopub.clone();
        let session = self.session.clone();
        let request = request.clone();
        let stdin = identity.map(|identity| (self.stdin.clone(), identity));
        tokio::spawn(async move {
            while let Some(message) = output.recv().await {
                match message {
                    ContextMessage::Event(event) => {
                        let _ = publish_event(&iopub, &session, &request, event, silent).await;
                    }
                    ContextMessage::Flush(complete) => {
                        let _ = complete.send(());
                    }
                    ContextMessage::Input { prompt, password, complete } => {
                        let result = if let Some((stdin, identity)) = &stdin { stdin.request(identity, &request, prompt, password).await } else { Err(anyhow::anyhow!("input is unavailable outside execution")) };
                        let _ = complete.send(result);
                    }
                }
            }
        });
        context
    }
}

async fn publish_event(iopub: &Iopub, session: &Session, request: &Message, event: LanguageEvent, silent: bool) -> anyhow::Result<()> {
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
) -> anyhow::Result<()> {
    let mut content = json!({
        "status": "error", "ename": "MissingField",
        "evalue": format!("missing required fields: {}", missing.join(", ")), "traceback": [],
    });
    if request.msg_type() == "execute_request" {
        content["execution_count"] = json!(execution_count);
        content["user_expressions"] = json!({});
        content["payload"] = json!([]);
        status(iopub, session, request, "busy").await?;
        send_iopub(iopub, session, request, "error", content.clone()).await?;
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
    let reply_type = request.msg_type().replace("_request", "_reply");
    send_reply(reply, session, request, &reply_type, content).await?;
    if request.msg_type() == "execute_request" { status(iopub, session, request, "idle").await?; }
    Ok(())
}

async fn execute(
    services: &ShellServices<impl LanguageSession>,
    identity: Bytes,
    request: &Message,
    interrupt: ExecutionInterrupt,
) -> anyhow::Result<(Value, bool)> {
    let language = &services.language;
    let iopub = &services.iopub;
    let session = &services.session;
    let execute = ExecuteRequest {
        code: request.content.get("code").and_then(Value::as_str).unwrap_or("").to_owned(),
        silent: request.content.get("silent").and_then(Value::as_bool).unwrap_or(false),
        store_history: request.content.get("store_history").and_then(Value::as_bool).unwrap_or(true),
        user_expressions: request.content.get("user_expressions").cloned().unwrap_or_else(|| json!({})),
        allow_stdin: request.content.get("allow_stdin").and_then(Value::as_bool).unwrap_or(true),
    };
    status(iopub, session, request, "busy").await?;
    if !execute.silent {
        send_iopub(iopub, session, request, "execute_input", json!({"code": execute.code, "execution_count": language.execution_count()})).await?;
    }

    let silent = execute.silent;
    let output = services.output_context(request, Some(identity), silent, interrupt);
    let outcome = language.execute(execute, output.clone()).await?;
    output.flush().await;

    let failed = outcome.error.is_some();
    let content = if let Some(error) = outcome.error {
        let error = json!({"ename": error.ename, "evalue": error.evalue, "traceback": error.traceback});
        send_iopub(iopub, session, request, "error", error.clone()).await?;
        json!({
            "status": "error", "execution_count": outcome.execution_count,
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
                    "execution_count": outcome.execution_count,
                    "data": result, "metadata": outcome.result_metadata,
                }),
            )
            .await?;
        }
        json!({
            "status": "ok", "execution_count": outcome.execution_count,
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
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.partial_cmp(&other.priority).unwrap_or(Ordering::Equal).then_with(|| other.order.cmp(&self.order))
    }
}

enum ShellOutcome { Continue, Shutdown }

struct ExecutionDone { msg_id: String, failed: bool, stop_on_error: bool }

enum ShellControl { Release { msg_id: String, status: String, complete: oneshot::Sender<bool> }, Interrupt, Stop { complete: oneshot::Sender<()> } }

async fn handle_shell(services: &ShellServices<impl LanguageSession>, inbound: Inbound) -> anyhow::Result<ShellOutcome> {
    let language = &services.language;
    let iopub = &services.iopub;
    let session = &services.session;
    let request = inbound.message;
    let reply = inbound.reply;
    let missing = missing_fields(&request);
    if !missing.is_empty() {
        reply_missing(language.execution_count(), iopub, session, &request, &reply, &missing).await?;
        return Ok(ShellOutcome::Continue);
    }
    match request.msg_type() {
        "shutdown_request" => {
            let content = json!({"status": "ok", "restart": request.content.get("restart").and_then(Value::as_bool).unwrap_or(false)});
            send_reply(&reply, session, &request, "shutdown_reply", content).await?;
            Ok(ShellOutcome::Shutdown)
        }
        "kernel_info_request" => {
            status(iopub, session, &request, "busy").await?;
            let info = language.kernel_info()?;
            let content = json!({
                "status": "ok", "protocol_version": "5.3",
                "implementation": info.implementation,
                "implementation_version": info.implementation_version,
                "banner": info.banner, "language_info": info.language_info,
                "help_links": [], "debugger": language.supports_debugger(),
                "supported_features": match (services.supports_subshells, language.supports_debugger()) {
                    (true, true) => vec!["kernel subshells", "debugger"],
                    (true, false) => vec!["kernel subshells"],
                    (false, true) => vec!["debugger"],
                    (false, false) => vec![],
                },
            });
            send_reply(&reply, session, &request, "kernel_info_reply", content).await?;
            status(iopub, session, &request, "idle").await?;
            Ok(ShellOutcome::Continue)
        }
        "complete_request" => {
            status(iopub, session, &request, "busy").await?;
            let content = language
                .complete(CompleteRequest {
                    code: request.content.get("code").and_then(Value::as_str).unwrap_or("").to_owned(),
                    cursor_pos: request.content.get("cursor_pos").and_then(Value::as_u64).unwrap_or(0),
                })
                .await?;
            send_reply(&reply, session, &request, "complete_reply", content).await?;
            status(iopub, session, &request, "idle").await?;
            Ok(ShellOutcome::Continue)
        }
        "inspect_request" => {
            status(iopub, session, &request, "busy").await?;
            let content = language
                .inspect(InspectRequest {
                    code: request.content.get("code").and_then(Value::as_str).unwrap_or("").to_owned(),
                    cursor_pos: request.content.get("cursor_pos").and_then(Value::as_u64).unwrap_or(0),
                    detail_level: request.content.get("detail_level").and_then(Value::as_u64).unwrap_or(0),
                })
                .await?;
            send_reply(&reply, session, &request, "inspect_reply", content).await?;
            status(iopub, session, &request, "idle").await?;
            Ok(ShellOutcome::Continue)
        }
        "is_complete_request" => {
            status(iopub, session, &request, "busy").await?;
            let content = language.is_complete(request.content.get("code").and_then(Value::as_str).unwrap_or("").to_owned()).await?;
            send_reply(&reply, session, &request, "is_complete_reply", content).await?;
            status(iopub, session, &request, "idle").await?;
            Ok(ShellOutcome::Continue)
        }
        "history_request" => {
            status(iopub, session, &request, "busy").await?;
            let content = language.history(request.content.clone()).await?;
            send_reply(&reply, session, &request, "history_reply", content).await?;
            status(iopub, session, &request, "idle").await?;
            Ok(ShellOutcome::Continue)
        }
        "comm_info_request" => {
            let content = language.comm_info(request.content.clone()).await?;
            send_reply(&reply, session, &request, "comm_info_reply", content).await?;
            Ok(ShellOutcome::Continue)
        }
        "connect_request" => {
            send_reply(&reply, session, &request, "connect_reply", services.connection.as_ref().clone()).await?;
            Ok(ShellOutcome::Continue)
        }
        "comm_open" | "comm_msg" | "comm_close" => {
            let output = services.output_context(&request, None, false, ExecutionInterrupt::default());
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
            output.flush().await;
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
    interrupt: ExecutionInterrupt,
) -> anyhow::Result<ExecutionDone> {
    let request = inbound.message;
    let msg_id = request.msg_id().to_owned();
    let stop_on_error = request.content.get("stop_on_error").and_then(Value::as_bool).unwrap_or(true);
    let (content, failed) = execute(&services, inbound.identity, &request, interrupt).await?;
    send_reply(&inbound.reply, &services.session, &request, "execute_reply", content).await?;
    status(&services.iopub, &services.session, &request, "idle").await?;
    Ok(ExecutionDone { msg_id, failed, stop_on_error })
}

async fn abort_execute(execution_count: u64, iopub: &Iopub, session: &Session, inbound: Inbound) -> anyhow::Result<()> {
    let request = inbound.message;
    status(iopub, session, &request, "busy").await?;
    let content = json!({
        "status": "aborted", "execution_count": execution_count,
        "user_expressions": {}, "payload": [],
    });
    send_reply(&inbound.reply, session, &request, "execute_reply", content).await?;
    status(iopub, session, &request, "idle").await
}

async fn interrupt_execute(execution_count: u64, iopub: &Iopub, session: &Session, inbound: Inbound) -> anyhow::Result<()> {
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

async fn begin_hold(execution_count: u64, iopub: &Iopub, session: &Session, item: &QueueItem) -> anyhow::Result<()> {
    let request = &item.inbound.message;
    status(iopub, session, request, "busy").await?;
    if !request.content.get("silent").and_then(Value::as_bool).unwrap_or(false) {
        let code = request.content.get("code").and_then(Value::as_str).unwrap_or("");
        send_iopub(iopub, session, request, "execute_input", json!({"code": code, "execution_count": execution_count})).await?;
    }
    Ok(())
}

async fn finish_hold(execution_count: u64, iopub: &Iopub, session: &Session, item: QueueItem, release_status: &str) -> anyhow::Result<bool> {
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

fn pop_runnable(queue: &mut BinaryHeap<QueueItem>, execution_locked: bool, held: Option<&Held>) -> Option<QueueItem> {
    let mut parked = vec![];
    let runnable = loop {
        let Some(item) = queue.pop() else { break None };
        let execute = item.inbound.message.msg_type() == "execute_request";
        let above_hold = held.is_none_or(|hold| item.priority > hold.item.priority);
        if !execute || (!execution_locked && above_hold) { break Some(item); }
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
    locked: Option<String>,
    unlocks: mpsc::UnboundedReceiver<String>,
    executions: JoinSet<anyhow::Result<ExecutionDone>>,
    active: HashMap<String, ExecutionInterrupt>,
    stopping: Option<oneshot::Sender<()>>,
    interrupting: bool,
}

impl<L: LanguageSession> Shell<L> {
    fn enqueue(&mut self, inbound: Inbound) {
        let priority = inbound.message.metadata.get("priority").and_then(Value::as_f64).unwrap_or(0.0);
        self.queue.push(QueueItem { priority, order: self.order, inbound });
        self.order += 1;
    }

    async fn abort_pending(&mut self) -> anyhow::Result<()> {
        while let Ok(inbound) = self.incoming.try_recv() { self.enqueue(inbound) }
        let mut keep = vec![];
        while let Some(item) = self.queue.pop() {
            if item.inbound.message.msg_type() == "execute_request" {
                abort_execute(self.services.language.execution_count(), &self.services.iopub, &self.services.session, item.inbound).await?;
            } else { keep.push(item); }
        }
        self.queue.extend(keep);
        Ok(())
    }

    fn unlock(&mut self, msg_id: Option<String>) { if self.locked.as_deref() == msg_id.as_deref() { self.locked = None } }

    async fn apply_control(&mut self, control: ShellControl) -> anyhow::Result<()> {
        match control {
            ShellControl::Release { msg_id, status: release_status, complete } => {
                let found = self.held.as_ref().is_some_and(|held| held.item.inbound.message.msg_id() == msg_id);
                if found {
                    let failed = finish_hold(
                        self.services.language.execution_count(),
                        &self.services.iopub,
                        &self.services.session,
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
                for interrupt in self.active.values() { let _ = interrupt.request(); }
                if let Some(held) = self.held.take() {
                    finish_hold(self.services.language.execution_count(), &self.services.iopub, &self.services.session, held.item, "interrupt")
                        .await?;
                }
                self.abort_pending().await?;
            }
            ShellControl::Stop { complete } => self.stopping = Some(complete),
        }
        Ok(())
    }

    async fn execution_done(&mut self, done: ExecutionDone) -> anyhow::Result<()> {
        self.active.remove(&done.msg_id);
        if self.locked.as_deref() == Some(done.msg_id.as_str()) { self.locked = None }
        if done.failed && done.stop_on_error && !self.interrupting { self.abort_pending().await? }
        Ok(())
    }

    async fn handle_item(&mut self, item: QueueItem) -> anyhow::Result<bool> {
        if item.inbound.message.msg_type() != "execute_request" {
            return Ok(matches!(handle_shell(&self.services, item.inbound).await?, ShellOutcome::Shutdown));
        }
        if self.interrupting {
            interrupt_execute(self.services.language.execution_count(), &self.services.iopub, &self.services.session, item.inbound).await?;
            return Ok(false);
        }
        let missing = missing_fields(&item.inbound.message);
        if !missing.is_empty() {
            reply_missing(
                self.services.language.execution_count(),
                &self.services.iopub,
                &self.services.session,
                &item.inbound.message,
                &item.inbound.reply,
                &missing,
            )
            .await?;
            self.abort_pending().await?;
        }
        else if item.inbound.message.metadata.get("hold").and_then(Value::as_bool).unwrap_or(false) {
            begin_hold(self.services.language.execution_count(), &self.services.iopub, &self.services.session, &item).await?;
            self.held = Some(Held { item, deadline: tokio::time::Instant::now() + self.services.config.hold_timeout });
        }
        else {
            let msg_id = item.inbound.message.msg_id().to_owned();
            let interrupt = ExecutionInterrupt::default();
            self.active.insert(msg_id.clone(), interrupt.clone());
            self.executions.spawn(run_execution(self.services.clone(), item.inbound, interrupt));
            self.locked = Some(msg_id);
        }
        Ok(false)
    }

    async fn run(mut self) -> anyhow::Result<()> {
        loop {
            while let Ok(inbound) = self.incoming.try_recv() { self.enqueue(inbound) }
            while let Ok(control) = self.controls.try_recv() { self.apply_control(control).await? }
            while let Ok(msg_id) = self.unlocks.try_recv() { self.unlock(Some(msg_id)) }
            while let Some(result) = self.executions.try_join_next() { self.execution_done(result??).await? }
            if self.interrupting && self.executions.is_empty() { self.interrupting = false }
            if self.stopping.is_some() && self.queue.is_empty() && self.executions.is_empty() && self.held.is_none() {
                self.services.language.shutdown().await?;
                let _ = self.stopping.take().unwrap().send(());
                return Ok(());
            }

            if let Some(item) = pop_runnable(&mut self.queue, self.locked.is_some(), self.held.as_ref()) {
                if self.handle_item(item).await? { return Ok(()); }
                continue;
            }

            tokio::select! {
                message = self.incoming.recv() => self.enqueue(message.ok_or_else(|| anyhow::anyhow!("shell service ended"))?),
                control = self.controls.recv() => self.apply_control(control.ok_or_else(|| anyhow::anyhow!("shell control ended"))?).await?,
                unlocked = self.unlocks.recv() => self.unlock(unlocked),
                result = self.executions.join_next(), if !self.executions.is_empty() => {
                    self.execution_done(result.expect("non-empty execution set")??).await?;
                }
                _ = wait_hold(self.held.as_ref().map(|held| held.deadline)), if self.held.is_some() => {
                    finish_hold(self.services.language.execution_count(), &self.services.iopub, &self.services.session,
                        self.held.take().unwrap().item, "timeout").await?;
                    self.abort_pending().await?;
                }
            }
        }
    }
}

struct ShellHandle { incoming: mpsc::Sender<Inbound>, controls: mpsc::Sender<ShellControl>, task: JoinHandle<anyhow::Result<()>> }

#[derive(Clone)]
struct KernelServices {
    iopub: Iopub,
    stdin: Stdin,
    session: Session,
    connection: Arc<Value>,
    supports_subshells: bool,
    config: KernelConfig,
    subshells: mpsc::UnboundedSender<SessionCommand>,
}

impl KernelServices {
    fn spawn_shell(&self, language: impl LanguageSession) -> ShellHandle {
        let (incoming, requests) = mpsc::channel(256);
        let (controls, shell_controls) = mpsc::channel(64);
        let (unlock_send, unlocks) = mpsc::unbounded_channel();
        let services = ShellServices {
            language,
            iopub: self.iopub.clone(),
            stdin: self.stdin.clone(),
            session: self.session.clone(),
            connection: self.connection.clone(),
            supports_subshells: self.supports_subshells,
            config: self.config,
            unlocks: unlock_send,
            subshells: self.subshells.clone(),
        };
        let shell = Shell {
            services,
            incoming: requests,
            controls: shell_controls,
            queue: BinaryHeap::new(),
            order: 0,
            held: None,
            locked: None,
            unlocks,
            executions: JoinSet::new(),
            active: HashMap::new(),
            stopping: None,
            interrupting: false,
        };
        ShellHandle { incoming, controls, task: tokio::spawn(shell.run()) }
    }
}

fn subshell_id(request: &Message) -> &str { request.header.get("subshell_id").and_then(Value::as_str).filter(|id| !id.is_empty()).unwrap_or("") }

async fn reply_subshell_not_found(iopub: &Iopub, session: &Session, inbound: Inbound) -> anyhow::Result<()> {
    let request = inbound.message;
    if !request.msg_type().ends_with("_request") { return Ok(()); }
    let id = subshell_id(&request);
    let mut content = json!({
        "status": "error", "ename": "SubshellNotFound",
        "evalue": format!("Unknown subshell_id {id:?}"), "traceback": [],
    });
    if request.msg_type() == "execute_request" {
        content["execution_count"] = json!(0);
        content["user_expressions"] = json!({});
        content["payload"] = json!([]);
        status(iopub, session, &request, "busy").await?;
        send_iopub(iopub, session, &request, "error", content.clone()).await?;
    }
    let reply_type = request.msg_type().replace("_request", "_reply");
    send_reply(&inbound.reply, session, &request, &reply_type, content).await?;
    if request.msg_type() == "execute_request" { status(iopub, session, &request, "idle").await? }
    Ok(())
}

async fn route_shell(
    inbound: Inbound,
    shells: &HashMap<String, ShellHandle>,
    route_overrides: &HashMap<String, String>,
    iopub: &Iopub,
    session: &Session,
) -> anyhow::Result<()> {
    let explicit = subshell_id(&inbound.message);
    let client_session = inbound.message.header.get("session").and_then(Value::as_str).unwrap_or("");
    let id = if !explicit.is_empty() { explicit.to_owned() } else if inbound.message.msg_type() == "execute_request" { route_overrides.get(client_session).cloned().unwrap_or_default() } else { String::new() };
    if let Some(target) = shells.get(&id) {
        if let Err(error) = target.incoming.send(inbound).await { reply_subshell_not_found(iopub, session, error.0).await? }
    }
    else { reply_subshell_not_found(iopub, session, inbound).await?; }
    Ok(())
}

async fn route_pending_shells(
    shell: &mut mpsc::Receiver<Inbound>,
    shells: &HashMap<String, ShellHandle>,
    route_overrides: &HashMap<String, String>,
    iopub: &Iopub,
    session: &Session,
) -> anyhow::Result<()> {
    while let Ok(inbound) = shell.try_recv() { route_shell(inbound, shells, route_overrides, iopub, session).await? }
    Ok(())
}

async fn stop_shell(shell: ShellHandle, interrupt: bool) {
    if interrupt { let _ = shell.controls.send(ShellControl::Interrupt).await; }
    let (complete, stopped) = oneshot::channel();
    let _ = shell.controls.send(ShellControl::Stop { complete }).await;
    let _ = stopped.await;
    let _ = shell.task.await;
}

async fn interrupt_shells(stdin: &Stdin, shells: &HashMap<String, ShellHandle>) {
    stdin.interrupt().await;
    for shell in shells.values() { let _ = shell.controls.send(ShellControl::Interrupt).await; }
}

pub async fn run_kernel(connection_file: impl AsRef<Path>, language: impl Language) -> anyhow::Result<()> {
    let interrupt = KernelInterrupter::default();
    let signal_interrupt = interrupt.clone();
    let signals = tokio::spawn(async move { while tokio::signal::ctrl_c().await.is_ok() { signal_interrupt.interrupt() } });
    let result = run_kernel_with_interrupter(connection_file, language, interrupt).await;
    signals.abort();
    result
}

pub async fn run_kernel_with_interrupter(
    connection_file: impl AsRef<Path>,
    language: impl Language,
    interrupt: KernelInterrupter,
) -> anyhow::Result<()> {
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
    tokio::spawn(serve_router(shell_listener, session.clone(), shell_send, None));
    tokio::spawn(serve_router(control_listener, session.clone(), control_send, None));
    let stdin = Stdin::new(stdin_listener, session.clone());
    let iopub = Iopub::new(session.clone(), config.iopub_capacity);
    tokio::spawn(iopub.clone().serve(iopub_listener));
    tokio::spawn(serve_heartbeat(heartbeat_listener));
    let supports_children = language.supports_children();
    let (subshell_send, mut subshell_commands) = mpsc::unbounded_channel();
    let kernel_services = KernelServices {
        iopub: iopub.clone(),
        stdin: stdin.clone(),
        session: session.clone(),
        connection: connection_content.clone(),
        supports_subshells: supports_children,
        config,
        subshells: subshell_send.clone(),
    };
    let mut shells = HashMap::new();
    let mut route_overrides = HashMap::<String, String>::new();
    let mut terminate = Box::pin(termination_signal());
    let parent = language.parent();
    let control_language = parent.clone();
    let debug_iopub = iopub.clone();
    let debug_session = session.clone();
    let runtime = tokio::runtime::Handle::current();
    parent.set_debug_sender(Arc::new(move |event| {
        let iopub = debug_iopub.clone();
        let session = debug_session.clone();
        runtime.spawn(async move { let _ = iopub.publish(session.message("debug_event", event, None)).await; });
    }))?;
    shells.insert(String::new(), kernel_services.spawn_shell(parent));
    loop {
        let inbound = tokio::select! {
            _ = &mut terminate => {
                stdin.interrupt().await;
                for (_, shell) in shells.drain() { stop_shell(shell, true).await }
                return Ok(());
            }
            _ = interrupt.notified() => {
                interrupt_shells(&stdin, &shells).await;
                continue;
            }
            message = shell.recv() => {
                let inbound = message.ok_or_else(|| anyhow::anyhow!("shell service ended"))?;
                route_shell(inbound, &shells, &route_overrides, &iopub, &session).await?;
                continue;
            }
            command = subshell_commands.recv() => {
                match command.ok_or_else(|| anyhow::anyhow!("subshell command service ended"))? {
                    SessionCommand::Open { client_session, complete } => {
                        let result = match route_overrides.entry(client_session) {
                            Entry::Occupied(_) => Err(anyhow::anyhow!("this client session already has a temporary subshell")),
                            Entry::Vacant(route) => match language.create_child().await {
                                Ok(child) => {
                                    let id = uuid::Uuid::new_v4().to_string();
                                    shells.insert(id.clone(), kernel_services.spawn_shell(child));
                                    route.insert(id.clone());
                                    Ok(id)
                                }
                                Err(error) => Err(error),
                            },
                        };
                        let _ = complete.send(result);
                    }
                    SessionCommand::Close { client_session, subshell_id, complete } => {
                        let result = if route_overrides.get(&client_session) == Some(&subshell_id) {
                            route_overrides.remove(&client_session);
                            if let Some(shell) = shells.remove(&subshell_id) { stop_shell(shell, false).await }
                            Ok(())
                        } else {
                            Err(anyhow::anyhow!("temporary subshell is not active"))
                        };
                        let _ = complete.send(result);
                    }
                }
                continue;
            }
            message = control.recv() => message.ok_or_else(|| anyhow::anyhow!("control service ended"))?,
        };
        let request = inbound.message;
        let reply = inbound.reply;
        match request.msg_type() {
            "shutdown_request" => {
                let content = json!({"status": "ok", "restart": request.content.get("restart").and_then(Value::as_bool).unwrap_or(false)});
                send_reply(&reply, &session, &request, "shutdown_reply", content).await?;
                stdin.interrupt().await;
                for (_, shell) in shells.drain() { stop_shell(shell, true).await }
                return Ok(());
            }
            "interrupt_request" => {
                route_pending_shells(&mut shell, &shells, &route_overrides, &iopub, &session).await?;
                interrupt_shells(&stdin, &shells).await;
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
                let id = uuid::Uuid::new_v4().to_string();
                let content = match language.create_child().await {
                    Ok(child) => {
                        shells.insert(id.clone(), kernel_services.spawn_shell(child));
                        json!({"status": "ok", "subshell_id": id})
                    }
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
                    stop_shell(shell, true).await;
                    json!({"status": "ok"})
                } else { json!({"status": "error", "ename": "SubshellNotFound", "evalue": format!("Unknown subshell_id {id:?}"), "traceback": []}) };
                send_reply(&reply, &session, &request, "delete_subshell_reply", content).await?;
            }
            "release_request" => {
                route_pending_shells(&mut shell, &shells, &route_overrides, &iopub, &session).await?;
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
                let result = control_language.debug(request.content.clone()).await?;
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
