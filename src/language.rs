use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug)]
pub struct KernelInfo {
    pub implementation: String,
    pub implementation_version: String,
    pub banner: String,
    pub language_info: Value,
}

#[derive(Clone, Debug)]
pub struct ExecuteRequest {
    pub code: String,
    pub silent: bool,
    pub store_history: bool,
    pub user_expressions: Value,
    pub allow_stdin: bool,
}

#[derive(Clone, Debug)]
pub struct CompleteRequest { pub code: String, pub cursor_pos: u64 }

#[derive(Clone, Debug)]
pub struct InspectRequest { pub code: String, pub cursor_pos: u64, pub detail_level: u64 }

#[derive(Clone, Debug)]
pub struct LanguageMessage { pub msg_type: String, pub content: Value, pub buffers: Vec<Vec<u8>> }

pub type DebugEventSender = Arc<dyn Fn(Value) + Send + Sync>;

#[derive(Clone, Debug)]
pub struct LanguageError { pub ename: String, pub evalue: String, pub traceback: Vec<String> }

#[derive(Clone, Debug)]
pub struct ExecuteOutcome {
    pub execution_count: u64,
    pub result: Option<Value>,
    pub result_metadata: Value,
    pub error: Option<LanguageError>,
    pub user_expressions: Value,
    pub payload: Value,
}

#[derive(Clone, Debug)]
pub enum LanguageEvent {
    Stream { name: String, text: String },
    Display { event: Value, buffers: Vec<Vec<u8>> },
    Message { msg_type: String, content: Value, metadata: Value, identity: Option<Vec<u8>>, buffers: Vec<Vec<u8>> },
}

pub(crate) enum ContextMessage {
    Event(LanguageEvent),
    Flush(oneshot::Sender<()>),
    Input { prompt: String, password: bool, complete: std::sync::mpsc::SyncSender<anyhow::Result<String>> },
}

pub(crate) enum SessionCommand {
    Open { client_session: String, complete: std::sync::mpsc::SyncSender<anyhow::Result<String>> },
    Close { client_session: String, subshell_id: String, complete: std::sync::mpsc::SyncSender<anyhow::Result<()>> },
}

pub type InterruptHandler = Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync>;

#[derive(Clone, Default)]
pub struct ExecutionInterrupt { requested: Arc<AtomicBool>, handler: Arc<Mutex<InterruptRegistration>> }

#[derive(Default)]
struct InterruptRegistration { handler: Option<InterruptHandler>, delivered: bool }

impl ExecutionInterrupt {
    pub fn request(&self) -> anyhow::Result<bool> {
        let first = !self.requested.swap(true, Ordering::AcqRel);
        let handler = {
            let mut registration = self.handler.lock().expect("execution interrupt lock poisoned");
            if registration.delivered { None } else if let Some(handler) = registration.handler.clone() {
                registration.delivered = true;
                Some(handler)
            } else { None }
        };
        if let Some(handler) = handler { handler()? }
        Ok(first)
    }

    pub fn requested(&self) -> bool { self.requested.load(Ordering::Acquire) }

    pub fn set_handler(&self, handler: InterruptHandler) -> anyhow::Result<()> {
        let deliver = {
            let mut registration = self.handler.lock().expect("execution interrupt lock poisoned");
            if registration.handler.is_some() { anyhow::bail!("execution interrupt handler already registered") }
            registration.handler = Some(handler.clone());
            if self.requested() && !registration.delivered {
                registration.delivered = true;
                true
            } else { false }
        };
        if deliver { handler()? }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ExecutionContext {
    events: mpsc::Sender<ContextMessage>,
    interrupt: ExecutionInterrupt,
    unlock: Option<Arc<Unlock>>,
    subshells: Option<Arc<SubshellAccess>>,
    parent: Arc<Value>,
}

struct Unlock { sent: AtomicBool, execution_id: String, events: mpsc::UnboundedSender<String> }

struct SubshellAccess { client_session: String, commands: mpsc::UnboundedSender<SessionCommand> }

impl ExecutionContext {
    pub(crate) fn new(
        events: mpsc::Sender<ContextMessage>,
        interrupt: ExecutionInterrupt,
        unlock: Option<(String, mpsc::UnboundedSender<String>)>,
        subshells: Option<(String, mpsc::UnboundedSender<SessionCommand>)>,
        parent: Value,
    ) -> Self {
        Self {
            events,
            interrupt,
            unlock: unlock.map(|(execution_id, events)| Arc::new(Unlock { sent: AtomicBool::new(false), execution_id, events })),
            subshells: subshells.map(|(client_session, commands)| Arc::new(SubshellAccess { client_session, commands })),
            parent: Arc::new(parent),
        }
    }

    pub fn stream(&self, name: impl Into<String>, text: impl Into<String>) {
        let _ = self.events.try_send(ContextMessage::Event(LanguageEvent::Stream { name: name.into(), text: text.into() }));
    }

    pub fn display(&self, event: Value) { self.display_buffers(event, vec![]); }

    pub fn display_buffers(&self, event: Value, buffers: Vec<Vec<u8>>) {
        let _ = self.events.try_send(ContextMessage::Event(LanguageEvent::Display { event, buffers }));
    }

    pub fn publish(&self, msg_type: String, content: Value, metadata: Value, identity: Option<Vec<u8>>, buffers: Vec<Vec<u8>>) {
        let _ = self.events.try_send(ContextMessage::Event(LanguageEvent::Message { msg_type, content, metadata, identity, buffers }));
    }

    pub fn input(&self, prompt: impl Into<String>, password: bool) -> anyhow::Result<String> {
        let (complete, result) = std::sync::mpsc::sync_channel(1);
        self.events.blocking_send(ContextMessage::Input { prompt: prompt.into(), password, complete })?;
        result.recv()?
    }

    pub fn unlock(&self) -> bool {
        let Some(unlock) = &self.unlock else { return false };
        if unlock.sent.swap(true, Ordering::AcqRel) { return false; }
        unlock.events.send(unlock.execution_id.clone()).is_ok()
    }

    pub fn open_subshell(&self) -> anyhow::Result<String> {
        let access = self.subshells.as_ref().ok_or_else(|| anyhow::anyhow!("subshells are not available"))?;
        let (complete, result) = std::sync::mpsc::sync_channel(1);
        access.commands.send(SessionCommand::Open { client_session: access.client_session.clone(), complete })?;
        result.recv()?
    }

    pub fn close_subshell(&self, subshell_id: String) -> anyhow::Result<()> {
        let access = self.subshells.as_ref().ok_or_else(|| anyhow::anyhow!("subshells are not available"))?;
        let (complete, result) = std::sync::mpsc::sync_channel(1);
        access.commands.send(SessionCommand::Close { client_session: access.client_session.clone(), subshell_id, complete })?;
        result.recv()?
    }

    pub fn parent(&self) -> Value { self.parent.as_ref().clone() }

    pub fn interrupted(&self) -> bool { self.interrupt.requested() }

    pub fn set_interrupt_handler(&self, handler: InterruptHandler) -> anyhow::Result<()> { self.interrupt.set_handler(handler) }

    pub(crate) async fn flush(&self) {
        let (send, receive) = oneshot::channel();
        if self.events.send(ContextMessage::Flush(send)).await.is_ok() { let _ = receive.await; }
    }
}

#[async_trait]
pub trait LanguageSession: Clone + Send + Sync + 'static {
    fn kernel_info(&self) -> anyhow::Result<KernelInfo>;
    fn supports_debugger(&self) -> bool { false }
    fn set_debug_sender(&self, _sender: DebugEventSender) -> anyhow::Result<()> { Ok(()) }
    fn execution_count(&self) -> u64 { 0 }
    async fn execute(&self, request: ExecuteRequest, context: ExecutionContext) -> anyhow::Result<ExecuteOutcome>;
    async fn complete(&self, request: CompleteRequest) -> anyhow::Result<Value> {
        Ok(json!({"status": "ok", "matches": [], "cursor_start": request.cursor_pos, "cursor_end": request.cursor_pos, "metadata": {}}))
    }
    async fn inspect(&self, _request: InspectRequest) -> anyhow::Result<Value> { Ok(json!({"status": "ok", "found": false, "data": {}, "metadata": {}})) }
    async fn is_complete(&self, _code: String) -> anyhow::Result<Value> { Ok(json!({"status": "unknown"})) }
    async fn history(&self, _request: Value) -> anyhow::Result<Value> { Ok(json!({"status": "ok", "history": []})) }
    async fn comm_info(&self, _request: Value) -> anyhow::Result<Value> { Ok(json!({"status": "ok", "comms": {}})) }
    async fn debug(&self, _request: Value) -> anyhow::Result<Value> {
        Ok(json!({"response": {"success": false, "message": "debugger not supported"}, "events": []}))
    }
    async fn message(&self, _message: LanguageMessage, _context: ExecutionContext) -> anyhow::Result<()> { Ok(()) }
    async fn shutdown(&self) -> anyhow::Result<()> { Ok(()) }
}

#[async_trait]
pub trait Language: Send + Sync + 'static {
    type Session: LanguageSession;
    fn parent(&self) -> Self::Session;
    fn supports_children(&self) -> bool { false }
    async fn create_child(&self) -> anyhow::Result<Self::Session>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn execution_interrupt_story() {
        let calls = Arc::new(AtomicUsize::new(0));
        let interrupt = ExecutionInterrupt::default();
        assert!(!interrupt.requested());
        assert!(interrupt.request().unwrap());
        assert!(interrupt.requested());

        let called = calls.clone();
        interrupt
            .set_handler(Arc::new(move || {
                called.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }))
            .unwrap();
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert!(!interrupt.request().unwrap());
        assert_eq!(calls.load(Ordering::Acquire), 1);

        let ready = ExecutionInterrupt::default();
        let called = calls.clone();
        ready
            .set_handler(Arc::new(move || {
                called.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }))
            .unwrap();
        assert!(ready.request().unwrap());
        assert_eq!(calls.load(Ordering::Acquire), 2);
    }
}
