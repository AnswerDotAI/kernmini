use std::{error::Error as StdError, fmt, io, sync::Arc};

pub type Result<T> = std::result::Result<T, Error>;

/// Operational failures, distinct from errors in an executed program (`LanguageError`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// The execution was cancelled; this is not a transient OS interrupted syscall.
    Interrupted,
    /// A required peer, channel, task, or interpreter worker has ended.
    Closed,
    /// A bounded wait expired, without implying that remote work was cancelled.
    TimedOut,
    /// The operation is unsupported or unavailable in the current context.
    Unavailable,
    /// Invalid arguments, configuration, or API usage.
    InvalidInput,
    /// An operating-system I/O failure; the original error is in the source chain.
    Io,
    /// Received data violated the wire protocol.
    Protocol,
    /// A language implementation failed operationally, rather than reporting a program error.
    Adapter,
}

/// A stable classification and diagnostic, retaining the underlying error across task boundaries.
#[derive(Clone, Debug)]
pub struct Error { kind: ErrorKind, message: String, source: Option<Arc<dyn StdError + Send + Sync>> }

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self { Self { kind, message: message.into(), source: None } }
    pub fn kind(&self) -> ErrorKind { self.kind }
    pub fn caused_by(mut self, source: impl Into<Box<dyn StdError + Send + Sync>>) -> Self { self.source = Some(Arc::from(source.into())); self }
    pub fn context(self, message: impl Into<String>) -> Self { Self::new(self.kind, message).caused_by(self) }
    pub fn adapter(source: impl Into<Box<dyn StdError + Send + Sync>>) -> Self { Self::new(ErrorKind::Adapter, "language adapter failed").caused_by(source) }
    pub(crate) fn interrupted() -> Self { Self::new(ErrorKind::Interrupted, "execution interrupted") }
    pub fn closed(resource: &str) -> Self { Self::new(ErrorKind::Closed, format!("{resource} closed")) }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)?;
        if let Some(source) = &self.source { write!(f, ": {source}")?; }
        Ok(())
    }
}

impl StdError for Error { fn source(&self) -> Option<&(dyn StdError + 'static)> { self.source.as_deref().map(|source| source as _) } }

impl From<io::Error> for Error { fn from(error: io::Error) -> Self { Self::new(ErrorKind::Io, "I/O failed").caused_by(error) } }
impl From<serde_json::Error> for Error { fn from(error: serde_json::Error) -> Self { Self::new(ErrorKind::InvalidInput, "invalid JSON").caused_by(error) } }
impl From<crate::WireError> for Error {
    fn from(error: crate::WireError) -> Self { Self::new(ErrorKind::Protocol, "invalid Jupyter message").caused_by(error) }
}
impl From<zmtpmini::Error> for Error {
    fn from(error: zmtpmini::Error) -> Self {
        let kind = match &error { zmtpmini::Error::Closed => ErrorKind::Closed, zmtpmini::Error::Io(_) => ErrorKind::Io, _ => ErrorKind::Protocol };
        Self::new(kind, "ZMTP connection failed").caused_by(error)
    }
}
impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error { fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self { Self::closed("kernel service") } }
impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(error: tokio::sync::oneshot::error::RecvError) -> Self { Self::closed("kernel service").caused_by(error) }
}
impl From<std::sync::mpsc::RecvError> for Error { fn from(error: std::sync::mpsc::RecvError) -> Self { Self::closed("kernel service").caused_by(error) } }
impl From<tokio::task::JoinError> for Error { fn from(error: tokio::task::JoinError) -> Self { Self::closed("kernel task").caused_by(error) } }
