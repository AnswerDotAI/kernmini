use crate::{Error, ErrorKind};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot, watch};

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<crate::Result<Value>>>>>;

struct DapInner {
    outgoing: mpsc::Sender<Vec<u8>>,
    pending: Pending,
    close: watch::Sender<Option<Error>>,
    next_seq: AtomicU64,
}

impl Drop for DapInner { fn drop(&mut self) { fail_pending(&self.pending, &self.close, Error::closed("DAP client")); } }

#[derive(Clone)]
pub struct DapClient { inner: Arc<DapInner> }

pub struct DapRequest { seq: u64, result: oneshot::Receiver<crate::Result<Value>>, pending: Pending }

impl DapClient {
    pub async fn connect(address: impl ToSocketAddrs) -> crate::Result<(Self, mpsc::UnboundedReceiver<Value>)> {
        let stream = TcpStream::connect(address).await?;
        let (reader, writer) = stream.into_split();
        let (outgoing, outgoing_rx) = mpsc::channel(64);
        let (events, event_rx) = mpsc::unbounded_channel();
        let (close, close_rx) = watch::channel(None);
        let pending = Pending::default();
        let inner = Arc::new(DapInner { outgoing, pending: pending.clone(), close: close.clone(), next_seq: AtomicU64::new(1) });

        let writer_pending = pending.clone();
        let writer_close = close.clone();
        tokio::spawn(async move {
            let error = write_messages(writer, outgoing_rx, close_rx).await.err().unwrap_or_else(|| Error::closed("DAP writer"));
            fail_pending(&writer_pending, &writer_close, error);
        });
        tokio::spawn(async move {
            let error = read_messages(reader, pending.clone(), events, close.subscribe()).await.err().unwrap_or_else(|| Error::closed("DAP reader"));
            fail_pending(&pending, &close, error);
        });
        Ok((Self { inner }, event_rx))
    }

    fn next_sequence(&self) -> u64 { self.inner.next_seq.fetch_add(1, Ordering::AcqRel) }

    fn request_sequence(&self, request: &mut Value) -> crate::Result<u64> {
        let content = request.as_object_mut().ok_or_else(|| Error::new(ErrorKind::InvalidInput, "DAP request must be an object"))?;
        let seq = content.get("seq").and_then(Value::as_u64).filter(|seq| *seq > 0).unwrap_or_else(|| {
            let seq = self.next_sequence();
            content.insert("seq".into(), Value::from(seq));
            seq
        });
        let next = seq.checked_add(1).ok_or_else(|| Error::new(ErrorKind::InvalidInput, "DAP request sequence is too large"))?;
        self.inner.next_seq.fetch_max(next, Ordering::AcqRel);
        Ok(seq)
    }

    pub async fn send(&self, mut request: Value) -> crate::Result<DapRequest> {
        if let Some(error) = self.inner.close.borrow().as_ref() { return Err(error.clone()); }
        let seq = self.request_sequence(&mut request)?;
        let frame = encode_message(&request)?;
        let (complete, result) = oneshot::channel();
        {
            let mut pending = self.inner.pending.lock().expect("DAP pending lock poisoned");
            if pending.contains_key(&seq) { return Err(Error::new(ErrorKind::InvalidInput, format!("DAP request {seq} is already pending"))); }
            pending.insert(seq, complete);
        }
        let request = DapRequest { seq, result, pending: self.inner.pending.clone() };
        if let Some(error) = self.inner.close.borrow().as_ref() { return Err(error.clone()); }
        self.inner.outgoing.send(frame).await.map_err(|_| self.inner.close.borrow().clone().unwrap_or_else(|| Error::closed("DAP connection")))?;
        Ok(request)
    }

    pub async fn request(&self, request: Value, timeout: Duration) -> crate::Result<Value> { self.send(request).await?.wait(timeout).await }

    pub fn close(&self) { fail_pending(&self.inner.pending, &self.inner.close, Error::closed("DAP client")); }
}

fn encode_message(message: &Value) -> crate::Result<Vec<u8>> {
    let payload = serde_json::to_vec(message)?;
    let mut frame = format!("Content-Length: {}\r\n\r\n", payload.len()).into_bytes();
    frame.extend(payload);
    Ok(frame)
}

impl DapRequest {
    pub fn sequence(&self) -> u64 { self.seq }

    pub async fn wait(mut self, timeout: Duration) -> crate::Result<Value> {
        match tokio::time::timeout(timeout, &mut self.result).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(Error::closed("DAP connection").caused_by(error)),
            Err(error) => Err(Error::new(ErrorKind::TimedOut, format!("waiting for DAP request {}", self.seq)).caused_by(error)),
        }
    }
}

impl Drop for DapRequest {
    fn drop(&mut self) {
        self.result.close();
        let mut pending = self.pending.lock().expect("DAP pending lock poisoned");
        if pending.get(&self.seq).is_some_and(oneshot::Sender::is_closed) { pending.remove(&self.seq); }
    }
}

fn fail_pending(pending: &Pending, close: &watch::Sender<Option<Error>>, error: Error) {
    close.send_if_modified(|reason| { if reason.is_some() { false } else { *reason = Some(error); true } });
    let error = close.borrow().as_ref().unwrap().clone();
    for (_, complete) in pending.lock().expect("DAP pending lock poisoned").drain() { let _ = complete.send(Err(error.clone())); }
}

async fn write_messages(
    mut writer: impl AsyncWrite + Unpin,
    mut outgoing: mpsc::Receiver<Vec<u8>>,
    mut close: watch::Receiver<Option<Error>>,
) -> crate::Result<()> {
    loop {
        if close.borrow().is_some() { return Ok(()); }
        tokio::select! {
            changed = close.changed() => {
                if changed.is_err() || close.borrow().is_some() { return Ok(()) }
            }
            frame = outgoing.recv() => match frame {
                Some(frame) => writer.write_all(&frame).await?,
                None => return Ok(()),
            }
        }
    }
}

async fn read_message(reader: &mut (impl AsyncBufRead + Unpin)) -> crate::Result<Value> {
    let mut content_length = None;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).await? == 0 { return Err(Error::closed("DAP connection")); }
        if header == "\r\n" { break; }
        if let Some(value) = header.strip_prefix("Content-Length:") {
            content_length =
                Some(value.trim().parse::<usize>().map_err(|error| Error::new(ErrorKind::Protocol, "invalid DAP Content-Length").caused_by(error))?);
        }
    }
    let length = content_length.ok_or_else(|| Error::new(ErrorKind::Protocol, "DAP message has no Content-Length"))?;
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await?;
    serde_json::from_slice(&payload).map_err(|error| Error::new(ErrorKind::Protocol, "invalid DAP JSON").caused_by(error))
}

async fn read_messages(
    reader: impl AsyncRead + Unpin,
    pending: Pending,
    events: mpsc::UnboundedSender<Value>,
    mut close: watch::Receiver<Option<Error>>,
) -> crate::Result<()> {
    let mut reader = BufReader::new(reader);
    loop {
        if close.borrow().is_some() { return Ok(()); }
        let message = tokio::select! {
            changed = close.changed() => {
                if changed.is_err() || close.borrow().is_some() { return Ok(()) }
                continue
            }
            message = read_message(&mut reader) => message?,
        };
        if message.get("type").and_then(Value::as_str) == Some("event") {
            let _ = events.send(message);
        } else if let Some(seq) = message.get("request_seq").and_then(Value::as_u64)
            && let Some(complete) = pending.lock().expect("DAP pending lock poisoned").remove(&seq)
        { let _ = complete.send(Ok(message)); }
    }
}
