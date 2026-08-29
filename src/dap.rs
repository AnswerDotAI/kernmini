use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot, watch};

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>>;

struct DapInner {
    outgoing: mpsc::Sender<Vec<u8>>,
    pending: Pending,
    close: watch::Sender<bool>,
    closed: Arc<AtomicBool>,
    next_seq: AtomicU64,
}

impl Drop for DapInner {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        let _ = self.close.send(true);
        fail_pending(&self.pending, "DAP client closed");
    }
}

#[derive(Clone)]
pub struct DapClient { inner: Arc<DapInner> }

pub struct DapRequest { seq: u64, result: oneshot::Receiver<Result<Value, String>>, pending: Pending }

impl DapClient {
    pub async fn connect(address: impl ToSocketAddrs) -> anyhow::Result<(Self, mpsc::UnboundedReceiver<Value>)> {
        let stream = TcpStream::connect(address).await?;
        let (reader, writer) = stream.into_split();
        let (outgoing, outgoing_rx) = mpsc::channel(64);
        let (events, event_rx) = mpsc::unbounded_channel();
        let (close, close_rx) = watch::channel(false);
        let pending = Pending::default();
        let closed = Arc::new(AtomicBool::new(false));
        let inner = Arc::new(DapInner { outgoing, pending: pending.clone(), close: close.clone(), closed: closed.clone(), next_seq: AtomicU64::new(1) });

        let writer_pending = pending.clone();
        let writer_close = close.clone();
        let writer_closed = closed.clone();
        tokio::spawn(async move {
            if let Err(error) = write_messages(writer, outgoing_rx, close_rx).await { fail_pending(&writer_pending, &error.to_string()) }
            writer_closed.store(true, Ordering::Release);
            let _ = writer_close.send(true);
        });
        tokio::spawn(async move {
            if let Err(error) = read_messages(reader, pending.clone(), events, close.subscribe()).await { fail_pending(&pending, &error.to_string()) }
            closed.store(true, Ordering::Release);
            let _ = close.send(true);
        });
        Ok((Self { inner }, event_rx))
    }

    fn next_sequence(&self) -> u64 { self.inner.next_seq.fetch_add(1, Ordering::AcqRel) }

    fn request_sequence(&self, request: &mut Value) -> anyhow::Result<u64> {
        let content = request.as_object_mut().ok_or_else(|| anyhow::anyhow!("DAP request must be an object"))?;
        let seq = content.get("seq").and_then(Value::as_u64).filter(|seq| *seq > 0).unwrap_or_else(|| {
            let seq = self.next_sequence();
            content.insert("seq".into(), Value::from(seq));
            seq
        });
        self.inner.next_seq.fetch_max(seq + 1, Ordering::AcqRel);
        Ok(seq)
    }

    pub async fn send(&self, mut request: Value) -> anyhow::Result<DapRequest> {
        anyhow::ensure!(!self.inner.closed.load(Ordering::Acquire), "DAP connection closed");
        let seq = self.request_sequence(&mut request)?;
        let frame = encode_message(&request)?;
        let (complete, result) = oneshot::channel();
        {
            let mut pending = self.inner.pending.lock().expect("DAP pending lock poisoned");
            anyhow::ensure!(!pending.contains_key(&seq), "DAP request {seq} is already pending");
            pending.insert(seq, complete);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            self.inner.pending.lock().expect("DAP pending lock poisoned").remove(&seq);
            anyhow::bail!("DAP connection closed")
        }
        if self.inner.outgoing.send(frame).await.is_err() {
            self.inner.pending.lock().expect("DAP pending lock poisoned").remove(&seq);
            anyhow::bail!("DAP connection closed")
        }
        Ok(DapRequest { seq, result, pending: self.inner.pending.clone() })
    }

    pub async fn request(&self, request: Value, timeout: Duration) -> anyhow::Result<Value> { self.send(request).await?.wait(timeout).await }

    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        let _ = self.inner.close.send(true);
        fail_pending(&self.inner.pending, "DAP client closed");
    }
}

fn encode_message(message: &Value) -> anyhow::Result<Vec<u8>> {
    let payload = serde_json::to_vec(message)?;
    let mut frame = format!("Content-Length: {}\r\n\r\n", payload.len()).into_bytes();
    frame.extend(payload);
    Ok(frame)
}

impl DapRequest {
    pub fn sequence(&self) -> u64 { self.seq }

    pub async fn wait(self, timeout: Duration) -> anyhow::Result<Value> {
        match tokio::time::timeout(timeout, self.result).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(error))) => Err(anyhow::anyhow!(error)),
            Ok(Err(_)) => anyhow::bail!("DAP connection closed"),
            Err(_) => {
                self.pending.lock().expect("DAP pending lock poisoned").remove(&self.seq);
                anyhow::bail!("timed out waiting for DAP request {}", self.seq)
            }
        }
    }
}

fn fail_pending(pending: &Pending, error: &str) {
    for (_, complete) in pending.lock().expect("DAP pending lock poisoned").drain() { let _ = complete.send(Err(error.to_owned())); }
}

async fn write_messages(mut writer: impl AsyncWrite + Unpin, mut outgoing: mpsc::Receiver<Vec<u8>>, mut close: watch::Receiver<bool>) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            changed = close.changed() => {
                if changed.is_err() || *close.borrow() { return Ok(()) }
            }
            frame = outgoing.recv() => match frame {
                Some(frame) => writer.write_all(&frame).await?,
                None => return Ok(()),
            }
        }
    }
}

async fn read_message(reader: &mut (impl AsyncBufRead + Unpin)) -> anyhow::Result<Value> {
    let mut content_length = None;
    loop {
        let mut header = String::new();
        anyhow::ensure!(reader.read_line(&mut header).await? != 0, "DAP connection closed");
        if header == "\r\n" { break; }
        if let Some(value) = header.strip_prefix("Content-Length:") { content_length = Some(value.trim().parse::<usize>()?) }
    }
    let length = content_length.ok_or_else(|| anyhow::anyhow!("DAP message has no Content-Length"))?;
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await?;
    Ok(serde_json::from_slice(&payload)?)
}

async fn read_messages(
    reader: impl AsyncRead + Unpin,
    pending: Pending,
    events: mpsc::UnboundedSender<Value>,
    mut close: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut reader = BufReader::new(reader);
    loop {
        let message = tokio::select! {
            changed = close.changed() => {
                if changed.is_err() || *close.borrow() { return Ok(()) }
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
