use crate::wire::{Message, Session};
use crate::{Error, ErrorKind};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify, mpsc, oneshot, watch};
use tokio::task::{JoinError, JoinSet};
use zmtpmini::{Incoming, Peer};

struct SendRequest { frames: Vec<Bytes>, complete: oneshot::Sender<crate::Result<()>> }

#[derive(Clone)]
pub struct ReplySink { outgoing: mpsc::Sender<SendRequest>, closed: watch::Receiver<Option<Error>> }

impl ReplySink {
    pub async fn send(&self, frames: Vec<Bytes>) -> crate::Result<()> {
        let (complete, done) = oneshot::channel();
        tokio::select! {
            biased;
            error = self.closed() => Err(error),
            result = async {
                self.outgoing.send(SendRequest { frames, complete }).await?;
                done.await?
            } => result,
        }
    }

    pub async fn closed(&self) -> Error {
        let mut closed = self.closed.clone();
        loop {
            if let Some(error) = closed.borrow_and_update().clone() { return error; }
            if closed.changed().await.is_err() { return Error::closed("router peer"); }
        }
    }
}

#[derive(Clone, Default)]
pub struct RouterPeers { peers: Arc<Mutex<HashMap<Bytes, ReplySink>>>, changed: Arc<Notify> }

impl RouterPeers {
    async fn insert(&self, identity: Bytes, reply: ReplySink) {
        self.peers.lock().await.insert(identity, reply);
        self.changed.notify_waiters();
    }

    async fn remove(&self, identity: &Bytes, reply: &ReplySink) {
        let mut peers = self.peers.lock().await;
        if peers.get(identity).is_some_and(|current| current.outgoing.same_channel(&reply.outgoing)) { peers.remove(identity); }
    }

    pub async fn wait(&self, identity: &Bytes) -> ReplySink {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(reply) = self.peers.lock().await.get(identity).cloned() { return reply; }
            notified.await;
        }
    }
}

pub struct Inbound { pub message: Message, pub reply: ReplySink, pub identity: Bytes }

pub async fn serve_router(listener: TcpListener, session: Session, incoming: mpsc::Sender<Inbound>, peers: Option<RouterPeers>) -> crate::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            stream = listener.accept() => {
                let (stream, _) = stream?;
                connections.spawn(router_peer(stream, session.clone(), incoming.clone(), peers.clone()));
            }
            result = connections.join_next(), if !connections.is_empty() => peer_finished(result.unwrap(), "router")?,
        }
    }
}

fn peer_finished(result: Result<crate::Result<()>, JoinError>, channel: &str) -> crate::Result<()> {
    if let Err(error) = result? && error.kind() != ErrorKind::Closed { eprintln!("{channel} peer ended: {error}"); }
    Ok(())
}

async fn router_peer(stream: TcpStream, session: Session, incoming: mpsc::Sender<Inbound>, peers: Option<RouterPeers>) -> crate::Result<()> {
    let peer = Peer::router(stream).await?;
    let identity = Bytes::copy_from_slice(peer.identity().unwrap_or_default());
    let (mut reader, mut writer) = peer.split();
    let (send, mut outgoing) = mpsc::channel::<SendRequest>(128);
    let (closed, close_rx) = watch::channel(None);
    let reply = ReplySink { outgoing: send, closed: close_rx };
    let writer_loop = async move {
        while let Some(request) = outgoing.recv().await {
            let result = writer.send(request.frames).await.map_err(Error::from);
            let _ = request.complete.send(result.clone());
            result?;
        }
        Ok::<_, Error>(())
    };
    if let Some(peers) = &peers { peers.insert(identity.clone(), reply.clone()).await; }
    let result = tokio::select! {
        result = writer_loop => result,
        result = async {
            loop {
                match reader.recv().await? {
                    Incoming::Message(frames) => match session.decode(frames) {
                        Ok(message) => incoming.send(Inbound { message, reply: reply.clone(), identity: identity.clone() }).await?,
                        Err(crate::WireError::DuplicateSignature) => continue,
                        Err(error) => return Err(Error::from(error)),
                    },
                    _ => return Err(Error::new(ErrorKind::Protocol, "unexpected command on ROUTER peer")),
                }
            }
        } => result,
    };
    closed.send_replace(Some(result.clone().err().unwrap_or_else(|| Error::closed("router peer"))));
    if let Some(peers) = &peers { peers.remove(&identity, &reply).await; }
    result
}

#[derive(Clone)]
pub struct Iopub { peers: Arc<Mutex<Vec<mpsc::Sender<Vec<Bytes>>>>>, session: Session, capacity: usize }

impl Iopub {
    pub fn new(session: Session, capacity: usize) -> Self { Self { peers: Arc::new(Mutex::new(vec![])), session, capacity } }

    pub async fn publish(&self, message: Message) -> crate::Result<()> {
        let frames = self.session.encode(&message)?;
        let mut peers = self.peers.lock().await;
        let mut i = 0;
        while i < peers.len() { if peers[i].send(frames.clone()).await.is_err() { peers.swap_remove(i); } else { i += 1; } }
        Ok(())
    }

    pub async fn serve(self, listener: TcpListener) -> crate::Result<()> {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                stream = listener.accept() => {
                    let (stream, _) = stream?;
                    let iopub = self.clone();
                    connections.spawn(async move { iopub.peer(stream).await });
                }
                result = connections.join_next(), if !connections.is_empty() => peer_finished(result.unwrap(), "iopub")?,
            }
        }
    }

    async fn peer(&self, stream: TcpStream) -> crate::Result<()> {
        let peer = Peer::xpublisher(stream).await?;
        let (mut reader, mut writer) = peer.split();
        let (send, mut outgoing) = mpsc::channel::<Vec<Bytes>>(self.capacity);
        self.peers.lock().await.push(send);
        loop {
            tokio::select! {
                event = reader.recv() => match event? {
                    Incoming::Subscribe(topic) => {
                        let subscription = String::from_utf8_lossy(&topic).into_owned();
                        let mut welcome = self.session.message("iopub_welcome", serde_json::json!({"subscription": subscription}), None);
                        if !topic.is_empty() { welcome.identities.push(topic) }
                        writer.send(self.session.encode(&welcome)?).await?;
                    }
                    Incoming::Cancel(_) => {}
                    Incoming::Ping(context) => writer.pong(&context).await?,
                    Incoming::Message(_) => return Err(Error::new(ErrorKind::Protocol, "unexpected message on IOPub peer")),
                },
                outgoing = outgoing.recv() => match outgoing {
                    Some(frames) => writer.send(frames).await?,
                    None => return Ok(()),
                }
            }
        }
    }
}

pub async fn serve_heartbeat(listener: TcpListener) -> crate::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            stream = listener.accept() => {
                let (stream, _) = stream?;
                connections.spawn(async move {
                    let (mut reader, mut writer) = Peer::reply(stream).await?.split();
                    loop {
                        match reader.recv().await? {
                            Incoming::Message(message) => writer.send(message).await?,
                            Incoming::Ping(context) => writer.pong(&context).await?,
                            Incoming::Subscribe(_) | Incoming::Cancel(_) => return Err(Error::new(ErrorKind::Protocol, "subscription command on heartbeat peer")),
                        }
                    }
                });
            }
            result = connections.join_next(), if !connections.is_empty() => peer_finished(result.unwrap(), "heartbeat")?,
        }
    }
}
