use crate::wire::{Message, Session};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use zmtpmini::{Incoming, Peer};

struct SendRequest { frames: Vec<Bytes>, complete: oneshot::Sender<anyhow::Result<()>> }

#[derive(Clone)]
pub struct ReplySink(mpsc::Sender<SendRequest>);

impl ReplySink {
    pub async fn send(&self, frames: Vec<Bytes>) -> anyhow::Result<()> {
        let (complete, done) = oneshot::channel();
        self.0.send(SendRequest { frames, complete }).await?;
        done.await??;
        Ok(())
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
        if peers.get(identity).is_some_and(|current| current.0.same_channel(&reply.0)) { peers.remove(identity); }
    }

    pub async fn wait(&self, identity: &Bytes) -> ReplySink {
        loop {
            let notified = self.changed.notified();
            if let Some(reply) = self.peers.lock().await.get(identity).cloned() { return reply; }
            notified.await;
        }
    }
}

pub struct Inbound { pub message: Message, pub reply: ReplySink, pub identity: Bytes }

pub async fn serve_router(listener: TcpListener, session: Session, incoming: mpsc::Sender<Inbound>, peers: Option<RouterPeers>) -> anyhow::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let session = session.clone();
        let incoming = incoming.clone();
        let peers = peers.clone();
        tokio::spawn(async move {
            let mut registered = None;
            let result: anyhow::Result<()> = async {
                let peer = Peer::router(stream).await?;
                let identity = Bytes::copy_from_slice(peer.identity().unwrap_or_default());
                let (mut reader, mut writer) = peer.split();
                let (send, mut outgoing) = mpsc::channel::<SendRequest>(128);
                let _writer_task = tokio::spawn(async move {
                    while let Some(request) = outgoing.recv().await {
                        let result = writer.send(request.frames).await.map_err(anyhow::Error::from);
                        let failed = result.is_err();
                        let _ = request.complete.send(result);
                        if failed { break; }
                    }
                });
                let reply = ReplySink(send);
                if let Some(peers) = &peers {
                    peers.insert(identity.clone(), reply.clone()).await;
                    registered = Some((identity.clone(), reply.clone()));
                }
                loop {
                    match reader.recv().await? {
                        Incoming::Message(frames) => match session.decode(frames) {
                            Ok(message) => incoming.send(Inbound { message, reply: reply.clone(), identity: identity.clone() }).await?,
                            Err(crate::wire::WireError::DuplicateSignature) => continue,
                            Err(error) => return Err(error.into()),
                        },
                        Incoming::Ping(context) => {
                            // ROUTER peers do not normally issue ZMTP PINGs. The split writer task
                            // owns command output, so treat an unsolicited one as a peer failure for now.
                            anyhow::bail!("unexpected ROUTER PING with {} context bytes", context.len())
                        }
                        Incoming::Subscribe(_) | Incoming::Cancel(_) => anyhow::bail!("subscription command on ROUTER peer"),
                    }
                }
                #[allow(unreachable_code)]
                {
                    _writer_task.await?;
                    Ok(())
                }
            }
            .await;
            if let (Some(peers), Some((identity, reply))) = (&peers, registered) { peers.remove(&identity, &reply).await }
            if let Err(error) = result
                && !matches!(error.downcast_ref::<zmtpmini::Error>(), Some(zmtpmini::Error::Closed))
            { eprintln!("router peer ended: {error:#}") }
        });
    }
}

#[derive(Clone)]
pub struct Iopub { peers: Arc<Mutex<Vec<mpsc::Sender<Vec<Bytes>>>>>, session: Session, capacity: usize }

impl Iopub {
    pub fn new(session: Session, capacity: usize) -> Self { Self { peers: Arc::new(Mutex::new(vec![])), session, capacity } }

    pub async fn publish(&self, message: Message) -> anyhow::Result<()> {
        let frames = self.session.encode(&message)?;
        let mut peers = self.peers.lock().await;
        peers.retain(|peer| match peer.try_send(frames.clone()) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => true,
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        });
        Ok(())
    }

    pub async fn serve(self, listener: TcpListener) -> anyhow::Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            let peer = Peer::xpublisher(stream).await?;
            let (mut reader, mut writer) = peer.split();
            let (send, mut outgoing) = mpsc::channel::<Vec<Bytes>>(self.capacity);
            self.peers.lock().await.push(send.clone());
            let session = self.session.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        event = reader.recv() => match event {
                            Ok(Incoming::Subscribe(topic)) => {
                                let subscription = String::from_utf8_lossy(&topic).into_owned();
                                let mut welcome = session.message("iopub_welcome", serde_json::json!({"subscription": subscription}), None);
                                if !topic.is_empty() { welcome.identities.push(topic) }
                                let Ok(frames) = session.encode(&welcome) else { break };
                                if writer.send(frames).await.is_err() { break }
                            }
                            Ok(Incoming::Cancel(_)) => {}
                            Ok(Incoming::Ping(context)) => if writer.pong(&context).await.is_err() { break },
                            Ok(Incoming::Message(_)) | Err(_) => break,
                        },
                        outgoing = outgoing.recv() => match outgoing {
                            Some(frames) => if writer.send(frames).await.is_err() { break },
                            None => break,
                        }
                    }
                }
            });
        }
    }
}

pub async fn serve_heartbeat(listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let result: anyhow::Result<()> = async {
                let (mut reader, mut writer) = Peer::reply(stream).await?.split();
                loop {
                    match reader.recv().await? {
                        Incoming::Message(message) => writer.send(message).await?,
                        Incoming::Ping(context) => writer.pong(&context).await?,
                        Incoming::Subscribe(_) | Incoming::Cancel(_) => anyhow::bail!("subscription command on heartbeat peer"),
                    }
                }
            }
            .await;
            if let Err(error) = result
                && !matches!(error.downcast_ref::<zmtpmini::Error>(), Some(zmtpmini::Error::Closed))
            { eprintln!("heartbeat peer ended: {error:#}") }
        });
    }
}
