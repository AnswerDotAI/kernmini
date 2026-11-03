use crate::{Error, Result};
use std::sync::mpsc;
use std::thread::Builder;
use tokio::sync::oneshot;

enum Job<T> { Call(Box<dyn FnOnce(&mut T) + Send>), Stop(oneshot::Sender<()>) }

/// Async access to state created, used and dropped on one thread; the state need not be Send.
pub struct ThreadWorker<T> { jobs: mpsc::Sender<Job<T>> }

impl<T> Clone for ThreadWorker<T> { fn clone(&self) -> Self { Self { jobs: self.jobs.clone() } } }

impl<T: 'static> ThreadWorker<T> {
    pub async fn start(builder: Builder, create: impl FnOnce() -> Result<T> + Send + 'static) -> Result<Self> {
        let (jobs, receive) = mpsc::channel();
        let (ready, initialized) = oneshot::channel();
        builder.spawn(move || {
            let mut state = match create() { Ok(state) => { let _ = ready.send(Ok(())); state } Err(error) => { let _ = ready.send(Err(error)); return; } };
            while let Ok(job) = receive.recv() {
                match job {
                    Job::Call(call) => call(&mut state),
                    Job::Stop(reply) => {
                        drop(state);
                        let _ = reply.send(());
                        return;
                    }
                }
            }
        })?;
        initialized.await.map_err(|error| Error::closed("interpreter worker during startup").caused_by(error))??;
        Ok(Self { jobs })
    }

    pub async fn call<R: Send + 'static>(&self, call: impl FnOnce(&mut T) -> R + Send + 'static) -> Result<R> {
        let (reply, result) = oneshot::channel();
        self.send(Job::Call(Box::new(move |state| { let _ = reply.send(call(state)); })))?;
        result.await.map_err(|error| Error::closed("interpreter worker").caused_by(error))
    }

    /// Wait until the interpreter has been dropped. Dropping all handles also stops the worker.
    pub async fn shutdown(&self) -> Result<()> {
        let (reply, result) = oneshot::channel();
        self.send(Job::Stop(reply))?;
        result.await.map_err(|error| Error::closed("interpreter worker during shutdown").caused_by(error))
    }

    fn send(&self, job: Job<T>) -> Result<()> { self.jobs.send(job).map_err(|_| Error::closed("interpreter worker")) }
}
