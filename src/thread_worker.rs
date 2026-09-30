use std::sync::mpsc;
use tokio::sync::oneshot;

enum Job<T> { Call(Box<dyn FnOnce(&mut T) + Send>), Stop(oneshot::Sender<()>) }

/// Async access to state created, used and dropped on one thread; the state need not be Send.
pub struct ThreadWorker<T> { jobs: mpsc::Sender<Job<T>> }

impl<T> Clone for ThreadWorker<T> { fn clone(&self) -> Self { Self { jobs: self.jobs.clone() } } }

impl<T: 'static> ThreadWorker<T> {
    pub async fn start(builder: std::thread::Builder, create: impl FnOnce() -> crate::Result<T> + Send + 'static) -> crate::Result<Self> {
        let (jobs, receive) = mpsc::channel();
        let (ready, initialized) = oneshot::channel();
        builder.spawn(move || {
            let mut state = match create() {
                Ok(state) => {
                    let _ = ready.send(Ok(()));
                    state
                }
                Err(error) => {
                    let _ = ready.send(Err(error));
                    return;
                }
            };
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
        initialized.await.map_err(|error| crate::Error::closed("interpreter worker during startup").caused_by(error))??;
        Ok(Self { jobs })
    }

    pub async fn call<R: Send + 'static>(&self, call: impl FnOnce(&mut T) -> R + Send + 'static) -> crate::Result<R> {
        let (reply, result) = oneshot::channel();
        self.send(Job::Call(Box::new(move |state| { let _ = reply.send(call(state)); })))?;
        result.await.map_err(|error| crate::Error::closed("interpreter worker").caused_by(error))
    }

    /// Wait until the interpreter has been dropped. Dropping all handles also stops the worker.
    pub async fn shutdown(&self) -> crate::Result<()> {
        let (reply, result) = oneshot::channel();
        self.send(Job::Stop(reply))?;
        result.await.map_err(|error| crate::Error::closed("interpreter worker during shutdown").caused_by(error))
    }

    fn send(&self, job: Job<T>) -> crate::Result<()> { self.jobs.send(job).map_err(|_| crate::Error::closed("interpreter worker")) }
}
