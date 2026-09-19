use std::sync::mpsc;
use tokio::sync::oneshot;

enum Job<T> { Call(Box<dyn FnOnce(&mut T) + Send>), Stop(oneshot::Sender<()>) }

/// Async access to state created, used and dropped on one thread; the state need not be Send.
pub struct ThreadWorker<T> { jobs: mpsc::Sender<Job<T>> }

impl<T> Clone for ThreadWorker<T> { fn clone(&self) -> Self { Self { jobs: self.jobs.clone() } } }

impl<T: 'static> ThreadWorker<T> {
    pub async fn start(builder: std::thread::Builder, create: impl FnOnce() -> anyhow::Result<T> + Send + 'static) -> anyhow::Result<Self> {
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
        initialized.await??;
        Ok(Self { jobs })
    }

    pub async fn call<R: Send + 'static>(&self, call: impl FnOnce(&mut T) -> R + Send + 'static) -> anyhow::Result<R> {
        let (reply, result) = oneshot::channel();
        self.send(Job::Call(Box::new(move |state| { let _ = reply.send(call(state)); })))?;
        Ok(result.await?)
    }

    /// Wait until the interpreter has been dropped. Dropping all handles also stops the worker.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let (reply, result) = oneshot::channel();
        self.send(Job::Stop(reply))?;
        Ok(result.await?)
    }

    fn send(&self, job: Job<T>) -> anyhow::Result<()> { self.jobs.send(job).map_err(|_| anyhow::anyhow!("interpreter worker stopped")) }
}
