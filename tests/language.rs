use kernmini::{Error, ErrorKind, ExecutionInterrupt, ThreadWorker};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    assert_eq!(ready.set_handler(Arc::new(|| Ok(()))).unwrap_err().kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn worker_errors_keep_their_meaning() -> kernmini::Result<()> {
    use std::error::Error as _;
    let error = Error::adapter(std::io::Error::from(std::io::ErrorKind::PermissionDenied)).context("starting interpreter");
    let failed = ThreadWorker::<()>::start(std::thread::Builder::new(), move || Err(error)).await;
    let error = failed.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Adapter);
    assert_eq!(error.source().unwrap().source().unwrap().downcast_ref::<std::io::Error>().unwrap().kind(), std::io::ErrorKind::PermissionDenied);

    let worker = ThreadWorker::start(std::thread::Builder::new(), || Ok(())).await?;
    assert_eq!(worker.call(|_| Err::<(), _>("language error")).await?, Err("language error"));
    assert_eq!(worker.call(|_| panic!("interpreter crashed")).await.unwrap_err().kind(), ErrorKind::Closed);
    assert_eq!(worker.call(|_| ()).await.unwrap_err().kind(), ErrorKind::Closed);

    let worker = ThreadWorker::start(std::thread::Builder::new(), || Ok(())).await?;
    worker.shutdown().await?;
    assert_eq!(worker.call(|_| ()).await.unwrap_err().kind(), ErrorKind::Closed);
    Ok(())
}
