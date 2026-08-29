use kernmini::ExecutionInterrupt;
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
}
