use super::*;

#[test]
fn terminal_credit_is_returned_once_before_foreign_wrapper_collection() {
    let (ended, mut completed) = mpsc::unbounded_channel();
    let lease = Completion::new(None, Some(ended), Arc::new(Active::default()));
    let still_retained = Arc::clone(&lease);
    drop(TerminalGuard(Some(lease)));
    assert!(completed.try_recv().is_ok());
    still_retained.release();
    assert!(completed.try_recv().is_err());
}

#[tokio::test]
async fn drain_waits_for_accepted_work_and_not_for_foreign_collection() {
    use std::future::Future;
    use std::task::Poll;
    let active = Arc::new(Active::default());
    let lease = Completion::new(None, None, Arc::clone(&active));
    let drain = active.drain();
    tokio::pin!(drain);
    std::future::poll_fn(|cx| {
        assert!(drain.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    lease.release();
    drain.await;
    assert_eq!(
        Arc::strong_count(&lease),
        1,
        "foreign owner can remain alive after terminal native completion"
    );
}
