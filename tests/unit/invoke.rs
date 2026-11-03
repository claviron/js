use super::*;
#[test]
fn invoke_outcomes_distinguish_unknown_execution_and_domain_rejection() {
    let value = outcome(Err(CallError::OutcomeUnknown));
    assert_eq!(value.code, "OUTCOME_UNKNOWN");
    let value =
        outcome(Err(CallError::Remote(Failure::Rejected(vec![0, 255]))));
    assert_eq!(value.code, "REJECTED");
    assert_eq!(value.payload, [0, 255]);
}
#[test]
fn cancellation_owns_exactly_one_terminal_slot() {
    let (sender, mut receiver) = oneshot::channel();
    let call = Call {
        generation: 1,
        cancel: CancellationToken::new(),
        state: Mutex::new(Some(sender)),
        receive: AsyncMutex::new(None),
    };
    assert!(call.cancel());
    assert!(!call.cancel());
    assert!(call.cancel.is_cancelled());
    assert_eq!(
        receiver.try_recv().unwrap().unwrap_err().kind,
        ErrorKind::Cancelled
    );
    call.settle(Ok(Outcome { code: "SUCCESS", payload: vec![1] }));
}

#[test]
fn retired_generation_prevents_native_reply_before_js_observes_it() {
    let generation =
        Arc::new(Generation { id: 1, cancelled: CancellationToken::new() });
    let (sender, mut receiver) = oneshot::channel();
    let incoming = Invocation(Arc::new(Incoming {
        operation: "echo".into(),
        payload: vec![0, 255],
        generation: Arc::clone(&generation),
        reply: Mutex::new(Some(sender)),
        terminal: watch::channel(Terminal::Open).0,
    }));
    generation.cancelled.cancel();
    assert_eq!(
        incoming.reply(0, vec![1]).unwrap_err().kind,
        ErrorKind::Cancelled
    );
    incoming.abort();
    assert_eq!(receiver.try_recv().unwrap(), Err(Failure::Closing));
    assert!(incoming.reply(0, vec![2]).is_err());
}
