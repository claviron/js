use super::*;
#[test]
fn generations_keep_old_signals_cancelled_and_idempotent_start_stable() {
    let mut state = State::initial();
    let first = Arc::clone(&state.generation);
    state.apply("start").unwrap();
    assert!(Arc::ptr_eq(&first, &state.generation));
    state.apply("stop").unwrap();
    assert!(!state.running);
    assert!(first.cancelled.is_cancelled());
    state.apply("start").unwrap();
    assert_eq!(state.generation.id, 2);
    assert!(!state.generation.cancelled.is_cancelled());
    let second = Arc::clone(&state.generation);
    state.apply("restart").unwrap();
    assert!(second.cancelled.is_cancelled());
    assert_eq!(state.generation.id, 3);
    state.generation = Arc::new(Generation {
        id: u64::MAX,
        cancelled: CancellationToken::new(),
    });
    assert!(state.apply("restart").is_err());
    assert!(!state.generation.cancelled.is_cancelled());
}
