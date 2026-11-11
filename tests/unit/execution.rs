use super::*;

#[test]
fn executor_mode_is_explicit_and_one_multi_worker_is_not_current_thread() {
    let current =
        RuntimeOptions::from_values(Some("current"), Some("1")).unwrap();
    let multi = RuntimeOptions::from_values(Some("multi"), Some("1")).unwrap();
    assert_ne!(current.execution, multi.execution);
    assert_eq!(
        current.execution,
        RuntimeOptions::from_values(Some("current_thread"), None)
            .unwrap()
            .execution
    );
    assert_eq!(
        multi.execution,
        RuntimeOptions::from_values(Some("multi_thread"), None)
            .unwrap()
            .execution
    );
    assert!(RuntimeOptions::from_values(Some("inherit"), None).is_err());
    assert!(
        RuntimeOptions::from_values(Some("current_thread"), Some("2")).is_err()
    );
    assert!(RuntimeOptions::from_values(None, Some("0")).is_err());
    assert!(RuntimeOptions::from_values(Some("unknown"), None).is_err());
    assert_eq!(
        RuntimeOptions::from_values(None, None).unwrap().worker_threads,
        None
    );
}
