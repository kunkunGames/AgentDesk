use super::retry_fixture::RetryFixture;
use super::*;

#[test]
fn provider_failure_after_tool_effect_is_terminal_not_retryable() {
    for mode in ["exit_after_progress", "eof_after_progress", "read_error"] {
        let fixture = RetryFixture::new(mode);
        let (tx, rx) = mpsc::channel();
        let result = execute_qwen_streaming_attempt(
            fixture.cli.to_str().unwrap(),
            &fixture.resolution(),
            "fixture",
            None,
            QwenResumeStrategy::Fresh,
            fixture.path().to_str().unwrap(),
            tx,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert!(matches!(result, QwenAttemptResult::Completed), "{mode}");
        let messages: Vec<_> = rx.try_iter().collect();
        assert_eq!(
            messages
                .iter()
                .filter(|m| matches!(m, StreamMessage::Error { .. }))
                .count(),
            1,
            "{mode}"
        );
        assert!(
            !messages
                .iter()
                .any(|m| matches!(m, StreamMessage::Done { .. })),
            "{mode}"
        );
        assert!(fixture.path().join("effect").is_file());
        assert_eq!(fixture.invocations(), 1);
    }
}

#[test]
fn initial_handshake_failure_remains_retryable_without_a_tool_effect() {
    let fixture = RetryFixture::new("startup_once");
    let (tx, rx) = mpsc::channel();
    let result = execute_qwen_streaming_attempt(
        fixture.cli.to_str().unwrap(),
        &fixture.resolution(),
        "fixture",
        None,
        QwenResumeStrategy::Fresh,
        fixture.path().to_str().unwrap(),
        tx,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert!(matches!(result, QwenAttemptResult::RetrySession { .. }));
    assert!(!fixture.path().join("effect").exists());
    assert!(rx.try_iter().next().is_none());
}
