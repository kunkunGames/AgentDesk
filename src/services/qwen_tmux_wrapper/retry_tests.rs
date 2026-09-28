use super::*;
use crate::services::qwen::retry_fixture::RetryFixture;

fn execute(fixture: &RetryFixture) -> (Result<(), String>, String) {
    let path = fixture.path().join("output.jsonl");
    let mut output = std::fs::File::create(&path).unwrap();
    let result = run_turn(
        &mut output,
        fixture.cli.to_str().unwrap(),
        None,
        fixture.path().to_str().unwrap(),
        "fixture",
        &mut None,
        None,
        true,
    );
    (result, std::fs::read_to_string(path).unwrap())
}

#[test]
fn interrupted_turn_keeps_evidence_and_does_not_repeat_tool_effects() {
    for mode in ["exit_after_progress", "eof_after_progress", "read_error"] {
        let fixture = RetryFixture::new(mode);
        let (result, output) = execute(&fixture);
        assert!(result.is_err(), "{mode}");
        assert_eq!(fixture.invocations(), 1, "{mode}: fresh replay occurred");
        assert!(fixture.path().join("effect").is_file());
        assert_eq!(
            output.matches("tool_use").count(),
            1,
            "{mode}: original evidence was rewound"
        );
    }
}

#[test]
fn startup_retry_can_succeed_before_any_external_effect() {
    let fixture = RetryFixture::new("startup_once");
    let (result, output) = execute(&fixture);
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(fixture.invocations(), 2);
    assert_eq!(output.matches("tool_use").count(), 1);
    assert!(output.contains("done"));
}

#[test]
fn native_budget_failure_is_retained_without_fresh_replay() {
    let fixture = RetryFixture::new("terminal_error");
    let (result, output) = execute(&fixture);
    assert!(
        result.is_ok(),
        "native terminal error is emitted for the reader"
    );
    assert_eq!(fixture.invocations(), 1);
    assert!(output.contains("native budget exhausted") && output.contains("is_error"));
    assert!(!fixture.path().join("effect").exists());
}
