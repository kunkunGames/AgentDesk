use super::*;
use crate::services::provider::cancel_token_cleanup::authority::KillAuthorizationState;
use crate::services::provider::cancel_token_cleanup::executor::{
    CleanupRequest, TmuxCleanupIntent,
};

struct Pane(String);

impl Drop for Pane {
    fn drop(&mut self) {
        crate::services::platform::tmux::kill_session(&self.0, "Qwen PID ownership test cleanup");
    }
}

#[test]
fn qwen_wrapper_pid_binding_keeps_identity_and_rejects_old_turn_cleanup() {
    let pane = Pane(format!(
        "AgentDesk-qwen-pid-binding-{}",
        uuid::Uuid::new_v4()
    ));
    let created =
        crate::services::platform::tmux::create_session(&pane.0, None, "sleep 30").unwrap();
    assert!(
        created.status.success(),
        "native tmux test session must start"
    );
    let old = CancelToken::new();
    old.bind_owned_tmux_session(ProviderKind::Qwen, &pane.0);
    bind_qwen_wrapper_pid(&old, &pane.0).unwrap();
    let captured = old
        .captured_child_process()
        .expect("wrapper PID must be captured");
    assert!(
        captured
            .identity
            .is_some_and(|identity| identity.matches(captured.pid))
    );

    let current = CancelToken::new();
    current.bind_owned_tmux_session(ProviderKind::Qwen, &pane.0);
    bind_qwen_wrapper_pid(&current, &pane.0).unwrap();
    old.bind_unmanaged_session_name(&pane.0);
    let result = old.request_cleanup(CleanupRequest {
        cancel_source: "execution_lease_lost".to_string(),
        intent: TmuxCleanupIntent::CleanupSession,
        termination_reason: None,
        hard_stop_target: None,
    });
    assert_eq!(result.authorization, KillAuthorizationState::Stale);
    assert!(!result.pid_killed && !result.tmux_killed);
    assert!(captured.identity.unwrap().matches(captured.pid));
}

#[test]
fn absent_qwen_pane_does_not_register_an_unverified_pid() {
    let token = CancelToken::new();
    let name = format!("AgentDesk-qwen-absent-{}", uuid::Uuid::new_v4());
    assert!(bind_qwen_wrapper_pid(&token, &name).is_err());
    assert!(token.child_pid_value().is_none());
}
