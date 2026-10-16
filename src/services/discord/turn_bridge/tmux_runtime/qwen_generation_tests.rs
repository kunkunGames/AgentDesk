use super::*;
use crate::services::provider::cancel_token_cleanup::authority::KillAuthorizationState;
use crate::services::provider::cancel_token_cleanup::executor::{
    CleanupRequest, TmuxCleanupIntent,
};

struct Pane(String);

impl Drop for Pane {
    fn drop(&mut self) {
        crate::services::platform::tmux::kill_session(
            &self.0,
            "Qwen recovery ownership test cleanup",
        );
    }
}

#[test]
fn qwen_runtime_recovery_retains_wrapper_identity_and_stale_generation_fence() {
    let pane = Pane(format!("AgentDesk-qwen-recovery-{}", uuid::Uuid::new_v4()));
    let created =
        crate::services::platform::tmux::create_session(&pane.0, None, "sleep 30").unwrap();
    assert!(created.status.success());
    let old = Arc::new(CancelToken::new());
    bind_cancel_token_tmux_runtime(&ProviderKind::Qwen, &old, &pane.0, "test recovery");
    let wrapper = old
        .captured_child_process()
        .expect("recovery must capture the pane owner");
    assert!(
        wrapper
            .identity
            .is_some_and(|identity| identity.matches(wrapper.pid))
    );

    let current = Arc::new(CancelToken::new());
    bind_cancel_token_tmux_runtime(&ProviderKind::Qwen, &current, &pane.0, "new turn recovery");
    // Compatibility rebind cannot silently downgrade the old generation fence.
    old.bind_unmanaged_session_name(&pane.0);
    let result = old.request_cleanup(CleanupRequest {
        cancel_source: "execution_lease_lost".to_string(),
        intent: TmuxCleanupIntent::CleanupSession,
        termination_reason: None,
        hard_stop_target: None,
    });
    assert_eq!(result.authorization, KillAuthorizationState::Stale);
    assert!(!result.pid_killed && !result.tmux_killed);
    assert!(wrapper.identity.unwrap().matches(wrapper.pid));
}
