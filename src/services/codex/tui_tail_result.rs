//! Cancellation and cleanup after reading a Codex Direct TUI rollout.
use super::{CancelToken, record_codex_tmux_termination, record_tmux_exit_reason};
use crate::services::codex_tui::verified_hold;

/// A cancelled tail returns no result; the bridge retains cleanup ownership.
pub(super) fn resolve_codex_tui_tail_result(
    tail_result: Result<crate::services::codex_tui::rollout_tail::CodexTuiTailResult, String>,
    cancel_token_for_post_tail: Option<&std::sync::Arc<CancelToken>>,
    tmux_session_name: &str,
) -> Result<Option<crate::services::codex_tui::rollout_tail::CodexTuiTailResult>, String> {
    let cancel_observed =
        || crate::services::provider::cancel_requested(cancel_token_for_post_tail.map(|t| &**t));

    match tail_result {
        Ok(result) => Ok(Some(result)),
        Err(error) => {
            if cancel_observed() {
                tracing::info!(
                    tmux_session = tmux_session_name,
                    error = %error,
                    "Codex rollout tail cancelled before transcript; suppressing tail Err and deferring tmux cleanup to cancel path"
                );
                return Ok(None);
            }
            if verified_hold::existing_incarnation(tmux_session_name) {
                verified_hold::wait_for_cancel(tmux_session_name, cancel_token_for_post_tail)?;
                return Ok(None);
            }
            tracing::warn!(
                tmux_session = tmux_session_name,
                error = %error,
                "Codex rollout tail failed; killing tmux session to avoid leak"
            );
            record_codex_tmux_termination(
                tmux_session_name,
                "codex_tui_provider",
                "rollout_tail_failed",
                &format!("codex rollout tail failed: {error}"),
                None,
            );
            record_tmux_exit_reason(tmux_session_name, &format!("rollout tail failed: {error}"));
            crate::services::platform::tmux::kill_session(
                tmux_session_name,
                "codex rollout tail failed",
            );
            Err(error)
        }
    }
}
