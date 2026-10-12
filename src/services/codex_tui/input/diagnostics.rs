//! Diagnostics for prompt readiness observations.
use std::time::Duration;

use super::{
    PROMPT_READY_DEBUG_TAIL_BYTES, PROMPT_READY_DEBUG_TAIL_LINES, PromptReadinessKind,
    PromptReadinessSnapshot,
};

pub(super) fn log_prompt_ready_timeout(
    session_name: &str,
    readiness: PromptReadinessKind,
    timeout: Duration,
    snapshot: &PromptReadinessSnapshot,
) {
    tracing::debug!(
        tmux_session_name = session_name,
        readiness = readiness.label(),
        timeout_secs = timeout.as_secs(),
        composer_marker_detected = snapshot.composer_marker_detected,
        prompt_draft_detected = snapshot.prompt_draft_detected,
        previous_tui_turn_still_running =
            snapshot.tmux_pane_alive && !snapshot.composer_marker_detected,
        tmux_pane_alive = snapshot.tmux_pane_alive,
        capture_available = snapshot.capture_available,
        pane_tail = %snapshot.pane_tail,
        "codex_tui prompt readiness timed out"
    );
}

pub(super) fn prompt_ready_debug_tail(pane: &str) -> String {
    let mut lines = pane
        .lines()
        .rev()
        .take(PROMPT_READY_DEBUG_TAIL_LINES)
        .map(|line| line.trim_end_matches('\r'))
        .collect::<Vec<_>>();
    lines.reverse();
    let tail = lines.join("\n");
    crate::utils::format::safe_suffix(tail.trim(), PROMPT_READY_DEBUG_TAIL_BYTES).to_string()
}
