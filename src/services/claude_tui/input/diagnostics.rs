//! Diagnostics for prompt readiness and submission observations.
use std::time::Duration;

use super::{PROMPT_READY_DEBUG_TAIL_LINES, PromptReadinessKind, PromptReadinessSnapshot};

pub(super) fn log_prompt_ready_timeout(
    session_name: &str,
    readiness: PromptReadinessKind,
    timeout: Duration,
    snapshot: &PromptReadinessSnapshot,
    previous_tui_turn_still_running: bool,
) {
    tracing::debug!(
        tmux_session_name = session_name,
        readiness = readiness.label(),
        timeout_secs = timeout.as_secs(),
        prompt_marker_detected = snapshot.prompt_marker_detected,
        prompt_draft_detected = snapshot.prompt_draft_detected,
        previous_tui_turn_still_running,
        tmux_pane_alive = snapshot.tmux_pane_alive,
        capture_available = snapshot.capture_available,
        pane_tail = %snapshot.pane_tail,
        "claude_tui prompt readiness timed out"
    );
    crate::services::claude::debug_log_to(
        "claude_tui.log",
        &format!(
            "prompt readiness timeout session={} readiness={} timeout={}s prompt_marker_detected={} prompt_draft_detected={} previous_tui_turn_still_running={} tmux_pane_alive={} capture_available={} pane_tail:\n{}",
            session_name,
            readiness.label(),
            timeout.as_secs(),
            snapshot.prompt_marker_detected,
            snapshot.prompt_draft_detected,
            previous_tui_turn_still_running,
            snapshot.tmux_pane_alive,
            snapshot.capture_available,
            snapshot.pane_tail
        ),
    );
}

/// #3889: record the confirmed MCP-auth cold-boot block. Logs the full pane tail
/// (which carries the welcome box + warning banner) to `claude_tui.log` so the
/// failure is diagnosable from a forensics file, not just a first-line WARN.
pub(super) fn log_prompt_ready_mcp_auth_block(
    session_name: &str,
    readiness: PromptReadinessKind,
    snapshot: &PromptReadinessSnapshot,
) {
    tracing::warn!(
        tmux_session_name = session_name,
        readiness = readiness.label(),
        prompt_marker_detected = snapshot.prompt_marker_detected,
        prompt_draft_detected = snapshot.prompt_draft_detected,
        tmux_pane_alive = snapshot.tmux_pane_alive,
        capture_available = snapshot.capture_available,
        pane_tail = %snapshot.pane_tail,
        "claude_tui cold-boot stranded on MCP server authentication welcome screen; failing fast with an actionable reason instead of blind-waiting the readiness timeout"
    );
    crate::services::claude::debug_log_to(
        "claude_tui.log",
        &format!(
            "prompt readiness mcp-auth block session={} readiness={} tmux_pane_alive={} capture_available={} pane_tail:\n{}",
            session_name,
            readiness.label(),
            snapshot.tmux_pane_alive,
            snapshot.capture_available,
            snapshot.pane_tail
        ),
    );
}

pub(super) fn log_prompt_submit_left_draft(session_name: &str, snapshot: &PromptReadinessSnapshot) {
    tracing::warn!(
        tmux_session_name = session_name,
        prompt_marker_detected = snapshot.prompt_marker_detected,
        prompt_draft_detected = snapshot.prompt_draft_detected,
        tmux_pane_alive = snapshot.tmux_pane_alive,
        capture_available = snapshot.capture_available,
        pane_tail = %snapshot.pane_tail,
        "claude_tui prompt submit still has a draft after Enter retries"
    );
    crate::services::claude::debug_log_to(
        "claude_tui.log",
        &format!(
            "prompt submit left draft session={} prompt_marker_detected={} prompt_draft_detected={} tmux_pane_alive={} capture_available={} pane_tail:\n{}",
            session_name,
            snapshot.prompt_marker_detected,
            snapshot.prompt_draft_detected,
            snapshot.tmux_pane_alive,
            snapshot.capture_available,
            snapshot.pane_tail
        ),
    );
}

pub(super) fn log_prompt_submit_capture_unavailable(
    session_name: &str,
    attempt: usize,
    snapshot: &PromptReadinessSnapshot,
) {
    tracing::warn!(
        tmux_session_name = session_name,
        attempt,
        tmux_pane_alive = snapshot.tmux_pane_alive,
        capture_available = snapshot.capture_available,
        "claude_tui post-submit capture unavailable; cannot confirm Enter took effect"
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
    lines.join("\n").trim().to_string()
}
