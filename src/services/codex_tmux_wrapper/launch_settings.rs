//! Codex wrapper resume and first-event admission settings.

use super::DEFAULT_CODEX_FIRST_EVENT_TIMEOUT_SECS;

pub(super) fn normalize_resume_session_id(resume_session_id: Option<&str>) -> Option<String> {
    resume_session_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(super) fn codex_first_event_timeout() -> std::time::Duration {
    let seconds = std::env::var("AGENTDESK_CODEX_FIRST_EVENT_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(DEFAULT_CODEX_FIRST_EVENT_TIMEOUT_SECS);
    std::time::Duration::from_secs(seconds)
}
