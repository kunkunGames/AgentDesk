//! Cached Discord placeholder update timing.

use std::time::Duration;

/// Parse `var` as a `u64` seconds `Duration`, falling back to `default_secs`.
fn env_duration_secs(var: &str, default_secs: u64) -> Duration {
    let secs = (std::env::var(var).ok()).and_then(|s| s.parse::<u64>().ok());
    Duration::from_secs(secs.unwrap_or(default_secs))
}

/// Minimum interval between Discord placeholder progress edits (AGENTDESK_STATUS_INTERVAL_SECS, default 5s).
pub(in crate::services) fn status_update_interval() -> Duration {
    static CACHED: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| env_duration_secs("AGENTDESK_STATUS_INTERVAL_SECS", 5))
}
