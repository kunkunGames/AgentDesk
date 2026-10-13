//! Capture the result shared by native and typed terminal stream events.

use super::debug_log;

pub(super) fn capture_terminal_result(
    result: &str,
    session_id: &Option<String>,
    final_result: &mut Option<String>,
    last_session_id: &mut Option<String>,
) {
    let result_preview: String = result.chars().take(100).collect();
    debug_log(&format!(
        "  >>> Done: result_len={}, session_id={:?}, preview={:?}",
        result.len(),
        session_id,
        result_preview
    ));
    *final_result = Some(result.to_owned());
    if session_id.is_some() {
        *last_session_id = session_id.clone();
    }
}

/// SDK→bridge mpsc disconnect diagnostics (#1589 follow-up). Emits a single
/// structured WARN line at every `execute_command_streaming` exit path so the
/// operator can see *why* the producer task ended whenever the bridge
/// subsequently observes `TryRecvError::Disconnected`. Pair-tracking against
/// the bridge-side handoff log line lets us classify each disconnect as
/// cancel / IO error / CLI crash / synthetic-done / normal-done without
/// guessing.
pub(crate) fn log_producer_exit(
    kind: &'static str,
    session_id: Option<&str>,
    channel_id: Option<u64>,
    line_count: usize,
    extra: serde_json::Value,
) {
    let ts = chrono::Local::now().format("%H:%M:%S");
    tracing::warn!(
        "  [{ts}] 🔚 claude producer exit kind={} channel={:?} session={:?} lines={} extra={}",
        kind,
        channel_id,
        session_id,
        line_count,
        extra
    );
}
