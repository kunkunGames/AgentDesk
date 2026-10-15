//! Failure classification is deterministic; unknown effects never auto-retry.
use crate::db::campaigns::graph::digest;
use crate::db::campaigns::graph::receipt::{
    MAX_COMMAND_BYTES, MAX_EVIDENCE_BYTES, MAX_OBSERVATIONS,
};
use serde_json::{Value, json};

pub fn failure(status: &str, result: &Value) -> (&'static str, Value) {
    let receipt = &result["campaign_receipt"];
    let category = match receipt["failure_category"].as_str() {
        Some("code") => "code",
        Some("plan") => "plan",
        Some("infrastructure") => "infrastructure",
        _ => "unknown",
    };
    let summary = receipt["summary"]
        .as_str()
        .unwrap_or("Terminal dispatch has no accepted business evidence");
    // Keep only bounded outcome evidence, never arbitrary provider transcripts
    // or configuration. Detailed logs stay with their existing redaction owner.
    let summary = redact(summary);
    let fingerprint = digest(format!("{category}:{status}:{summary}").as_bytes());
    (
        category,
        json!({"status":status,"category":category,"summary":summary,"fingerprint":fingerprint}),
    )
}
pub(super) fn redact(value: &str) -> String {
    let re = regex::Regex::new(
        r"(?i)(bearer\s+|(?:token|password|api[_-]?key|secret)\s*[:=]\s*)[^\s,;]+",
    )
    .unwrap();
    re.replace_all(&value.chars().take(2000).collect::<String>(), "[redacted]")
        .into_owned()
}
pub fn receipt(result: &Value) -> bool {
    let r = &result["campaign_receipt"];
    matches!(r["outcome"].as_str(), Some("passed" | "no_work"))
        && r["summary"].as_str().is_some_and(|s| !s.trim().is_empty())
        && r["observations"].as_array().is_some_and(|a| {
            !a.is_empty()
                && a.len() <= MAX_OBSERVATIONS
                && a.iter().all(|o| {
                    o["command"]
                        .as_str()
                        .is_some_and(|s| !s.trim().is_empty() && s.len() <= MAX_COMMAND_BYTES)
                        && o["exit_code"].as_i64() == Some(0)
                        && o["evidence"]
                            .as_str()
                            .is_some_and(|s| !s.trim().is_empty() && s.len() <= MAX_EVIDENCE_BYTES)
                })
        })
}
/// Exact repetitions are meaningful only when the provider emits new events.
/// Re-reading a quiet dispatch row is not a new sample and cannot prove a stall.
pub fn repeated(samples: &[String], threshold: usize) -> bool {
    threshold >= 3
        && samples.len() >= threshold
        && !samples.last().unwrap().is_empty()
        && samples[samples.len() - threshold..]
            .windows(2)
            .all(|w| w[0] == w[1])
}
