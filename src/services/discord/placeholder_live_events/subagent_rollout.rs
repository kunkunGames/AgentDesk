//! Subagent rollout consumer (#3086).
//!
//! Reconstructs the Claude TUI's `Done (N tool uses · M tokens · Xs)` summary
//! for a finished subagent. Two sources, both defensive (partial/malformed
//! lines, missing fields → graceful partial/empty summary, never a panic):
//!
//! 1. **In-stream fast path** — the parent transcript's Task `tool_result`
//!    record carries a top-level `toolUseResult` object with `totalToolUseCount`
//!    / `totalTokens` / `totalDurationMs` (and `agentId`). This is the exact
//!    same accounting the TUI renders and needs no disk IO.
//! 2. **Rollout parity path** — the per-subagent
//!    `<project>/<sessionId>/subagents/agent-<agentId>.jsonl` rollout. Used when
//!    the in-stream totals are absent. `tool_count` = number of `tool_use`
//!    blocks; `duration_secs` = last−first timestamp span; `tokens` = the LAST
//!    assistant message's full usage (input + cache_creation + cache_read +
//!    output), which mirrors `toolUseResult.totalTokens` (verified against real
//!    rollouts).

use std::path::{Path, PathBuf};

use chrono::DateTime;
use serde_json::Value;

use crate::services::agent_protocol::SubagentSummary;

/// Extracts the TUI summary from a parent-transcript `tool_result` record's
/// top-level `toolUseResult` object. Returns `(summary, agent_id)` when the
/// record looks like a finished subagent (i.e. it carries an `agentId` and/or
/// any of the `total*` accounting fields). Returns `None` for ordinary tool
/// results that have no subagent accounting.
pub(super) fn summary_from_tool_use_result(
    value: &Value,
) -> Option<(SubagentSummary, Option<String>)> {
    let result = value.get("toolUseResult")?;
    // `toolUseResult` may be a string (non-subagent tools) — only objects carry
    // the subagent accounting.
    let result = result.as_object()?;

    let agent_id = result
        .get("agentId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|id| !id.trim().is_empty());

    let tool_count = result.get("totalToolUseCount").and_then(as_u64_lenient);
    let tokens = result.get("totalTokens").and_then(as_u64_lenient);
    // Round partial seconds up so a sub-second subagent still reads `1s` rather
    // than `0s`; `0ms` stays `0s`.
    let duration_secs = result
        .get("totalDurationMs")
        .and_then(as_u64_lenient)
        .map(|ms| ms.div_ceil(1000));

    let summary = SubagentSummary {
        tool_count,
        tokens,
        duration_secs,
    };

    // Only treat this as a subagent completion when there is an agentId or at
    // least one accounting field — otherwise it is an ordinary tool result.
    if agent_id.is_none() && summary.is_empty() {
        return None;
    }
    Some((summary, agent_id))
}

/// Resolves the per-subagent rollout file for `agent_id` under the Claude
/// project directory derived from `cwd` + `session_id`, then computes the
/// summary from it. Returns an empty/partial summary on any IO or parse trouble
/// (never panics). `claude_home` is overridable for tests.
pub(super) fn summary_from_rollout_for(
    cwd: &Path,
    session_id: &str,
    agent_id: &str,
    claude_home: Option<&Path>,
) -> SubagentSummary {
    match rollout_path_for(cwd, session_id, agent_id, claude_home) {
        Some(path) => summary_from_rollout_file(&path),
        None => SubagentSummary::default(),
    }
}

/// Builds the candidate `subagents/agent-<id>.jsonl` path. Picks the first
/// project-dir candidate whose file exists, else the first candidate.
fn rollout_path_for(
    cwd: &Path,
    session_id: &str,
    agent_id: &str,
    claude_home: Option<&Path>,
) -> Option<PathBuf> {
    if session_id.trim().is_empty() || agent_id.trim().is_empty() {
        return None;
    }
    // Defensive: agent ids and session ids are filename components; reject any
    // value that could escape the subagents directory.
    if !is_safe_path_component(session_id) || !is_safe_path_component(agent_id) {
        return None;
    }
    let candidates =
        crate::services::claude_tui::transcript_tail::claude_project_dir_candidates_for_cwd(
            cwd,
            claude_home,
        )
        .ok()?;
    let rel = Path::new(session_id)
        .join("subagents")
        .join(format!("agent-{agent_id}.jsonl"));
    let paths: Vec<PathBuf> = candidates.into_iter().map(|dir| dir.join(&rel)).collect();
    paths
        .iter()
        .find(|path| path.exists())
        .cloned()
        .or_else(|| paths.into_iter().next())
}

fn is_safe_path_component(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
}

/// Parses a `subagents/agent-<id>.jsonl` rollout file into a [`SubagentSummary`].
/// Malformed/partial lines are skipped individually; a missing file yields an
/// empty summary. Never panics.
fn summary_from_rollout_file(path: &Path) -> SubagentSummary {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return SubagentSummary::default();
    };
    summary_from_rollout_str(&contents)
}

/// Core, IO-free rollout parser (testable directly).
pub(super) fn summary_from_rollout_str(contents: &str) -> SubagentSummary {
    let mut tool_count: u64 = 0;
    let mut last_usage_tokens: Option<u64> = None;
    let mut first_ts: Option<i64> = None;
    let mut last_ts: Option<i64> = None;
    let mut ts_count: usize = 0;
    let mut saw_any = false;

    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            // Skip a partial/malformed line (e.g. a trailing half-flushed write).
            continue;
        };
        saw_any = true;

        if let Some(ts) = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_timestamp_secs)
        {
            if first_ts.is_none() {
                first_ts = Some(ts);
            }
            last_ts = Some(ts);
            ts_count += 1;
        }

        if value.get("type").and_then(Value::as_str) == Some("assistant") {
            if let Some(message) = value.get("message") {
                if let Some(content) = message.get("content").and_then(Value::as_array) {
                    for block in content {
                        if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                            tool_count = tool_count.saturating_add(1);
                        }
                    }
                }
                if let Some(usage) = message.get("usage") {
                    let total = usage_total_tokens(usage);
                    if total > 0 {
                        // `totalTokens` mirrors the LAST assistant message's
                        // full usage (cumulative context), so keep overwriting.
                        last_usage_tokens = Some(total);
                    }
                }
            }
        }
    }

    if !saw_any {
        return SubagentSummary::default();
    }

    // A duration needs at least two timestamped lines to span; a lone timestamp
    // has no meaningful range.
    let duration_secs = match (first_ts, last_ts) {
        (Some(first), Some(last)) if ts_count >= 2 && last >= first => Some((last - first) as u64),
        _ => None,
    };

    SubagentSummary {
        tool_count: (tool_count > 0).then_some(tool_count),
        tokens: last_usage_tokens,
        duration_secs,
    }
}

fn usage_total_tokens(usage: &Value) -> u64 {
    [
        "input_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "output_tokens",
    ]
    .into_iter()
    .filter_map(|key| usage.get(key).and_then(as_u64_lenient))
    .fold(0u64, u64::saturating_add)
}

fn parse_timestamp_secs(raw: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(raw.trim())
        .ok()
        .map(|dt| dt.timestamp())
}

/// Reads a numeric JSON value as `u64`, tolerating integer, float, and
/// stringified-number encodings.
fn as_u64_lenient(value: &Value) -> Option<u64> {
    if let Some(n) = value.as_u64() {
        return Some(n);
    }
    if let Some(f) = value.as_f64() {
        if f.is_finite() && f >= 0.0 {
            return Some(f as u64);
        }
    }
    value.as_str().and_then(|s| s.trim().parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tool_use_result_summary_extracts_all_fields() {
        let value = json!({
            "type": "user",
            "toolUseResult": {
                "agentId": "a5e810f97737bf4bd",
                "totalToolUseCount": 38,
                "totalTokens": 90157,
                "totalDurationMs": 109275,
            }
        });
        let (summary, agent_id) = summary_from_tool_use_result(&value).expect("subagent result");
        assert_eq!(agent_id.as_deref(), Some("a5e810f97737bf4bd"));
        assert_eq!(summary.tool_count, Some(38));
        assert_eq!(summary.tokens, Some(90157));
        // 109275ms → 110s (ceil) — TUI rounds up partial seconds.
        assert_eq!(summary.duration_secs, Some(110));
    }

    #[test]
    fn tool_use_result_string_is_not_a_subagent() {
        let value = json!({ "type": "user", "toolUseResult": "ok" });
        assert!(summary_from_tool_use_result(&value).is_none());
    }

    #[test]
    fn tool_use_result_missing_is_none() {
        let value = json!({ "type": "user" });
        assert!(summary_from_tool_use_result(&value).is_none());
    }

    #[test]
    fn rollout_str_computes_tool_count_tokens_and_duration() {
        let contents = [
            json!({
                "type": "assistant",
                "timestamp": "2026-05-20T23:00:00.000Z",
                "message": {
                    "content": [{"type": "tool_use", "name": "Bash"}],
                    "usage": {"input_tokens": 5, "output_tokens": 100}
                }
            })
            .to_string(),
            json!({
                "type": "assistant",
                "timestamp": "2026-05-20T23:02:30.000Z",
                "message": {
                    "content": [
                        {"type": "tool_use", "name": "Read"},
                        {"type": "text", "text": "done"}
                    ],
                    "usage": {
                        "input_tokens": 10,
                        "cache_creation_input_tokens": 1000,
                        "cache_read_input_tokens": 2000,
                        "output_tokens": 200
                    }
                }
            })
            .to_string(),
        ]
        .join("\n");
        let summary = summary_from_rollout_str(&contents);
        assert_eq!(summary.tool_count, Some(2));
        // Last assistant message's full usage = 10+1000+2000+200 = 3210.
        assert_eq!(summary.tokens, Some(3210));
        // 23:02:30 − 23:00:00 = 150s.
        assert_eq!(summary.duration_secs, Some(150));
    }

    #[test]
    fn rollout_str_skips_malformed_lines_without_panic() {
        let contents = [
            "{ this is not json".to_string(),
            json!({
                "type": "assistant",
                "timestamp": "2026-05-20T23:00:00.000Z",
                "message": {"content": [{"type": "tool_use", "name": "Bash"}]}
            })
            .to_string(),
            "".to_string(),
            "   ".to_string(),
            "{\"partial\":".to_string(),
        ]
        .join("\n");
        let summary = summary_from_rollout_str(&contents);
        assert_eq!(summary.tool_count, Some(1));
        assert_eq!(summary.tokens, None);
        // Only one timestamped line → no span.
        assert_eq!(summary.duration_secs, None);
    }

    #[test]
    fn rollout_str_empty_is_empty_summary() {
        assert!(summary_from_rollout_str("").is_empty());
        assert!(summary_from_rollout_str("\n\n   \n").is_empty());
    }

    #[test]
    fn rejects_unsafe_path_components() {
        assert!(!is_safe_path_component("../escape"));
        assert!(!is_safe_path_component("with/slash"));
        assert!(is_safe_path_component("a5e810f97737bf4bd"));
        assert!(is_safe_path_component(
            "f525f356-9cf1-4c45-b992-4e1210ee68be"
        ));
    }
}
