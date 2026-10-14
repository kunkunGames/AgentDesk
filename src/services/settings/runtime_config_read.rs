//! Runtime defaults, explicit override metadata, and fail-closed internal-task admission reads.

use serde_json::{Map, Value, json};
use std::collections::HashSet;

use super::{
    RUNTIME_CONFIG_EXPLICIT_KEYS_META, RUNTIME_CONFIG_KEYS, runtime_config_yaml_overrides,
};

pub(super) fn runtime_config_defaults_map(config: &crate::config::Config) -> Map<String, Value> {
    let mut defaults = json!({
        "dispatchPollSec": 30,
        "agentSyncSec": 300,
        "githubIssueSyncSec": 900,
        "claudeRateLimitPollSec": 120,
        "codexRateLimitPollSec": 120,
        "issueTriagePollSec": 300,
        "ceoWarnDepth": 3,
        "maxRetries": 3,
        "maxEntryRetries": 3,
        "internalTasksEnabled": false,
        "campaignSuperiorAgentId": crate::db::campaigns::graph::learning::policy::DEFAULT_SUPERIOR_AGENT,
        "campaignDagTemplateMinSamples": crate::db::campaigns::graph::learning::policy::DEFAULT_MIN_SAMPLES,
        "staleDispatchedGraceMin": 2,
        "staleDispatchedTerminalStatuses": "cancelled,failed",
        "staleDispatchedRecoverNullDispatch": true,
        "staleDispatchedRecoverMissingDispatch": true,
        "reviewReminderMin": 30,
        "rateLimitWarningPct": 80,
        "rateLimitDangerPct": 95,
        "githubRepoCacheSec": 300,
        "rateLimitStaleSec": 600,
        "dispatchRateLimitGateEnabled": true,
        "dispatchRateLimitGateDangerPct": 100,
    })
    .as_object()
    .cloned()
    .unwrap_or_default();
    for (key, value) in runtime_config_yaml_overrides(config) {
        defaults.insert(key, value);
    }
    defaults
}

pub fn runtime_config_defaults(config: &crate::config::Config) -> Value {
    Value::Object(runtime_config_defaults_map(config))
}

/// Explicit opt-in for internal cards. A missing, malformed or non-boolean
/// setting never grants execution permission. Read at admission, not at boot.
pub(crate) async fn internal_tasks_enabled<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
) -> Result<bool, String> {
    let raw: Option<String> = sqlx::query_scalar(
        "SELECT value FROM kv_meta WHERE key='runtime-config' AND (expires_at IS NULL OR expires_at>NOW())",
    ).fetch_optional(executor).await.map_err(|error| format!("read internal task setting: {error}"))?;
    Ok(raw
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .and_then(|value| value.get("internalTasksEnabled").and_then(Value::as_bool))
        .unwrap_or(false))
}

pub(crate) fn explicit_runtime_config_keys(values: &Map<String, Value>) -> HashSet<String> {
    values
        .get(RUNTIME_CONFIG_EXPLICIT_KEYS_META)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|key| RUNTIME_CONFIG_KEYS.contains(key))
        .map(ToOwned::to_owned)
        .collect()
}
