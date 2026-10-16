//! Backend evidence and identity-fenced stale-session maintenance.
//!
//! Native exits belong to the existing turn finalizer. This facade only settles
//! a proven-dead tmux owner, using the canonical dispatch transaction.

use crate::services::agent_protocol::RuntimeHandoffKind;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

fn runtime_kind(session_key: &str) -> Option<RuntimeHandoffKind> {
    use crate::services::session_backend::process_session_evidence;
    let name = crate::services::discord::session_identity::tmux_name_from_session_key(session_key)?;
    runtime_kind_for_name(&name, process_session_evidence(&name).registration)
}

fn runtime_kind_for_name(
    name: &str,
    registration: crate::services::session_backend::NativeRegistryPresence,
) -> Option<RuntimeHandoffKind> {
    use crate::services::session_backend::NativeRegistryPresence;
    // A native handle overrides an obsolete hosting marker. Registry contention
    // is unobserved ownership, not permission to fall back to an old tmux mode.
    match registration {
        NativeRegistryPresence::Registered => return Some(RuntimeHandoffKind::ProcessBackend),
        NativeRegistryPresence::Unknown => return None,
        NativeRegistryPresence::Absent => {}
    }
    #[cfg(unix)]
    {
        crate::services::tmux_common::resolve_tmux_runtime_kind_marker(name)
    }
    #[cfg(not(unix))]
    {
        let _ = name;
        None
    }
}

pub(super) fn runtime_evidence(session_key: &str) -> Value {
    use crate::services::session_backend::{NativeRegistryPresence, process_session_evidence};
    let Some(name) =
        crate::services::discord::session_identity::tmux_name_from_session_key(session_key)
    else {
        return json!({"runtime_kind":null,"liveness":"unknown","reason":"session_identity_unresolved"});
    };
    let evidence = process_session_evidence(&name);
    let kind = runtime_kind_for_name(&name, evidence.registration);
    let native = matches!(kind, Some(RuntimeHandoffKind::ProcessBackend));
    let liveness = if native || evidence.registration == NativeRegistryPresence::Unknown {
        serde_json::to_value(evidence.liveness).unwrap_or(Value::Null)
    } else {
        json!({"state":"unknown", "reason": if kind.is_some() {"tmux_probe_required"} else {"runtime_kind_unavailable"}})
    };
    json!({"runtime_kind":kind, "liveness":liveness["state"], "reason":liveness["reason"]})
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct RuntimeEvidence {
    runtime_kind: Option<RuntimeHandoffKind>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct SessionSnapshot {
    session_key: String,
    status: String,
    provider: Option<String>,
    channel_id: Option<String>,
    instance_id: Option<String>,
    turn_generation: i64,
    #[serde(default)]
    active_turn_nonce: Option<String>,
    last_heartbeat: DateTime<Utc>,
    active_dispatch_id: Option<String>,
    active_dispatch_status: Option<String>,
    runtime_evidence: RuntimeEvidence,
}

#[derive(Deserialize)]
struct RecoveryRequest {
    snapshot: SessionSnapshot,
    #[serde(default)]
    clear_active_dispatch_id: bool,
}

#[cfg(all(test, unix))]
mod tests;

fn preserved(reason: &str) -> Value {
    json!({"ok":true,"applied":false,"reason":reason})
}

pub(super) fn recover_stale_session_raw(pool: Option<&PgPool>, payload: &str) -> String {
    let request: RecoveryRequest = match serde_json::from_str(payload) {
        Ok(request) => request,
        Err(error) => {
            return json!({"error":format!("invalid stale session snapshot: {error}")}).to_string();
        }
    };
    if request.snapshot.session_key.trim().is_empty()
        || !matches!(request.snapshot.status.as_str(), "turn_active" | "working")
        || request.snapshot.turn_generation < 0
    {
        return preserved("invalid_snapshot").to_string();
    }
    let Some(pool) = pool else {
        return super::unavailable();
    };
    #[cfg(unix)]
    {
        let Some(name) = crate::services::discord::session_identity::tmux_name_from_session_key(
            &request.snapshot.session_key,
        ) else {
            return preserved("session_identity_unresolved").to_string();
        };
        crate::services::tmux_common::with_tmux_source_authority(&name, |_| {
            use crate::services::platform::tmux::{PaneLiveness, pane_liveness};
            if !matches!(
                runtime_kind(&request.snapshot.session_key),
                Some(
                    RuntimeHandoffKind::LegacyTmuxWrapper
                        | RuntimeHandoffKind::ClaudeTui
                        | RuntimeHandoffKind::CodexTui
                )
            ) {
                return preserved("native_or_unknown_runtime_preserved").to_string();
            }
            if runtime_kind(&request.snapshot.session_key)
                != request.snapshot.runtime_evidence.runtime_kind
            {
                return preserved("runtime_identity_changed").to_string();
            }
            match pane_liveness(&name) {
                PaneLiveness::Live => return preserved("tmux_owner_live").to_string(),
                PaneLiveness::ProbeError => return preserved("tmux_liveness_unknown").to_string(),
                PaneLiveness::DeadOrAbsent => {}
            }
            match crate::utils::async_bridge::block_on_pg_result(
                pool,
                move |bridge_pool| async move {
                    recover_on_pg(&bridge_pool, request)
                        .await
                        .map(|value| value.to_string())
                },
                |error| json!({"error":error}).to_string(),
            ) {
                Ok(result) => result,
                Err(error) => crate::engine::ops::ensure_js_error_json(error),
            }
        })
    }
    #[cfg(not(unix))]
    {
        let _ = (pool, request);
        preserved("runtime_probe_unsupported").to_string()
    }
}

async fn recover_on_pg(pool: &PgPool, request: RecoveryRequest) -> Result<Value, String> {
    let expected = request.snapshot;
    if super::host_repair::resolve_policy_target_pg(pool, &expected.session_key)
        .await
        .is_err()
    {
        return Ok(preserved("host_identity_unresolved"));
    }
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| format!("begin stale session recovery: {error}"))?;
    sqlx::query("SET LOCAL lock_timeout = '1s'")
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("bound stale session locks: {error}"))?;
    // Match canonical settlement's dispatch-before-session write ordering.
    let current_dispatch_status = if let Some(id) = expected.active_dispatch_id.as_deref() {
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM task_dispatches WHERE id = $1 FOR UPDATE",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| format!("lock stale dispatch: {error}"))?
    } else {
        None
    };
    let current = sqlx::query(
        "SELECT session_key, status, provider, channel_id, instance_id,
                idle_recap_turn_generation AS turn_generation, active_turn_nonce, hosted_execution IS NULL AS legacy_host, last_heartbeat, active_dispatch_id,
                last_heartbeat < NOW() - INTERVAL '10 minutes' AS heartbeat_stale
         FROM sessions WHERE session_key = $1 FOR UPDATE",
    )
    .bind(&expected.session_key)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|error| format!("lock stale session: {error}"))?;
    let Some(row) = current else {
        return Ok(preserved("session_missing"));
    };
    if !row.try_get::<bool, _>("legacy_host").unwrap_or(false) {
        return Ok(preserved("host_identity_changed"));
    }
    let heartbeat_stale = row
        .try_get::<Option<bool>, _>("heartbeat_stale")
        .map_err(|error| error.to_string())?
        .unwrap_or(false);
    let current = SessionSnapshot {
        session_key: row
            .try_get("session_key")
            .map_err(|error| error.to_string())?,
        status: row.try_get("status").map_err(|error| error.to_string())?,
        provider: row.try_get("provider").map_err(|error| error.to_string())?,
        channel_id: row
            .try_get("channel_id")
            .map_err(|error| error.to_string())?,
        instance_id: row
            .try_get("instance_id")
            .map_err(|error| error.to_string())?,
        turn_generation: row
            .try_get("turn_generation")
            .map_err(|error| error.to_string())?,
        active_turn_nonce: row
            .try_get("active_turn_nonce")
            .map_err(|error| error.to_string())?,
        last_heartbeat: row
            .try_get("last_heartbeat")
            .map_err(|error| error.to_string())?,
        active_dispatch_id: row
            .try_get("active_dispatch_id")
            .map_err(|error| error.to_string())?,
        active_dispatch_status: current_dispatch_status,
        runtime_evidence: RuntimeEvidence {
            runtime_kind: runtime_kind(&expected.session_key),
        },
    };
    if current != expected {
        return Ok(preserved("session_identity_changed"));
    }
    if !heartbeat_stale {
        return Ok(preserved("session_heartbeat_fresh"));
    }
    // Native/unknown identities cannot enter the terminal writer even if a
    // caller passes a tmux-looking stale snapshot after a runtime replacement.
    if !matches!(
        runtime_kind(&expected.session_key),
        Some(
            RuntimeHandoffKind::LegacyTmuxWrapper
                | RuntimeHandoffKind::ClaudeTui
                | RuntimeHandoffKind::CodexTui
        )
    ) {
        return Ok(preserved("runtime_identity_changed"));
    }
    let name = crate::services::discord::session_identity::tmux_name_from_session_key(
        &expected.session_key,
    )
    .ok_or_else(|| "session identity unresolved".to_string())?;
    let pane =
        tokio::task::spawn_blocking(move || crate::services::platform::tmux::pane_liveness(&name))
            .await
            .map_err(|error| format!("recheck stale tmux owner: {error}"))?;
    match pane {
        crate::services::platform::tmux::PaneLiveness::Live => {
            return Ok(preserved("tmux_owner_live"));
        }
        crate::services::platform::tmux::PaneLiveness::ProbeError => {
            return Ok(preserved("tmux_liveness_unknown"));
        }
        crate::services::platform::tmux::PaneLiveness::DeadOrAbsent => {}
    }
    let mut deferred = Vec::new();
    if let Some(id) = expected.active_dispatch_id.as_deref() {
        if matches!(
            expected.active_dispatch_status.as_deref(),
            Some("pending" | "dispatched")
        ) {
            let result = json!({"reason":"stale_tmux_owner_proven_dead", "recovery_source":"timeout_backend_evidence"});
            let changed = crate::dispatch::set_dispatch_status_on_pg_tx_async(
                &mut tx,
                id,
                "failed",
                Some(&result),
                "timeout_stale_session_recovery",
                Some(&["pending", "dispatched"]),
                true,
                false,
                Some(&mut deferred),
            )
            .await
            .map_err(|error| format!("canonical stale dispatch settlement: {error}"))?;
            if changed == 0 {
                return Ok(preserved("dispatch_settlement_not_applied"));
            }
        } else if !matches!(
            expected.active_dispatch_status.as_deref(),
            Some("completed" | "failed" | "cancelled")
        ) {
            return Ok(preserved("dispatch_status_unknown"));
        }
    }
    // Canonical settlement may already clear active_dispatch_id; the locked
    // row and generation fence prevent a new turn from being idled here.
    sqlx::query(
        "UPDATE sessions SET status='idle',
        active_dispatch_id=CASE WHEN $2 THEN NULL ELSE active_dispatch_id END,
        last_heartbeat=NOW() WHERE session_key=$1",
    )
    .bind(&expected.session_key)
    .bind(request.clear_active_dispatch_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("idle settled stale session: {error}"))?;
    tx.commit()
        .await
        .map_err(|error| format!("commit stale session recovery: {error}"))?;
    for event in deferred {
        event.emit();
    }
    if let Some(id) = expected.active_dispatch_id {
        crate::services::dispatches::wait_queue::spawn_cached_constraint_release_wake(
            pool.clone(),
            "constraint_release",
            id,
            "dispatch_terminal_status",
        );
    }
    Ok(json!({"ok":true,"applied":true,"reason":"tmux_owner_proven_dead"}))
}
