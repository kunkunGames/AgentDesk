//! Reconcile retained execution observations through the existing node heartbeat.
//! Local process exit is not proof that a remote model request stopped. Keep the
//! original lease/budget until the provider's remaining effects are settled.
use super::*;
use serde::{Deserialize, Serialize};
use sqlx::Row;

mod local_process;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeIdentity {
    version: u8,
    boot_id: String,
    pid_namespace: u64,
    pid: u32,
    starttime: String,
    uid: u32,
}

impl RuntimeIdentity {
    pub(crate) fn capture(
        pid: u32,
        original: crate::services::process::ProcessIdentity,
    ) -> Result<Self, String> {
        local_process::capture(pid, original)
    }

    pub(super) fn validate(&self) -> Result<(), String> {
        if self.version != 1
            || self.pid == 0
            || self.pid > i32::MAX as u32
            || self.uid == 0
            || self.pid_namespace == 0
            || uuid::Uuid::parse_str(&self.boot_id).is_err()
            || !self.starttime.parse::<u128>().is_ok_and(|value| value > 0)
        {
            return Err("invalid managed runtime identity".into());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fixture() -> Self {
        // Database contract fixture only. It deliberately cannot match a real
        // boot/namespace and can never provide production stop evidence.
        Self {
            version: 1,
            boot_id: uuid::Uuid::nil().to_string(),
            pid_namespace: 1,
            pid: 42,
            starttime: "1".into(),
            uid: 65534,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StopObservation {
    pub(crate) local_writer_status: &'static str,
    inference_status: &'static str,
    reason: &'static str,
}

impl StopObservation {
    fn new(local_writer_status: &'static str, reason: &'static str) -> Self {
        Self {
            local_writer_status,
            inference_status: "unknown",
            reason,
        }
    }
}

pub(crate) async fn observe_retained(pool: &PgPool, node: &str) -> Result<u64, String> {
    let rows = sqlx::query("SELECT provider,channel_id,nonce,runtime_identity FROM node_execution_leases WHERE instance_id=$1 AND managed AND runtime_identity IS NOT NULL AND execution_state IN ('running','unknown') AND (execution_state='unknown' OR deadline_at<=clock_timestamp() OR expires_at<=clock_timestamp()) ORDER BY stop_observed_at ASC NULLS FIRST LIMIT 128")
        .bind(node).fetch_all(pool).await.map_err(|error| format!("retained runtime lookup: {error}"))?;
    if rows.is_empty() {
        return Ok(0);
    }
    let identities: Vec<Value> = rows.iter().map(|row| row.get("runtime_identity")).collect();
    let observations = tokio::task::spawn_blocking(move || {
        let scope = local_process::Snapshot::capture();
        identities
            .iter()
            .map(
                |value| match serde_json::from_value::<RuntimeIdentity>(value.clone()) {
                    Ok(identity) if identity.validate().is_ok() => scope.observe(&identity),
                    _ => StopObservation::new("unobservable", "invalid_persisted_identity"),
                },
            )
            .collect::<Vec<_>>()
    })
    .await
    .map_err(|error| format!("retained runtime observation: {error}"))?;
    let mut changed = 0;
    for (row, observation) in rows.into_iter().zip(observations) {
        let identity: Value = row.get("runtime_identity");
        // The nonce + unchanged runtime bind prevent a late observer from
        // updating a replacement attempt. Never delete/refund on PID evidence.
        changed += sqlx::query("UPDATE node_execution_leases SET execution_state='unknown',stop_observation=$5,stop_observed_at=clock_timestamp(),stop_reason=COALESCE(stop_reason,'runtime requires provider effect reconciliation') WHERE instance_id=$1 AND provider=$2 AND channel_id=$3 AND nonce=$4 AND runtime_identity=$6 AND execution_state IN ('running','unknown') AND (execution_state='unknown' OR deadline_at<=clock_timestamp() OR expires_at<=clock_timestamp())")
            .bind(node).bind(row.get::<String,_>("provider")).bind(row.get::<String,_>("channel_id"))
            .bind(row.get::<uuid::Uuid,_>("nonce")).bind(serde_json::to_value(observation).map_err(|error| error.to_string())?)
            .bind(identity).execute(pool).await.map_err(|error| format!("retained runtime observation commit: {error}"))?.rows_affected();
    }
    Ok(changed)
}
