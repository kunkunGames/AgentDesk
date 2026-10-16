//! Durable attempt admission and tool reservations on the existing lease row.

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionLimits {
    pub max_elapsed_secs: u64,
    pub max_tool_calls: u32,
}

impl ExecutionLimits {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if !(1..=604_800).contains(&self.max_elapsed_secs)
            || !(1..=10_000).contains(&self.max_tool_calls)
        {
            return Err(
                "execution limits require 1..604800 seconds and 1..10000 tool calls".into(),
            );
        }
        Ok(())
    }
}

pub(super) struct LeaseAdmission {
    pub resource_key: String,
    pub limits: Option<ExecutionLimits>,
}

pub(crate) struct ManagedAttempt {
    pool: PgPool,
    runtime: tokio::runtime::Handle,
    node: String,
    provider: String,
    channel: String,
    nonce: uuid::Uuid,
    deadline: Instant,
    finished: AtomicBool,
}

pub(super) struct LeaseIdentity {
    pub node: String,
    pub provider: String,
    pub channel: String,
    pub nonce: uuid::Uuid,
}

impl ManagedAttempt {
    pub(super) fn new(pool: PgPool, identity: LeaseIdentity, deadline: Instant) -> Self {
        Self {
            pool,
            runtime: tokio::runtime::Handle::current(),
            node: identity.node,
            provider: identity.provider,
            channel: identity.channel,
            nonce: identity.nonce,
            deadline,
            finished: AtomicBool::new(false),
        }
    }

    pub(crate) fn is_expired(&self) -> bool {
        !self.finished.load(Ordering::Acquire) && Instant::now() >= self.deadline
    }

    pub(crate) fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Commit admission before the network send. Failure/timeout after this
    /// point is unknown, even if the peer acknowledges a best-effort abort.
    pub(crate) fn admit_prompt(&self, session: &str) -> Result<(), String> {
        self.blocking(async {
            let changed = sqlx::query("UPDATE node_execution_leases SET execution_state='running', provider_session_id=$5 WHERE instance_id=$1 AND provider=$2 AND channel_id=$3 AND nonce=$4 AND managed AND execution_state='reserved' AND runtime_identity IS NOT NULL AND deadline_at>clock_timestamp() AND expires_at>clock_timestamp()")
                .bind(&self.node).bind(&self.provider).bind(&self.channel).bind(self.nonce).bind(session)
                .execute(&self.pool).await?.rows_affected();
            Ok(changed == 1)
        }, "prompt admission")
    }

    pub(crate) fn bind_runtime(
        &self,
        identity: &super::recovery::RuntimeIdentity,
    ) -> Result<(), String> {
        identity.validate()?;
        let identity = serde_json::to_value(identity).map_err(|error| error.to_string())?;
        self.blocking(async {
            let changed = sqlx::query("UPDATE node_execution_leases SET runtime_identity=$5 WHERE instance_id=$1 AND provider=$2 AND channel_id=$3 AND nonce=$4 AND managed AND execution_state='reserved' AND (runtime_identity IS NULL OR runtime_identity=$5) AND deadline_at>clock_timestamp() AND expires_at>clock_timestamp()")
                .bind(&self.node).bind(&self.provider).bind(&self.channel).bind(self.nonce).bind(identity)
                .execute(&self.pool).await?.rows_affected();
            Ok(changed == 1)
        }, "runtime identity binding")
    }

    /// Reserve once before replying `once` to the native permission gate.
    /// Duplicate delivery of an ID consumes no additional budget. A lost reply
    /// retains its reservation; neither retry nor restart resets this array.
    pub(crate) fn admit_permission(
        &self,
        session: &str,
        permission_id: &str,
    ) -> Result<(), String> {
        if permission_id.is_empty() || permission_id.len() > 256 {
            return Err("invalid managed native permission identity".into());
        }
        self.blocking(async {
            let changed = sqlx::query("UPDATE node_execution_leases SET permission_ids=CASE WHEN $6=ANY(permission_ids) THEN permission_ids ELSE array_append(permission_ids,$6) END WHERE instance_id=$1 AND provider=$2 AND channel_id=$3 AND nonce=$4 AND provider_session_id=$5 AND managed AND execution_state='running' AND deadline_at>clock_timestamp() AND expires_at>clock_timestamp() AND ($6=ANY(permission_ids) OR cardinality(permission_ids)<max_tool_calls)")
                .bind(&self.node).bind(&self.provider).bind(&self.channel).bind(self.nonce).bind(session).bind(permission_id)
                .execute(&self.pool).await?.rows_affected();
            Ok(changed == 1)
        }, "tool admission")
    }

    /// Called only after the native adapter observes its matching terminal and
    /// verifies no live native tools remain in the supported process-free tool set.
    pub(crate) fn record_quiesced(&self, session: &str) -> Result<(), String> {
        self.blocking(async {
            let changed = sqlx::query("UPDATE node_execution_leases SET execution_state='quiesced' WHERE instance_id=$1 AND provider=$2 AND channel_id=$3 AND nonce=$4 AND provider_session_id=$5 AND managed AND execution_state='running' AND deadline_at>clock_timestamp() AND expires_at>clock_timestamp()")
                .bind(&self.node).bind(&self.provider).bind(&self.channel).bind(self.nonce).bind(session)
                .execute(&self.pool).await?.rows_affected();
            Ok(changed == 1)
        }, "quiescence observation")?;
        self.finished.store(true, Ordering::Release);
        Ok(())
    }

    fn blocking<F>(&self, future: F, action: &str) -> Result<(), String>
    where
        F: std::future::Future<Output = Result<bool, sqlx::Error>>,
    {
        if self.is_expired() {
            return Err(format!(
                "managed execution deadline exceeded before {action}"
            ));
        }
        self.runtime.block_on(async {
            match tokio::time::timeout(super::LEASE_IO_TIMEOUT.min(self.remaining()), future).await {
                Ok(Ok(true)) => Ok(()),
                Ok(Ok(false)) => Err(format!("managed execution {action} rejected: budget, lease or attempt no longer admits it")),
                Ok(Err(error)) => Err(format!("managed execution {action}: {error}")),
                Err(_) => Err(format!("managed execution {action} timed out; admission is unknown")),
            }
        })
    }

    pub(super) async fn finish(&self) -> Result<(), sqlx::Error> {
        // This write is diagnostic. Even if it fails, the durable running row
        // already blocks replacement and is protected against TTL deletion.
        sqlx::query("UPDATE node_execution_leases SET execution_state='unknown', stop_reason='provider returned without verified quiescence' WHERE instance_id=$1 AND provider=$2 AND channel_id=$3 AND nonce=$4 AND managed AND execution_state='running'")
            .bind(&self.node).bind(&self.provider).bind(&self.channel).bind(self.nonce)
            .execute(&self.pool).await?;
        sqlx::query("DELETE FROM node_execution_leases WHERE instance_id=$1 AND provider=$2 AND channel_id=$3 AND nonce=$4 AND managed AND execution_state IN ('reserved','quiesced')")
            .bind(&self.node).bind(&self.provider).bind(&self.channel).bind(self.nonce)
            .execute(&self.pool).await?;
        Ok(())
    }
}
