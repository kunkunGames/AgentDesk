use anyhow::{Result, anyhow};
use poise::serenity_prelude::ChannelId;
use serde::Serialize;
use sqlx::{PgPool, Row};
use std::sync::Arc;

use crate::services::discord::health::{HealthRegistry, clear_provider_channel_runtime};
use crate::services::provider::ProviderKind;
use crate::services::turn_lifecycle::{TurnLifecycleTarget, force_kill_turn};

use super::store::RoutineRecord;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutineSessionCommand {
    Reset,
    Kill,
}

impl RoutineSessionCommand {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reset => "reset",
            Self::Kill => "kill",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RoutineSessionControlResult {
    pub action: &'static str,
    pub routine_id: String,
    pub agent_id: String,
    pub provider: String,
    pub channel_id: String,
    pub session_key: Option<String>,
    pub tmux_session: String,
    pub provider_clear_behavior: &'static str,
    pub runtime_cleared: bool,
    pub tmux_killed: bool,
    pub inflight_cleared: bool,
    pub lifecycle_path: &'static str,
    pub queued_remaining: Option<usize>,
    pub queue_preserved: bool,
    pub disconnected_sessions: u64,
}

#[derive(Clone)]
pub struct RoutineSessionController {
    pool: Arc<PgPool>,
    health_registry: Option<Arc<HealthRegistry>>,
}

#[derive(Debug, Clone)]
struct RoutineSessionTarget {
    agent_id: String,
    provider: ProviderKind,
    channel_id: ChannelId,
    session_key: Option<String>,
    tmux_session: String,
}

#[derive(Debug, Clone)]
struct RoutineSessionRow {
    session_key: Option<String>,
}

impl RoutineSessionController {
    pub fn new(pool: Arc<PgPool>, health_registry: Option<Arc<HealthRegistry>>) -> Self {
        Self {
            pool,
            health_registry,
        }
    }

    pub async fn control_persistent_session(
        &self,
        routine: &RoutineRecord,
        command: RoutineSessionCommand,
        reason: &str,
    ) -> Result<RoutineSessionControlResult> {
        ensure_persistent_routine(routine)?;
        let target = self.resolve_target(routine).await?;
        let provider_clear_behavior = provider_clear_behavior(&target.provider);

        let mut runtime_cleared = false;
        let mut tmux_killed = false;
        let mut inflight_cleared = false;
        let mut lifecycle_path = "registry-unavailable";
        let mut queued_remaining = None;
        let mut queue_preserved = true;
        let mut disconnected_sessions = 0;

        match command {
            RoutineSessionCommand::Reset => {
                if let Some(registry) = self.health_registry.as_deref() {
                    runtime_cleared = clear_provider_channel_runtime(
                        registry,
                        target.provider.as_str(),
                        target.channel_id,
                        target.session_key.as_deref(),
                    )
                    .await;
                    lifecycle_path = if runtime_cleared {
                        "runtime-clear"
                    } else {
                        "runtime-clear-unavailable"
                    };
                }
            }
            RoutineSessionCommand::Kill => {
                let lifecycle = force_kill_turn(
                    self.health_registry.as_deref(),
                    &TurnLifecycleTarget {
                        provider: Some(target.provider.clone()),
                        channel_id: Some(target.channel_id),
                        tmux_name: target.tmux_session.clone(),
                    },
                    reason,
                    "routine_session_kill",
                )
                .await;
                tmux_killed = lifecycle.tmux_killed;
                inflight_cleared = lifecycle.inflight_cleared;
                lifecycle_path = lifecycle.lifecycle_path;
                queued_remaining = lifecycle.queue_depth;
                queue_preserved = lifecycle.queue_preserved;
                disconnected_sessions = self.disconnect_matching_sessions(&target).await?;
            }
        }

        Ok(RoutineSessionControlResult {
            action: command.as_str(),
            routine_id: routine.id.clone(),
            agent_id: target.agent_id,
            provider: target.provider.as_str().to_string(),
            channel_id: target.channel_id.get().to_string(),
            session_key: target.session_key,
            tmux_session: target.tmux_session,
            provider_clear_behavior,
            runtime_cleared,
            tmux_killed,
            inflight_cleared,
            lifecycle_path,
            queued_remaining,
            queue_preserved,
            disconnected_sessions,
        })
    }

    async fn resolve_target(&self, routine: &RoutineRecord) -> Result<RoutineSessionTarget> {
        let agent_id = routine
            .agent_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "persistent routine {} is not attached to an agent",
                    routine.id
                )
            })?
            .to_string();

        let bindings = crate::db::agents::load_agent_channel_bindings_pg(&self.pool, &agent_id)
            .await
            .map_err(|error| {
                anyhow!("load agent bindings for routine session {agent_id}: {error}")
            })?
            .ok_or_else(|| anyhow!("agent {agent_id} not found for routine session control"))?;
        let provider = bindings
            .resolved_primary_provider_kind()
            .ok_or_else(|| anyhow!("agent {agent_id} primary provider is not configured"))?;
        let primary_channel = bindings
            .primary_channel()
            .ok_or_else(|| anyhow!("agent {agent_id} primary channel is not configured"))?;
        let channel_id =
            crate::server::routes::dispatches::resolve_channel_alias_pub(&primary_channel)
                .or_else(|| primary_channel.parse::<u64>().ok())
                .ok_or_else(|| {
                    anyhow!("agent {agent_id} primary channel is invalid: {primary_channel}")
                })?;
        let channel = ChannelId::new(channel_id);

        let session = self
            .load_latest_session(&agent_id, &provider, channel_id)
            .await?;
        let session_key = session.and_then(|row| row.session_key);
        let tmux_session = session_key
            .as_deref()
            .and_then(tmux_name_from_session_key)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| provider.build_tmux_session_name(&primary_channel));

        Ok(RoutineSessionTarget {
            agent_id,
            provider,
            channel_id: channel,
            session_key,
            tmux_session,
        })
    }

    async fn load_latest_session(
        &self,
        agent_id: &str,
        provider: &ProviderKind,
        channel_id: u64,
    ) -> Result<Option<RoutineSessionRow>> {
        let row = sqlx::query(
            r#"
            SELECT session_key
            FROM sessions
            WHERE agent_id = $1
              AND LOWER(COALESCE(provider, '')) = LOWER($2)
              AND status IN ('working', 'idle', 'connected')
              AND (thread_channel_id = $3 OR thread_channel_id IS NULL)
            ORDER BY
              CASE WHEN thread_channel_id = $3 THEN 0 ELSE 1 END,
              CASE status WHEN 'working' THEN 0 WHEN 'idle' THEN 1 WHEN 'connected' THEN 2 ELSE 3 END,
              last_heartbeat DESC NULLS LAST,
              created_at DESC
            LIMIT 1
            "#,
        )
        .bind(agent_id)
        .bind(provider.as_str())
        .bind(channel_id.to_string())
        .fetch_optional(&*self.pool)
        .await
        .map_err(|error| anyhow!("load routine session for {agent_id}: {error}"))?;

        row.map(|row| {
            Ok(RoutineSessionRow {
                session_key: row
                    .try_get("session_key")
                    .map_err(|error| anyhow!("decode routine session_key: {error}"))?,
            })
        })
        .transpose()
    }

    async fn disconnect_matching_sessions(&self, target: &RoutineSessionTarget) -> Result<u64> {
        let session_key = target.session_key.as_deref();
        let result = sqlx::query(
            r#"
            UPDATE sessions
            SET status = 'disconnected',
                active_dispatch_id = NULL,
                claude_session_id = NULL,
                raw_provider_session_id = NULL
            WHERE agent_id = $1
              AND LOWER(COALESCE(provider, '')) = LOWER($2)
              AND status <> 'disconnected'
              AND (
                ($3::text IS NOT NULL AND thread_channel_id = $3)
                OR ($4::text IS NOT NULL AND session_key = $4)
              )
            "#,
        )
        .bind(&target.agent_id)
        .bind(target.provider.as_str())
        .bind(target.channel_id.get().to_string())
        .bind(session_key)
        .execute(&*self.pool)
        .await
        .map_err(|error| {
            anyhow!(
                "disconnect routine sessions for {}: {error}",
                target.agent_id
            )
        })?;

        Ok(result.rows_affected())
    }
}

fn ensure_persistent_routine(routine: &RoutineRecord) -> Result<()> {
    if routine.execution_strategy == "persistent" {
        Ok(())
    } else {
        Err(anyhow!(
            "routine {} requires execution_strategy=persistent for session control",
            routine.id
        ))
    }
}

fn tmux_name_from_session_key(session_key: &str) -> Option<&str> {
    session_key
        .split_once(':')
        .map(|(_, tmux_name)| tmux_name)
        .filter(|value| !value.trim().is_empty())
}

pub fn provider_clear_behavior(provider: &ProviderKind) -> &'static str {
    if *provider == ProviderKind::Claude {
        "runtime clear plus /clear in the existing Claude tmux session"
    } else if provider.uses_managed_tmux_backend() {
        "runtime clear plus managed process session reset for the provider tmux session"
    } else {
        "runtime mailbox clear only; provider has no managed tmux reset hook"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tmux_name_from_session_key_uses_suffix_after_host() {
        assert_eq!(
            tmux_name_from_session_key("host:AgentDesk-codex-sandbox-cdx"),
            Some("AgentDesk-codex-sandbox-cdx")
        );
        assert_eq!(tmux_name_from_session_key("missing-separator"), None);
        assert_eq!(tmux_name_from_session_key("host:"), None);
    }

    #[test]
    fn provider_clear_behavior_documents_supported_reset_paths() {
        assert!(provider_clear_behavior(&ProviderKind::Claude).contains("/clear"));
        assert!(provider_clear_behavior(&ProviderKind::Codex).contains("managed process"));
        assert!(provider_clear_behavior(&ProviderKind::Gemini).contains("mailbox clear only"));
    }
}
