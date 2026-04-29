use serde::Serialize;
use sqlx::PgPool;
use std::sync::Arc;

use super::runtime::RoutineRunOutcome;
use super::store::{ClaimedRoutineRun, RoutineRecord, RoutineStore};

#[derive(Clone)]
pub struct RoutineDiscordLogger {
    pool: Arc<PgPool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoutineDiscordLogStatus {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

impl RoutineDiscordLogger {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    pub async fn log_routine_event(
        &self,
        routine: &RoutineRecord,
        event: RoutineLifecycleEvent,
    ) -> RoutineDiscordLogStatus {
        self.log_to_routine_target(
            routine.agent_id.as_deref(),
            event.reason_code(),
            &format!("routine:{}:{}", routine.id, event.reason_code()),
            &routine_lifecycle_message(routine, event),
        )
        .await
    }

    pub async fn log_routine_event_by_id(
        &self,
        store: &RoutineStore,
        routine_id: &str,
        event: RoutineLifecycleEvent,
    ) -> RoutineDiscordLogStatus {
        match store.get_routine(routine_id).await {
            Ok(Some(routine)) => self.log_routine_event(&routine, event).await,
            Ok(None) => RoutineDiscordLogStatus::failed(format!(
                "routine {routine_id} not found while logging {}",
                event.reason_code()
            )),
            Err(error) => RoutineDiscordLogStatus::failed(format!(
                "failed to load routine {routine_id} for discord log: {error}"
            )),
        }
    }

    pub async fn log_run_started(
        &self,
        store: &RoutineStore,
        claimed: &ClaimedRoutineRun,
    ) -> RoutineDiscordLogStatus {
        let status = self
            .log_to_routine_target(
                claimed.agent_id.as_deref(),
                "routine_run_started",
                &format!(
                    "routine:{}:run:{}:started",
                    claimed.routine_id, claimed.run_id
                ),
                &run_started_message(claimed),
            )
            .await;
        self.persist_run_log_status(store, &claimed.run_id, &status)
            .await;
        status
    }

    pub async fn log_run_outcome(
        &self,
        store: &RoutineStore,
        outcome: &RoutineRunOutcome,
    ) -> RoutineDiscordLogStatus {
        let routine = match store.get_routine(&outcome.routine_id).await {
            Ok(value) => value,
            Err(error) => {
                let status = RoutineDiscordLogStatus::failed(format!(
                    "failed to load routine {} for discord log: {error}",
                    outcome.routine_id
                ));
                self.persist_run_log_status(store, &outcome.run_id, &status)
                    .await;
                return status;
            }
        };
        let Some(routine) = routine else {
            let status = RoutineDiscordLogStatus::failed(format!(
                "routine {} not found while logging run {}",
                outcome.routine_id, outcome.run_id
            ));
            self.persist_run_log_status(store, &outcome.run_id, &status)
                .await;
            return status;
        };

        let reason_code = format!("routine_run_{}", outcome.status);
        let status = self
            .log_to_routine_target(
                routine.agent_id.as_deref(),
                &reason_code,
                &format!(
                    "routine:{}:run:{}:{}",
                    outcome.routine_id, outcome.run_id, outcome.status
                ),
                &run_outcome_message(&routine, outcome),
            )
            .await;
        self.persist_run_log_status(store, &outcome.run_id, &status)
            .await;
        status
    }

    async fn log_to_routine_target(
        &self,
        agent_id: Option<&str>,
        reason_code: &str,
        session_key: &str,
        content: &str,
    ) -> RoutineDiscordLogStatus {
        let Some(agent_id) = normalized_agent_id(agent_id) else {
            return RoutineDiscordLogStatus::skipped();
        };

        let target = match resolve_agent_channel_target(&self.pool, agent_id).await {
            Ok(target) => target,
            Err(error) => {
                return RoutineDiscordLogStatus::failed(error);
            }
        };

        match crate::services::message_outbox::enqueue_outbox_pg(
            &self.pool,
            crate::services::message_outbox::OutboxMessage {
                target: &target,
                content,
                bot: "notify",
                source: "routine-runtime",
                reason_code: Some(reason_code),
                session_key: Some(session_key),
            },
        )
        .await
        {
            Ok(_) => RoutineDiscordLogStatus::ok(),
            Err(error) => RoutineDiscordLogStatus::failed(format!(
                "failed to enqueue routine discord log: {error}"
            )),
        }
    }

    async fn persist_run_log_status(
        &self,
        store: &RoutineStore,
        run_id: &str,
        status: &RoutineDiscordLogStatus,
    ) {
        if let Err(error) = store
            .record_discord_log_result(run_id, &status.status, status.warning.as_deref())
            .await
        {
            tracing::warn!(
                run_id,
                error = %error,
                "failed to persist routine discord log status"
            );
        }
    }
}

impl RoutineDiscordLogStatus {
    fn ok() -> Self {
        Self {
            status: "ok".to_string(),
            warning: None,
        }
    }

    fn skipped() -> Self {
        Self {
            status: "skipped".to_string(),
            warning: None,
        }
    }

    fn failed(error: impl ToString) -> Self {
        let warning = error.to_string();
        tracing::warn!(warning, "routine discord log failed");
        Self {
            status: "failed".to_string(),
            warning: Some(warning),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutineLifecycleEvent {
    Attached,
    Paused,
    Resumed,
    Detached,
}

impl RoutineLifecycleEvent {
    fn reason_code(self) -> &'static str {
        match self {
            Self::Attached => "routine_attached",
            Self::Paused => "routine_paused",
            Self::Resumed => "routine_resumed",
            Self::Detached => "routine_detached",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Attached => "attached",
            Self::Paused => "paused",
            Self::Resumed => "resumed",
            Self::Detached => "detached",
        }
    }
}

async fn resolve_agent_channel_target(pool: &PgPool, agent_id: &str) -> Result<String, String> {
    let bindings = crate::db::agents::load_agent_channel_bindings_pg(pool, agent_id)
        .await
        .map_err(|error| format!("load agent bindings for routine log {agent_id}: {error}"))?
        .ok_or_else(|| format!("agent {agent_id} not found for routine log"))?;
    let primary_channel = bindings
        .primary_channel()
        .ok_or_else(|| format!("agent {agent_id} has no primary channel for routine log"))?;
    let channel_id = crate::server::routes::dispatches::resolve_channel_alias_pub(&primary_channel)
        .or_else(|| primary_channel.parse::<u64>().ok())
        .ok_or_else(|| {
            format!(
                "agent {agent_id} primary channel is invalid for routine log: {primary_channel}"
            )
        })?;
    Ok(format!("channel:{channel_id}"))
}

fn normalized_agent_id(agent_id: Option<&str>) -> Option<&str> {
    agent_id.map(str::trim).filter(|value| !value.is_empty())
}

fn routine_lifecycle_message(routine: &RoutineRecord, event: RoutineLifecycleEvent) -> String {
    format!(
        "루틴 {}: {} / id {} / script {} / status {}",
        event.label(),
        compact(&routine.name, 80),
        short_id(&routine.id),
        compact(&routine.script_ref, 80),
        routine.status
    )
}

fn run_started_message(claimed: &ClaimedRoutineRun) -> String {
    format!(
        "루틴 실행 시작: {} / run {} / script {}",
        compact(&claimed.name, 80),
        short_id(&claimed.run_id),
        compact(&claimed.script_ref, 80)
    )
}

fn run_outcome_message(routine: &RoutineRecord, outcome: &RoutineRunOutcome) -> String {
    let mut message = format!(
        "루틴 실행 결과: {} / run {} / action {} / status {}",
        compact(&routine.name, 80),
        short_id(&outcome.run_id),
        outcome.action,
        outcome.status
    );
    if let Some(error) = outcome
        .error
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        message.push_str(" / error ");
        message.push_str(&compact(error, 160));
    }
    message
}

fn short_id(value: &str) -> String {
    value.chars().take(8).collect()
}

fn compact(value: &str, max_chars: usize) -> String {
    let trimmed = value.trim();
    let mut result: String = trimmed.chars().take(max_chars).collect();
    if trimmed.chars().count() > max_chars {
        result.push_str("...");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn compact_caps_long_values() {
        assert_eq!(compact("abcdef", 3), "abc...");
        assert_eq!(compact(" abc ", 10), "abc");
    }

    #[test]
    fn run_outcome_message_includes_error_preview() {
        let routine = RoutineRecord {
            id: "routine-123456789".to_string(),
            agent_id: Some("maker".to_string()),
            script_ref: "daily-summary.js".to_string(),
            name: "Daily Summary".to_string(),
            status: "enabled".to_string(),
            execution_strategy: "fresh".to_string(),
            schedule: None,
            next_due_at: None,
            last_run_at: None,
            last_result: None,
            checkpoint: None,
            in_flight_run_id: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let outcome = RoutineRunOutcome {
            run_id: "run-123456789".to_string(),
            routine_id: routine.id.clone(),
            script_ref: routine.script_ref.clone(),
            action: "complete".to_string(),
            status: "failed".to_string(),
            result_json: Some(json!({"error": "boom"})),
            error: Some("boom".to_string()),
            fresh_context_guaranteed: false,
        };

        let message = run_outcome_message(&routine, &outcome);

        assert!(message.contains("Daily Summary"));
        assert!(message.contains("run-1234"));
        assert!(message.contains("status failed"));
        assert!(message.contains("error boom"));
    }

    #[test]
    fn missing_agent_is_skipped_without_warning() {
        let status = RoutineDiscordLogStatus::skipped();
        assert_eq!(status.status, "skipped");
        assert_eq!(status.warning, None);
    }
}
