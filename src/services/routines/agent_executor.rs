use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;

use crate::services::discord::health::HealthRegistry;

use super::runtime::RoutineRunOutcome;
use super::store::{ClaimedRoutineRun, NextDueAtUpdate, RoutineStore, RunningAgentRoutineRun};

const AGENT_COMPLETION_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const FRESH_CONTEXT_GUARANTEED: bool = false;

#[derive(Clone)]
pub struct RoutineAgentExecutor {
    pool: Arc<PgPool>,
    health_registry: Option<Arc<HealthRegistry>>,
    completion_timeout: Duration,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct AgentTurnCompletion {
    assistant_message: String,
    duration_ms: Option<i64>,
    created_at: DateTime<Utc>,
}

impl RoutineAgentExecutor {
    pub fn new(pool: Arc<PgPool>, health_registry: Option<Arc<HealthRegistry>>) -> Self {
        Self {
            pool,
            health_registry,
            completion_timeout: AGENT_COMPLETION_TIMEOUT,
        }
    }

    pub async fn start_agent_run(
        &self,
        store: &RoutineStore,
        claimed: ClaimedRoutineRun,
        prompt: String,
        checkpoint: Option<Value>,
        next_due_at: Option<DateTime<Utc>>,
    ) -> Result<RoutineRunOutcome> {
        let action = "agent".to_string();
        let result = self
            .start_turn(&claimed, &prompt, &checkpoint, next_due_at)
            .await;
        match result {
            Ok(started) => {
                let updated = store
                    .mark_agent_turn_started(
                        &claimed.run_id,
                        &started.turn_id,
                        Some(started.result_json.clone()),
                    )
                    .await?;
                if !updated {
                    return Err(anyhow!(
                        "routine agent run {} vanished before turn_id could be stored",
                        claimed.run_id
                    ));
                }

                Ok(RoutineRunOutcome {
                    run_id: claimed.run_id,
                    routine_id: claimed.routine_id,
                    script_ref: claimed.script_ref,
                    action,
                    status: "running".to_string(),
                    result_json: Some(started.result_json),
                    error: None,
                    fresh_context_guaranteed: FRESH_CONTEXT_GUARANTEED,
                })
            }
            Err(error) => {
                let message = error.to_string();
                let result_json = Some(json!({
                    "status": "failed_to_start",
                    "error": message,
                    "routine_id": claimed.routine_id,
                    "run_id": claimed.run_id,
                    "script_ref": claimed.script_ref,
                    "fresh_context_guaranteed": FRESH_CONTEXT_GUARANTEED,
                }));
                store
                    .fail_agent_run(&claimed.run_id, &message, result_json.clone(), None)
                    .await?;
                Ok(RoutineRunOutcome {
                    run_id: claimed.run_id,
                    routine_id: claimed.routine_id,
                    script_ref: claimed.script_ref,
                    action,
                    status: "failed".to_string(),
                    result_json,
                    error: Some(message),
                    fresh_context_guaranteed: FRESH_CONTEXT_GUARANTEED,
                })
            }
        }
    }

    pub async fn poll_agent_runs(
        &self,
        store: &RoutineStore,
        limit: u32,
    ) -> Result<Vec<RoutineRunOutcome>> {
        let pending = store.list_running_agent_runs(limit).await?;
        let mut outcomes = Vec::new();
        for run in pending {
            if self.has_timed_out(&run).await? {
                let message = format!(
                    "routine agent turn timed out after {} seconds",
                    self.completion_timeout.as_secs()
                );
                let result_json = Some(merge_pending_result(&run, "timeout", Some(&message), None));
                let closed = store
                    .fail_agent_run(&run.run_id, &message, result_json.clone(), None)
                    .await?;
                if !closed {
                    continue;
                }
                outcomes.push(RoutineRunOutcome {
                    run_id: run.run_id,
                    routine_id: run.routine_id,
                    script_ref: run.script_ref,
                    action: "agent".to_string(),
                    status: "failed".to_string(),
                    result_json,
                    error: Some(message),
                    fresh_context_guaranteed: FRESH_CONTEXT_GUARANTEED,
                });
                continue;
            }

            match self.find_turn_completion(&run.turn_id).await? {
                Some(completion) => {
                    let checkpoint = pending_checkpoint(run.result_json.as_ref());
                    let next_due_at = pending_next_due_at(run.result_json.as_ref());
                    let last_result = assistant_preview(&completion.assistant_message);
                    let result_json =
                        Some(completed_result(&run, &completion, last_result.as_str()));
                    let closed = store
                        .complete_agent_run(
                            &run.run_id,
                            result_json.clone(),
                            checkpoint,
                            Some(last_result.as_str()),
                            match next_due_at {
                                Some(value) => NextDueAtUpdate::Set(value),
                                None => NextDueAtUpdate::Clear,
                            },
                        )
                        .await?;
                    if !closed {
                        continue;
                    }
                    outcomes.push(RoutineRunOutcome {
                        run_id: run.run_id,
                        routine_id: run.routine_id,
                        script_ref: run.script_ref,
                        action: "agent".to_string(),
                        status: "succeeded".to_string(),
                        result_json,
                        error: None,
                        fresh_context_guaranteed: FRESH_CONTEXT_GUARANTEED,
                    });
                }
                None => {
                    store.heartbeat_run(&run.run_id).await?;
                }
            }
        }
        Ok(outcomes)
    }

    async fn start_turn(
        &self,
        claimed: &ClaimedRoutineRun,
        prompt: &str,
        checkpoint: &Option<Value>,
        next_due_at: Option<DateTime<Utc>>,
    ) -> Result<StartedAgentTurn> {
        let agent_id = claimed
            .agent_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("routine agent action requires routine.agent_id"))?;
        let Some(registry) = self.health_registry.as_deref() else {
            return Err(anyhow!(
                "routine agent action requires discord runtime health registry"
            ));
        };

        let bindings = crate::db::agents::load_agent_channel_bindings_pg(&self.pool, agent_id)
            .await
            .map_err(|error| anyhow!("load agent bindings for {agent_id}: {error}"))?
            .ok_or_else(|| anyhow!("agent {agent_id} not found"))?;
        let provider = bindings
            .resolved_primary_provider_kind()
            .ok_or_else(|| anyhow!("agent {agent_id} primary provider is not configured"))?;
        let primary_channel = bindings
            .primary_channel()
            .ok_or_else(|| anyhow!("agent {agent_id} primary channel is not configured"))?;
        let Some(channel_id_num) =
            crate::server::routes::dispatches::resolve_channel_alias_pub(&primary_channel)
                .or_else(|| primary_channel.parse::<u64>().ok())
        else {
            return Err(anyhow!(
                "agent {agent_id} primary channel is invalid: {primary_channel}"
            ));
        };

        let metadata = Some(json!({
            "routine_id": claimed.routine_id,
            "routine_run_id": claimed.run_id,
            "script_ref": claimed.script_ref,
            "execution_strategy": claimed.execution_strategy,
            "fresh_context_guaranteed": FRESH_CONTEXT_GUARANTEED,
        }));
        let channel_name_hint = primary_channel
            .chars()
            .all(|ch| ch.is_ascii_digit())
            .then_some(None)
            .unwrap_or_else(|| Some(primary_channel.clone()));
        let outcome = crate::services::discord::health::start_headless_agent_turn(
            registry,
            poise::serenity_prelude::ChannelId::new(channel_id_num),
            provider.clone(),
            prompt.to_string(),
            Some("routine".to_string()),
            metadata,
            channel_name_hint,
        )
        .await
        .map_err(|error| anyhow!("start routine agent turn for {agent_id}: {error}"))?;

        let result_json = json!({
            "status": "started",
            "turn_id": outcome.turn_id,
            "agent_id": agent_id,
            "provider": provider.as_str(),
            "channel_id": channel_id_num.to_string(),
            "routine_id": claimed.routine_id,
            "run_id": claimed.run_id,
            "script_ref": claimed.script_ref,
            "completion_evidence": "session_transcripts",
            "fresh_context_guaranteed": FRESH_CONTEXT_GUARANTEED,
            "checkpoint": checkpoint,
            "next_due_at": next_due_at.map(|value| value.to_rfc3339()),
        });

        Ok(StartedAgentTurn {
            turn_id: outcome.turn_id,
            result_json,
        })
    }

    async fn find_turn_completion(&self, turn_id: &str) -> Result<Option<AgentTurnCompletion>> {
        sqlx::query_as(
            r#"
            SELECT assistant_message, duration_ms, created_at
            FROM session_transcripts
            WHERE turn_id = $1
              AND BTRIM(assistant_message) <> ''
            LIMIT 1
            "#,
        )
        .bind(turn_id)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|error| anyhow!("lookup routine agent transcript {turn_id}: {error}"))
    }

    async fn has_timed_out(&self, run: &RunningAgentRoutineRun) -> Result<bool> {
        let timeout_secs = i64::try_from(self.completion_timeout.as_secs())
            .map_err(|_| anyhow!("routine agent completion timeout exceeds i64 seconds"))?;
        sqlx::query_scalar(
            r#"
            SELECT $1::timestamptz + ($2::bigint * INTERVAL '1 second') <= NOW()
            "#,
        )
        .bind(run.started_at)
        .bind(timeout_secs)
        .fetch_one(&*self.pool)
        .await
        .map_err(|error| anyhow!("check routine agent timeout {}: {error}", run.run_id))
    }
}

struct StartedAgentTurn {
    turn_id: String,
    result_json: Value,
}

fn completed_result(
    run: &RunningAgentRoutineRun,
    completion: &AgentTurnCompletion,
    assistant_preview: &str,
) -> Value {
    json!({
        "status": "completed",
        "turn_id": run.turn_id,
        "routine_id": run.routine_id,
        "run_id": run.run_id,
        "script_ref": run.script_ref,
        "completion_evidence": "session_transcripts",
        "assistant_message_preview": assistant_preview,
        "assistant_message_chars": completion.assistant_message.chars().count(),
        "duration_ms": completion.duration_ms,
        "transcript_created_at": completion.created_at,
        "fresh_context_guaranteed": FRESH_CONTEXT_GUARANTEED,
    })
}

fn merge_pending_result(
    run: &RunningAgentRoutineRun,
    status: &str,
    error: Option<&str>,
    completion: Option<&AgentTurnCompletion>,
) -> Value {
    json!({
        "status": status,
        "turn_id": run.turn_id,
        "routine_id": run.routine_id,
        "run_id": run.run_id,
        "script_ref": run.script_ref,
        "error": error,
        "duration_ms": completion.and_then(|value| value.duration_ms),
        "fresh_context_guaranteed": FRESH_CONTEXT_GUARANTEED,
    })
}

fn pending_checkpoint(result_json: Option<&Value>) -> Option<Value> {
    result_json
        .and_then(|value| value.get("checkpoint"))
        .filter(|value| !value.is_null())
        .cloned()
}

fn pending_next_due_at(result_json: Option<&Value>) -> Option<DateTime<Utc>> {
    result_json
        .and_then(|value| value.get("next_due_at"))
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn assistant_preview(message: &str) -> String {
    const MAX_CHARS: usize = 500;
    let trimmed = message.trim();
    let mut preview: String = trimmed.chars().take(MAX_CHARS).collect();
    if trimmed.chars().count() > MAX_CHARS {
        preview.push_str("...");
    }
    preview
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_checkpoint_ignores_null() {
        assert_eq!(pending_checkpoint(Some(&json!({"checkpoint": null}))), None);
        assert_eq!(
            pending_checkpoint(Some(&json!({"checkpoint": {"cursor": 3}}))),
            Some(json!({"cursor": 3}))
        );
    }

    #[test]
    fn assistant_preview_caps_long_messages() {
        let long = "a".repeat(600);
        let preview = assistant_preview(&long);
        assert_eq!(preview.chars().count(), 503);
        assert!(preview.ends_with("..."));
    }
}
