use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::Row;

use super::action::RoutineAction;
use super::agent_executor::RoutineAgentExecutor;
use super::discord_log::RoutineDiscordLogger;
use super::loader::{
    RoutineScriptLoader, RoutineTickAgent, RoutineTickContext, RoutineTickRoutine, RoutineTickRun,
};
use super::store::{ClaimedRoutineRun, RoutineStore};

#[derive(Debug, Clone, Serialize)]
pub struct RoutineRunOutcome {
    pub run_id: String,
    pub routine_id: String,
    pub script_ref: String,
    pub action: String,
    pub status: String,
    pub result_json: Option<Value>,
    pub error: Option<String>,
    pub fresh_context_guaranteed: bool,
}

pub async fn run_due_tick(
    store: &RoutineStore,
    loader: &RoutineScriptLoader,
    agent_executor: Option<&RoutineAgentExecutor>,
    discord_logger: Option<&RoutineDiscordLogger>,
    max_due_per_tick: u32,
) -> Result<Vec<RoutineRunOutcome>> {
    let claimed = store.claim_due_runs(max_due_per_tick).await?;
    let mut outcomes = Vec::with_capacity(claimed.len());
    for run in claimed {
        if let Some(logger) = discord_logger {
            logger.log_run_started(store, &run).await;
        }
        match execute_claimed_script_run(store, loader, agent_executor, run).await {
            Ok(Some(outcome)) => {
                if let Some(logger) = discord_logger {
                    logger.log_run_outcome(store, &outcome).await;
                }
                outcomes.push(outcome);
            }
            Ok(None) => {
                tracing::info!("routine due run was closed before outcome capture");
            }
            Err(error) => {
                tracing::warn!(error = %error, "routine due run failed before outcome capture");
            }
        }
    }
    Ok(outcomes)
}

pub async fn poll_agent_turns(
    store: &RoutineStore,
    agent_executor: &RoutineAgentExecutor,
    max_per_tick: u32,
) -> Result<Vec<RoutineRunOutcome>> {
    agent_executor.poll_agent_runs(store, max_per_tick).await
}

pub async fn execute_claimed_script_run(
    store: &RoutineStore,
    loader: &RoutineScriptLoader,
    agent_executor: Option<&RoutineAgentExecutor>,
    claimed: ClaimedRoutineRun,
) -> Result<Option<RoutineRunOutcome>> {
    let fresh_context_guaranteed = false;
    let agent = load_tick_agent_context(store, claimed.agent_id.as_deref()).await?;
    let context = RoutineTickContext {
        routine: RoutineTickRoutine {
            id: claimed.routine_id.clone(),
            agent_id: claimed.agent_id.clone(),
            script_ref: claimed.script_ref.clone(),
            name: claimed.name.clone(),
            execution_strategy: claimed.execution_strategy.clone(),
            fresh_context_guaranteed,
        },
        run: RoutineTickRun {
            id: claimed.run_id.clone(),
            lease_expires_at: claimed.lease_expires_at,
        },
        agent,
        checkpoint: claimed.checkpoint.clone(),
        now: chrono::Utc::now(),
    };

    store.heartbeat_run(&claimed.run_id).await?;
    let action = match loader.execute_tick(&claimed.script_ref, context) {
        Ok(action) => action,
        Err(error) => {
            let message = error.to_string();
            let result_json = Some(json!({
                "error": message,
                "script_ref": claimed.script_ref,
            }));
            let closed = store
                .fail_run(&claimed.run_id, &message, result_json.clone(), None)
                .await?;
            if !closed {
                return Ok(None);
            }
            return Ok(Some(RoutineRunOutcome {
                run_id: claimed.run_id,
                routine_id: claimed.routine_id,
                script_ref: claimed.script_ref,
                action: "error".to_string(),
                status: "failed".to_string(),
                result_json,
                error: Some(message),
                fresh_context_guaranteed,
            }));
        }
    };

    close_action(
        store,
        agent_executor,
        claimed,
        action,
        fresh_context_guaranteed,
    )
    .await
}

async fn load_tick_agent_context(
    store: &RoutineStore,
    agent_id: Option<&str>,
) -> Result<Option<RoutineTickAgent>> {
    let Some(agent_id) = agent_id.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };

    let row = sqlx::query(
        r#"
        SELECT a.id,
               COALESCE(NULLIF(BTRIM(a.status), ''), 'idle') AS status,
               (SELECT td2.id
                  FROM task_dispatches td2
                  JOIN kanban_cards kc ON kc.latest_dispatch_id = td2.id
                 WHERE td2.to_agent_id = a.id
                   AND kc.status = 'in_progress'
                 ORDER BY td2.created_at DESC NULLS LAST, td2.id DESC
                 LIMIT 1) AS current_task_id,
               (SELECT s.thread_channel_id
                  FROM sessions s
                 WHERE s.agent_id = a.id
                   AND s.status = 'working'
                 ORDER BY s.last_heartbeat DESC NULLS LAST, s.id DESC
                 LIMIT 1) AS current_thread_channel_id,
               EXISTS (
                   SELECT 1
                     FROM sessions s
                    WHERE s.agent_id = a.id
                      AND s.status = 'working'
               ) AS has_working_session
          FROM agents a
         WHERE a.id = $1
        "#,
    )
    .bind(agent_id)
    .fetch_optional(store.pool())
    .await
    .map_err(|error| anyhow::anyhow!("load routine tick agent context for {agent_id}: {error}"))?;

    let Some(row) = row else {
        return Ok(None);
    };

    let status = row
        .try_get::<String, _>("status")
        .unwrap_or_else(|_| "idle".to_string());
    let current_task_id = row
        .try_get::<Option<String>, _>("current_task_id")
        .ok()
        .flatten();
    let current_thread_channel_id = row
        .try_get::<Option<String>, _>("current_thread_channel_id")
        .ok()
        .flatten();
    let has_working_session = row
        .try_get::<bool, _>("has_working_session")
        .unwrap_or(false);
    let has_active_task = current_task_id.is_some();
    let has_busy_signal =
        has_working_session || current_thread_channel_id.is_some() || has_active_task;
    let is_idle = status == "idle" && !has_busy_signal;

    Ok(Some(RoutineTickAgent {
        id: agent_id.to_string(),
        status,
        is_idle,
        current_task_id,
        current_thread_channel_id,
    }))
}

async fn close_action(
    store: &RoutineStore,
    agent_executor: Option<&RoutineAgentExecutor>,
    claimed: ClaimedRoutineRun,
    action: RoutineAction,
    fresh_context_guaranteed: bool,
) -> Result<Option<RoutineRunOutcome>> {
    let action_name = action.action_name().to_string();
    match action {
        RoutineAction::Complete {
            result_json,
            checkpoint,
            last_result,
            next_due_at,
        } => {
            let closed = store
                .finish_run(
                    &claimed.run_id,
                    result_json.clone(),
                    checkpoint,
                    last_result.as_deref(),
                    next_due_at,
                )
                .await?;
            if !closed {
                return Ok(None);
            }
            Ok(Some(RoutineRunOutcome {
                run_id: claimed.run_id,
                routine_id: claimed.routine_id,
                script_ref: claimed.script_ref,
                action: action_name,
                status: "succeeded".to_string(),
                result_json,
                error: None,
                fresh_context_guaranteed,
            }))
        }
        RoutineAction::Skip {
            reason,
            result_json,
            checkpoint,
            last_result,
            next_due_at,
        } => {
            let result_json =
                result_json.or_else(|| reason.as_ref().map(|reason| json!({ "reason": reason })));
            let closed = store
                .skip_run(
                    &claimed.run_id,
                    result_json.clone(),
                    checkpoint,
                    last_result.as_deref().or(reason.as_deref()),
                    next_due_at,
                )
                .await?;
            if !closed {
                return Ok(None);
            }
            Ok(Some(RoutineRunOutcome {
                run_id: claimed.run_id,
                routine_id: claimed.routine_id,
                script_ref: claimed.script_ref,
                action: action_name,
                status: "skipped".to_string(),
                result_json,
                error: None,
                fresh_context_guaranteed,
            }))
        }
        RoutineAction::Pause {
            reason,
            result_json,
            checkpoint,
            last_result,
        } => {
            let result_json =
                result_json.or_else(|| reason.as_ref().map(|reason| json!({ "reason": reason })));
            let closed = store
                .pause_after_run(
                    &claimed.run_id,
                    result_json.clone(),
                    checkpoint,
                    last_result.as_deref().or(reason.as_deref()),
                )
                .await?;
            if !closed {
                return Ok(None);
            }
            Ok(Some(RoutineRunOutcome {
                run_id: claimed.run_id,
                routine_id: claimed.routine_id,
                script_ref: claimed.script_ref,
                action: action_name,
                status: "paused".to_string(),
                result_json,
                error: None,
                fresh_context_guaranteed,
            }))
        }
        RoutineAction::Agent {
            prompt,
            checkpoint,
            next_due_at,
        } => {
            let Some(agent_executor) = agent_executor else {
                let message = "RoutineAction.agent requires RoutineAgentExecutor";
                let result_json = Some(json!({
                    "error": message,
                    "fresh_context_guaranteed": fresh_context_guaranteed,
                }));
                let closed = store
                    .fail_agent_run(&claimed.run_id, message, result_json.clone(), None)
                    .await?;
                if !closed {
                    return Ok(None);
                }
                return Ok(Some(RoutineRunOutcome {
                    run_id: claimed.run_id,
                    routine_id: claimed.routine_id,
                    script_ref: claimed.script_ref,
                    action: action_name,
                    status: "failed".to_string(),
                    result_json,
                    error: Some(message.to_string()),
                    fresh_context_guaranteed,
                }));
            };
            agent_executor
                .start_agent_run(store, claimed, prompt, checkpoint, next_due_at)
                .await
                .map(Some)
        }
    }
}
