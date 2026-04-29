use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};

use super::action::RoutineAction;
use super::agent_executor::RoutineAgentExecutor;
use super::loader::{RoutineScriptLoader, RoutineTickContext, RoutineTickRoutine, RoutineTickRun};
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
    max_due_per_tick: u32,
) -> Result<Vec<RoutineRunOutcome>> {
    let claimed = store.claim_due_runs(max_due_per_tick).await?;
    let mut outcomes = Vec::with_capacity(claimed.len());
    for run in claimed {
        match execute_claimed_script_run(store, loader, agent_executor, run).await {
            Ok(outcome) => outcomes.push(outcome),
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
) -> Result<RoutineRunOutcome> {
    let fresh_context_guaranteed = false;
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
            store
                .fail_run(&claimed.run_id, &message, result_json.clone(), None)
                .await?;
            return Ok(RoutineRunOutcome {
                run_id: claimed.run_id,
                routine_id: claimed.routine_id,
                script_ref: claimed.script_ref,
                action: "error".to_string(),
                status: "failed".to_string(),
                result_json,
                error: Some(message),
                fresh_context_guaranteed,
            });
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

async fn close_action(
    store: &RoutineStore,
    agent_executor: Option<&RoutineAgentExecutor>,
    claimed: ClaimedRoutineRun,
    action: RoutineAction,
    fresh_context_guaranteed: bool,
) -> Result<RoutineRunOutcome> {
    let action_name = action.action_name().to_string();
    match action {
        RoutineAction::Complete {
            result_json,
            checkpoint,
            last_result,
            next_due_at,
        } => {
            store
                .finish_run(
                    &claimed.run_id,
                    result_json.clone(),
                    checkpoint,
                    last_result.as_deref(),
                    next_due_at,
                )
                .await?;
            Ok(RoutineRunOutcome {
                run_id: claimed.run_id,
                routine_id: claimed.routine_id,
                script_ref: claimed.script_ref,
                action: action_name,
                status: "succeeded".to_string(),
                result_json,
                error: None,
                fresh_context_guaranteed,
            })
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
            store
                .skip_run(
                    &claimed.run_id,
                    result_json.clone(),
                    checkpoint,
                    last_result.as_deref().or(reason.as_deref()),
                    next_due_at,
                )
                .await?;
            Ok(RoutineRunOutcome {
                run_id: claimed.run_id,
                routine_id: claimed.routine_id,
                script_ref: claimed.script_ref,
                action: action_name,
                status: "skipped".to_string(),
                result_json,
                error: None,
                fresh_context_guaranteed,
            })
        }
        RoutineAction::Pause {
            reason,
            result_json,
            checkpoint,
            last_result,
        } => {
            let result_json =
                result_json.or_else(|| reason.as_ref().map(|reason| json!({ "reason": reason })));
            store
                .pause_after_run(
                    &claimed.run_id,
                    result_json.clone(),
                    checkpoint,
                    last_result.as_deref().or(reason.as_deref()),
                )
                .await?;
            Ok(RoutineRunOutcome {
                run_id: claimed.run_id,
                routine_id: claimed.routine_id,
                script_ref: claimed.script_ref,
                action: action_name,
                status: "paused".to_string(),
                result_json,
                error: None,
                fresh_context_guaranteed,
            })
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
                store
                    .fail_agent_run(&claimed.run_id, message, result_json.clone(), None)
                    .await?;
                return Ok(RoutineRunOutcome {
                    run_id: claimed.run_id,
                    routine_id: claimed.routine_id,
                    script_ref: claimed.script_ref,
                    action: action_name,
                    status: "failed".to_string(),
                    result_json,
                    error: Some(message.to_string()),
                    fresh_context_guaranteed,
                });
            };
            agent_executor
                .start_agent_run(store, claimed, prompt, checkpoint, next_due_at)
                .await
        }
    }
}
