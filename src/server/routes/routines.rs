use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::{AppError, AppResult, ErrorCode};
use crate::services::routines::{
    NewRoutine, RoutineAgentExecutor, RoutineDiscordLogger, RoutineLifecycleEvent, RoutinePatch,
    RoutineScriptLoader, RoutineSessionCommand, RoutineSessionController, RoutineStore,
    execute_claimed_script_run,
};

use super::AppState;

#[derive(Debug, Deserialize)]
pub struct ListRoutinesQuery {
    pub agent_id: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListRunsQuery {
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct AttachRoutineBody {
    pub agent_id: Option<String>,
    pub script_ref: String,
    pub name: Option<String>,
    pub execution_strategy: Option<String>,
    pub schedule: Option<String>,
    pub next_due_at: Option<DateTime<Utc>>,
    pub checkpoint: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct PatchRoutineBody {
    pub name: Option<String>,
    pub execution_strategy: Option<String>,
    pub schedule: Option<Option<String>>,
    pub next_due_at: Option<Option<DateTime<Utc>>>,
    pub checkpoint: Option<Option<Value>>,
}

#[derive(Debug, Deserialize)]
pub struct ResumeRoutineBody {
    pub next_due_at: Option<DateTime<Utc>>,
}

pub async fn list_routines(
    State(state): State<AppState>,
    Query(query): Query<ListRoutinesQuery>,
) -> AppResult<Json<Value>> {
    let store = routine_store(&state)?;
    let routines = store
        .list_routines(query.agent_id.as_deref(), query.status.as_deref())
        .await
        .map_err(store_error)?;
    Ok(Json(json!({ "routines": routines })))
}

pub async fn get_routine(
    State(state): State<AppState>,
    Path(routine_id): Path<String>,
) -> AppResult<Json<Value>> {
    let store = routine_store(&state)?;
    let Some(routine) = store.get_routine(&routine_id).await.map_err(store_error)? else {
        return Err(AppError::not_found(format!(
            "routine {routine_id} not found"
        )));
    };
    Ok(Json(json!({ "routine": routine })))
}

pub async fn list_routine_runs(
    State(state): State<AppState>,
    Path(routine_id): Path<String>,
    Query(query): Query<ListRunsQuery>,
) -> AppResult<Json<Value>> {
    let store = routine_store(&state)?;
    if store
        .get_routine(&routine_id)
        .await
        .map_err(store_error)?
        .is_none()
    {
        return Err(AppError::not_found(format!(
            "routine {routine_id} not found"
        )));
    }
    let runs = store
        .list_runs(&routine_id, query.limit.unwrap_or(20))
        .await
        .map_err(store_error)?;
    Ok(Json(json!({ "runs": runs })))
}

pub async fn attach_routine(
    State(state): State<AppState>,
    Json(body): Json<AttachRoutineBody>,
) -> AppResult<(StatusCode, Json<Value>)> {
    let store = routine_store(&state)?;
    if body.script_ref.trim().is_empty() {
        return Err(AppError::bad_request("script_ref is required"));
    }
    let name = body.name.unwrap_or_else(|| fallback_name(&body.script_ref));
    let execution_strategy = body
        .execution_strategy
        .unwrap_or_else(|| "fresh".to_string());
    validate_execution_strategy_request(&execution_strategy)?;
    let routine = store
        .attach_routine(NewRoutine {
            agent_id: body.agent_id,
            script_ref: body.script_ref,
            name,
            execution_strategy,
            schedule: body.schedule,
            next_due_at: body.next_due_at,
            checkpoint: body.checkpoint,
        })
        .await
        .map_err(store_error)?;
    let discord_log = routine_discord_logger(&state)?
        .log_routine_event(&routine, RoutineLifecycleEvent::Attached)
        .await;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "routine": routine, "discord_log": discord_log })),
    ))
}

pub async fn patch_routine(
    State(state): State<AppState>,
    Path(routine_id): Path<String>,
    Json(body): Json<PatchRoutineBody>,
) -> AppResult<Json<Value>> {
    let store = routine_store(&state)?;
    if let Some(strategy) = body.execution_strategy.as_deref() {
        validate_execution_strategy_request(strategy)?;
    }
    let patch = RoutinePatch {
        name: body.name,
        execution_strategy: body.execution_strategy,
        schedule: body.schedule,
        next_due_at: body.next_due_at,
        checkpoint: body.checkpoint,
    };
    let Some(routine) = store
        .patch_routine(&routine_id, patch)
        .await
        .map_err(store_error)?
    else {
        return Err(AppError::not_found(format!(
            "routine {routine_id} not found"
        )));
    };
    Ok(Json(json!({ "routine": routine })))
}

pub async fn pause_routine(
    State(state): State<AppState>,
    Path(routine_id): Path<String>,
) -> AppResult<Json<Value>> {
    let store = routine_store(&state)?;
    let changed = store
        .pause_routine(&routine_id)
        .await
        .map_err(store_error)?;
    if !changed {
        return Err(AppError::not_found(format!(
            "enabled routine {routine_id} not found"
        )));
    }
    let discord_log = routine_discord_logger(&state)?
        .log_routine_event_by_id(&store, &routine_id, RoutineLifecycleEvent::Paused)
        .await;
    Ok(Json(
        json!({ "ok": true, "routine_id": routine_id, "discord_log": discord_log }),
    ))
}

pub async fn resume_routine(
    State(state): State<AppState>,
    Path(routine_id): Path<String>,
    Json(body): Json<ResumeRoutineBody>,
) -> AppResult<Json<Value>> {
    let store = routine_store(&state)?;
    let changed = store
        .resume_routine(&routine_id, body.next_due_at)
        .await
        .map_err(store_error)?;
    if !changed {
        return Err(AppError::not_found(format!(
            "paused routine {routine_id} not found"
        )));
    }
    let discord_log = routine_discord_logger(&state)?
        .log_routine_event_by_id(&store, &routine_id, RoutineLifecycleEvent::Resumed)
        .await;
    Ok(Json(
        json!({ "ok": true, "routine_id": routine_id, "discord_log": discord_log }),
    ))
}

pub async fn detach_routine(
    State(state): State<AppState>,
    Path(routine_id): Path<String>,
) -> AppResult<Json<Value>> {
    let store = routine_store(&state)?;
    let changed = store
        .detach_routine(&routine_id)
        .await
        .map_err(store_error)?;
    if !changed {
        return Err(AppError::conflict(format!(
            "routine {routine_id} is missing, already detached, or currently running"
        )));
    }
    let discord_log = routine_discord_logger(&state)?
        .log_routine_event_by_id(&store, &routine_id, RoutineLifecycleEvent::Detached)
        .await;
    Ok(Json(
        json!({ "ok": true, "routine_id": routine_id, "discord_log": discord_log }),
    ))
}

pub async fn run_routine_now(
    State(state): State<AppState>,
    Path(routine_id): Path<String>,
) -> AppResult<Json<Value>> {
    if !state.config.routines.enabled {
        return Err(AppError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Config,
            "routines are disabled by config",
        ));
    }

    let store = routine_store(&state)?;
    if store
        .get_routine(&routine_id)
        .await
        .map_err(store_error)?
        .is_none()
    {
        return Err(AppError::not_found(format!(
            "routine {routine_id} not found"
        )));
    }

    let loader = RoutineScriptLoader::new().map_err(|error| {
        AppError::internal(format!("routine script loader init failed: {error}"))
            .with_code(ErrorCode::Internal)
    })?;
    loader
        .load_dir(&state.config.routines.dir)
        .map_err(|error| {
            AppError::internal(format!("routine script registry load failed: {error}"))
                .with_code(ErrorCode::Config)
        })?;

    let Some(claimed) = store
        .claim_run_now(&routine_id)
        .await
        .map_err(store_error)?
    else {
        return Err(AppError::conflict(format!(
            "routine {routine_id} is not enabled or already running"
        )));
    };

    let agent_executor = routine_agent_executor(&state)?;
    let discord_logger = routine_discord_logger(&state)?;
    discord_logger.log_run_started(&store, &claimed).await;
    let outcome = execute_claimed_script_run(&store, &loader, Some(&agent_executor), claimed)
        .await
        .map_err(store_error)?;
    let discord_log = discord_logger.log_run_outcome(&store, &outcome).await;
    Ok(Json(
        json!({ "outcome": outcome, "discord_log": discord_log }),
    ))
}

pub async fn reset_routine_session(
    State(state): State<AppState>,
    Path(routine_id): Path<String>,
) -> AppResult<Json<Value>> {
    control_routine_session(&state, routine_id, RoutineSessionCommand::Reset).await
}

pub async fn kill_routine_session(
    State(state): State<AppState>,
    Path(routine_id): Path<String>,
) -> AppResult<Json<Value>> {
    control_routine_session(&state, routine_id, RoutineSessionCommand::Kill).await
}

async fn control_routine_session(
    state: &AppState,
    routine_id: String,
    command: RoutineSessionCommand,
) -> AppResult<Json<Value>> {
    let store = routine_store(state)?;
    let Some(routine) = store.get_routine(&routine_id).await.map_err(store_error)? else {
        return Err(AppError::not_found(format!(
            "routine {routine_id} not found"
        )));
    };
    if routine.execution_strategy != "persistent" {
        return Err(AppError::conflict(format!(
            "routine {routine_id} uses execution_strategy={}; session control requires persistent",
            routine.execution_strategy
        )));
    }
    if routine
        .agent_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        return Err(AppError::conflict(format!(
            "routine {routine_id} is not attached to an agent"
        )));
    }

    let reason = format!(
        "routine persistent session {} via POST /api/routines/{}/session/{}",
        command.as_str(),
        routine_id,
        command.as_str()
    );
    let session = routine_session_controller(state)?
        .control_persistent_session(&routine, command, &reason)
        .await
        .map_err(session_control_error)?;
    let session_changed = session.runtime_cleared
        || session.tmux_killed
        || session.inflight_cleared
        || session.disconnected_sessions > 0;
    let interrupted_run_id = if session_changed {
        store
            .interrupt_in_flight_run(
                &routine_id,
                &reason,
                Some(json!({
                    "status": "interrupted_by_session_control",
                    "routine_id": routine_id,
                    "action": command.as_str(),
                    "provider": session.provider.clone(),
                    "channel_id": session.channel_id.clone(),
                    "session_key": session.session_key.clone(),
                    "tmux_session": session.tmux_session.clone(),
                })),
            )
            .await
            .map_err(store_error)?
    } else {
        None
    };

    Ok(Json(json!({
        "ok": true,
        "session": session,
        "interrupted_run_id": interrupted_run_id,
    })))
}

fn routine_store(state: &AppState) -> AppResult<RoutineStore> {
    let Some(pool) = state.pg_pool.clone() else {
        return Err(AppError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Database,
            "postgres pool unavailable; routines require postgresql",
        ));
    };
    Ok(RoutineStore::new(std::sync::Arc::new(pool)))
}

fn routine_agent_executor(state: &AppState) -> AppResult<RoutineAgentExecutor> {
    let Some(pool) = state.pg_pool.clone() else {
        return Err(AppError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Database,
            "postgres pool unavailable; routines require postgresql",
        ));
    };
    Ok(RoutineAgentExecutor::new(
        std::sync::Arc::new(pool),
        state.health_registry.clone(),
    ))
}

fn routine_discord_logger(state: &AppState) -> AppResult<RoutineDiscordLogger> {
    let Some(pool) = state.pg_pool.clone() else {
        return Err(AppError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Database,
            "postgres pool unavailable; routines require postgresql",
        ));
    };
    Ok(RoutineDiscordLogger::new(std::sync::Arc::new(pool)))
}

fn routine_session_controller(state: &AppState) -> AppResult<RoutineSessionController> {
    let Some(pool) = state.pg_pool.clone() else {
        return Err(AppError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Database,
            "postgres pool unavailable; routines require postgresql",
        ));
    };
    Ok(RoutineSessionController::new(
        std::sync::Arc::new(pool),
        state.health_registry.clone(),
    ))
}

fn fallback_name(script_ref: &str) -> String {
    std::path::Path::new(script_ref)
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string()
}

fn validate_execution_strategy_request(strategy: &str) -> AppResult<()> {
    match strategy {
        "fresh" | "persistent" => Ok(()),
        other => Err(AppError::bad_request(format!(
            "unsupported routine execution_strategy '{other}'; expected fresh or persistent"
        ))),
    }
}

fn store_error(error: anyhow::Error) -> AppError {
    AppError::internal(error.to_string()).with_code(ErrorCode::Database)
}

fn session_control_error(error: anyhow::Error) -> AppError {
    let message = error.to_string();
    if message.contains("not found") {
        AppError::not_found(message)
    } else if message.contains("not configured")
        || message.contains("not attached")
        || message.contains("invalid")
        || message.contains("requires execution_strategy")
    {
        AppError::conflict(message)
    } else {
        AppError::internal(message)
    }
}
