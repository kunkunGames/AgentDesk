//! Operator enrollment and candidate-specific approval. A request selects a
//! protected registry entry; it cannot supply a command or a passing receipt.
use super::AppState;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::server::test_phase_runs::checks;
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use serde::Deserialize;
use serde_json::{Value, json};

fn authorized_pool<'a>(state: &'a AppState, headers: &HeaderMap) -> AppResult<&'a sqlx::PgPool> {
    let authorized = state
        .config
        .server
        .auth_token
        .as_deref()
        .filter(|s| !s.is_empty())
        .zip(super::auth::extract_bearer(headers))
        .is_some_and(|(expected, actual)| {
            crate::utils::auth::constant_time_token_eq(expected, actual)
        });
    if !authorized {
        return Err(AppError::new(
            StatusCode::UNAUTHORIZED,
            ErrorCode::Policy,
            "managed check administration requires the server admin bearer",
        ));
    }
    state
        .pg_pool
        .as_ref()
        .ok_or_else(|| AppError::internal("canonical PostgreSQL pool unavailable"))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Enrollment {
    template: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Approval {
    candidate_digest: String,
    dispatch_id: String,
    contract_digest: String,
    reason: String,
}

pub(crate) async fn enroll(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(card): Path<String>,
    Json(body): Json<Enrollment>,
) -> AppResult<Json<Value>> {
    let value = checks::enroll(authorized_pool(&state, &headers)?, &card, &body.template)
        .await
        .map_err(|e| AppError::conflict(e.to_string()))?;
    Ok(Json(value))
}

pub(crate) async fn inspect(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(card): Path<String>,
) -> AppResult<Json<Value>> {
    Ok(Json(
        checks::inspect(authorized_pool(&state, &headers)?, &card)
            .await
            .map_err(|e| AppError::conflict(e.to_string()))?,
    ))
}

pub(crate) async fn approve(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(card): Path<String>,
    Json(body): Json<Approval>,
) -> AppResult<Json<Value>> {
    let pool = authorized_pool(&state, &headers)?;
    checks::approve(
        pool,
        &card,
        &body.candidate_digest,
        &body.dispatch_id,
        &body.contract_digest,
        &body.reason,
    )
    .await
    .map_err(|e| AppError::conflict(e.to_string()))?;
    checks::kick(pool.clone(), state.engine.clone());
    Ok(Json(json!({"approved":true})))
}
