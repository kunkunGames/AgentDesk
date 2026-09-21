use crate::services::cluster::execution_requirements::ExecutionRequirements;
use crate::{
    app_state::AppState,
    error::{AppError, AppResult},
};
use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::{Value, json};

pub(super) async fn get(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<Value>> {
    let pool = state
        .pg_pool_ref()
        .ok_or_else(|| AppError::internal("postgres unavailable"))?;
    let policy: Option<Value> =
        sqlx::query_scalar("SELECT execution_requirements FROM agents WHERE id=$1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(Json(
        json!({"execution_requirements":policy.ok_or_else(|| AppError::not_found("agent not found"))?}),
    ))
}

pub(super) async fn put(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(value): Json<Value>,
) -> AppResult<Json<Value>> {
    let policy = ExecutionRequirements::parse(value).map_err(AppError::bad_request)?;
    let value = serde_json::to_value(policy).map_err(|e| AppError::internal(e.to_string()))?;
    let pool = state
        .pg_pool_ref()
        .ok_or_else(|| AppError::internal("postgres unavailable"))?;
    let result =
        sqlx::query("UPDATE agents SET execution_requirements=$2, updated_at=NOW() WHERE id=$1")
            .bind(id)
            .bind(&value)
            .execute(pool)
            .await
            .map_err(|e| AppError::internal(e.to_string()))?;
    if result.rows_affected() == 0 {
        return Err(AppError::not_found("agent not found"));
    }
    Ok(Json(json!({"execution_requirements":value})))
}
