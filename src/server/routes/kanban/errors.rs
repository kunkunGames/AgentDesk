//! Kanban API error classification and response conversion.

use crate::error::{AppError, ErrorCode};
use axum::{Json, http::StatusCode};

pub(super) fn pg_pool_required_error() -> AppError {
    AppError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Database,
        "postgres backend required for kanban transition (#1384)",
    )
}

pub(super) fn database_error(message: impl Into<String>) -> AppError {
    let message = message.into();
    // The string-only Kanban DB facade preserves this exact machine marker.
    // All other database failures keep their existing classification.
    if message == "campaign_graph_card_contract_immutable"
        || message == "error returned from database: campaign_graph_card_contract_immutable"
    {
        return AppError::conflict("Issued campaign graph card execution contract is immutable");
    }
    AppError::internal(message).with_code(ErrorCode::Database)
}

pub(super) fn kanban_error(status: StatusCode, message: impl Into<String>) -> AppError {
    AppError::new(status, ErrorCode::Kanban, message)
}

pub(super) fn tuple_error(response: (StatusCode, Json<serde_json::Value>)) -> AppError {
    let (status, Json(body)) = response;
    let message = body
        .get("error")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("kanban request failed")
        .to_string();
    kanban_error(status, message)
}

pub(super) fn pg_pool_required_anyhow() -> anyhow::Error {
    anyhow::anyhow!("postgres backend required for kanban transition (#1384)")
}
