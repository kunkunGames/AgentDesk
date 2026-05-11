use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde_json::json;

use super::AppState;
use crate::services::automation_candidate_materializer::{
    AutomationCandidateMaterializer, IterationResultInput, MaterializerError,
};

fn pg_unavailable() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "postgres pool not configured"})),
    )
}

fn materializer_error_response(error: MaterializerError) -> (StatusCode, Json<serde_json::Value>) {
    match error {
        MaterializerError::CardNotFound => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "automation candidate card not found"})),
        ),
        MaterializerError::MissingProgram(msg) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": msg, "code": "MISSING_PROGRAM_CONTRACT"})),
        ),
        MaterializerError::AllowedPathsViolation { path } => (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": format!("path '{}' is not in allowed_write_paths", path),
                "code": "ALLOWED_PATHS_VIOLATION",
                "path": path,
            })),
        ),
        MaterializerError::DuplicateIteration => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "iteration result already recorded for this card/iteration",
                "code": "DUPLICATE_ITERATION",
            })),
        ),
        MaterializerError::Database(msg) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": msg})),
        ),
    }
}

/// POST /api/automation-candidates/{card_id}/iteration-result
///
/// LLM submits iteration result after running against the card's program contract.
/// Rust computes the keep/discard verdict deterministically; LLM cannot override it.
pub async fn submit_iteration_result(
    State(state): State<AppState>,
    Path(card_id): Path<String>,
    Json(body): Json<IterationResultInput>,
) -> (StatusCode, Json<serde_json::Value>) {
    let Some(pool) = state.pg_pool_ref() else {
        return pg_unavailable();
    };

    if body.iteration < 1 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "iteration must be >= 1"})),
        );
    }

    if body.branch.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "branch is required"})),
        );
    }

    let materializer = AutomationCandidateMaterializer::new(pool.clone());
    match materializer.submit_iteration_result(&card_id, body).await {
        Ok(output) => {
            crate::server::ws::emit_event(
                &state.broadcast_tx,
                "automation_candidate_iteration",
                json!({
                    "card_id": card_id,
                    "verdict": output.verdict,
                    "action": output.action,
                    "child_card_id": output.child_card_id,
                }),
            );
            (
                StatusCode::CREATED,
                Json(json!({
                    "record": output.record,
                    "verdict": output.verdict,
                    "action": output.action,
                    "child_card_id": output.child_card_id,
                })),
            )
        }
        Err(error) => materializer_error_response(error),
    }
}

/// GET /api/automation-candidates/{card_id}/iterations
///
/// List all iteration records for a card in chronological order.
pub async fn list_iterations(
    State(state): State<AppState>,
    Path(card_id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let Some(pool) = state.pg_pool_ref() else {
        return pg_unavailable();
    };

    let materializer = AutomationCandidateMaterializer::new(pool.clone());
    match materializer.list_iterations(&card_id).await {
        Ok(records) => (StatusCode::OK, Json(json!({"iterations": records}))),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error})),
        ),
    }
}

/// POST /api/automation-candidates/{card_id}/approve
///
/// Human (or auto_apply_after_green) approves the final iteration.
/// Sets review_status = 'approved'. The card stays in 'review' until
/// a downstream job (merge, deploy) consumes the approval.
pub async fn approve_candidate(
    State(state): State<AppState>,
    Path(card_id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let Some(pool) = state.pg_pool_ref() else {
        return pg_unavailable();
    };

    let materializer = AutomationCandidateMaterializer::new(pool.clone());
    match materializer.approve_candidate(&card_id).await {
        Ok(()) => {
            crate::server::ws::emit_event(
                &state.broadcast_tx,
                "automation_candidate_approved",
                json!({"card_id": card_id}),
            );
            (StatusCode::OK, Json(json!({"status": "approved", "card_id": card_id})))
        }
        Err(error) => materializer_error_response(error),
    }
}
