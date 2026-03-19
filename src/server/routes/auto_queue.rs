use axum::{
    extract::Path,
    http::StatusCode,
    Json,
};
use serde_json::json;

/// POST /api/auto-queue/generate
pub async fn generate() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::OK, Json(json!({ "run": null, "entries": [] })))
}

/// POST /api/auto-queue/activate
pub async fn activate() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::OK, Json(json!({ "dispatched": [], "count": 0 })))
}

/// GET /api/auto-queue/status
pub async fn status() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(json!({ "run": null, "entries": [], "agents": {} })),
    )
}

/// PATCH /api/auto-queue/entries/{id}/skip
pub async fn skip_entry(Path(_id): Path<String>) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::OK, Json(json!({ "ok": true })))
}

/// PATCH /api/auto-queue/runs/{id}
pub async fn update_run(Path(_id): Path<String>) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::OK, Json(json!({ "ok": true })))
}

/// PATCH /api/auto-queue/reorder
pub async fn reorder() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::OK, Json(json!({ "ok": true })))
}
