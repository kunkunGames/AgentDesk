use axum::{
    http::StatusCode,
    Json,
};
use serde_json::json;

/// GET /api/cron-jobs
pub async fn list_cron_jobs() -> (StatusCode, Json<serde_json::Value>) {
    // Stub: ADK에는 cron 테이블이 없음. 빈 배열 반환.
    (StatusCode::OK, Json(json!({ "jobs": [] })))
}
