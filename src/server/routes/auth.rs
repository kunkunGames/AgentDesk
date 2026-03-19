use axum::Json;
use serde_json::json;

/// GET /api/auth/session — stub that always returns OK (no real auth)
pub async fn get_session() -> Json<serde_json::Value> {
    Json(json!({
        "ok": true,
        "csrf_token": ""
    }))
}
