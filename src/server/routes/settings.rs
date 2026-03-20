use axum::{Json, extract::State, http::StatusCode};
use serde_json::json;

use super::AppState;

/// GET /api/settings
pub async fn get_settings(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let conn = match state.db.lock() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            );
        }
    };

    let value: String = conn
        .query_row(
            "SELECT value FROM kv_meta WHERE key = 'settings'",
            [],
            |row| row.get(0),
        )
        .unwrap_or_else(|_| "{}".to_string());

    let parsed: serde_json::Value = serde_json::from_str(&value).unwrap_or(json!({}));

    (StatusCode::OK, Json(parsed))
}

/// PUT /api/settings
pub async fn put_settings(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    let conn = match state.db.lock() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            );
        }
    };

    let value_str = serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string());

    if let Err(e) = conn.execute(
        "INSERT OR REPLACE INTO kv_meta (key, value) VALUES ('settings', ?1)",
        [&value_str],
    ) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        );
    }

    (StatusCode::OK, Json(json!({"ok": true})))
}

/// GET /api/settings/runtime-config
pub async fn get_runtime_config(
    State(state): State<AppState>,
) -> (StatusCode, Json<serde_json::Value>) {
    let conn = match state.db.lock() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            );
        }
    };

    let value: String = conn
        .query_row(
            "SELECT value FROM kv_meta WHERE key = 'runtime-config'",
            [],
            |row| row.get(0),
        )
        .unwrap_or_else(|_| "{}".to_string());

    let parsed: serde_json::Value = serde_json::from_str(&value).unwrap_or(json!({}));

    (StatusCode::OK, Json(parsed))
}

/// PUT /api/settings/runtime-config
pub async fn put_runtime_config(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    let conn = match state.db.lock() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            );
        }
    };

    let value_str = serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string());

    if let Err(e) = conn.execute(
        "INSERT OR REPLACE INTO kv_meta (key, value) VALUES ('runtime-config', ?1)",
        [&value_str],
    ) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        );
    }

    (StatusCode::OK, Json(json!({"ok": true})))
}
