use axum::{Json, extract::State, http::StatusCode};
use serde_json::json;

use super::AppState;

// ── Handlers ───────────────────────────────────────────────────

/// GET /api/discord-bindings
pub async fn list_bindings(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let conn = match state.db.lock() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            );
        }
    };

    let mut stmt = match conn.prepare(
        "SELECT id, discord_channel_id, discord_channel_alt
         FROM agents
         WHERE discord_channel_id IS NOT NULL
         ORDER BY id",
    ) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("prepare: {e}")})),
            );
        }
    };

    let rows = stmt
        .query_map([], |row| {
            Ok(json!({
                "agentId": row.get::<_, String>(0)?,
                "channelId": row.get::<_, String>(1)?,
                "channelName": row.get::<_, Option<String>>(2)?,
                "source": "config",
            }))
        })
        .ok();

    let bindings: Vec<serde_json::Value> = match rows {
        Some(iter) => iter.filter_map(|r| r.ok()).collect(),
        None => Vec::new(),
    };

    (StatusCode::OK, Json(json!({"bindings": bindings})))
}
