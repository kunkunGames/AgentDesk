use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::json;

use super::AppState;

// ── Query / Body types ────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ListDispatchedSessionsQuery {
    #[serde(rename = "includeMerged")]
    pub include_merged: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateDispatchedSessionBody {
    pub status: Option<String>,
    pub active_dispatch_id: Option<String>,
    pub model: Option<String>,
    pub tokens: Option<i64>,
    pub cwd: Option<String>,
    pub session_info: Option<String>,
}

// ── Handlers ──────────────────────────────────────────────────

/// GET /api/dispatched-sessions
pub async fn list_dispatched_sessions(
    State(state): State<AppState>,
    Query(params): Query<ListDispatchedSessionsQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let conn = match state.db.lock() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            )
        }
    };

    let include_all = params.include_merged.as_deref() == Some("1");

    let sql = if include_all {
        "SELECT s.id, s.session_key, s.agent_id, s.provider, s.status, s.active_dispatch_id,
                s.model, s.tokens, s.cwd, s.last_heartbeat, s.session_info,
                a.department, a.sprite_number, a.avatar_emoji, a.xp,
                d.name AS department_name, d.name_ko AS department_name_ko, d.color AS department_color
         FROM sessions s
         LEFT JOIN agents a ON s.agent_id = a.id
         LEFT JOIN departments d ON a.department = d.id
         ORDER BY s.id"
    } else {
        "SELECT s.id, s.session_key, s.agent_id, s.provider, s.status, s.active_dispatch_id,
                s.model, s.tokens, s.cwd, s.last_heartbeat, s.session_info,
                a.department, a.sprite_number, a.avatar_emoji, a.xp,
                d.name AS department_name, d.name_ko AS department_name_ko, d.color AS department_color
         FROM sessions s
         LEFT JOIN agents a ON s.agent_id = a.id
         LEFT JOIN departments d ON a.department = d.id
         WHERE s.active_dispatch_id IS NOT NULL
         ORDER BY s.id"
    };

    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("prepare: {e}")})),
            )
        }
    };

    let rows = stmt
        .query_map([], |row| {
            let agent_id = row.get::<_, Option<String>>(2)?;
            let session_key = row.get::<_, Option<String>>(1)?;
            let last_heartbeat = row.get::<_, Option<String>>(9)?;
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "session_key": session_key,
                "agent_id": agent_id,
                "provider": row.get::<_, Option<String>>(3)?,
                "status": row.get::<_, Option<String>>(4)?,
                "active_dispatch_id": row.get::<_, Option<String>>(5)?,
                "model": row.get::<_, Option<String>>(6)?,
                "tokens": row.get::<_, i64>(7)?,
                "cwd": row.get::<_, Option<String>>(8)?,
                "last_heartbeat": last_heartbeat,
                "session_info": row.get::<_, Option<String>>(10)?,
                // alias fields for frontend compatibility
                "linked_agent_id": agent_id,
                "last_seen_at": last_heartbeat,
                "name": session_key,
                // joined agent fields
                "department_id": row.get::<_, Option<String>>(11)?,
                "sprite_number": row.get::<_, Option<i64>>(12)?,
                "avatar_emoji": row.get::<_, Option<String>>(13).ok().flatten().unwrap_or_else(|| "\u{1F916}".to_string()),
                "stats_xp": row.get::<_, i64>(14).unwrap_or(0),
                "connected_at": null,
                // joined department fields
                "department_name": row.get::<_, Option<String>>(15)?,
                "department_name_ko": row.get::<_, Option<String>>(16)?,
                "department_color": row.get::<_, Option<String>>(17)?,
            }))
        })
        .ok();

    let sessions: Vec<serde_json::Value> = match rows {
        Some(iter) => iter.filter_map(|r| r.ok()).collect(),
        None => Vec::new(),
    };

    (StatusCode::OK, Json(json!({"sessions": sessions})))
}

/// PATCH /api/dispatched-sessions/:id
pub async fn update_dispatched_session(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<UpdateDispatchedSessionBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let conn = match state.db.lock() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            )
        }
    };

    let mut sets: Vec<String> = Vec::new();
    let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    let mut idx = 1;

    if let Some(ref status) = body.status {
        sets.push(format!("status = ?{}", idx));
        values.push(Box::new(status.clone()));
        idx += 1;
    }
    if let Some(ref dispatch_id) = body.active_dispatch_id {
        sets.push(format!("active_dispatch_id = ?{}", idx));
        values.push(Box::new(dispatch_id.clone()));
        idx += 1;
    }
    if let Some(ref model) = body.model {
        sets.push(format!("model = ?{}", idx));
        values.push(Box::new(model.clone()));
        idx += 1;
    }
    if let Some(tokens) = body.tokens {
        sets.push(format!("tokens = ?{}", idx));
        values.push(Box::new(tokens));
        idx += 1;
    }
    if let Some(ref cwd) = body.cwd {
        sets.push(format!("cwd = ?{}", idx));
        values.push(Box::new(cwd.clone()));
        idx += 1;
    }
    if let Some(ref session_info) = body.session_info {
        sets.push(format!("session_info = ?{}", idx));
        values.push(Box::new(session_info.clone()));
        idx += 1;
    }

    if sets.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "no fields to update"})),
        );
    }

    let sql = format!(
        "UPDATE sessions SET {} WHERE id = ?{}",
        sets.join(", "),
        idx
    );
    values.push(Box::new(id));

    let params_ref: Vec<&dyn rusqlite::types::ToSql> =
        values.iter().map(|v| v.as_ref()).collect();
    match conn.execute(&sql, params_ref.as_slice()) {
        Ok(0) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "session not found"})),
        ),
        Ok(_) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        ),
    }
}
