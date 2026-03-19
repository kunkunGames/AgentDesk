use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::json;

use super::AppState;
use crate::dispatch;
use crate::engine::hooks::Hook;

// ── Query / Body types ─────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ListDispatchesQuery {
    pub status: Option<String>,
    pub kanban_card_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateDispatchBody {
    pub kanban_card_id: String,
    pub to_agent_id: String,
    pub dispatch_type: Option<String>,
    pub title: String,
    pub context: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateDispatchBody {
    pub status: Option<String>,
    pub result: Option<serde_json::Value>,
}

// ── Handlers ───────────────────────────────────────────────────

/// GET /api/dispatches
pub async fn list_dispatches(
    State(state): State<AppState>,
    Query(params): Query<ListDispatchesQuery>,
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

    let mut sql = String::from(
        "SELECT id, kanban_card_id, from_agent_id, to_agent_id, dispatch_type, status, title, context, result, parent_dispatch_id, chain_depth, created_at, updated_at FROM task_dispatches WHERE 1=1"
    );
    let mut bind_values: Vec<String> = Vec::new();

    if let Some(ref status) = params.status {
        bind_values.push(status.clone());
        sql.push_str(&format!(" AND status = ?{}", bind_values.len()));
    }
    if let Some(ref card_id) = params.kanban_card_id {
        bind_values.push(card_id.clone());
        sql.push_str(&format!(" AND kanban_card_id = ?{}", bind_values.len()));
    }

    sql.push_str(" ORDER BY created_at DESC");

    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("prepare: {e}")})),
            )
        }
    };

    let params_ref: Vec<&dyn rusqlite::types::ToSql> =
        bind_values.iter().map(|v| v as &dyn rusqlite::types::ToSql).collect();

    let rows = stmt
        .query_map(params_ref.as_slice(), |row| dispatch_row_to_json(row))
        .ok();

    let dispatches: Vec<serde_json::Value> = match rows {
        Some(iter) => iter.filter_map(|r| r.ok()).collect(),
        None => Vec::new(),
    };

    (StatusCode::OK, Json(json!({"dispatches": dispatches})))
}

/// GET /api/dispatches/:id
pub async fn get_dispatch(
    State(state): State<AppState>,
    Path(id): Path<String>,
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

    match conn.query_row(
        "SELECT id, kanban_card_id, from_agent_id, to_agent_id, dispatch_type, status, title, context, result, parent_dispatch_id, chain_depth, created_at, updated_at FROM task_dispatches WHERE id = ?1",
        [&id],
        |row| dispatch_row_to_json(row),
    ) {
        Ok(d) => (StatusCode::OK, Json(json!({"dispatch": d}))),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            (StatusCode::NOT_FOUND, Json(json!({"error": "dispatch not found"})))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        ),
    }
}

/// POST /api/dispatches
pub async fn create_dispatch(
    State(state): State<AppState>,
    Json(body): Json<CreateDispatchBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let dispatch_type = body.dispatch_type.unwrap_or_else(|| "implementation".to_string());
    let context = body.context.unwrap_or(json!({}));

    match dispatch::create_dispatch(
        &state.db,
        &state.engine,
        &body.kanban_card_id,
        &body.to_agent_id,
        &dispatch_type,
        &body.title,
        &context,
    ) {
        Ok(d) => (StatusCode::CREATED, Json(json!({"dispatch": d}))),
        Err(e) => {
            let msg = format!("{e}");
            if msg.contains("not found") {
                (StatusCode::NOT_FOUND, Json(json!({"error": msg})))
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": msg})))
            }
        }
    }
}

/// PATCH /api/dispatches/:id
pub async fn update_dispatch(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateDispatchBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    // If status is "completed", use the dispatch engine's complete_dispatch
    if body.status.as_deref() == Some("completed") {
        let result = body.result.unwrap_or(json!({}));
        match dispatch::complete_dispatch(&state.db, &state.engine, &id, &result) {
            Ok(d) => return (StatusCode::OK, Json(json!({"dispatch": d}))),
            Err(e) => {
                let msg = format!("{e}");
                if msg.contains("not found") {
                    return (StatusCode::NOT_FOUND, Json(json!({"error": msg})));
                }
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": msg})),
                );
            }
        }
    }

    // Generic status update
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

    if let Some(ref result) = body.result {
        let result_str = serde_json::to_string(result).unwrap_or_default();
        sets.push(format!("result = ?{}", idx));
        values.push(Box::new(result_str));
        idx += 1;
    }

    if sets.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "no fields to update"})));
    }

    sets.push("updated_at = datetime('now')".to_string());

    let sql = format!("UPDATE task_dispatches SET {} WHERE id = ?{}", sets.join(", "), idx);
    values.push(Box::new(id.clone()));

    let params_ref: Vec<&dyn rusqlite::types::ToSql> = values.iter().map(|v| v.as_ref()).collect();
    match conn.execute(&sql, params_ref.as_slice()) {
        Ok(0) => {
            return (StatusCode::NOT_FOUND, Json(json!({"error": "dispatch not found"})));
        }
        Ok(_) => {}
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            );
        }
    }

    // If the new status is "completed" (edge case: should have been caught above), fire hook
    if body.status.as_deref() == Some("completed") {
        let kanban_card_id: Option<String> = conn
            .query_row(
                "SELECT kanban_card_id FROM task_dispatches WHERE id = ?1",
                [&id],
                |row| row.get(0),
            )
            .ok();
        drop(conn);

        let _ = state.engine.fire_hook(
            Hook::OnDispatchCompleted,
            json!({
                "dispatch_id": id,
                "kanban_card_id": kanban_card_id,
            }),
        );
    } else {
        drop(conn);
    }

    // Read back
    let conn = match state.db.lock() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            )
        }
    };

    match conn.query_row(
        "SELECT id, kanban_card_id, from_agent_id, to_agent_id, dispatch_type, status, title, context, result, parent_dispatch_id, chain_depth, created_at, updated_at FROM task_dispatches WHERE id = ?1",
        [&id],
        |row| dispatch_row_to_json(row),
    ) {
        Ok(d) => (StatusCode::OK, Json(json!({"dispatch": d}))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        ),
    }
}

// ── Helpers ────────────────────────────────────────────────────

fn dispatch_row_to_json(row: &rusqlite::Row) -> rusqlite::Result<serde_json::Value> {
    Ok(json!({
        "id": row.get::<_, String>(0)?,
        "kanban_card_id": row.get::<_, Option<String>>(1)?,
        "from_agent_id": row.get::<_, Option<String>>(2)?,
        "to_agent_id": row.get::<_, Option<String>>(3)?,
        "dispatch_type": row.get::<_, Option<String>>(4)?,
        "status": row.get::<_, String>(5)?,
        "title": row.get::<_, Option<String>>(6)?,
        "context": row.get::<_, Option<String>>(7)?.and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok()),
        "result": row.get::<_, Option<String>>(8)?.and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok()),
        "parent_dispatch_id": row.get::<_, Option<String>>(9)?,
        "chain_depth": row.get::<_, i64>(10)?,
        "created_at": row.get::<_, String>(11)?,
        "updated_at": row.get::<_, String>(12)?,
    }))
}
