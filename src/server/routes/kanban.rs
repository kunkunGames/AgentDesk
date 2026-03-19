use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::json;

use super::AppState;
use crate::engine::hooks::Hook;

// ── Query / Body types ─────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ListCardsQuery {
    pub status: Option<String>,
    pub repo_id: Option<String>,
    pub assigned_agent_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateCardBody {
    pub title: String,
    pub repo_id: Option<String>,
    pub priority: Option<String>,
    pub github_issue_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateCardBody {
    pub title: Option<String>,
    pub status: Option<String>,
    pub priority: Option<String>,
    pub assigned_agent_id: Option<String>,
    pub repo_id: Option<String>,
    pub github_issue_url: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct AssignCardBody {
    pub agent_id: String,
}

// ── Handlers ───────────────────────────────────────────────────

/// GET /api/kanban-cards
pub async fn list_cards(
    State(state): State<AppState>,
    Query(params): Query<ListCardsQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let result = state.db.lock().map_err(|e| format!("{e}"));
    let conn = match result {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))),
    };

    let mut sql = String::from(
        "SELECT id, repo_id, title, status, priority, assigned_agent_id, github_issue_url, github_issue_number, latest_dispatch_id, review_round, metadata, created_at, updated_at FROM kanban_cards WHERE 1=1"
    );
    let mut bind_values: Vec<String> = Vec::new();

    if let Some(ref status) = params.status {
        bind_values.push(status.clone());
        sql.push_str(&format!(" AND status = ?{}", bind_values.len()));
    }
    if let Some(ref repo_id) = params.repo_id {
        bind_values.push(repo_id.clone());
        sql.push_str(&format!(" AND repo_id = ?{}", bind_values.len()));
    }
    if let Some(ref agent_id) = params.assigned_agent_id {
        bind_values.push(agent_id.clone());
        sql.push_str(&format!(" AND assigned_agent_id = ?{}", bind_values.len()));
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
        .query_map(params_ref.as_slice(), |row| card_row_to_json(row))
        .ok();

    let cards: Vec<serde_json::Value> = match rows {
        Some(iter) => iter.filter_map(|r| r.ok()).collect(),
        None => Vec::new(),
    };

    (StatusCode::OK, Json(json!({"cards": cards})))
}

/// GET /api/kanban-cards/:id
pub async fn get_card(
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
        "SELECT id, repo_id, title, status, priority, assigned_agent_id, github_issue_url, github_issue_number, latest_dispatch_id, review_round, metadata, created_at, updated_at FROM kanban_cards WHERE id = ?1",
        [&id],
        |row| card_row_to_json(row),
    ) {
        Ok(card) => (StatusCode::OK, Json(json!({"card": card}))),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            (StatusCode::NOT_FOUND, Json(json!({"error": "card not found"})))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        ),
    }
}

/// POST /api/kanban-cards
pub async fn create_card(
    State(state): State<AppState>,
    Json(body): Json<CreateCardBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let id = uuid::Uuid::new_v4().to_string();
    let priority = body.priority.unwrap_or_else(|| "medium".to_string());

    let conn = match state.db.lock() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            )
        }
    };

    let result = conn.execute(
        "INSERT INTO kanban_cards (id, repo_id, title, status, priority, github_issue_url, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'backlog', ?4, ?5, datetime('now'), datetime('now'))",
        rusqlite::params![id, body.repo_id, body.title, priority, body.github_issue_url],
    );

    if let Err(e) = result {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        );
    }

    match conn.query_row(
        "SELECT id, repo_id, title, status, priority, assigned_agent_id, github_issue_url, github_issue_number, latest_dispatch_id, review_round, metadata, created_at, updated_at FROM kanban_cards WHERE id = ?1",
        [&id],
        |row| card_row_to_json(row),
    ) {
        Ok(card) => (StatusCode::CREATED, Json(json!({"card": card}))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        ),
    }
}

/// PATCH /api/kanban-cards/:id
pub async fn update_card(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateCardBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Read old status for transition hook
    let old_status: Option<String> = {
        let conn = match state.db.lock() {
            Ok(c) => c,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("{e}")})),
                )
            }
        };

        conn.query_row(
            "SELECT status FROM kanban_cards WHERE id = ?1",
            [&id],
            |row| row.get(0),
        )
        .ok()
    };

    if old_status.is_none() {
        return (StatusCode::NOT_FOUND, Json(json!({"error": "card not found"})));
    }
    let old_status = old_status.unwrap();

    // Build dynamic UPDATE
    let mut sets: Vec<String> = Vec::new();
    let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    let mut idx = 1;

    macro_rules! push_field {
        ($field:expr, $val:expr) => {
            if let Some(ref v) = $val {
                sets.push(format!("{} = ?{}", $field, idx));
                values.push(Box::new(v.clone()));
                idx += 1;
            }
        };
    }

    push_field!("title", body.title);
    push_field!("status", body.status);
    push_field!("priority", body.priority);
    push_field!("assigned_agent_id", body.assigned_agent_id);
    push_field!("repo_id", body.repo_id);
    push_field!("github_issue_url", body.github_issue_url);

    if let Some(ref meta) = body.metadata {
        let meta_str = serde_json::to_string(meta).unwrap_or_default();
        sets.push(format!("metadata = ?{}", idx));
        values.push(Box::new(meta_str));
        idx += 1;
    }

    if sets.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "no fields to update"})));
    }

    sets.push(format!("updated_at = datetime('now')"));

    let sql = format!("UPDATE kanban_cards SET {} WHERE id = ?{}", sets.join(", "), idx);
    values.push(Box::new(id.clone()));

    let conn = match state.db.lock() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            )
        }
    };

    let params_ref: Vec<&dyn rusqlite::types::ToSql> = values.iter().map(|v| v.as_ref()).collect();
    match conn.execute(&sql, params_ref.as_slice()) {
        Ok(0) => {
            return (StatusCode::NOT_FOUND, Json(json!({"error": "card not found"})));
        }
        Ok(_) => {}
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            );
        }
    }

    let card = conn.query_row(
        "SELECT id, repo_id, title, status, priority, assigned_agent_id, github_issue_url, github_issue_number, latest_dispatch_id, review_round, metadata, created_at, updated_at FROM kanban_cards WHERE id = ?1",
        [&id],
        |row| card_row_to_json(row),
    );

    let new_status = body.status.clone();
    drop(conn);

    // Fire hooks if status changed
    if let Some(ref new_s) = new_status {
        if new_s != &old_status {
            let _ = state.engine.fire_hook(
                Hook::OnCardTransition,
                json!({
                    "card_id": id,
                    "from": old_status,
                    "to": new_s,
                }),
            );

            // Terminal states
            let terminal = ["done", "failed", "cancelled"];
            if terminal.contains(&new_s.as_str()) {
                let _ = state.engine.fire_hook(
                    Hook::OnCardTerminal,
                    json!({
                        "card_id": id,
                        "status": new_s,
                    }),
                );
            }
        }
    }

    match card {
        Ok(c) => (StatusCode::OK, Json(json!({"card": c}))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        ),
    }
}

/// POST /api/kanban-cards/:id/assign
pub async fn assign_card(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<AssignCardBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let old_status: Option<String> = {
        let conn = match state.db.lock() {
            Ok(c) => c,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("{e}")})),
                )
            }
        };
        conn.query_row(
            "SELECT status FROM kanban_cards WHERE id = ?1",
            [&id],
            |row| row.get(0),
        )
        .ok()
    };

    if old_status.is_none() {
        return (StatusCode::NOT_FOUND, Json(json!({"error": "card not found"})));
    }
    let old_status = old_status.unwrap();

    let conn = match state.db.lock() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            )
        }
    };

    match conn.execute(
        "UPDATE kanban_cards SET assigned_agent_id = ?1, status = 'ready', updated_at = datetime('now') WHERE id = ?2",
        rusqlite::params![body.agent_id, id],
    ) {
        Ok(0) => {
            return (StatusCode::NOT_FOUND, Json(json!({"error": "card not found"})));
        }
        Ok(_) => {}
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            );
        }
    }

    let card = conn.query_row(
        "SELECT id, repo_id, title, status, priority, assigned_agent_id, github_issue_url, github_issue_number, latest_dispatch_id, review_round, metadata, created_at, updated_at FROM kanban_cards WHERE id = ?1",
        [&id],
        |row| card_row_to_json(row),
    );
    drop(conn);

    // Fire transition hook
    if old_status != "ready" {
        let _ = state.engine.fire_hook(
            Hook::OnCardTransition,
            json!({
                "card_id": id,
                "from": old_status,
                "to": "ready",
            }),
        );
    }

    match card {
        Ok(c) => (StatusCode::OK, Json(json!({"card": c}))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        ),
    }
}

// ── Helpers ────────────────────────────────────────────────────

fn card_row_to_json(row: &rusqlite::Row) -> rusqlite::Result<serde_json::Value> {
    Ok(json!({
        "id": row.get::<_, String>(0)?,
        "repo_id": row.get::<_, Option<String>>(1)?,
        "title": row.get::<_, String>(2)?,
        "status": row.get::<_, String>(3)?,
        "priority": row.get::<_, String>(4)?,
        "assigned_agent_id": row.get::<_, Option<String>>(5)?,
        "github_issue_url": row.get::<_, Option<String>>(6)?,
        "github_issue_number": row.get::<_, Option<i64>>(7)?,
        "latest_dispatch_id": row.get::<_, Option<String>>(8)?,
        "review_round": row.get::<_, i64>(9)?,
        "metadata": row.get::<_, Option<String>>(10)?.and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok()),
        "created_at": row.get::<_, String>(11)?,
        "updated_at": row.get::<_, String>(12)?,
    }))
}
