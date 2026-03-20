use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::json;

use super::AppState;

/// POST /api/auto-queue/generate
pub async fn generate() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::OK, Json(json!({ "run": null, "entries": [] })))
}

/// POST /api/auto-queue/activate
pub async fn activate() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(json!({ "dispatched": [], "count": 0 })),
    )
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

// ── Enqueue types ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct EnqueueBody {
    pub repo: String,
    pub issue_number: i64,
    pub agent_id: Option<String>,
}

/// POST /api/auto-queue/enqueue
pub async fn enqueue(
    State(state): State<AppState>,
    Json(body): Json<EnqueueBody>,
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

    // Resolve agent_id: use provided or fall back to repo's default_agent_id
    let agent_id = match body.agent_id {
        Some(ref id) if !id.is_empty() => id.clone(),
        _ => {
            match conn.query_row(
                "SELECT default_agent_id FROM kanban_repos WHERE full_name = ?1",
                [&body.repo],
                |row| row.get::<_, Option<String>>(0),
            ) {
                Ok(Some(id)) if !id.is_empty() => id,
                _ => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(
                            json!({"error": "no agent_id provided and repo has no default_agent_id"}),
                        ),
                    );
                }
            }
        }
    };

    // Create or find kanban card
    let card_id = uuid::Uuid::new_v4().to_string();
    let title = format!("Issue #{}", body.issue_number);

    if let Err(e) = conn.execute(
        "INSERT INTO kanban_cards (id, repo, issue_number, status, title)
         VALUES (?1, ?2, ?3, 'ready', ?4)
         ON CONFLICT(repo, issue_number) DO UPDATE SET status = 'ready'",
        rusqlite::params![card_id, body.repo, body.issue_number, title],
    ) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("upsert card: {e}")})),
        );
    }

    // Resolve actual card id (may differ if conflict triggered update)
    let actual_card_id: String = match conn.query_row(
        "SELECT id FROM kanban_cards WHERE repo = ?1 AND issue_number = ?2",
        rusqlite::params![body.repo, body.issue_number],
        |row| row.get(0),
    ) {
        Ok(id) => id,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("resolve card: {e}")})),
            );
        }
    };

    // Insert into dispatch_queue if not already queued
    let already_queued: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM dispatch_queue WHERE kanban_card_id = ?1",
            [&actual_card_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0)
        > 0;

    if !already_queued {
        let queue_id = uuid::Uuid::new_v4().to_string();
        if let Err(e) = conn.execute(
            "INSERT INTO dispatch_queue (id, kanban_card_id, agent_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![queue_id, actual_card_id, agent_id],
        ) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("insert queue: {e}")})),
            );
        }
    }

    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "card_id": actual_card_id,
            "agent_id": agent_id,
        })),
    )
}
