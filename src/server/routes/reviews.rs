use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::json;

use super::AppState;

// ── Body types ─────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct DecisionItem {
    pub item_id: i64,
    pub decision: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateDecisionsBody {
    pub decisions: Vec<DecisionItem>,
}

// ── Handlers ───────────────────────────────────────────────────

/// PATCH /api/kanban-reviews/:id/decisions
pub async fn update_decisions(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateDecisionsBody>,
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

    // The id here refers to a dispatch_id that groups review decisions
    // Update each decision by item_id within this dispatch
    for item in &body.decisions {
        let valid = item.decision == "accept" || item.decision == "reject";
        if !valid {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("invalid decision '{}', must be 'accept' or 'reject'", item.decision)})),
            );
        }

        let affected = conn.execute(
            "UPDATE review_decisions SET decision = ?1, decided_at = datetime('now') WHERE dispatch_id = ?2 AND id = ?3",
            rusqlite::params![item.decision, id, item.item_id],
        ).unwrap_or(0);

        // If no row was updated, try inserting (upsert pattern)
        if affected == 0 {
            conn.execute(
                "INSERT OR REPLACE INTO review_decisions (id, dispatch_id, decision, decided_at) VALUES (?1, ?2, ?3, datetime('now'))",
                rusqlite::params![item.item_id, id, item.decision],
            ).ok();
        }
    }

    // Return all decisions for this dispatch
    let mut stmt = match conn.prepare(
        "SELECT id, kanban_card_id, dispatch_id, item_index, decision, decided_at
         FROM review_decisions
         WHERE dispatch_id = ?1
         ORDER BY id",
    ) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("prepare: {e}")})),
            )
        }
    };

    let rows = stmt
        .query_map([&id], |row| {
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "kanban_card_id": row.get::<_, Option<String>>(1)?,
                "dispatch_id": row.get::<_, Option<String>>(2)?,
                "item_index": row.get::<_, Option<i64>>(3)?,
                "decision": row.get::<_, Option<String>>(4)?,
                "decided_at": row.get::<_, Option<String>>(5)?,
            }))
        })
        .ok();

    let decisions: Vec<serde_json::Value> = match rows {
        Some(iter) => iter.filter_map(|r| r.ok()).collect(),
        None => Vec::new(),
    };

    (StatusCode::OK, Json(json!({"review": {"dispatch_id": id, "decisions": decisions}})))
}

/// POST /api/kanban-reviews/:id/trigger-rework
pub async fn trigger_rework(
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

    // Find the kanban_card_id from review_decisions for this dispatch
    let card_id: Option<String> = conn
        .query_row(
            "SELECT kanban_card_id FROM review_decisions WHERE dispatch_id = ?1 LIMIT 1",
            [&id],
            |row| row.get(0),
        )
        .ok()
        .flatten();

    // Also try looking up from task_dispatches if no review_decision found
    let card_id = card_id.or_else(|| {
        conn.query_row(
            "SELECT kanban_card_id FROM task_dispatches WHERE id = ?1",
            [&id],
            |row| row.get(0),
        )
        .ok()
        .flatten()
    });

    let card_id = match card_id {
        Some(cid) => cid,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "review or dispatch not found"})),
            );
        }
    };

    match conn.execute(
        "UPDATE kanban_cards SET status = 'in_progress', updated_at = datetime('now') WHERE id = ?1",
        [&card_id],
    ) {
        Ok(0) => (StatusCode::NOT_FOUND, Json(json!({"error": "card not found"}))),
        Ok(_) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        ),
    }
}
