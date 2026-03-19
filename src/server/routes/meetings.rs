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
pub struct IssueRepoBody {
    pub repo: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StartMeetingBody {
    pub agenda: Option<String>,
    pub channel_id: Option<String>,
    pub primary_provider: Option<String>,
}

// ── Handlers ───────────────────────────────────────────────────

/// GET /api/round-table-meetings
pub async fn list_meetings(
    State(state): State<AppState>,
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

    let mut stmt = match conn.prepare(
        "SELECT id, channel_id, title, status, effective_rounds, started_at, completed_at, summary
         FROM meetings
         ORDER BY started_at DESC",
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
        .query_map([], |row| meeting_row_to_json(row))
        .ok();

    let mut meetings: Vec<serde_json::Value> = match rows {
        Some(iter) => iter.filter_map(|r| r.ok()).collect(),
        None => Vec::new(),
    };

    // Attach transcripts to each meeting (+ entries alias for frontend)
    for meeting in meetings.iter_mut() {
        if let Some(meeting_id) = meeting.get("id").and_then(|v| v.as_str()) {
            let transcripts = load_transcripts(&conn, meeting_id);
            let obj = meeting.as_object_mut().unwrap();
            obj.insert("transcripts".to_string(), json!(&transcripts));
            obj.insert("entries".to_string(), json!(transcripts));
        }
    }

    (StatusCode::OK, Json(json!({"meetings": meetings})))
}

/// GET /api/round-table-meetings/:id
pub async fn get_meeting(
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
        "SELECT id, channel_id, title, status, effective_rounds, started_at, completed_at, summary
         FROM meetings WHERE id = ?1",
        [&id],
        |row| meeting_row_to_json(row),
    ) {
        Ok(mut meeting) => {
            let transcripts = load_transcripts(&conn, &id);
            let obj = meeting.as_object_mut().unwrap();
            obj.insert("transcripts".to_string(), json!(&transcripts));
            obj.insert("entries".to_string(), json!(transcripts));
            (StatusCode::OK, Json(json!({"meeting": meeting})))
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            (StatusCode::NOT_FOUND, Json(json!({"error": "meeting not found"})))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        ),
    }
}

/// DELETE /api/round-table-meetings/:id
pub async fn delete_meeting(
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

    // Delete transcripts first
    let _ = conn.execute("DELETE FROM meeting_transcripts WHERE meeting_id = ?1", [&id]);

    match conn.execute("DELETE FROM meetings WHERE id = ?1", [&id]) {
        Ok(0) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "meeting not found"})),
        ),
        Ok(_) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        ),
    }
}

/// PATCH /api/round-table-meetings/:id/issue-repo
pub async fn update_issue_repo(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<IssueRepoBody>,
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

    // Check meeting exists
    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM meetings WHERE id = ?1",
            [&id],
            |row| row.get::<_, i64>(0),
        )
        .map(|c| c > 0)
        .unwrap_or(false);

    if !exists {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "meeting not found"})),
        );
    }

    // Store issue_repo in kv_meta (meetings table doesn't have issue_repo column)
    let key = format!("meeting_issue_repo:{}", id);
    let value = body.repo.as_deref().unwrap_or("");

    if let Err(e) = conn.execute(
        "INSERT OR REPLACE INTO kv_meta (key, value) VALUES (?1, ?2)",
        rusqlite::params![key, value],
    ) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        );
    }

    // Read back meeting
    match conn.query_row(
        "SELECT id, channel_id, title, status, effective_rounds, started_at, completed_at, summary
         FROM meetings WHERE id = ?1",
        [&id],
        |row| meeting_row_to_json(row),
    ) {
        Ok(mut meeting) => {
            meeting.as_object_mut().unwrap().insert("issue_repo".to_string(), json!(body.repo));
            (StatusCode::OK, Json(json!({"ok": true, "meeting": meeting})))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{e}")})),
        ),
    }
}

/// POST /api/round-table-meetings/:id/issues — stub
pub async fn create_issues(
    Path(_id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "skipped": true,
            "results": [],
            "summary": {
                "created": 0,
                "failed": 0,
                "reason": "Issue creation is handled by the Discord bot layer"
            }
        })),
    )
}

/// POST /api/round-table-meetings/:id/issues/discard — stub
pub async fn discard_issue(
    Path(_id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::OK, Json(json!({"ok": true})))
}

/// POST /api/round-table-meetings/:id/issues/discard-all — stub
pub async fn discard_all_issues(
    Path(_id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::OK, Json(json!({"ok": true})))
}

/// POST /api/round-table-meetings/start — stub
pub async fn start_meeting(
    Json(_body): Json<StartMeetingBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "message": "Meeting start is handled by the Discord bot layer"
        })),
    )
}

// ── Helpers ────────────────────────────────────────────────────

fn meeting_row_to_json(row: &rusqlite::Row) -> rusqlite::Result<serde_json::Value> {
    let title = row.get::<_, Option<String>>(2)?;
    let effective_rounds = row.get::<_, Option<i64>>(4)?;
    Ok(json!({
        "id": row.get::<_, String>(0)?,
        "channel_id": row.get::<_, Option<String>>(1)?,
        "title": title,
        "status": row.get::<_, Option<String>>(3)?,
        "effective_rounds": effective_rounds,
        "started_at": row.get::<_, Option<String>>(5)?,
        "completed_at": row.get::<_, Option<String>>(6)?,
        "summary": row.get::<_, Option<String>>(7)?,
        // alias fields for frontend compatibility
        "agenda": title,
        "total_rounds": effective_rounds.unwrap_or(0),
        // additional fields expected by frontend (defaults)
        "primary_provider": null,
        "reviewer_provider": null,
        "participant_names": null,
        "issues_created": null,
        "proposed_issues": null,
        "issue_creation_results": null,
        "issue_repo": null,
        "created_at": null,
    }))
}

fn load_transcripts(conn: &rusqlite::Connection, meeting_id: &str) -> Vec<serde_json::Value> {
    let mut stmt = match conn.prepare(
        "SELECT id, meeting_id, seq, round, speaker_agent_id, speaker_name, content, is_summary
         FROM meeting_transcripts
         WHERE meeting_id = ?1
         ORDER BY seq ASC",
    ) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let rows = stmt
        .query_map([meeting_id], |row| {
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "meeting_id": row.get::<_, String>(1)?,
                "seq": row.get::<_, Option<i64>>(2)?,
                "round": row.get::<_, Option<i64>>(3)?,
                "speaker_agent_id": row.get::<_, Option<String>>(4)?,
                "speaker_name": row.get::<_, Option<String>>(5)?,
                "content": row.get::<_, Option<String>>(6)?,
                "is_summary": row.get::<_, bool>(7).unwrap_or(false),
            }))
        })
        .ok();

    match rows {
        Some(iter) => iter.filter_map(|r| r.ok()).collect(),
        None => Vec::new(),
    }
}
