use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::json;

use super::AppState;

/// GET /api/skills/catalog
pub async fn catalog(
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
        "SELECT id, name, description, source_path, trigger_patterns, updated_at
         FROM skills ORDER BY name",
    ) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("query prepare failed: {e}")})),
            )
        }
    };

    let rows = stmt
        .query_map([], |row| {
            Ok(json!({
                "id": row.get::<_, String>(0)?,
                "name": row.get::<_, Option<String>>(1)?,
                "description": row.get::<_, Option<String>>(2)?,
                "source_path": row.get::<_, Option<String>>(3)?,
                "trigger_patterns": row.get::<_, Option<String>>(4)?,
                "updated_at": row.get::<_, Option<String>>(5)?,
            }))
        })
        .ok();

    let catalog = match rows {
        Some(iter) => iter.filter_map(|r| r.ok()).collect::<Vec<_>>(),
        None => Vec::new(),
    };

    (StatusCode::OK, Json(json!({ "catalog": catalog })))
}

#[derive(Debug, Deserialize)]
pub struct RankingQuery {
    window: Option<String>,
    limit: Option<i64>,
}

/// GET /api/skills/ranking?window=7d&limit=20
pub async fn ranking(
    State(state): State<AppState>,
    Query(params): Query<RankingQuery>,
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

    let window = params.window.as_deref().unwrap_or("7d");
    let limit = params.limit.unwrap_or(20);

    let date_filter = match window {
        "30d" => "AND su.used_at > datetime('now', '-30 days')",
        "90d" => "AND su.used_at > datetime('now', '-90 days')",
        "all" => "",
        _ => "AND su.used_at > datetime('now', '-7 days')", // default 7d
    };

    // Overall ranking: skill usage count + last used
    let overall_sql = format!(
        "SELECT s.id, s.name, COUNT(su.id) AS usage_count, MAX(su.used_at) AS last_used
         FROM skills s
         LEFT JOIN skill_usage su ON su.skill_id = s.id {date_filter}
         GROUP BY s.id
         ORDER BY usage_count DESC
         LIMIT ?1"
    );

    let overall = {
        let mut stmt = match conn.prepare(&overall_sql) {
            Ok(s) => s,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("query prepare failed: {e}")})),
                )
            }
        };

        let rows = stmt
            .query_map([limit], |row| {
                Ok(json!({
                    "skill_id": row.get::<_, String>(0)?,
                    "name": row.get::<_, Option<String>>(1)?,
                    "usage_count": row.get::<_, i64>(2)?,
                    "last_used": row.get::<_, Option<String>>(3)?,
                }))
            })
            .ok();

        match rows {
            Some(iter) => iter.filter_map(|r| r.ok()).collect::<Vec<_>>(),
            None => Vec::new(),
        }
    };

    // By-agent ranking
    let by_agent_sql = format!(
        "SELECT su.agent_id, s.id AS skill_id, s.name, COUNT(su.id) AS usage_count
         FROM skill_usage su
         JOIN skills s ON s.id = su.skill_id
         WHERE su.agent_id IS NOT NULL {date_filter}
         GROUP BY su.agent_id, s.id
         ORDER BY su.agent_id, usage_count DESC"
    );

    let by_agent = {
        let mut stmt = match conn.prepare(&by_agent_sql) {
            Ok(s) => s,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("query prepare failed: {e}")})),
                )
            }
        };

        let rows = stmt
            .query_map([], |row| {
                Ok(json!({
                    "agent_id": row.get::<_, String>(0)?,
                    "skill_id": row.get::<_, String>(1)?,
                    "name": row.get::<_, Option<String>>(2)?,
                    "usage_count": row.get::<_, i64>(3)?,
                }))
            })
            .ok();

        match rows {
            Some(iter) => iter.filter_map(|r| r.ok()).collect::<Vec<_>>(),
            None => Vec::new(),
        }
    };

    (
        StatusCode::OK,
        Json(json!({
            "window": window,
            "overall": overall,
            "byAgent": by_agent,
        })),
    )
}
