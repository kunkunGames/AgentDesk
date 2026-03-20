use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::json;

use super::AppState;

#[derive(Debug, Deserialize)]
pub struct StatsQuery {
    #[serde(rename = "officeId")]
    pub office_id: Option<String>,
}

/// GET /api/stats
pub async fn get_stats(
    State(state): State<AppState>,
    Query(params): Query<StatsQuery>,
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

    // Determine agent filter based on officeId
    let agent_ids: Option<Vec<String>> = if let Some(ref oid) = params.office_id {
        let mut stmt = conn
            .prepare("SELECT agent_id FROM office_agents WHERE office_id = ?1")
            .unwrap();
        let ids: Vec<String> = stmt
            .query_map([oid], |row| row.get(0))
            .ok()
            .map(|iter| iter.filter_map(|r| r.ok()).collect())
            .unwrap_or_default();
        Some(ids)
    } else {
        None
    };

    // Helper: build WHERE clause for agent filtering
    let agent_where = |col: &str| -> String {
        match &agent_ids {
            Some(ids) if !ids.is_empty() => {
                let placeholders: Vec<String> = ids
                    .iter()
                    .map(|id| format!("'{}'", id.replace('\'', "''")))
                    .collect();
                format!("{} IN ({})", col, placeholders.join(","))
            }
            Some(_) => format!("{} = '__none__'", col), // empty office
            None => "1=1".to_string(),
        }
    };

    // ── agents stats ──
    let total: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM agents WHERE {}", agent_where("id")),
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    let working: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM agents WHERE status = 'working' AND {}",
                agent_where("id")
            ),
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    let idle = total - working;

    // ── top_agents (by XP, top 10) ──
    let top_agents = {
        let sql = format!(
            "SELECT id, name, name_ko, avatar_emoji, xp
             FROM agents WHERE {} ORDER BY xp DESC LIMIT 10",
            agent_where("id")
        );
        let mut stmt = conn.prepare(&sql).unwrap();
        let rows: Vec<serde_json::Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "name_ko": row.get::<_, Option<String>>(2)?,
                    "avatar_emoji": row.get::<_, Option<String>>(3)?,
                    "stats_xp": row.get::<_, f64>(4).unwrap_or(0.0) as i64,
                    "stats_tasks_done": 0,
                    "stats_tokens": 0,
                }))
            })
            .ok()
            .map(|iter| iter.filter_map(|r| r.ok()).collect())
            .unwrap_or_default();
        rows
    };

    // ── departments stats ──
    let departments = {
        let dept_filter = if params.office_id.is_some() {
            format!(
                "WHERE d.id IN (SELECT DISTINCT oa.department_id FROM office_agents oa WHERE oa.office_id = '{}' AND oa.department_id IS NOT NULL)",
                params
                    .office_id
                    .as_deref()
                    .unwrap_or("")
                    .replace('\'', "''")
            )
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT d.id, d.name, d.name_ko, d.icon, d.color,
                    (SELECT COUNT(*) FROM agents a WHERE a.department = d.id AND {agent_filter}) as total_agents,
                    (SELECT COUNT(*) FROM agents a WHERE a.department = d.id AND a.status = 'working' AND {agent_filter}) as working_agents,
                    (SELECT COALESCE(SUM(a.xp), 0) FROM agents a WHERE a.department = d.id AND {agent_filter}) as sum_xp
             FROM departments d {dept_filter}
             ORDER BY d.sort_order, d.id",
            agent_filter = agent_where("a.id"),
            dept_filter = dept_filter,
        );
        let mut stmt = conn.prepare(&sql).unwrap();
        let rows: Vec<serde_json::Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, Option<String>>(1)?,
                    "name_ko": row.get::<_, Option<String>>(2)?,
                    "icon": row.get::<_, Option<String>>(3)?,
                    "color": row.get::<_, Option<String>>(4)?,
                    "total_agents": row.get::<_, i64>(5).unwrap_or(0),
                    "working_agents": row.get::<_, i64>(6).unwrap_or(0),
                    "sum_xp": row.get::<_, i64>(7).unwrap_or(0),
                }))
            })
            .ok()
            .map(|iter| iter.filter_map(|r| r.ok()).collect())
            .unwrap_or_default();
        rows
    };

    // ── dispatched_count ──
    let dispatched_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sessions WHERE active_dispatch_id IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    // ── kanban stats ──
    let kanban = {
        let open_total: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM kanban_cards WHERE status NOT IN ('done', 'cancelled')",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        let review_queue: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM kanban_cards WHERE status = 'review'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        let blocked: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM kanban_cards WHERE status = 'blocked'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        let failed: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM kanban_cards WHERE status = 'failed'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        // by_status
        let mut by_status = serde_json::Map::new();
        let statuses = [
            "backlog",
            "ready",
            "requested",
            "in_progress",
            "review",
            "blocked",
            "failed",
            "done",
            "cancelled",
        ];
        for status in &statuses {
            let count: i64 = conn
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM kanban_cards WHERE status = '{}'",
                        status
                    ),
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            by_status.insert(status.to_string(), json!(count));
        }

        // top_repos
        let top_repos = {
            let mut stmt = conn
                .prepare(
                    "SELECT repo_id, COUNT(*) as cnt FROM kanban_cards
                     WHERE repo_id IS NOT NULL AND status NOT IN ('done', 'cancelled')
                     GROUP BY repo_id ORDER BY cnt DESC LIMIT 5",
                )
                .unwrap();
            let rows: Vec<serde_json::Value> = stmt
                .query_map([], |row| {
                    Ok(json!({
                        "github_repo": row.get::<_, String>(0)?,
                        "open_count": row.get::<_, i64>(1)?,
                    }))
                })
                .ok()
                .map(|iter| iter.filter_map(|r| r.ok()).collect())
                .unwrap_or_default();
            rows
        };

        json!({
            "open_total": open_total,
            "review_queue": review_queue,
            "blocked": blocked,
            "failed": failed,
            "waiting_acceptance": 0,
            "stale_in_progress": 0,
            "by_status": by_status,
            "top_repos": top_repos,
        })
    };

    // ── github_closed_today ──
    let github_closed_today: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM kanban_cards WHERE status = 'done' AND date(updated_at) = date('now') AND github_issue_url IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    (
        StatusCode::OK,
        Json(json!({
            "agents": {
                "total": total,
                "working": working,
                "idle": idle,
                "break": 0,
                "offline": 0,
            },
            "top_agents": top_agents,
            "departments": departments,
            "dispatched_count": dispatched_count,
            "kanban": kanban,
            "github_closed_today": github_closed_today,
        })),
    )
}
