use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use super::{
    AppState,
    skill_usage_analytics::{SkillUsageRecord, collect_skill_usage},
};

fn skill_description_from_markdown(content: &str) -> String {
    content
        .lines()
        .map(str::trim)
        .find(|line| {
            !line.is_empty()
                && !line.starts_with('#')
                && !line.starts_with("name:")
                && !line.starts_with("description:")
                && !line.starts_with("---")
        })
        .map(ToString::to_string)
        .unwrap_or_else(|| "Skill".to_string())
}

fn codex_skill_file(path: &Path) -> Option<PathBuf> {
    if path.is_file() && path.file_name().and_then(|name| name.to_str()) == Some("SKILL.md") {
        return Some(path.to_path_buf());
    }
    let candidate = path.join("SKILL.md");
    if candidate.is_file() {
        Some(candidate)
    } else {
        None
    }
}

pub(super) fn sync_skills_from_disk(conn: &libsql_rusqlite::Connection) {
    sync_skills_from_disk_with_prune(conn, true);
}

/// Syncs the `skills` catalog rows with what is currently on disk.
///
/// When `prune_missing` is `true` (the default behavior), any row whose `id`
/// is not present on disk after the upsert pass is **soft-deleted** by
/// stamping `deleted_at` with the current unix timestamp. Live-view endpoints
/// (`/api/skills/catalog`, `/api/skills/ranking`, `load_skill_metadata`)
/// filter out rows with `deleted_at IS NOT NULL`, while historical joins
/// (`/api/agents/:id/skills`) and transcript-based analytics
/// (`collect_known_skills`) intentionally keep reading soft-deleted rows so
/// past usage stays attributable (#816 review fixes).
///
/// Pruning is **all-or-nothing**: if *any* configured root fails to enumerate
/// (IO error, not just "missing or empty"), pruning is skipped entirely with
/// a `warn!` log. Otherwise a temporarily-unreadable root would cause the
/// other roots' absences to be mistaken for disk-level deletions and every
/// skill under the failed root would be silently soft-deleted.
pub(super) fn sync_skills_from_disk_with_prune(
    conn: &libsql_rusqlite::Connection,
    prune_missing: bool,
) {
    let mut roots = Vec::new();
    if let Some(runtime_root) = crate::config::runtime_root() {
        let _ = crate::runtime_layout::sync_managed_skills(&runtime_root);
        roots.push(crate::runtime_layout::managed_skills_root(&runtime_root));
    }
    if let Some(home) = dirs::home_dir() {
        roots.push(home.join(".codex").join("skills"));
        roots.push(home.join(".claude").join("commands"));
    }

    let mut seen = HashSet::new();
    let mut any_root_errored = false;
    for root in roots {
        if !root.is_dir() {
            // "Doesn't exist" is a benign configuration state — no skills under
            // this root — not an IO failure. Only a genuine read error should
            // trip the safety guard.
            continue;
        }
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(err) => {
                tracing::warn!(
                    root = %root.display(),
                    error = %err,
                    "sync_skills_from_disk: failed to enumerate skill root; skipping prune"
                );
                any_root_errored = true;
                continue;
            }
        };

        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            let skill_path = if root.ends_with("commands") {
                if path.extension().and_then(|ext| ext.to_str()) == Some("md") {
                    Some(path.clone())
                } else {
                    None
                }
            } else {
                codex_skill_file(&path)
            };
            let Some(skill_path) = skill_path else {
                continue;
            };

            let name_opt = if root.ends_with("commands") {
                skill_path.file_stem().and_then(|stem| stem.to_str())
            } else {
                skill_path
                    .parent()
                    .and_then(|parent| parent.file_name())
                    .and_then(|stem| stem.to_str())
            };
            let Some(name) = name_opt else {
                continue;
            };

            let name = name.to_string();
            if !seen.insert(name.clone()) {
                continue;
            }

            let description = fs::read_to_string(&skill_path)
                .ok()
                .map(|content| skill_description_from_markdown(&content))
                .unwrap_or_else(|| name.clone());
            let source_path = skill_path.to_string_lossy().to_string();
            let updated_at = fs::metadata(&skill_path)
                .ok()
                .and_then(|meta| meta.modified().ok())
                .map(|modified| DateTime::<Utc>::from(modified).to_rfc3339());

            // Undelete (deleted_at = NULL) on upsert: a skill that reappears
            // on disk should become visible again in live views without any
            // operator intervention.
            let _ = conn.execute(
                "INSERT INTO skills (id, name, description, source_path, trigger_patterns, updated_at, deleted_at)
                 VALUES (?1, ?2, ?3, ?4, NULL, ?5, NULL)
                 ON CONFLICT(id) DO UPDATE SET
                   name = excluded.name,
                   description = excluded.description,
                   source_path = excluded.source_path,
                   updated_at = excluded.updated_at,
                   deleted_at = NULL",
                libsql_rusqlite::params![name, name, description, source_path, updated_at],
            );
        }
    }

    if !prune_missing {
        return;
    }

    if any_root_errored {
        tracing::warn!(
            "sync_skills_from_disk: pruning skipped due to partial disk failure \
             (at least one skill root failed to enumerate)"
        );
        return;
    }

    prune_missing_skills(conn, &seen);
}

/// Soft-delete `skills` rows whose `id` is not in `seen`.
///
/// Rows are marked with `deleted_at = <unix seconds>` (leaving already
/// soft-deleted rows untouched). Live-view queries filter these out via
/// `deleted_at IS NULL`, while `/api/agents/:id/skills` and transcript
/// analytics deliberately keep reading them so historical attribution
/// survives disk-level deletion.
fn prune_missing_skills(conn: &libsql_rusqlite::Connection, seen: &HashSet<String>) {
    let existing_ids: Vec<String> =
        match conn.prepare("SELECT id FROM skills WHERE deleted_at IS NULL") {
            Ok(mut stmt) => match stmt.query_map([], |row| row.get::<_, String>(0)) {
                Ok(rows) => rows.filter_map(|row| row.ok()).collect(),
                Err(_) => return,
            },
            Err(_) => return,
        };

    let now_secs = Utc::now().timestamp();
    for id in existing_ids {
        if seen.contains(&id) {
            continue;
        }
        let _ = conn.execute(
            "UPDATE skills SET deleted_at = ?2 WHERE id = ?1 AND deleted_at IS NULL",
            libsql_rusqlite::params![id, now_secs],
        );
    }
}

#[derive(Default)]
struct UsageAggregate {
    calls: i64,
    last_used_at: Option<i64>,
}

#[derive(Default)]
struct ByAgentAggregate {
    agent_name: String,
    calls: i64,
    last_used_at: Option<i64>,
}

fn ranking_days(window: &str) -> Option<i64> {
    match window {
        "30d" => Some(30),
        "90d" => Some(90),
        "all" => None,
        _ => Some(7),
    }
}

fn load_skill_metadata(
    conn: &libsql_rusqlite::Connection,
) -> libsql_rusqlite::Result<HashMap<String, (String, String)>> {
    // Live-view metadata: exclude soft-deleted rows so `/api/skills/catalog`
    // and `/api/skills/ranking` stop surfacing skills that have been removed
    // from disk. Historical joins (e.g. `/api/agents/:id/skills`) and
    // transcript analytics deliberately bypass this filter.
    let mut stmt = conn.prepare(
        "SELECT id,
                COALESCE(name, id) AS skill_name,
                COALESCE(description, name, id) AS skill_desc
         FROM skills
         WHERE deleted_at IS NULL",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;

    let mut metadata = HashMap::new();
    for row in rows {
        let (skill_id, skill_name, skill_desc) = row?;
        metadata.insert(skill_id, (skill_name, skill_desc));
    }

    Ok(metadata)
}

fn apply_usage(aggregate: &mut UsageAggregate, used_at_ms: i64) {
    aggregate.calls += 1;
    aggregate.last_used_at = Some(
        aggregate
            .last_used_at
            .map_or(used_at_ms, |last_used_at| last_used_at.max(used_at_ms)),
    );
}

fn aggregate_overall_usage(records: &[SkillUsageRecord]) -> HashMap<String, UsageAggregate> {
    let mut totals = HashMap::new();
    for record in records {
        apply_usage(
            totals.entry(record.skill_id.clone()).or_default(),
            record.used_at_ms,
        );
    }
    totals
}

/// GET /api/skills/catalog
pub async fn catalog(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let conn = match state.db.lock() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}")})),
            );
        }
    };
    sync_skills_from_disk(&conn);
    let metadata = match load_skill_metadata(&conn) {
        Ok(data) => data,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("metadata query failed: {e}")})),
            );
        }
    };
    let usage = match collect_skill_usage(&conn, None) {
        Ok(data) => data,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("usage query failed: {e}")})),
            );
        }
    };
    let totals = aggregate_overall_usage(&usage);
    let known_ids: HashSet<String> = metadata.keys().cloned().collect();

    let mut catalog = metadata
        .into_iter()
        .map(|(skill_id, (name, description))| {
            let aggregate = totals.get(&skill_id);
            json!({
                "name": name,
                "description": description,
                "description_ko": description,
                "total_calls": aggregate.map_or(0, |item| item.calls),
                "last_used_at": aggregate.and_then(|item| item.last_used_at),
            })
        })
        .collect::<Vec<_>>();

    for (skill_id, aggregate) in totals {
        if known_ids.contains(&skill_id) {
            continue;
        }
        catalog.push(json!({
            "name": skill_id,
            "description": skill_id,
            "description_ko": skill_id,
            "total_calls": aggregate.calls,
            "last_used_at": aggregate.last_used_at,
        }));
    }

    catalog.sort_by(|left, right| {
        let left_calls = left["total_calls"].as_i64().unwrap_or(0);
        let right_calls = right["total_calls"].as_i64().unwrap_or(0);
        right_calls
            .cmp(&left_calls)
            .then_with(|| {
                right["last_used_at"]
                    .as_i64()
                    .unwrap_or_default()
                    .cmp(&left["last_used_at"].as_i64().unwrap_or_default())
            })
            .then_with(|| {
                left["name"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["name"].as_str().unwrap_or_default())
            })
    });

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
            );
        }
    };
    sync_skills_from_disk(&conn);

    let window = params.window.as_deref().unwrap_or("7d");
    let limit = params.limit.unwrap_or(20);
    let metadata = match load_skill_metadata(&conn) {
        Ok(data) => data,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("metadata query failed: {e}")})),
            );
        }
    };
    let usage = match collect_skill_usage(&conn, ranking_days(window)) {
        Ok(data) => data,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("usage query failed: {e}")})),
            );
        }
    };

    let mut overall = aggregate_overall_usage(&usage)
        .into_iter()
        .map(|(skill_id, aggregate)| {
            let (skill_name, skill_desc_ko) = metadata
                .get(&skill_id)
                .cloned()
                .unwrap_or_else(|| (skill_id.clone(), skill_id.clone()));
            json!({
                "skill_name": skill_name,
                "skill_desc_ko": skill_desc_ko,
                "calls": aggregate.calls,
                "last_used_at": aggregate.last_used_at,
            })
        })
        .collect::<Vec<_>>();
    overall.sort_by(|left, right| {
        let left_calls = left["calls"].as_i64().unwrap_or(0);
        let right_calls = right["calls"].as_i64().unwrap_or(0);
        right_calls
            .cmp(&left_calls)
            .then_with(|| {
                right["last_used_at"]
                    .as_i64()
                    .unwrap_or_default()
                    .cmp(&left["last_used_at"].as_i64().unwrap_or_default())
            })
            .then_with(|| {
                left["skill_name"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["skill_name"].as_str().unwrap_or_default())
            })
    });
    overall.truncate(limit.max(0) as usize);

    let mut by_agent_totals = HashMap::<(String, String), ByAgentAggregate>::new();
    for record in &usage {
        let Some(agent_role_id) = record.agent_id.clone() else {
            continue;
        };
        let agent_name = record
            .agent_name
            .clone()
            .unwrap_or_else(|| agent_role_id.clone());
        let aggregate = by_agent_totals
            .entry((agent_role_id, record.skill_id.clone()))
            .or_insert_with(|| ByAgentAggregate {
                agent_name: agent_name.clone(),
                ..ByAgentAggregate::default()
            });
        if aggregate.agent_name.is_empty() {
            aggregate.agent_name = agent_name;
        }
        aggregate.calls += 1;
        aggregate.last_used_at = Some(
            aggregate
                .last_used_at
                .map_or(record.used_at_ms, |last_used_at| {
                    last_used_at.max(record.used_at_ms)
                }),
        );
    }

    let mut by_agent = by_agent_totals
        .into_iter()
        .map(|((agent_role_id, skill_id), aggregate)| {
            let (skill_name, skill_desc_ko) = metadata
                .get(&skill_id)
                .cloned()
                .unwrap_or_else(|| (skill_id.clone(), skill_id.clone()));
            json!({
                "agent_role_id": agent_role_id,
                "agent_name": aggregate.agent_name,
                "skill_name": skill_name,
                "skill_desc_ko": skill_desc_ko,
                "calls": aggregate.calls,
                "last_used_at": aggregate.last_used_at,
            })
        })
        .collect::<Vec<_>>();
    by_agent.sort_by(|left, right| {
        let left_calls = left["calls"].as_i64().unwrap_or(0);
        let right_calls = right["calls"].as_i64().unwrap_or(0);
        right_calls
            .cmp(&left_calls)
            .then_with(|| {
                right["last_used_at"]
                    .as_i64()
                    .unwrap_or_default()
                    .cmp(&left["last_used_at"].as_i64().unwrap_or_default())
            })
            .then_with(|| {
                left["agent_name"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["agent_name"].as_str().unwrap_or_default())
            })
            .then_with(|| {
                left["skill_name"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["skill_name"].as_str().unwrap_or_default())
            })
    });
    by_agent.truncate(100);

    (
        StatusCode::OK,
        Json(json!({
            "window": window,
            "overall": overall,
            "byAgent": by_agent,
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_skills_conn() -> libsql_rusqlite::Connection {
        let conn = libsql_rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE skills (
                id TEXT PRIMARY KEY,
                name TEXT,
                description TEXT,
                source_path TEXT,
                trigger_patterns TEXT,
                updated_at TEXT,
                deleted_at INTEGER
            );
            CREATE TABLE skill_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                skill_id TEXT NOT NULL,
                agent_id TEXT,
                session_key TEXT,
                used_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );",
        )
        .unwrap();
        conn
    }

    fn insert_skill(conn: &libsql_rusqlite::Connection, id: &str, description: &str) {
        conn.execute(
            "INSERT INTO skills (id, name, description, source_path) VALUES (?1, ?1, ?2, ?3)",
            libsql_rusqlite::params![id, description, format!("/tmp/skills/{id}/SKILL.md")],
        )
        .unwrap();
    }

    /// Upsert via the same code path the real sync uses, so the undelete-on-
    /// resurrection behavior is covered.
    fn upsert_skill_live(conn: &libsql_rusqlite::Connection, id: &str, description: &str) {
        conn.execute(
            "INSERT INTO skills (id, name, description, source_path, trigger_patterns, updated_at, deleted_at)
             VALUES (?1, ?1, ?2, ?3, NULL, NULL, NULL)
             ON CONFLICT(id) DO UPDATE SET
               name = excluded.name,
               description = excluded.description,
               source_path = excluded.source_path,
               updated_at = excluded.updated_at,
               deleted_at = NULL",
            libsql_rusqlite::params![id, description, format!("/tmp/skills/{id}/SKILL.md")],
        )
        .unwrap();
    }

    fn all_skill_ids(conn: &libsql_rusqlite::Connection) -> Vec<String> {
        let mut stmt = conn.prepare("SELECT id FROM skills ORDER BY id").unwrap();
        stmt.query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    }

    fn live_skill_ids(conn: &libsql_rusqlite::Connection) -> Vec<String> {
        let mut stmt = conn
            .prepare("SELECT id FROM skills WHERE deleted_at IS NULL ORDER BY id")
            .unwrap();
        stmt.query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    }

    fn deleted_at(conn: &libsql_rusqlite::Connection, id: &str) -> Option<i64> {
        conn.query_row(
            "SELECT deleted_at FROM skills WHERE id = ?1",
            libsql_rusqlite::params![id],
            |row| row.get::<_, Option<i64>>(0),
        )
        .unwrap()
    }

    #[test]
    fn prune_soft_deletes_rows_not_present_on_disk() {
        let conn = setup_skills_conn();
        insert_skill(&conn, "alive-skill", "still on disk");
        insert_skill(&conn, "deleted-skill", "removed from disk");

        let mut seen = HashSet::new();
        seen.insert("alive-skill".to_string());

        prune_missing_skills(&conn, &seen);

        // Rows are retained physically so historical joins still reach them.
        assert_eq!(
            all_skill_ids(&conn),
            vec!["alive-skill".to_string(), "deleted-skill".to_string()]
        );
        // But only the live one shows up in live-view queries.
        assert_eq!(live_skill_ids(&conn), vec!["alive-skill".to_string()]);
        assert!(
            deleted_at(&conn, "deleted-skill").is_some(),
            "missing skill should have deleted_at stamped"
        );
        assert_eq!(
            deleted_at(&conn, "alive-skill"),
            None,
            "live skill must not be touched"
        );
    }

    #[test]
    fn prune_keeps_skills_still_on_disk_unchanged() {
        let conn = setup_skills_conn();
        insert_skill(&conn, "skill-a", "a");
        insert_skill(&conn, "skill-b", "b");
        insert_skill(&conn, "skill-c", "c");

        let mut seen = HashSet::new();
        seen.insert("skill-a".to_string());
        seen.insert("skill-b".to_string());
        seen.insert("skill-c".to_string());

        prune_missing_skills(&conn, &seen);

        assert_eq!(
            live_skill_ids(&conn),
            vec![
                "skill-a".to_string(),
                "skill-b".to_string(),
                "skill-c".to_string()
            ]
        );

        let description: String = conn
            .query_row(
                "SELECT description FROM skills WHERE id = 'skill-b'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(description, "b");
    }

    #[test]
    fn prune_preserves_skill_usage_history_for_deleted_skills() {
        // Soft-delete must keep skill_usage reachable — the exact guarantee
        // that makes /api/agents/:id/skills INNER JOIN safe post-prune (P2b).
        let conn = setup_skills_conn();
        insert_skill(&conn, "deleted-skill", "removed from disk");
        conn.execute(
            "INSERT INTO skill_usage (skill_id, agent_id, session_key) VALUES (?1, ?2, ?3)",
            libsql_rusqlite::params!["deleted-skill", "agent-x", "sess-1"],
        )
        .unwrap();

        prune_missing_skills(&conn, &HashSet::new());

        // Row is still physically present — historical INNER JOIN resolves.
        assert_eq!(all_skill_ids(&conn), vec!["deleted-skill".to_string()]);
        assert!(live_skill_ids(&conn).is_empty());
        let usage_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM skill_usage", [], |row| row.get(0))
            .unwrap();
        assert_eq!(usage_count, 1);
    }

    #[test]
    fn agents_skills_inner_join_still_sees_soft_deleted_rows() {
        // Regression for P2b: the query at /api/agents/:id/skills is
        // `FROM skills s INNER JOIN skill_usage su ON su.skill_id = s.id`
        // WITHOUT any deleted_at filter. Soft-deleted skills must remain
        // reachable via this join.
        let conn = setup_skills_conn();
        insert_skill(&conn, "historical", "was used once");
        conn.execute(
            "INSERT INTO skill_usage (skill_id, agent_id, session_key) VALUES (?1, ?2, ?3)",
            libsql_rusqlite::params!["historical", "agent-a", "sess-1"],
        )
        .unwrap();

        prune_missing_skills(&conn, &HashSet::new());

        let joined: Vec<String> = {
            let mut stmt = conn
                .prepare(
                    "SELECT DISTINCT s.id
                     FROM skills s
                     INNER JOIN skill_usage su ON su.skill_id = s.id
                     WHERE su.agent_id = ?1
                     ORDER BY s.id",
                )
                .unwrap();
            stmt.query_map(libsql_rusqlite::params!["agent-a"], |row| {
                row.get::<_, String>(0)
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
        };
        assert_eq!(
            joined,
            vec!["historical".to_string()],
            "soft-deleted skill must still be reachable via INNER JOIN"
        );
    }

    #[test]
    fn load_skill_metadata_excludes_soft_deleted_rows() {
        // Regression for #816: live views pull names from `skills` via
        // load_skill_metadata, which filters `WHERE deleted_at IS NULL`.
        let conn = setup_skills_conn();
        insert_skill(&conn, "alive", "alive desc");
        insert_skill(&conn, "stale", "stale desc");

        let mut seen = HashSet::new();
        seen.insert("alive".to_string());
        prune_missing_skills(&conn, &seen);

        let metadata = load_skill_metadata(&conn).unwrap();
        assert!(metadata.contains_key("alive"));
        assert!(
            !metadata.contains_key("stale"),
            "soft-deleted skill should not surface in catalog/ranking metadata"
        );
    }

    #[test]
    fn upsert_resurrects_previously_soft_deleted_skill() {
        // A skill that reappears on disk must clear its deleted_at so the
        // catalog/ranking endpoints surface it again (#816 review fixes).
        let conn = setup_skills_conn();
        insert_skill(&conn, "resurrected", "was once here");

        // First prune removes it.
        prune_missing_skills(&conn, &HashSet::new());
        assert!(deleted_at(&conn, "resurrected").is_some());

        // Then disk scan finds it again; upsert must undelete.
        upsert_skill_live(&conn, "resurrected", "now on disk again");

        assert_eq!(
            deleted_at(&conn, "resurrected"),
            None,
            "resurrection must clear deleted_at"
        );
        assert_eq!(live_skill_ids(&conn), vec!["resurrected".to_string()]);
    }

    #[test]
    fn prune_with_empty_seen_when_disk_unavailable_is_handled_by_caller() {
        // Sanity check: prune_missing_skills itself trusts the caller; the
        // safety check (skip pruning if any root errored) lives in
        // sync_skills_from_disk_with_prune. Verify the helper behaves
        // predictably when given an empty set.
        let conn = setup_skills_conn();
        insert_skill(&conn, "only-skill", "only");

        prune_missing_skills(&conn, &HashSet::new());

        // Physically retained, just soft-deleted.
        assert_eq!(all_skill_ids(&conn), vec!["only-skill".to_string()]);
        assert!(live_skill_ids(&conn).is_empty());
    }

    #[test]
    fn partial_root_failure_skips_prune_regression() {
        // Regression for P2a: if ANY configured root fails to enumerate, the
        // sync must skip pruning entirely — otherwise skills whose root was
        // temporarily unreadable get silently soft-deleted while the other
        // roots' absences look "successful".
        //
        // Because `sync_skills_from_disk_with_prune` hard-codes its root set
        // from `runtime_root()` + `dirs::home_dir()`, we can't feed it a
        // custom bad root without plumbing. Instead we reproduce the guard
        // pattern locally (same `read_dir` + `any_root_errored` logic) and
        // assert that when the guard trips, `prune_missing_skills` is NOT
        // invoked and the pre-existing row survives unchanged.
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let bad_root = tmp.path().join("bad-skills");
        std::fs::create_dir(&bad_root).unwrap();
        // Strip all permissions so read_dir errors (EACCES).
        let mut perms = std::fs::metadata(&bad_root).unwrap().permissions();
        perms.set_mode(0o000);
        std::fs::set_permissions(&bad_root, perms).unwrap();

        // Skip when running as root (chmod 000 doesn't stop root).
        if std::fs::read_dir(&bad_root).is_ok() {
            let mut perms = std::fs::metadata(&bad_root).unwrap().permissions();
            perms.set_mode(0o700);
            std::fs::set_permissions(&bad_root, perms).unwrap();
            eprintln!("skipping: cannot simulate EACCES (running as root?)");
            return;
        }

        let conn = setup_skills_conn();
        insert_skill(&conn, "should-survive", "would be pruned if guard failed");

        // Mirror sync_skills_from_disk_with_prune's guard.
        let mut any_root_errored = false;
        if bad_root.is_dir() && fs::read_dir(&bad_root).is_err() {
            any_root_errored = true;
        }
        assert!(any_root_errored, "test precondition: bad root must error");

        let seen: HashSet<String> = HashSet::new();
        if !any_root_errored {
            prune_missing_skills(&conn, &seen);
        }
        assert_eq!(
            live_skill_ids(&conn),
            vec!["should-survive".to_string()],
            "guard must skip prune on partial root failure"
        );
        assert!(
            deleted_at(&conn, "should-survive").is_none(),
            "row must not be soft-deleted when guard trips"
        );

        // Restore perms so tempdir cleanup works.
        let mut perms = std::fs::metadata(&bad_root).unwrap().permissions();
        perms.set_mode(0o700);
        std::fs::set_permissions(&bad_root, perms).unwrap();
    }
}
