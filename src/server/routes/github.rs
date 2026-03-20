use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::json;

use super::AppState;
use crate::github;

// ── Body types ─────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RegisterRepoBody {
    pub id: String,
}

// ── Handlers ───────────────────────────────────────────────────

/// GET /api/github/repos
pub async fn list_repos(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    match github::list_repos(&state.db) {
        Ok(repos) => {
            let items: Vec<serde_json::Value> = repos
                .into_iter()
                .map(|r| {
                    json!({
                        "id": r.id,
                        "display_name": r.display_name,
                        "sync_enabled": r.sync_enabled,
                        "last_synced_at": r.last_synced_at,
                    })
                })
                .collect();
            (StatusCode::OK, Json(json!({"repos": items})))
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))),
    }
}

/// POST /api/github/repos
pub async fn register_repo(
    State(state): State<AppState>,
    Json(body): Json<RegisterRepoBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    if body.id.is_empty() || !body.id.contains('/') {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "id must be in 'owner/repo' format"})),
        );
    }

    match github::register_repo(&state.db, &body.id) {
        Ok(repo) => (
            StatusCode::CREATED,
            Json(json!({
                "repo": {
                    "id": repo.id,
                    "display_name": repo.display_name,
                    "sync_enabled": repo.sync_enabled,
                    "last_synced_at": repo.last_synced_at,
                }
            })),
        ),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))),
    }
}

/// POST /api/github/repos/:owner/:repo/sync
pub async fn sync_repo(
    State(state): State<AppState>,
    Path((owner, repo)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    let repo_id = format!("{owner}/{repo}");

    // Check repo exists
    {
        let conn = match state.db.lock() {
            Ok(c) => c,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("{e}")})),
                );
            }
        };

        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM github_repos WHERE id = ?1",
                [&repo_id],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if !exists {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("repo '{}' not registered", repo_id)})),
            );
        }
    }

    // Check if gh is available
    if !github::gh_available() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "gh CLI is not available on this system"})),
        );
    }

    // Fetch issues
    let issues = match github::sync::fetch_issues(&repo_id) {
        Ok(i) => i,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": format!("gh fetch failed: {e}")})),
            );
        }
    };

    // Triage new issues
    let triaged = github::triage::triage_new_issues(&state.db, &repo_id, &issues).unwrap_or(0);

    // Sync state
    let sync_result = match github::sync::sync_github_issues_for_repo(&state.db, &repo_id, &issues)
    {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("sync failed: {e}")})),
            );
        }
    };

    (
        StatusCode::OK,
        Json(json!({
            "synced": true,
            "repo": repo_id,
            "issues_fetched": issues.len(),
            "cards_created": triaged,
            "cards_closed": sync_result.closed_count,
            "inconsistencies": sync_result.inconsistency_count,
        })),
    )
}
