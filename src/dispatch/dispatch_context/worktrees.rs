//! Card-owned worktree resolution and admission for issue and internal tasks.

use anyhow::Result;
use sqlx::PgPool;

use super::{load_card_issue_repo_pg, resolve_card_repo_dir_with_context_pg};

pub(crate) async fn resolve_card_worktree(
    pool: &PgPool,
    card_id: &str,
    context: Option<&serde_json::Value>,
) -> Result<Option<(String, String, String)>> {
    let Some((issue_number, _repo_id)) = load_card_issue_repo_pg(pool, card_id).await else {
        return Ok(None);
    };
    let Some(repo_dir) =
        resolve_card_repo_dir_with_context_pg(pool, card_id, context, "resolve worktree repo")
            .await?
    else {
        return Ok(None);
    };
    let worktree = match issue_number {
        Some(issue) => crate::services::platform::find_worktree_for_issue(&repo_dir, issue),
        None => crate::services::git::worktree_resolver::find_worktree_for_card(&repo_dir, card_id),
    };
    Ok(worktree.map(|wt| (wt.path, wt.branch, wt.commit)))
}

pub(crate) async fn ensure_card_worktree(
    pool: &PgPool,
    card_id: &str,
    context: Option<&serde_json::Value>,
) -> Result<Option<(String, String, String, bool)>> {
    let Some((issue_number, _repo_id)) = load_card_issue_repo_pg(pool, card_id).await else {
        return Ok(None);
    };
    if issue_number.is_none() {
        anyhow::ensure!(
            crate::services::settings::internal_tasks_enabled(pool)
                .await
                .map_err(anyhow::Error::msg)?,
            "GitHub issue required: enable internalTasksEnabled to admit internal Kanban work"
        );
    }
    let Some(repo_dir) =
        resolve_card_repo_dir_with_context_pg(pool, card_id, context, "create worktree repo")
            .await?
    else {
        return Ok(None);
    };
    let owned_card = card_id.to_owned();
    let ensured = tokio::task::spawn_blocking(move || match issue_number {
        Some(issue) => {
            crate::services::platform::shell::ensure_worktree_for_issue(&repo_dir, issue)
        }
        None => crate::services::git::worktree_resolver::ensure_worktree_for_card(
            &repo_dir,
            &owned_card,
        ),
    })
    .await
    .map_err(|error| anyhow::anyhow!("Worktree worker failed for card {card_id}: {error}"))?
    .map_err(|error| anyhow::anyhow!("Cannot create worktree for card {card_id}: {error}"))?;
    Ok(Some((
        ensured.path,
        ensured.branch,
        ensured.commit,
        ensured.created,
    )))
}
