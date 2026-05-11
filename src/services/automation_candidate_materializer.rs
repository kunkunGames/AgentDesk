use std::path::{Component, Path};

use sqlx::{PgPool, Row};

use crate::db::automation_candidates::{
    InsertIterationParams, IterationRecord, approve_candidate_card_pg, compute_verdict,
    create_child_candidate_card_pg, insert_iteration_pg, is_final_iteration,
    list_iterations_for_card_pg, load_card_final_gate_pg, load_card_program_pg,
    load_card_repo_dir_pg, transition_card_status_pg, update_card_program_current_iteration_pg,
};
use crate::services::git::{
    automation_branch_name, ensure_automation_worktree, find_automation_worktree,
    remove_automation_worktree,
};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct IterationResultInput {
    pub iteration: i32,
    pub branch: String,
    pub commit_hash: Option<String>,
    pub metric_before: Option<f64>,
    pub metric_after: Option<f64>,
    pub is_simplification: Option<bool>,
    pub status: String,
    pub description: Option<String>,
    pub allowed_write_paths_used: Option<Vec<String>>,
    pub run_seconds: Option<i32>,
    pub crash_trace: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct IterationResultOutput {
    pub record: IterationRecordView,
    pub verdict: &'static str,
    pub action: MaterializerAction,
    pub child_card_id: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializerAction {
    /// Iteration kept; executor-v2 will drive the next one.
    KeepContinue,
    /// All iterations exhausted; card moved to review for final gate.
    KeepFinalGate,
    /// Iteration discarded; current card → review, child card created for retry.
    DiscardRequeue,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct IterationRecordView {
    pub id: String,
    pub card_id: String,
    pub iteration: i32,
    pub branch: String,
    pub commit_hash: Option<String>,
    pub metric_before: Option<f64>,
    pub metric_after: Option<f64>,
    pub is_simplification: bool,
    pub status: String,
    pub description: Option<String>,
    pub allowed_write_paths_used: Vec<String>,
    pub run_seconds: Option<i32>,
    pub crash_trace: Option<String>,
    pub created_at: String,
}

impl From<IterationRecord> for IterationRecordView {
    fn from(r: IterationRecord) -> Self {
        Self {
            id: r.id,
            card_id: r.card_id,
            iteration: r.iteration,
            branch: r.branch,
            commit_hash: r.commit_hash,
            metric_before: r.metric_before,
            metric_after: r.metric_after,
            is_simplification: r.is_simplification,
            status: r.status,
            description: r.description,
            allowed_write_paths_used: r.allowed_write_paths_used,
            run_seconds: r.run_seconds,
            crash_trace: r.crash_trace,
            created_at: r.created_at.to_rfc3339(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PrepareWorktreeOutput {
    pub path: String,
    pub branch: String,
    pub commit: String,
    pub created: bool,
}

#[derive(Debug)]
pub enum MaterializerError {
    CardNotFound,
    MissingProgram(String),
    AllowedPathsViolation { path: String },
    DuplicateIteration,
    WorktreeError(String),
    Database(String),
}

impl std::fmt::Display for MaterializerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CardNotFound => write!(f, "card not found"),
            Self::MissingProgram(msg) => write!(f, "program contract missing: {msg}"),
            Self::AllowedPathsViolation { path } => {
                write!(f, "path '{path}' is not in allowed_write_paths")
            }
            Self::DuplicateIteration => {
                write!(f, "iteration result already exists for this card/iteration")
            }
            Self::WorktreeError(msg) => write!(f, "worktree error: {msg}"),
            Self::Database(msg) => write!(f, "database error: {msg}"),
        }
    }
}

pub struct AutomationCandidateMaterializer {
    pool: PgPool,
}

impl AutomationCandidateMaterializer {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn submit_iteration_result(
        &self,
        card_id: &str,
        input: IterationResultInput,
    ) -> Result<IterationResultOutput, MaterializerError> {
        // 1. Load card and program contract
        let program = load_card_program_pg(&self.pool, card_id)
            .await
            .map_err(MaterializerError::Database)?
            .ok_or_else(|| {
                MaterializerError::MissingProgram(
                    "card not found or has no metadata.program".to_string(),
                )
            })?;

        let allowed_write_paths: Vec<String> = program
            .get("allowed_write_paths")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        if allowed_write_paths.is_empty() {
            return Err(MaterializerError::MissingProgram(
                "allowed_write_paths must be non-empty".to_string(),
            ));
        }

        // 2. Validate allowed_write_paths_used against contract
        let paths_used = input.allowed_write_paths_used.clone().unwrap_or_default();
        for path in &paths_used {
            if !allowed_write_paths
                .iter()
                .any(|allowed| allowed_path_matches(path, allowed))
            {
                return Err(MaterializerError::AllowedPathsViolation { path: path.clone() });
            }
        }

        // 3. Compute deterministic verdict
        let is_simplification = input.is_simplification.unwrap_or(false);
        let verdict = compute_verdict(
            input.metric_before,
            input.metric_after,
            is_simplification,
            &input.status,
        );

        // 4. Insert iteration record
        let record = insert_iteration_pg(
            &self.pool,
            InsertIterationParams {
                card_id: card_id.to_string(),
                iteration: input.iteration,
                branch: input.branch.clone(),
                commit_hash: input.commit_hash.clone(),
                metric_before: input.metric_before,
                metric_after: input.metric_after,
                is_simplification,
                status: verdict.to_string(),
                description: input.description.clone(),
                allowed_write_paths_used: paths_used,
                run_seconds: input.run_seconds,
                crash_trace: input.crash_trace.clone(),
            },
        )
        .await
        .map_err(|error| {
            if error.contains("unique") || error.contains("duplicate") || error.contains("23505") {
                MaterializerError::DuplicateIteration
            } else {
                MaterializerError::Database(error)
            }
        })?;

        // 5. Act on verdict
        let (action, child_card_id) = match verdict {
            "keep" if is_final_iteration(input.iteration) => {
                update_card_program_current_iteration_pg(&self.pool, card_id, input.iteration)
                    .await
                    .map_err(MaterializerError::Database)?;
                // All iterations done — move to review for final gate
                transition_card_status_pg(&self.pool, card_id, "review")
                    .await
                    .map_err(MaterializerError::Database)?;
                (MaterializerAction::KeepFinalGate, None)
            }
            "keep" => {
                update_card_program_current_iteration_pg(&self.pool, card_id, input.iteration)
                    .await
                    .map_err(MaterializerError::Database)?;
                (MaterializerAction::KeepContinue, None)
            }
            _ => {
                // Discard: transition current card to review, create child ready card
                let card_row = sqlx::query(
                    "SELECT title, metadata::text AS metadata FROM kanban_cards WHERE id = $1",
                )
                .bind(card_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| MaterializerError::Database(e.to_string()))?;

                let (parent_title, parent_metadata) = match card_row {
                    Some(row) => {
                        let title: String =
                            row.try_get("title").unwrap_or_else(|_| card_id.to_string());
                        let meta_raw: Option<String> = row.try_get("metadata").unwrap_or(None);
                        let meta = meta_raw
                            .as_deref()
                            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                            .unwrap_or(serde_json::Value::Object(Default::default()));
                        (title, meta)
                    }
                    None => return Err(MaterializerError::CardNotFound),
                };

                // Best-effort worktree cleanup for the discarded iteration.
                if let Some(repo_dir) = program
                    .get("repo_dir")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                {
                    let branch = automation_branch_name(card_id, input.iteration);
                    if let Some(wt) = find_automation_worktree(repo_dir, card_id, input.iteration) {
                        let _ = remove_automation_worktree(repo_dir, &wt.path, &branch);
                    }
                }

                transition_card_status_pg(&self.pool, card_id, "review")
                    .await
                    .map_err(MaterializerError::Database)?;

                let child_id = create_child_candidate_card_pg(
                    &self.pool,
                    card_id,
                    &parent_title,
                    input.iteration + 1,
                    &parent_metadata,
                )
                .await
                .map_err(MaterializerError::Database)?;

                (MaterializerAction::DiscardRequeue, Some(child_id))
            }
        };

        Ok(IterationResultOutput {
            record: record.into(),
            verdict,
            action,
            child_card_id,
        })
    }

    pub async fn list_iterations(&self, card_id: &str) -> Result<Vec<IterationRecordView>, String> {
        list_iterations_for_card_pg(&self.pool, card_id)
            .await
            .map(|records| records.into_iter().map(IterationRecordView::from).collect())
    }

    /// Prepare (create or find) a git worktree for `card_id` at `iteration`.
    ///
    /// Requires `metadata.program.repo_dir` to be set on the card.
    pub async fn prepare_worktree(
        &self,
        card_id: &str,
        iteration: i32,
    ) -> Result<PrepareWorktreeOutput, MaterializerError> {
        if iteration < 1 {
            return Err(MaterializerError::MissingProgram(
                "iteration must be >= 1".to_string(),
            ));
        }

        let repo_dir = load_card_repo_dir_pg(&self.pool, card_id)
            .await
            .map_err(MaterializerError::Database)?
            .ok_or_else(|| {
                MaterializerError::MissingProgram(
                    "metadata.program.repo_dir is required for worktree isolation".to_string(),
                )
            })?;

        if repo_dir.trim().is_empty() {
            return Err(MaterializerError::MissingProgram(
                "metadata.program.repo_dir must not be empty".to_string(),
            ));
        }

        let info = ensure_automation_worktree(&repo_dir, card_id, iteration)
            .map_err(MaterializerError::WorktreeError)?;

        Ok(PrepareWorktreeOutput {
            path: info.path,
            branch: info.branch,
            commit: info.commit,
            created: info.created,
        })
    }

    pub async fn approve_candidate(&self, card_id: &str) -> Result<String, MaterializerError> {
        approve_candidate_card_pg(&self.pool, card_id)
            .await
            .map_err(MaterializerError::Database)?;

        let final_gate = load_card_final_gate_pg(&self.pool, card_id)
            .await
            .map_err(MaterializerError::Database)?
            .unwrap_or_else(|| "manual_review".to_string());

        Ok(final_gate)
    }
}

fn allowed_path_matches(path: &str, allowed: &str) -> bool {
    let path = Path::new(path);
    let allowed = Path::new(allowed);
    is_clean_relative_path(path) && is_clean_relative_path(allowed) && path.starts_with(allowed)
}

fn is_clean_relative_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

#[cfg(test)]
mod allowed_path_tests {
    use super::allowed_path_matches;

    #[test]
    fn accepts_exact_and_child_paths() {
        assert!(allowed_path_matches("src/foo", "src/foo"));
        assert!(allowed_path_matches("src/foo/bar.rs", "src/foo"));
    }

    #[test]
    fn rejects_prefix_siblings_and_traversal() {
        assert!(!allowed_path_matches("src/foo2/bar.rs", "src/foo"));
        assert!(!allowed_path_matches("src/foo/../bar.rs", "src/foo"));
        assert!(!allowed_path_matches("../src/foo.rs", "src"));
    }

    #[test]
    fn rejects_absolute_or_empty_paths() {
        assert!(!allowed_path_matches("/src/foo.rs", "src"));
        assert!(!allowed_path_matches("src/foo.rs", ""));
        assert!(!allowed_path_matches("", "src"));
    }
}
