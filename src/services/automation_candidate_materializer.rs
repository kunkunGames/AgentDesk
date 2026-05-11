use std::path::{Component, Path};

use sqlx::{PgPool, Row};

use crate::db::automation_candidates::{
    InsertIterationParams, IterationRecord, MaterializeCandidateCardParams,
    MaterializedCandidateCard, MetricDirection, approve_candidate_card_pg, compute_verdict,
    create_child_candidate_card_pg, insert_iteration_pg, is_final_iteration,
    list_iterations_for_card_pg, load_card_final_gate_pg, load_card_program_pg,
    load_card_repo_dir_pg, materialize_candidate_card_pg, transition_card_status_pg,
    update_card_program_current_iteration_pg,
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

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MaterializeCandidateInput {
    pub title: String,
    pub repo_id: Option<String>,
    pub priority: Option<String>,
    pub assigned_agent_id: Option<String>,
    pub description: Option<String>,
    pub source: Option<String>,
    pub dedupe_key: Option<String>,
    #[serde(default)]
    pub start_ready: bool,
    pub program: CandidateProgramInput,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct CandidateProgramInput {
    pub repo_dir: String,
    pub allowed_write_paths: Vec<String>,
    pub metric_name: String,
    pub metric_target: f64,
    pub metric_direction: Option<String>,
    pub final_gate: Option<String>,
    pub iteration_budget: Option<i32>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MaterializeCandidateOutput {
    pub card_id: String,
    pub created: bool,
    pub status: String,
    pub pipeline_stage_id: &'static str,
    pub loop_enabled: bool,
    pub start_ready: bool,
    pub discriminator: AutomationCandidateDiscriminator,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AutomationCandidateDiscriminator {
    pub pipeline_stage_id: &'static str,
    pub metadata_enabled_path: &'static str,
    pub required_program_fields: Vec<&'static str>,
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

#[derive(Debug, Clone, serde::Serialize)]
pub struct ApproveCandidateOutput {
    pub final_gate: String,
    pub effective_final_gate: String,
    pub next_action: String,
    pub side_effect_simulation: SideEffectSimulation,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SideEffectSimulation {
    pub safe_for_auto_apply: bool,
    pub checks: Vec<SideEffectCheck>,
    pub latest_iteration: Option<i32>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SideEffectCheck {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
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

    pub async fn materialize_candidate(
        &self,
        input: MaterializeCandidateInput,
    ) -> Result<MaterializeCandidateOutput, MaterializerError> {
        let title = input.title.trim();
        if title.is_empty() {
            return Err(MaterializerError::MissingProgram(
                "title is required".to_string(),
            ));
        }

        let metadata = normalize_candidate_metadata(&input)?;
        let metadata_json = serde_json::to_string(&metadata)
            .map_err(|error| MaterializerError::Database(error.to_string()))?;

        let MaterializedCandidateCard {
            card_id,
            created,
            status,
        } = materialize_candidate_card_pg(
            &self.pool,
            MaterializeCandidateCardParams {
                title: title.to_string(),
                repo_id: input.repo_id.clone(),
                priority: input.priority.clone(),
                assigned_agent_id: input.assigned_agent_id.clone(),
                description: input.description.clone(),
                metadata_json,
                dedupe_key: input.dedupe_key.clone(),
                start_ready: input.start_ready,
            },
        )
        .await
        .map_err(MaterializerError::Database)?;

        Ok(MaterializeCandidateOutput {
            card_id,
            created,
            status,
            pipeline_stage_id: "automation-candidate",
            loop_enabled: true,
            start_ready: input.start_ready,
            discriminator: automation_candidate_discriminator(),
        })
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
        let metric_direction = MetricDirection::parse(
            program
                .get("metric_direction")
                .or_else(|| program.get("direction"))
                .and_then(|v| v.as_str()),
        );
        let verdict = compute_verdict(
            input.metric_before,
            input.metric_after,
            is_simplification,
            &input.status,
            metric_direction,
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
            "keep"
                if is_final_iteration(input.iteration)
                    || is_final_program_iteration(input.iteration, &program) =>
            {
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

    pub async fn approve_candidate(
        &self,
        card_id: &str,
    ) -> Result<ApproveCandidateOutput, MaterializerError> {
        if load_card_program_pg(&self.pool, card_id)
            .await
            .map_err(MaterializerError::Database)?
            .is_none()
        {
            return Err(MaterializerError::CardNotFound);
        }

        approve_candidate_card_pg(&self.pool, card_id)
            .await
            .map_err(MaterializerError::Database)?;

        let final_gate = load_card_final_gate_pg(&self.pool, card_id)
            .await
            .map_err(MaterializerError::Database)?
            .unwrap_or_else(|| "manual_review".to_string());

        let simulation = self.simulate_side_effects(card_id).await?;
        let effective_final_gate =
            if final_gate == "auto_apply_after_green" && simulation.safe_for_auto_apply {
                "auto_apply_after_green"
            } else {
                "manual_review"
            }
            .to_string();
        let next_action = if effective_final_gate == "auto_apply_after_green" {
            "monitor_ci_and_merge"
        } else {
            "await_manual_merge"
        }
        .to_string();

        Ok(ApproveCandidateOutput {
            final_gate,
            effective_final_gate,
            next_action,
            side_effect_simulation: simulation,
        })
    }

    async fn simulate_side_effects(
        &self,
        card_id: &str,
    ) -> Result<SideEffectSimulation, MaterializerError> {
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

        let iterations = list_iterations_for_card_pg(&self.pool, card_id)
            .await
            .map_err(MaterializerError::Database)?;
        let latest = iterations.last();
        let mut checks = Vec::new();

        checks.push(SideEffectCheck {
            name: "latest_iteration_kept",
            passed: latest.map(|r| r.status == "keep").unwrap_or(false),
            detail: latest
                .map(|r| format!("latest iteration {} status={}", r.iteration, r.status))
                .unwrap_or_else(|| "no iteration result recorded".to_string()),
        });

        let paths_used = latest
            .map(|r| r.allowed_write_paths_used.clone())
            .unwrap_or_default();
        let all_paths_allowed = !paths_used.is_empty()
            && paths_used.iter().all(|path| {
                allowed_write_paths
                    .iter()
                    .any(|allowed| allowed_path_matches(path, allowed))
            });
        checks.push(SideEffectCheck {
            name: "paths_within_contract",
            passed: all_paths_allowed,
            detail: if paths_used.is_empty() {
                "latest iteration did not report changed paths".to_string()
            } else {
                format!("{} reported paths checked", paths_used.len())
            },
        });

        let high_risk_paths: Vec<String> = paths_used
            .iter()
            .filter(|path| is_high_risk_auto_apply_path(path))
            .cloned()
            .collect();
        checks.push(SideEffectCheck {
            name: "no_high_risk_paths",
            passed: high_risk_paths.is_empty(),
            detail: if high_risk_paths.is_empty() {
                "no high-risk paths reported".to_string()
            } else {
                format!(
                    "high-risk paths require manual review: {}",
                    high_risk_paths.join(", ")
                )
            },
        });

        let safe_for_auto_apply = checks.iter().all(|check| check.passed);
        Ok(SideEffectSimulation {
            safe_for_auto_apply,
            checks,
            latest_iteration: latest.map(|r| r.iteration),
        })
    }
}

fn normalize_candidate_metadata(
    input: &MaterializeCandidateInput,
) -> Result<serde_json::Value, MaterializerError> {
    let repo_dir = input.program.repo_dir.trim();
    if repo_dir.is_empty() {
        return Err(MaterializerError::MissingProgram(
            "program.repo_dir is required".to_string(),
        ));
    }

    let allowed_write_paths = normalize_allowed_paths(&input.program.allowed_write_paths)?;
    let metric_name = input.program.metric_name.trim();
    if metric_name.is_empty() {
        return Err(MaterializerError::MissingProgram(
            "program.metric_name is required".to_string(),
        ));
    }
    if !input.program.metric_target.is_finite() {
        return Err(MaterializerError::MissingProgram(
            "program.metric_target must be finite".to_string(),
        ));
    }

    let metric_direction = match input
        .program
        .metric_direction
        .as_deref()
        .unwrap_or("lower_is_better")
        .trim()
    {
        "lower" | "lower_is_better" | "minimize" | "min" => "lower_is_better",
        "higher" | "higher_is_better" | "maximize" | "max" => "higher_is_better",
        other => {
            return Err(MaterializerError::MissingProgram(format!(
                "unsupported program.metric_direction: {other}"
            )));
        }
    };

    let final_gate = match input
        .program
        .final_gate
        .as_deref()
        .unwrap_or("manual_review")
        .trim()
    {
        "manual_review" => "manual_review",
        "auto_apply_after_green" => "auto_apply_after_green",
        other => {
            return Err(MaterializerError::MissingProgram(format!(
                "unsupported program.final_gate: {other}"
            )));
        }
    };

    let iteration_budget = input.program.iteration_budget.unwrap_or(3).clamp(1, 10);
    Ok(serde_json::json!({
        "automation_candidate": {
            "enabled": true,
            "loop_enabled": true,
            "source": input.source.as_deref().unwrap_or("user"),
            "dedupe_key": input.dedupe_key.as_deref().unwrap_or(""),
        },
        "program": {
            "repo_dir": repo_dir,
            "allowed_write_paths": allowed_write_paths,
            "metric_name": metric_name,
            "metric_target": input.program.metric_target,
            "metric_direction": metric_direction,
            "final_gate": final_gate,
            "iteration_budget": iteration_budget,
            "current_iteration": 0,
            "description": input.description.as_deref().unwrap_or(input.title.as_str()),
        },
    }))
}

fn normalize_allowed_paths(paths: &[String]) -> Result<Vec<String>, MaterializerError> {
    let mut normalized = Vec::new();
    for path in paths {
        let trimmed = path.trim();
        if trimmed.is_empty() {
            continue;
        }
        let candidate = Path::new(trimmed);
        if !is_clean_relative_path(candidate) {
            return Err(MaterializerError::AllowedPathsViolation {
                path: trimmed.to_string(),
            });
        }
        if !normalized.iter().any(|p: &String| p == trimmed) {
            normalized.push(trimmed.to_string());
        }
    }
    if normalized.is_empty() {
        return Err(MaterializerError::MissingProgram(
            "program.allowed_write_paths must be non-empty clean relative paths".to_string(),
        ));
    }
    Ok(normalized)
}

fn automation_candidate_discriminator() -> AutomationCandidateDiscriminator {
    AutomationCandidateDiscriminator {
        pipeline_stage_id: "automation-candidate",
        metadata_enabled_path: "metadata.automation_candidate.enabled",
        required_program_fields: vec![
            "repo_dir",
            "allowed_write_paths",
            "metric_name",
            "metric_target",
            "metric_direction",
            "final_gate",
            "iteration_budget",
        ],
    }
}

fn is_final_program_iteration(iteration: i32, program: &serde_json::Value) -> bool {
    let budget = program
        .get("iteration_budget")
        .and_then(|v| v.as_i64())
        .unwrap_or(10)
        .clamp(1, 10) as i32;
    iteration >= budget
}

fn allowed_path_matches(path: &str, allowed: &str) -> bool {
    let path = Path::new(path);
    let allowed = Path::new(allowed);
    is_clean_relative_path(path) && is_clean_relative_path(allowed) && path.starts_with(allowed)
}

fn is_high_risk_auto_apply_path(path: &str) -> bool {
    path.starts_with("migrations/")
        || path.starts_with("scripts/deploy")
        || path.starts_with(".github/workflows/")
        || path.contains("secrets")
        || path.ends_with(".pem")
        || path.ends_with(".key")
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
    use super::{
        CandidateProgramInput, MaterializeCandidateInput, allowed_path_matches,
        is_final_program_iteration, normalize_candidate_metadata,
    };

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

    #[test]
    fn materialized_metadata_marks_loop_candidate() {
        let metadata = normalize_candidate_metadata(&MaterializeCandidateInput {
            title: "candidate".to_string(),
            repo_id: None,
            priority: None,
            assigned_agent_id: None,
            description: Some("desc".to_string()),
            source: Some("routine_recommender".to_string()),
            dedupe_key: Some("pattern:1".to_string()),
            start_ready: false,
            program: CandidateProgramInput {
                repo_dir: "/repo".to_string(),
                allowed_write_paths: vec!["src/services".to_string()],
                metric_name: "failure_count".to_string(),
                metric_target: 0.0,
                metric_direction: Some("lower".to_string()),
                final_gate: Some("manual_review".to_string()),
                iteration_budget: Some(4),
            },
        })
        .expect("valid metadata");

        assert_eq!(
            metadata["automation_candidate"]["enabled"],
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            metadata["automation_candidate"]["loop_enabled"],
            serde_json::Value::Bool(true)
        );
        assert_eq!(metadata["program"]["metric_direction"], "lower_is_better");
        assert_eq!(
            metadata["program"]["iteration_budget"],
            serde_json::json!(4)
        );
    }

    #[test]
    fn program_iteration_budget_clamps() {
        assert!(is_final_program_iteration(
            3,
            &serde_json::json!({"iteration_budget": 3})
        ));
        assert!(!is_final_program_iteration(
            2,
            &serde_json::json!({"iteration_budget": 3})
        ));
        assert!(is_final_program_iteration(
            10,
            &serde_json::json!({"iteration_budget": 99})
        ));
    }
}
