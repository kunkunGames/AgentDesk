use crate::db::{
    Db,
    auto_queue::{self, GenerateCandidateRecord, GenerateCardFilter},
};
use crate::services::service_error::{ServiceError, ServiceResult};

#[derive(Clone)]
pub struct AutoQueueService {
    db: Db,
}

#[derive(Debug, Clone, Default)]
pub struct PrepareGenerateInput {
    pub repo: Option<String>,
    pub agent_id: Option<String>,
    pub issue_numbers: Option<Vec<i64>>,
}

#[derive(Debug, Clone)]
pub struct GenerateCandidate {
    pub card_id: String,
    pub agent_id: String,
    pub priority: String,
    pub description: Option<String>,
    pub metadata: Option<String>,
    pub github_issue_number: Option<i64>,
    pub batch_phase: i64,
}

impl AutoQueueService {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    pub fn prepare_generate_cards(
        &self,
        input: &PrepareGenerateInput,
    ) -> ServiceResult<Vec<GenerateCandidate>> {
        if let Some(issue_numbers) = input.issue_numbers.as_ref().filter(|nums| !nums.is_empty()) {
            let transition_plan = {
                let conn = self
                    .db
                    .read_conn()
                    .map_err(|error| ServiceError::internal(format!("{error}")))?;
                crate::pipeline::ensure_loaded();
                let backlog_cards = auto_queue::list_backlog_cards(
                    &conn,
                    &GenerateCardFilter {
                        repo: input.repo.clone(),
                        agent_id: input.agent_id.clone(),
                        issue_numbers: Some(issue_numbers.clone()),
                    },
                )
                .map_err(|error| ServiceError::internal(format!("load backlog cards: {error}")))?;

                let mut plan = Vec::with_capacity(backlog_cards.len());
                for card in backlog_cards {
                    let effective = crate::pipeline::resolve_for_card(
                        &conn,
                        card.repo_id.as_deref(),
                        card.assigned_agent_id.as_deref(),
                    );
                    let prep_path = if effective.is_valid_state("ready") {
                        effective
                            .free_path_to_state("backlog", "ready")
                            .or_else(|| effective.free_path_to_dispatchable("backlog"))
                    } else {
                        effective.free_path_to_dispatchable("backlog")
                    };
                    let Some(path) = prep_path else {
                        return Err(ServiceError::bad_request(format!(
                            "card {} has no free path from backlog to ready/dispatchable state",
                            card.card_id
                        )));
                    };
                    plan.push((card.card_id, path));
                }
                plan
            };

            for (card_id, path) in transition_plan {
                for step in &path {
                    crate::kanban::transition_status_no_hooks(
                        &self.db,
                        &card_id,
                        step,
                        "auto-queue-generate",
                    )
                    .map_err(|error| {
                        ServiceError::bad_request(format!(
                            "failed to auto-transition card {card_id} to {step}: {error}"
                        ))
                    })?;
                }
            }
        }

        let conn = self
            .db
            .read_conn()
            .map_err(|error| ServiceError::internal(format!("{error}")))?;
        crate::pipeline::ensure_loaded();
        let enqueueable_states = crate::pipeline::try_get()
            .map(enqueueable_states_for)
            .unwrap_or_else(|| vec!["ready".to_string(), "requested".to_string()]);
        let cards = auto_queue::list_generate_candidates(
            &conn,
            &GenerateCardFilter {
                repo: input.repo.clone(),
                agent_id: input.agent_id.clone(),
                issue_numbers: input.issue_numbers.clone(),
            },
            &enqueueable_states,
        )
        .map_err(|error| ServiceError::internal(format!("load generate cards: {error}")))?;

        Ok(cards.into_iter().map(GenerateCandidate::from).collect())
    }

    pub fn count_cards_by_status(
        &self,
        repo: Option<&str>,
        agent_id: Option<&str>,
        status: &str,
    ) -> ServiceResult<i64> {
        let conn = self
            .db
            .read_conn()
            .map_err(|error| ServiceError::internal(format!("{error}")))?;
        auto_queue::count_cards_by_status(&conn, repo, agent_id, status)
            .map_err(|error| ServiceError::internal(format!("count cards: {error}")))
    }
}

impl From<GenerateCandidateRecord> for GenerateCandidate {
    fn from(record: GenerateCandidateRecord) -> Self {
        Self {
            card_id: record.card_id,
            agent_id: record.agent_id,
            priority: record.priority,
            description: record.description,
            metadata: record.metadata,
            github_issue_number: record.github_issue_number,
            batch_phase: 0,
        }
    }
}

fn enqueueable_states_for(pipeline: &crate::pipeline::PipelineConfig) -> Vec<String> {
    let mut states: Vec<String> = pipeline
        .dispatchable_states()
        .iter()
        .map(|state| state.to_string())
        .collect();

    if pipeline.is_valid_state("requested") && !states.iter().any(|state| state == "requested") {
        states.push("requested".to_string());
    }
    if pipeline.is_valid_state("ready") && !states.iter().any(|state| state == "ready") {
        states.push("ready".to_string());
    }

    states
}
