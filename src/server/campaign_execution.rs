//! Optional protected execution plan for the existing Campaign ledger.
//! The policy tick supplies finite Auto-Queue batches; it never executes a model
//! or marks a card done. Check receipts and the existing FSM remain authoritative.
mod inputs;
#[cfg(all(test, target_os = "linux"))]
mod native_tests;
mod plan;

use crate::{
    db::campaigns::{self, Campaign, CampaignInput, CampaignStatus, NodeStatus, handoff},
    engine::PolicyEngine,
    server::test_phase_runs::checks,
};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

static IN_FLIGHT: AtomicBool = AtomicBool::new(false);
struct Flight;
impl Drop for Flight {
    fn drop(&mut self) {
        IN_FLIGHT.store(false, Ordering::Release);
    }
}

pub(crate) fn kick(pool: PgPool, engine: PolicyEngine) {
    let Some(registry) = std::env::var_os("AGENTDESK_CAMPAIGN_REGISTRY") else {
        return;
    };
    let Some(check_registry) = std::env::var_os("AGENTDESK_CHECK_REGISTRY") else {
        tracing::warn!("campaign registry requires a protected check registry");
        return;
    };
    if IN_FLIGHT.swap(true, Ordering::AcqRel) {
        return;
    }
    tokio::spawn(async move {
        let _flight = Flight;
        let plans =
            tokio::task::spawn_blocking(move || -> anyhow::Result<BTreeMap<String, plan::Plan>> {
                let bytes = crate::services::platform::managed_process::read_protected_config(
                    Path::new(&registry),
                )?;
                let plans: BTreeMap<String, plan::Plan> = serde_json::from_str(&bytes)?;
                anyhow::ensure!(plans.len() <= 32, "campaign registry exceeds 32 goals");
                Ok(plans)
            })
            .await;
        match plans {
            Ok(Ok(plans)) => {
                for (id, plan) in plans {
                    match supply(&pool, &id, &plan, Path::new(&check_registry)).await {
                        Ok(runs) => {
                            for run in runs {
                                let body = crate::services::auto_queue::route::ActivateBody {
                                    run_id: Some(run.clone()),
                                    repo: Some(plan.repo.clone()),
                                    agent_id: Some(plan.agent_id.clone()),
                                    thread_group: None,
                                    unified_thread: Some(false),
                                    active_only: None,
                                };
                                if let Err(error) =
                                    crate::services::auto_queue::route::activate_with_bridge_pg(
                                        engine.clone(),
                                        body,
                                    )
                                    .await
                                {
                                    tracing::warn!(%id,%run,%error,"campaign batch activation deferred");
                                }
                            }
                        }
                        Err(error) => tracing::warn!(%id,%error,"campaign supply held"),
                    }
                }
            }
            other => tracing::warn!(?other, "protected campaign registry could not be loaded"),
        }
    });
}

async fn supply(
    pool: &PgPool,
    id: &str,
    plan: &plan::Plan,
    registry: &Path,
) -> anyhow::Result<Vec<String>> {
    let campaign = campaigns::get(pool, id).await?;
    plan.validate(&campaign)?;
    if campaign.status != CampaignStatus::Active {
        return Ok(Vec::new());
    }
    let agent = sqlx::query("SELECT provider,execution_requirements FROM agents WHERE id=$1")
        .bind(&plan.agent_id)
        .fetch_one(pool)
        .await?;
    let requirements =
        crate::services::cluster::execution_requirements::ExecutionRequirements::parse(
            agent.get("execution_requirements"),
        )
        .map_err(anyhow::Error::msg)?;
    let provider: String = agent.get("provider");
    anyhow::ensure!(
        requirements.execution_limits.is_some()
            && crate::services::provider::ProviderKind::from_str(&provider)
                .and_then(|p| p.execution_adapter())
                .and_then(|a| a.managed_execution_protocol())
                == Some(1),
        "campaign requires a provider with managed execution and finite limits"
    );
    checks::validate_assignment(pool, Some(&plan.repo), Some(&plan.agent_id)).await?;
    let plan_digest = plan.digest()?;
    let mut artifacts = BTreeMap::new();
    let mut supplied = BTreeMap::new();
    let mut holds = BTreeMap::new();
    for (node, config) in &plan.nodes {
        let card = plan::card_id(&config.task_key);
        if let Some(row) = sqlx::query("SELECT status,metadata,agentdesk_check_disposition(k) AS disposition FROM kanban_cards k WHERE id=$1")
            .bind(&card)
            .fetch_optional(pool)
            .await?
        {
            let meta: Value = row.get("metadata");
            anyhow::ensure!(
                meta["campaign_id"] == id
                    && meta["node_id"] == *node
                    && meta["plan_digest"] == plan_digest,
                "logical task {node} already belongs to a different approved plan"
            );
            supplied.insert(node.clone(), row.get::<String, _>("status"));
            if row.get::<String,_>("disposition")=="await_human" {
                holds.insert(node.clone(),"Protected checks need operator attention or approval; inspect the managed-check receipt.".to_string());
            }
            // An unverified ledger 'completed' value cannot unblock a successor.
            match checks::completed_artifact(pool, &card).await {
                Ok(Some(artifact)) => { artifacts.insert(node.clone(), artifact); },
                Ok(None) => {},
                Err(error) => { holds.insert(node.clone(),format!("Verified artifact unavailable: {error}")); },
            }
        }
    }
    let mut cards = Vec::new();
    // A bad workspace blocks its branch only. Independent ready work still runs.
    for node in &campaign.nodes {
        let id = &node.input.id;
        if supplied.contains_key(id)
            || !node
                .input
                .dependencies
                .iter()
                .all(|d| artifacts.contains_key(d))
        {
            continue;
        }
        if cards.len() == 16 {
            break;
        }
        let config = &plan.nodes[id];
        let dependencies = node
            .input
            .dependencies
            .iter()
            .map(|d| (d.clone(), artifacts[d].clone()))
            .collect();
        match inputs::prepare(pool,registry,config,&dependencies).await {
            Ok(prepared) => cards.push(handoff::PreparedCard {
                id:plan::card_id(&config.task_key),node_id:id.clone(),title:node.input.title.clone(),
                description:format!("{}\n\nAcceptance:\n{}\n\nVerified dependency files:\n{}",
                    node.input.details,node.input.acceptance.join("\n"),prepared.manifest),
                agent_id:plan.agent_id.clone(),repo:plan.repo.clone(),
                metadata:json!({"campaign_id":campaign.id,"node_id":id,"task_key":config.task_key,
                    "plan_digest":plan_digest,"input_manifest":prepared.manifest,"managed_campaign":true,"auto_rework":config.auto_rework}),
                contract:serde_json::to_value(&prepared.contract)?,capture_key:prepared.key,capture_job:prepared.job,
            }),
            Err(error) => {
                tracing::warn!(campaign=%campaign.id,node=%id,%error,"campaign node preparation held");
                holds.insert(id.clone(),format!("Input preparation held: {error}"));
            },
        }
    }
    let publication = handoff::publish(pool, &campaign, &cards).await;
    let mut release_error = None;
    for card in &cards {
        if let Err(error) = inputs::release(pool, &card.capture_key, &card.capture_job).await {
            release_error = Some(error);
        }
    }
    publication?;
    if let Some(error) = release_error {
        return Err(error);
    }
    reconcile_progress(pool, &campaign, &artifacts, &supplied, &holds).await?;
    // Retry activation after restart using persisted queue rows, never append to
    // an existing finite batch or replace its already-created dispatches.
    Ok(sqlx::query_scalar("SELECT DISTINCT r.id FROM auto_queue_runs r JOIN auto_queue_entries e ON e.run_id=r.id JOIN kanban_cards k ON k.id=e.kanban_card_id WHERE k.metadata->>'campaign_id'=$1 AND k.metadata->>'plan_digest'=$2 AND r.status IN ('generated','active') ORDER BY r.id")
        .bind(id).bind(plan_digest).fetch_all(pool).await?)
}

async fn reconcile_progress(
    pool: &PgPool,
    campaign: &Campaign,
    artifacts: &BTreeMap<String, checks::VerifiedArtifact>,
    supplied: &BTreeMap<String, String>,
    holds: &BTreeMap<String, String>,
) -> anyhow::Result<()> {
    let mut changed = false;
    let mut nodes = campaign
        .nodes
        .iter()
        .map(|n| n.input.clone())
        .collect::<Vec<_>>();
    for node in &mut nodes {
        let status = if artifacts.contains_key(&node.id) {
            NodeStatus::Completed
        } else if holds.contains_key(&node.id) {
            NodeStatus::Blocked
        } else if let Some(status) = supplied.get(&node.id) {
            if matches!(status.as_str(), "blocked" | "failed") {
                NodeStatus::Blocked
            } else if matches!(status.as_str(), "backlog" | "ready" | "requested") {
                NodeStatus::Pending
            } else {
                NodeStatus::Running
            }
        } else {
            NodeStatus::Pending
        };
        if node.status != status {
            node.status = status;
            changed = true;
        }
        let blocker = holds
            .get(&node.id)
            .map(|text| text.chars().take(2000).collect::<String>());
        if node.blocker != blocker {
            node.blocker = blocker;
            changed = true;
        }
    }
    let status = if nodes.iter().all(|n| n.status == NodeStatus::Completed) {
        CampaignStatus::Completed
    } else {
        campaign.status
    };
    if changed || status != campaign.status {
        // CAS prevents a concurrent operator pause/cancel from being overwritten.
        campaigns::replace(
            pool,
            &campaign.id,
            campaign.revision,
            CampaignInput {
                title: campaign.title.clone(),
                description: campaign.description.clone(),
                status,
                round: campaign.round,
                nodes,
            },
        )
        .await?;
    }
    Ok(())
}
