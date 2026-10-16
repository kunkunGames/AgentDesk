//! A source audit cannot authorize new effects before fresh independent QA.
use super::SettledExternalEffect;
use crate::db::campaigns::graph::{Effect, Graph, Kind, contract_digest};
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use std::collections::BTreeSet;

fn ancestors(graph: &Graph, node: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut pending = vec![node.to_owned()];
    while let Some(id) = pending.pop() {
        if let Some(task) = graph.nodes.iter().find(|n| n.id == id) {
            for dependency in &task.dependencies {
                if found.insert(dependency.clone()) {
                    pending.push(dependency.clone());
                }
            }
        }
    }
    found
}

pub(crate) async fn validate_graph(
    tx: &mut Transaction<'_, Postgres>,
    campaign: &str,
    graph: &Graph,
    audit: &SettledExternalEffect,
) -> Result<()> {
    let gate = graph.settled_effect_gates.iter().find(|g| g.audit_sha256 == audit.audit_sha256)
        .ok_or_else(|| anyhow::anyhow!("settled effect recovery requires an audit-bound fresh inspection and independent verification gate"))?;
    let inspect = graph
        .nodes
        .iter()
        .find(|n| n.id == gate.inspection)
        .ok_or_else(|| anyhow::anyhow!("missing settled inspection"))?;
    let verify = graph
        .nodes
        .iter()
        .find(|n| n.id == gate.verification)
        .ok_or_else(|| anyhow::anyhow!("missing settled verification"))?;
    ensure!(
        inspect.effect == Effect::ArtifactOnly
            && inspect.kind == Kind::Inspect
            && inspect.dependencies.is_empty()
            && verify.effect == Effect::ArtifactOnly
            && verify.kind == Kind::Verify
            && verify.agent != inspect.agent
            && verify.dependencies.contains(&inspect.id),
        "settled recovery requires a read-only inspection root and its directly dependent independent target QA"
    );
    let prior: Vec<Value> = sqlx::query_scalar(
        "SELECT graph FROM campaign_graph_versions
        WHERE campaign_id=$1 AND status IN ('active','superseded')",
    )
    .bind(campaign)
    .fetch_all(&mut **tx)
    .await?;
    let mut preserved_gate = false;
    let mut previously_declared = BTreeSet::new();
    for raw in prior {
        let previous: Graph = serde_json::from_value(raw)?;
        if previous.settled_effect_gates.iter().any(|old| old == gate) {
            for node in &previous.nodes {
                previously_declared.insert((node.task_key.clone(), contract_digest(node)?));
            }
            ensure!(
                previous
                    .nodes
                    .iter()
                    .any(|old| old.task_key == inspect.task_key
                        && contract_digest(old).ok() == contract_digest(inspect).ok())
                    && previous
                        .nodes
                        .iter()
                        .any(|old| old.task_key == verify.task_key
                            && contract_digest(old).ok() == contract_digest(verify).ok()),
                "a committed settled-effect gate cannot change its task identities or contracts"
            );
            preserved_gate = true;
        }
    }
    let fresh_gate = !sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE task_key IN ($1,$2))",
    )
    .bind(&inspect.task_key)
    .bind(&verify.task_key)
    .fetch_one(&mut **tx)
    .await?;
    if !preserved_gate {
        ensure!(
            fresh_gate,
            "settled recovery must use fresh unpublished inspection and target QA identities"
        );
    }
    for node in graph.nodes.iter().filter(|n| n.effect == Effect::External) {
        let already_published = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks
            WHERE campaign_id=$1 AND task_key=$2 AND contract_digest=$3)",
        )
        .bind(campaign)
        .bind(&node.task_key)
        .bind(contract_digest(node)?)
        .fetch_one(&mut **tx)
        .await?;
        if !already_published {
            ensure!(
                fresh_gate
                    || previously_declared
                        .contains(&(node.task_key.clone(), contract_digest(node)?)),
                "a newly introduced external contract requires fresh unpublished inspection and target QA"
            );
            ensure!(
                ancestors(graph, &node.id).contains(&verify.id),
                "every new external effect must depend on the audit-bound fresh independent target QA"
            );
        }
    }
    Ok(())
}
