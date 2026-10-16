//! A refreshed producer cannot invalidate an already published input contract.
use super::super::Graph;
use anyhow::{Result, ensure};
use sqlx::{Postgres, Transaction};
use std::collections::BTreeSet;

pub(super) async fn require_unpublished(
    tx: &mut Transaction<'_, Postgres>,
    graph: &Graph,
    root: &str,
) -> Result<()> {
    let mut affected: BTreeSet<String> = [root.to_owned()].into();
    loop {
        let before = affected.len();
        for node in &graph.nodes {
            if node.dependencies.iter().any(|d| affected.contains(d)) {
                affected.insert(node.id.clone());
            }
        }
        if affected.len() == before {
            break;
        }
    }
    for node in graph
        .nodes
        .iter()
        .filter(|n| n.id != root && affected.contains(&n.id))
    {
        ensure!(
            !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE task_key=$1)",
            )
            .bind(&node.task_key)
            .fetch_one(&mut **tx)
            .await?,
            "a dependent task is already published; preserve its input contract"
        );
    }
    Ok(())
}
