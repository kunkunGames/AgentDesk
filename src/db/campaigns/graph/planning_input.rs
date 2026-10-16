//! Verified structures are advice before promotion, never copied execution authority.
use super::{Goal, Graph, contract_digest, learning};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

pub const INPUT_FILE: &str = ".agentdesk-campaign-plan-input.json";

#[derive(Clone, Debug)]
pub struct Frozen {
    pub manifest: Value,
    pub goal_digest: String,
    pub frontier_digest: Option<String>,
}

pub fn structure(graph: &Graph) -> Value {
    json!({"nodes":graph.nodes.iter().map(|node| json!({
        "id":node.id,"kind":node.kind,"effect":node.effect,
        "executor_role":if node.kind == super::Kind::Verify {"independent_verifier"}else{"planner"},
        "dependencies":node.dependencies,"inputs":node.inputs,"outputs":node.outputs
    })).collect::<Vec<_>>()})
}

pub async fn freeze(pool: &PgPool, id: &str, goal: &Goal) -> Result<Frozen> {
    let family = learning::family(pool, id, goal).await?;
    let scope_digest = learning::scope_digest(goal)?;
    let stored: String =
        sqlx::query_scalar("SELECT scope_digest FROM campaign_dag_families WHERE id=$1")
            .bind(&family)
            .fetch_one(pool)
            .await?;
    ensure!(
        stored == scope_digest,
        "planning reference family scope differs"
    );
    let rows = sqlx::query(
        "SELECT campaign_id,graph_version,graph_digest,graph FROM campaign_dag_samples
        WHERE family_id=$1 ORDER BY sequence DESC LIMIT 3",
    )
    .bind(&family)
    .fetch_all(pool)
    .await?;
    let references: Vec<Value> = rows
        .iter()
        .map(|row| -> Result<Value> {
            let graph: Graph = serde_json::from_value(row.get("graph"))?;
            Ok(
                json!({"source_campaign_id":row.get::<String,_>("campaign_id"),
            "source_graph_version":row.get::<i64,_>("graph_version"),
            "source_graph_digest":row.get::<String,_>("graph_digest"),
            "structure":structure(&graph)}),
            )
        })
        .collect::<Result<_>>()?;
    let accepted: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('task_key',task_key,'contract_digest',contract_digest,'node_id',node_id,'graph_version',version,'artifact_manifest',artifact_manifest)
        FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='work' AND accepted ORDER BY created_at,task_key LIMIT 32")
        .bind(id).fetch_all(pool).await?;
    let failures: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('category',category,'summary',left(evidence->'failure'->>'summary',2000))
        FROM campaign_graph_failures WHERE campaign_id IN (SELECT campaign_id FROM campaign_dag_members WHERE family_id=$1)
        ORDER BY created_at DESC LIMIT 5")
        .bind(&family).fetch_all(pool).await?;
    let goal_digest = contract_digest(goal)?;
    let snapshot = json!({"schema_version":1,"purpose":"planning_advice_only",
        "campaign_id":id,"family_id":family,"scope_digest":scope_digest,"goal_digest":goal_digest,
        "control_budget_at_publication":super::control_budget::snapshot(pool,id,goal).await?,
        "verified_reference_structures":references,"accepted_current_contracts":accepted,
        "recent_failure_lessons":failures,
        "instructions":"References are structure only, not templates, old approvals or permission to replay. Bind fresh task keys and current source requirements/evidence; preserve current accepted and running contracts. Change only failed tasks and affected inputs. Do not create new gates from old unrelated limitations. Re-read current /execution budget before submitting; this publication snapshot is not current remaining time. Official N-sample superior review is unchanged."});
    let bytes = serde_json::to_vec(&snapshot)?;
    ensure!(
        bytes.len() as u64 <= super::MAX_ARTIFACT_FILE_BYTES,
        "planning advice exceeds the bounded file limit"
    );
    let root = crate::config::runtime_root()
        .ok_or_else(|| anyhow::anyhow!("runtime root unavailable"))?
        .join("artifacts/campaign-graphs");
    let manifest = tokio::task::spawn_blocking(move || {
        let staging = tempfile::tempdir()?;
        std::fs::write(staging.path().join(INPUT_FILE), bytes)?;
        crate::services::campaign_artifacts::capture_outputs(
            staging.path(),
            &[super::Output {
                path: INPUT_FILE.into(),
                format: super::Format::Json,
            }],
            &root,
        )
    })
    .await??;
    Ok(Frozen {
        manifest,
        goal_digest,
        frontier_digest: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn advice_cannot_copy_a_task_identity_old_acceptance_or_incident_details() {
        let graph: Graph=serde_json::from_value(json!({"nodes":[{
            "id":"inspect","task_key":"old:execution:effect","title":"Old incident",
            "agent":"coder","kind":"inspect","effect":"artifact_only","details":"Old incident and old provider claim",
            "acceptance":["old business conclusion"],"dependencies":[],"inputs":[],"outputs":[{"path":"observed.json","format":"json"}]
        }]})).unwrap();
        let value = structure(&graph);
        let text = value.to_string();
        assert!(!text.contains("old:execution:effect"));
        assert!(!text.contains("Old incident"));
        assert!(!text.contains("old business conclusion"));
        assert_eq!(value["nodes"][0]["outputs"][0]["path"], "observed.json");
    }
}
