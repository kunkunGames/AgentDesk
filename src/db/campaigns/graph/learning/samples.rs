//! One completed and independently verified run is one sample, not each retry.
use crate::db::campaigns::graph::{Goal, Graph};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

/// Shared qualification for queries with `s` (execution spec) and `v`
/// (its active graph). Retired revisions never fence a completed current graph.
pub(crate) const VERIFIED_NODES_SQL: &str = "NOT EXISTS(
    SELECT 1 FROM jsonb_array_elements(v.graph->'nodes') n WHERE NOT EXISTS(
        SELECT 1 FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id
        JOIN task_dispatches d ON d.id=t.observation->>'dispatch_id' AND d.kanban_card_id=t.card_id
        WHERE t.campaign_id=s.campaign_id AND t.task_key=n->>'task_key' AND t.role='work' AND t.accepted
        AND t.observation->>'outcome'='accepted' AND t.artifact_manifest->>'bundle'<>''
        AND k.status='done' AND d.status='completed'))";

pub async fn collect(pool: &PgPool, id: &str, goal: &Goal) -> Result<bool> {
    let family = super::family(pool, id, goal).await?;
    let family_scope: String =
        sqlx::query_scalar("SELECT scope_digest FROM campaign_dag_families WHERE id=$1")
            .bind(&family)
            .fetch_one(pool)
            .await?;
    if family_scope != super::scope_digest(goal)? {
        return Ok(false);
    }
    let mut tx = pool.begin().await?;
    // Serialize sample commits with review claims. Sequence order must not
    // change underneath a frozen N-sample review cohort.
    sqlx::query("SELECT id FROM campaign_dag_families WHERE id=$1 FOR UPDATE")
        .bind(&family)
        .fetch_one(&mut *tx)
        .await?;
    let row = sqlx::query(
        "SELECT s.spec,s.active_version,s.manager_enabled,v.graph,v.digest,v.status
        FROM campaign_execution_specs s JOIN campaigns c ON c.id=s.campaign_id
        JOIN campaign_graph_versions v ON v.campaign_id=s.campaign_id AND v.version=s.active_version
        WHERE s.campaign_id=$1 AND c.document->>'status'='completed' AND s.hold_reason IS NULL
        AND v.status='active' FOR SHARE OF c,s,v",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    if row.get::<bool, _>("manager_enabled")
        && !super::super::workflow::store::completion_proved_on_tx(&mut tx, id).await?
    {
        return Ok(false);
    }
    let graph: Graph = serde_json::from_value(row.get("graph"))?;
    graph.validate(goal)?;
    let mut evidence = Vec::new();
    for node in &graph.nodes {
        let task=sqlx::query("SELECT t.observation,t.artifact_manifest,k.assigned_agent_id,
            d.status AS dispatch_status,k.status AS card_status,d.result::text AS receipt,t.execution_profile,(SELECT to_jsonb(o) FROM campaign_dispatch_observations o WHERE o.dispatch_id=d.id) AS execution_observation
            FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id
            JOIN task_dispatches d ON d.id=t.observation->>'dispatch_id' AND d.kanban_card_id=t.card_id
            WHERE t.campaign_id=$1 AND t.task_key=$2 AND t.role='work' AND t.accepted
            AND t.observation->>'outcome'='accepted'")
            .bind(id).bind(&node.task_key).fetch_optional(&mut *tx).await?;
        let Some(task) = task else {
            return Ok(false);
        };
        let manifest: Option<Value> = task.get("artifact_manifest");
        if task.get::<String, _>("dispatch_status") != "completed"
            || task.get::<String, _>("card_status") != "done"
            || !manifest
                .as_ref()
                .is_some_and(|m| m["bundle"].as_str().is_some_and(|s| !s.is_empty()))
        {
            return Ok(false);
        }
        evidence.push(json!({"node_id":node.id,"kind":node.kind,"agent":task.get::<Option<String>,_>("assigned_agent_id"),
            "observation":task.get::<Option<Value>,_>("observation"),"artifact_manifest":manifest,
            "execution_profile":task.get::<Option<Value>,_>("execution_profile"),"execution_observation":task.get::<Option<Value>,_>("execution_observation"),
            "receipt":super::super::receipt::sample_evidence(&task.get::<Option<String>,_>("receipt")
                .and_then(|raw|serde_json::from_str::<Value>(&raw).ok()).unwrap_or(Value::Null))}));
    }
    ensure!(
        evidence.len() == graph.nodes.len(),
        "incomplete sample evidence"
    );
    let added=sqlx::query("INSERT INTO campaign_dag_samples(family_id,campaign_id,graph_version,graph_digest,graph,goal,evidence)
        VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(campaign_id) DO NOTHING")
        .bind(family).bind(id).bind(row.get::<Option<i64>,_>("active_version"))
        .bind(row.get::<String,_>("digest")).bind(serde_json::to_value(graph)?)
        .bind(row.get::<Value,_>("spec")).bind(json!(evidence)).execute(&mut *tx).await?.rows_affected();
    tx.commit().await?;
    Ok(added == 1)
}

/// Upgrade/restart also discovers completed occurrences not yet collected.
pub async fn collect_family(pool: &PgPool, family: &str) -> Result<()> {
    let query = format!("SELECT s.campaign_id,s.spec FROM campaign_execution_specs s
        JOIN campaigns c ON c.id=s.campaign_id JOIN campaign_dag_members m ON m.campaign_id=s.campaign_id
        JOIN campaign_dag_families f ON f.id=m.family_id
        JOIN campaign_graph_versions v ON v.campaign_id=s.campaign_id AND v.version=s.active_version AND v.status='active'
        WHERE m.family_id=$1 AND c.document->>'status'='completed'
        AND s.hold_reason IS NULL
        AND f.scope=jsonb_build_object('objective',s.spec->'objective','success',s.spec->'success','constraints',s.spec->'constraints',
            'repo',s.spec->'repo','planner',s.spec->'planner','verifier',s.spec->'verifier','agents',s.spec->'agents')
            || CASE WHEN jsonb_array_length(COALESCE(s.spec->'prerequisites','[]'::jsonb))>0
                THEN jsonb_build_object('prerequisites',s.spec->'prerequisites') ELSE '{{}}'::jsonb END
        AND {VERIFIED_NODES_SQL}
        AND NOT EXISTS(SELECT 1 FROM campaign_dag_samples sample WHERE sample.campaign_id=s.campaign_id)
        ORDER BY c.updated_at LIMIT 32");
    let ids: Vec<(String, Value)> = sqlx::query_as(&query).bind(family).fetch_all(pool).await?;
    for (id, goal) in ids {
        collect(pool, &id, &serde_json::from_value(goal)?).await?;
    }
    Ok(())
}
