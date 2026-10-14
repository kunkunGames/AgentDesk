//! Durable frontier snapshots and atomic manager receipt ownership.
use super::{Action, Decision, Frontier, GoalValidation, INPUT_FILE};
use crate::db::campaigns::{
    self,
    graph::{self, Goal, Graph, contract_digest, planning_input::Frozen},
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, Postgres, Row, Transaction};

pub(crate) struct RevisionOwner<'a> {
    key: &'a str,
}
impl RevisionOwner<'_> {
    pub(crate) fn key(&self) -> &str {
        self.key
    }
}

pub async fn snapshot(pool: &PgPool, id: &str) -> Result<Frontier> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let frontier = snapshot_on_connection(&mut tx, id).await?;
    tx.commit().await?;
    Ok(frontier)
}

pub(crate) async fn snapshot_on_connection(
    connection: &mut PgConnection,
    id: &str,
) -> Result<Frontier> {
    let row = sqlx::query(
        "SELECT s.spec,s.active_version,v.graph,v.digest FROM campaign_execution_specs s
        JOIN campaign_graph_versions v ON v.campaign_id=s.campaign_id AND v.version=s.active_version
        WHERE s.campaign_id=$1 AND s.manager_enabled AND v.status='active'",
    )
    .bind(id)
    .fetch_one(&mut *connection)
    .await?;
    let goal: Goal = serde_json::from_value(row.get("spec"))?;
    let graph: Graph = serde_json::from_value(row.get("graph"))?;
    let keys: Vec<String> = graph
        .nodes
        .iter()
        .map(|node| node.task_key.clone())
        .collect();
    let rows = sqlx::query("SELECT t.task_key,t.node_id,t.contract_digest,t.accepted,t.observation,t.artifact_manifest,
        d.id AS dispatch_id,d.status AS dispatch_status,d.result::text AS result
        FROM campaign_graph_tasks t LEFT JOIN LATERAL (SELECT id,status,result FROM task_dispatches
            WHERE kanban_card_id=t.card_id AND dispatch_type IN ('implementation','rework')
            ORDER BY created_at DESC,id DESC LIMIT 1) d ON true
        WHERE t.campaign_id=$1 AND t.role='work' AND t.task_key=ANY($2) ORDER BY t.task_key")
        .bind(id).bind(&keys).fetch_all(&mut *connection).await?;
    let tasks = rows.iter().map(|row| {
        let raw: Option<String> = row.get("result");
        let decoded = decode(raw.as_deref());
        json!({"task_key":row.get::<String,_>("task_key"),"node_id":row.get::<String,_>("node_id"),
            "contract_digest":row.get::<String,_>("contract_digest"),"accepted":row.get::<bool,_>("accepted"),
            "observation":row.get::<Option<Value>,_>("observation"),"artifact_manifest":row.get::<Option<Value>,_>("artifact_manifest"),
            "dispatch_id":row.get::<Option<String>,_>("dispatch_id"),"dispatch_status":row.get::<Option<String>,_>("dispatch_status"),
            "result_digest":raw.as_deref().map(|raw| graph::digest(raw.as_bytes())),
            "receipt":graph::receipt::sample_evidence(&decoded)})
    }).collect();
    let failures = sqlx::query_scalar::<_,Value>("SELECT jsonb_build_object('id',id,'task_key',task_key,'category',category,'evidence',evidence)
        FROM campaign_graph_failures WHERE campaign_id=$1 AND (task_key=ANY($2) OR EXISTS(
            SELECT 1 FROM campaign_graph_tasks control WHERE control.task_key=campaign_graph_failures.task_key
            AND control.role IN ('plan','validate','manage','finish') AND control.version >= $3)) ORDER BY id")
        .bind(id).bind(&keys).bind(row.get::<i64,_>("active_version")).fetch_all(&mut *connection).await?;
    let goal_digest = contract_digest(&goal)?;
    let version: i64 = row.get("active_version");
    let unmet_goal_assessments = sqlx::query_scalar::<_,Value>("SELECT jsonb_build_object('task_key',task_key,'validation',observation->'goal_validation')
        FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='finish' AND version=$2 AND accepted
        AND execution_profile->>'goal_digest'=$3 AND observation->'goal_validation'->'approved'='false'::jsonb
        ORDER BY task_key")
        .bind(id).bind(version).bind(&goal_digest).fetch_all(&mut *connection).await?;
    Ok(Frontier {
        campaign_id: id.into(),
        goal_digest,
        goal,
        version,
        graph_digest: row.get("digest"),
        graph,
        tasks,
        failures,
        unmet_goal_assessments,
    })
}

fn decode(raw: Option<&str>) -> Value {
    let value = raw
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .unwrap_or(Value::Null);
    if let Some(encoded) = value.as_str() {
        serde_json::from_str(encoded).unwrap_or(Value::Null)
    } else {
        value
    }
}

pub async fn freeze(pool: &PgPool, id: &str, goal: &Goal) -> Result<Frozen> {
    let frontier = snapshot(pool, id).await?;
    ensure!(
        frontier.goal_digest == contract_digest(goal)?,
        "workflow goal changed"
    );
    let frontier_digest = frontier.digest()?;
    let root = crate::config::runtime_root()
        .ok_or_else(|| anyhow::anyhow!("runtime root unavailable"))?
        .join("artifacts/campaign-graphs");
    let bytes = serde_json::to_vec(
        &json!({"schema_version":1,"snapshot_digest":frontier_digest,
        "frontier":frontier,"artifact_root":root,
        "instructions":"Read accepted artifact files under artifact_root/<bundle>/<declared path>. Verify their SHA before relying on them. Receipts are observations, not business approval. Preserve published contracts; revise only unpublished remaining work unless selecting a captured recoverable failure. Do not repeat effects or historical checks. Complete requires independent goal verification."}),
    )?;
    ensure!(
        bytes.len() as u64 <= graph::MAX_ARTIFACT_FILE_BYTES,
        "workflow input exceeds file limit"
    );
    let manifest = tokio::task::spawn_blocking(move || {
        let staging = tempfile::tempdir()?;
        std::fs::write(staging.path().join(INPUT_FILE), bytes)?;
        crate::services::campaign_artifacts::capture_outputs(
            staging.path(),
            &[graph::Output {
                path: INPUT_FILE.into(),
                format: graph::Format::Json,
            }],
            &root,
        )
    })
    .await??;
    Ok(Frozen {
        manifest,
        goal_digest: contract_digest(goal)?,
        frontier_digest: Some(frontier_digest),
    })
}

/// Accept the exact native result and its owned frozen input in one campaign transaction.
pub async fn accept(
    pool: &PgPool,
    id: &str,
    key: &str,
    dispatch: &str,
    result: &Value,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    graph::store::lock(&mut tx, id).await?;
    let state = sqlx::query("SELECT hold_reason,manager_enabled FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE")
        .bind(id).fetch_one(&mut *tx).await?;
    ensure!(
        state.get::<bool, _>("manager_enabled")
            && state.get::<Option<String>, _>("hold_reason").is_none(),
        "workflow manager is disabled or held"
    );
    let row = sqlx::query("SELECT t.role,t.version,t.execution_profile,t.observation,k.assigned_agent_id,d.result::text AS result
        FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id
        JOIN task_dispatches d ON d.kanban_card_id=t.card_id
        WHERE t.campaign_id=$1 AND t.task_key=$2 AND d.id=$3 AND d.status='completed'
            AND d.dispatch_type IN ('implementation','rework') FOR SHARE OF t,k,d")
        .bind(id).bind(key).bind(dispatch).fetch_one(&mut *tx).await?;
    let raw: Option<String> = row.get("result");
    ensure!(
        decode(raw.as_deref()) == *result,
        "workflow native receipt changed"
    );
    graph::result::validate_dispatch(result)?;
    if row.get::<Option<Value>, _>("observation").is_some() {
        return Ok(());
    }
    let frontier = snapshot_on_connection(&mut tx, id).await?;
    let profile: Value = row.get("execution_profile");
    let role: String = row.get("role");
    let digest = frontier.digest()?;
    if profile["workflow_snapshot_digest"] != digest
        || profile["goal_digest"] != frontier.goal_digest
        || row.get::<Option<i64>, _>("version") != Some(frontier.version)
    {
        sqlx::query("UPDATE campaign_graph_tasks SET observation=$2 WHERE task_key=$1 AND observation IS NULL")
            .bind(key).bind(json!({"dispatch_id":dispatch,"outcome":"superseded","reason":"Workflow frontier changed before decision observation"}))
            .execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(());
    }
    let assigned: Option<String> = row.get("assigned_agent_id");
    let mut observation = json!({"dispatch_id":dispatch,"outcome":"accepted","graph_version":frontier.version,"snapshot_digest":digest});
    if role == "manage" {
        ensure!(
            assigned.as_deref() == Some(frontier.goal.planner.as_str()),
            "workflow manager executor differs"
        );
        let decision: Decision = serde_json::from_value(result["workflow_decision"].clone())?;
        let replacement: Option<Graph> = result
            .get("campaign_graph")
            .filter(|value| !value.is_null())
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?;
        frontier.validate_decision(&decision, replacement.as_ref())?;
        if let Some(graph) = replacement {
            let version = if let Some(failure) = decision.failure_id.as_deref() {
                graph::store::stage_on_tx(&mut tx, id, &frontier.goal, &graph, Some(failure))
                    .await?
            } else {
                let owner = RevisionOwner { key };
                graph::store::stage_progress_on_tx(&mut tx, id, &frontier.goal, &graph, &owner)
                    .await?
            };
            sqlx::query("UPDATE campaign_graph_versions SET planner_agent_id=$3 WHERE campaign_id=$1 AND version=$2 AND planner_agent_id IS NULL")
                .bind(id).bind(version).bind(&frontier.goal.planner).execute(&mut *tx).await?;
            observation["candidate_version"] = json!(version);
        }
        observation["workflow_decision"] = serde_json::to_value(decision)?;
        if let Some(seconds) = observation["workflow_decision"]["wake_after_secs"].as_u64() {
            observation["wake_at"] =
                json!(chrono::Utc::now() + chrono::Duration::seconds(seconds as i64));
        }
    } else {
        ensure!(
            role == "finish" && assigned.as_deref() == Some(frontier.goal.verifier.as_str()),
            "goal verification requires its independent executor"
        );
        let validation: GoalValidation = serde_json::from_value(result["goal_validation"].clone())?;
        frontier.validate_goal_assessment(&validation, result)?;
        observation["goal_validation"] = serde_json::to_value(validation)?;
    }
    sqlx::query("UPDATE campaign_graph_tasks SET accepted=TRUE,observation=$2 WHERE task_key=$1 AND observation IS NULL")
        .bind(key).bind(observation).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn complete(pool: &PgPool, id: &str, key: &str) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let campaign = graph::store::lock(&mut tx, id).await?;
    let state = sqlx::query("SELECT hold_reason,manager_enabled FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE")
        .bind(id).fetch_one(&mut *tx).await?;
    ensure!(
        state.get::<bool, _>("manager_enabled")
            && state.get::<Option<String>, _>("hold_reason").is_none(),
        "workflow manager is disabled or held"
    );
    let frontier = snapshot_on_connection(&mut tx, id).await?;
    let row = sqlx::query("SELECT t.observation,d.result::text AS result,k.assigned_agent_id FROM campaign_graph_tasks t
        JOIN task_dispatches d ON d.id=t.observation->>'dispatch_id' JOIN kanban_cards k ON k.id=t.card_id
        WHERE t.campaign_id=$1 AND t.task_key=$2 AND t.role='finish' AND t.accepted AND d.status='completed'")
        .bind(id).bind(key).fetch_one(&mut *tx).await?;
    ensure!(
        row.get::<Option<String>, _>("assigned_agent_id").as_deref()
            == Some(frontier.goal.verifier.as_str()),
        "goal verifier identity differs"
    );
    let observation: Value = row.get("observation");
    let validation: GoalValidation =
        serde_json::from_value(observation["goal_validation"].clone())?;
    let raw: Option<String> = row.get("result");
    let result = decode(raw.as_deref());
    graph::result::validate_dispatch(&result)?;
    ensure!(
        result["goal_validation"] == observation["goal_validation"],
        "retained goal validation changed"
    );
    frontier.validate_goal(&validation, &result)?;
    if !super::lifecycle::settled(&mut tx, id).await? {
        return Ok(false);
    }
    let root = crate::config::runtime_root()
        .ok_or_else(|| anyhow::anyhow!("runtime root unavailable"))?
        .join("artifacts/campaign-graphs");
    let manifests: std::collections::BTreeMap<String, Value> = validation
        .criteria
        .iter()
        .filter_map(|proof| {
            frontier.task(&proof.task_key).map(|task| {
                (
                    task["artifact_manifest"]["bundle"]
                        .as_str()
                        .unwrap_or("")
                        .to_owned(),
                    task["artifact_manifest"].clone(),
                )
            })
        })
        .collect();
    tokio::task::spawn_blocking(move || -> Result<()> {
        for manifest in manifests.values() {
            crate::services::campaign_artifacts::verify(&root, manifest)?;
        }
        Ok(())
    })
    .await??;
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&campaign)?)?;
    for node in &mut input.nodes {
        let work = frontier.graph.nodes.iter().find(|work| work.id == node.id);
        node.status = if work.is_some_and(|work| frontier.accepted(&work.task_key)) {
            campaigns::NodeStatus::Completed
        } else {
            campaigns::NodeStatus::Skipped
        };
    }
    input.status = campaigns::CampaignStatus::Completed;
    campaigns::replace_on_tx(&mut tx, id, campaign.revision, input).await?;
    tx.commit().await?;
    Ok(true)
}

/// Template learning never treats a managed ledger status as goal proof.
pub(crate) async fn completion_proved_on_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
) -> Result<bool> {
    let frontier = snapshot_on_connection(tx, id).await?;
    let digest = frontier.digest()?;
    let row=sqlx::query("SELECT t.observation,d.result::text AS result,k.assigned_agent_id FROM campaign_graph_tasks t
        JOIN task_dispatches d ON d.id=t.observation->>'dispatch_id' AND d.kanban_card_id=t.card_id
        JOIN kanban_cards k ON k.id=t.card_id WHERE t.campaign_id=$1 AND t.role='finish' AND t.accepted
        AND t.version=$2 AND d.status='completed' AND t.execution_profile->>'goal_digest'=$3
        AND t.observation->>'snapshot_digest'=$4 AND t.observation->'goal_validation'->'approved'='true'::jsonb
        ORDER BY t.task_key LIMIT 1")
        .bind(id).bind(frontier.version).bind(&frontier.goal_digest).bind(digest).fetch_optional(&mut **tx).await?;
    let Some(row) = row else {
        return Ok(false);
    };
    if row.get::<Option<String>, _>("assigned_agent_id").as_deref()
        != Some(frontier.goal.verifier.as_str())
    {
        return Ok(false);
    }
    let raw: Option<String> = row.get("result");
    let result = decode(raw.as_deref());
    let observation: Value = row.get("observation");
    let validation: GoalValidation =
        serde_json::from_value(observation["goal_validation"].clone())?;
    Ok(graph::result::validate_dispatch(&result).is_ok()
        && result["goal_validation"] == observation["goal_validation"]
        && frontier.validate_goal(&validation, &result).is_ok())
}

pub(crate) async fn verify_revision_owner(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    key: &str,
) -> Result<()> {
    let frontier = snapshot_on_connection(tx, id).await?;
    let digest = frontier.digest()?;
    let valid: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t
        WHERE t.campaign_id=$1 AND t.task_key=$2 AND t.role='manage' AND t.accepted
            AND t.observation->'workflow_decision'->>'action'='revise'
            AND t.execution_profile->>'workflow_snapshot_digest'=$3
            AND t.observation->>'snapshot_digest'=$3)",
    )
    .bind(id)
    .bind(key)
    .bind(digest)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        valid,
        "normal workflow revision lost its accepted manager frontier"
    );
    Ok(())
}

/// A paused policy change preserves the Goal, receipts and lifetime counters.
pub async fn configure(pool: &PgPool, id: &str, revision: i64, enabled: bool) -> Result<i64> {
    let mut tx = pool.begin().await?;
    let row: sqlx::types::Json<campaigns::Campaign> =
        sqlx::query_scalar("SELECT document FROM campaigns WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    let campaign = row.0;
    ensure!(campaign.revision == revision, "campaign revision conflict");
    ensure!(
        campaign.status == campaigns::CampaignStatus::Paused,
        "pause the campaign before changing its workflow policy"
    );
    sqlx::query("SELECT campaign_id FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE")
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        super::lifecycle::settled(&mut tx, id).await?,
        "campaign ownership and cleanup must settle before policy change"
    );
    let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE campaign_id=$1 AND role IN ('plan','manage','finish') AND observation IS NULL AND NOT accepted)")
        .bind(id).fetch_one(&mut *tx).await?;
    ensure!(!pending, "campaign still has a pending control owner");
    sqlx::query("UPDATE campaign_execution_specs SET manager_enabled=$2 WHERE campaign_id=$1")
        .bind(id)
        .bind(enabled)
        .execute(&mut *tx)
        .await?;
    let input: campaigns::CampaignInput = serde_json::from_value(serde_json::to_value(&campaign)?)?;
    let updated = campaigns::replace_on_tx(&mut tx, id, campaign.revision, input).await?;
    tx.commit().await?;
    Ok(updated.revision)
}
