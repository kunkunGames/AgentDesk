//! PostgreSQL owns graph activation and logical-task admission atomically.
use super::{
    Goal, Graph, MAX_ARTIFACT_BUNDLE_BYTES, MAX_ARTIFACT_FILE_BYTES, MAX_TASK_INPUTS,
    MAX_TASK_OUTPUTS, Task, contract_digest,
};
use crate::db::campaigns::{self, Campaign, CampaignInput, CampaignStatus};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Row, Transaction, types::Json};

pub(crate) async fn lock(tx: &mut Transaction<'_, Postgres>, id: &str) -> Result<Campaign> {
    let c: Json<Campaign> =
        sqlx::query_scalar("SELECT document FROM campaigns WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(
        c.0.status == CampaignStatus::Active,
        "campaign is not active"
    );
    Ok(c.0)
}
pub async fn enroll(pool: &PgPool, id: &str, revision: i64, goal: &Goal) -> Result<()> {
    enroll_with_manager(pool, id, revision, goal, false).await
}
pub async fn enroll_with_manager(
    pool: &PgPool,
    id: &str,
    revision: i64,
    goal: &Goal,
    manager_enabled: bool,
) -> Result<()> {
    goal.validate()?;
    ensure!(
        !manager_enabled || goal.budget.max_dispatches >= 6,
        "compact workflow requires at least six lifetime dispatches for planning, independent checks and goal assessment"
    );
    if goal.schedule.is_some() {
        super::schedule::budget::resolve(goal, None)?;
    } else {
        ensure!(
            goal.budget.max_evolutions <= super::MAX_INITIAL_EVOLUTIONS,
            "initial admission allows at most three evolutions; additional lifetime planning requires an audited operator budget"
        );
    }
    ensure!(
        crate::services::settings::internal_tasks_enabled(pool)
            .await
            .map_err(anyhow::Error::msg)?,
        "internalTasksEnabled is required"
    );
    for agent in &goal.agents {
        let provider: String = sqlx::query_scalar("SELECT provider FROM agents WHERE id=$1")
            .bind(agent)
            .fetch_one(pool)
            .await?;
        ensure!(
            matches!(provider.as_str(), "codex" | "qwen" | "opencode"),
            "executor must use Codex, Qwen or OpenCode"
        );
    }
    let mut tx = pool.begin().await?;
    let c = lock(&mut tx, id).await?;
    ensure!(c.revision == revision, "campaign revision conflict");
    ensure!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM routines WHERE campaign_id=$1)"
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?,
        "routine-bound Campaign cannot enroll in graph execution"
    );
    sqlx::query(
        "INSERT INTO campaign_execution_specs(campaign_id,spec,occurrence_budget,manager_enabled) VALUES($1,$2,$3,$4)",
    )
    .bind(id)
    .bind(serde_json::to_value(goal)?)
    .bind(
        goal.schedule
            .as_ref()
            .map(|_| serde_json::to_value(&goal.budget))
            .transpose()?,
    )
    .bind(manager_enabled)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}
pub async fn goal(pool: &PgPool, id: &str) -> Result<Goal> {
    let value: Value =
        sqlx::query_scalar("SELECT spec FROM campaign_execution_specs WHERE campaign_id=$1")
            .bind(id)
            .fetch_one(pool)
            .await?;
    Ok(serde_json::from_value(value)?)
}
pub async fn status(pool: &PgPool, id: &str) -> Result<Value> {
    let artifact_limits = json!({
        "max_file_bytes": MAX_ARTIFACT_FILE_BYTES,
        "max_bundle_bytes": MAX_ARTIFACT_BUNDLE_BYTES,
        "max_outputs_per_task": MAX_TASK_OUTPUTS,
        "max_inputs_per_task": MAX_TASK_INPUTS,
    });
    let Some(row) = sqlx::query(
        "SELECT spec,active_version,hold_reason,preflight,occurrence_budget,manager_enabled FROM campaign_execution_specs WHERE campaign_id=$1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(json!({"enrolled": false, "artifact_limits": artifact_limits}));
    };
    let versions:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(v) FROM campaign_graph_versions v WHERE campaign_id=$1 ORDER BY version DESC").bind(id).fetch_all(pool).await?;
    let failures:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(f) FROM campaign_graph_failures f WHERE campaign_id=$1 ORDER BY created_at DESC LIMIT 50").bind(id).fetch_all(pool).await?;
    let tasks: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(t) FROM campaign_graph_tasks t WHERE campaign_id=$1 ORDER BY created_at",
    )
    .bind(id)
    .fetch_all(pool)
    .await?;
    let observed_tools:i64=sqlx::query_scalar("SELECT COUNT(*) FROM campaign_graph_progress p JOIN task_dispatches d ON d.id=p.dispatch_id JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id WHERE t.campaign_id=$1").bind(id).fetch_one(pool).await?;
    let elapsed = super::progress::elapsed_seconds(pool, id).await?;
    let control_budget =
        super::control_budget::snapshot(pool, id, &serde_json::from_value(row.get("spec"))?)
            .await?;
    let revisions:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(r) FROM campaign_goal_revisions r WHERE campaign_id=$1 ORDER BY created_at DESC LIMIT 50").bind(id).fetch_all(pool).await?;
    let occurrences:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(o)||jsonb_build_object('status',c.document->>'status','hold_reason',s.hold_reason,'accepted_work_tasks',w.accepted,'published_work_tasks',w.published,'sampled',EXISTS(SELECT 1 FROM campaign_dag_samples sample WHERE sample.campaign_id=o.child_id)) FROM campaign_graph_occurrences o JOIN campaigns c ON c.id=o.child_id JOIN campaign_execution_specs s ON s.campaign_id=o.child_id LEFT JOIN LATERAL (SELECT count(*) AS published,count(*) FILTER(WHERE t.accepted) AS accepted FROM campaign_graph_tasks t JOIN campaign_graph_versions v ON v.campaign_id=s.campaign_id AND v.version=s.active_version WHERE t.campaign_id=o.child_id AND t.role='work' AND EXISTS(SELECT 1 FROM jsonb_array_elements(v.graph->'nodes') n WHERE n->>'task_key'=t.task_key)) w ON true WHERE o.campaign_id=$1 ORDER BY scheduled_at DESC LIMIT 30").bind(id).fetch_all(pool).await?;
    let schedule_revisions:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(r) FROM campaign_schedule_revisions r WHERE campaign_id=$1 ORDER BY created_at DESC,request_id LIMIT 30").bind(id).fetch_all(pool).await?;
    Ok(
        json!({"enrolled":true,"manager_enabled":row.get::<bool,_>("manager_enabled"),"budget_enforcement":control_budget.usage,"control_budget":control_budget,"occurrence_budget":row.get::<Option<Value>,_>("occurrence_budget"),"schedule_revisions":schedule_revisions,"preflight":row.get::<Option<Value>,_>("preflight"),"metrics":super::metrics::inspection(pool,id).await?,"dag_learning":super::learning::inspection(pool,id).await?,"artifact_limits":artifact_limits,"elapsed_dispatch_seconds":elapsed,"occurrences":occurrences,"goal":row.get::<Value,_>("spec"),"goal_revisions":revisions,"active_version":row.get::<Option<i64>,_>("active_version"),"hold_reason":row.get::<Option<String>,_>("hold_reason"),"versions":versions,"tasks":tasks,"failures":failures,"observed_tool_calls":observed_tools,"tool_enforcement":"post-event observation; no provider pre-tool interception claim","cost":null}),
    )
}
#[path = "store/ownership.rs"]
mod ownership;

/// Executed/uncertain effects, verified artifacts and live ownership remain pinned.
/// Dispatch-pinned audited non-attempts or settled failures can release history.
async fn pinned_tasks(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
) -> Result<ownership::PinSelection> {
    ownership::pinned_tasks(tx, id).await
}
async fn validate_settled_gates(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    graph: &Graph,
    audits: &[super::recovery::replan::SettledExternalEffect],
) -> Result<()> {
    ensure!(
        graph.settled_effect_gates.iter().all(|gate| audits
            .iter()
            .any(|audit| audit.audit_sha256 == gate.audit_sha256)),
        "settled effect gate lacks a valid server-authenticated latest-dispatch audit"
    );
    for audit in audits {
        super::recovery::replan::settled_effect::validate_graph(tx, id, graph, audit).await?;
    }
    Ok(())
}

/// Recheck a settled execution fence immediately before external publication,
/// including idempotent returns. Physical target preconditions are rechecked
/// by the bounded execution adapter immediately before each actual mutation.
async fn admit_settled_publication(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    p: &Publication<'_>,
) -> Result<String> {
    if p.role != "work" {
        return Ok(p.prompt.to_owned());
    }
    let Some(version) = p.version else {
        return Ok(p.prompt.to_owned());
    };
    let raw: Value = sqlx::query_scalar(
        "SELECT graph FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2 FOR SHARE",
    )
    .bind(id)
    .bind(version)
    .fetch_one(&mut **tx)
    .await?;
    let graph: Graph = serde_json::from_value(raw)?;
    if graph.settled_effect_gates.is_empty() {
        return Ok(p.prompt.to_owned());
    }
    let node = graph
        .nodes
        .iter()
        .find(|node| node.task_key == p.key)
        .ok_or_else(|| {
            anyhow::anyhow!("settled publication task is absent from its pinned graph")
        })?;
    ensure!(
        contract_digest(node)? == p.digest && node.agent == p.agent,
        "settled publication cannot change its pinned contract or executor"
    );
    let selection = pinned_tasks(tx, id).await?;
    validate_settled_gates(tx, id, &graph, &selection.settled).await?;
    if node.effect == super::Effect::External {
        for gate in &graph.settled_effect_gates {
            let inspection = graph
                .nodes
                .iter()
                .find(|n| n.id == gate.inspection)
                .ok_or_else(|| anyhow::anyhow!("missing settled inspection"))?;
            let verification = graph
                .nodes
                .iter()
                .find(|n| n.id == gate.verification)
                .ok_or_else(|| anyhow::anyhow!("missing settled target QA"))?;
            let accepted = sqlx::query("SELECT task_key FROM campaign_graph_tasks WHERE campaign_id=$1 AND task_key IN ($2,$3)
            AND accepted AND length(artifact_manifest->>'bundle')=64 FOR SHARE")
            .bind(id).bind(&inspection.task_key).bind(&verification.task_key).fetch_all(&mut **tx).await?;
            ensure!(
                accepted.len() == 2,
                "settled external effects require accepted fresh inspection and independent target QA artifacts before publication"
            );
        }
    }
    let prompt = format!(
        "{}\n\nServer-retained settled-effect evidence (administrator attestation, not business acceptance):\n{}\nIndependently verify all original source/config targets and current owners. The target manifest hash proves integrity, not completeness. Keep missing historical child exits missing. Before every actual external mutation recheck the exact source/config/target preconditions and ownership; matching targets invoke no mutator.",
        p.prompt,
        serde_json::to_string(&selection.settled)?
    );
    ensure!(
        prompt.len() <= 262144,
        "settled task prompt and complete audit evidence exceed the native bound"
    );
    Ok(prompt)
}
pub async fn stage(pool: &PgPool, id: &str, graph: &Graph, failure: Option<&str>) -> Result<i64> {
    let goal = goal(pool, id).await?;
    let mut tx = pool.begin().await?;
    lock(&mut tx, id).await?;
    let version = stage_on_tx(&mut tx, id, &goal, graph, failure).await?;
    tx.commit().await?;
    Ok(version)
}
pub(crate) async fn stage_on_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    goal: &Goal,
    graph: &Graph,
    failure: Option<&str>,
) -> Result<i64> {
    stage_version_on_tx(tx, id, goal, graph, failure, None).await
}
pub(crate) async fn stage_progress_on_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    goal: &Goal,
    graph: &Graph,
    owner: &super::workflow::store::RevisionOwner<'_>,
) -> Result<i64> {
    stage_version_on_tx(tx, id, goal, graph, None, Some(owner.key())).await
}
async fn stage_version_on_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    goal: &Goal,
    graph: &Graph,
    failure: Option<&str>,
    manager: Option<&str>,
) -> Result<i64> {
    graph.validate(goal)?;
    let digest = contract_digest(graph)?;
    let state=sqlx::query("SELECT active_version,hold_reason,manager_enabled FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE").bind(id).fetch_one(&mut **tx).await?;
    ensure!(
        state.get::<Option<String>, _>("hold_reason").is_none(),
        "campaign held"
    );
    // Recheck even a digest-idempotent staging call: later retries, result
    // changes or new owners restore the original effect fence.
    let selection = pinned_tasks(tx, id).await?;
    for row in selection.pinned {
        let key: String = row.get("task_key");
        let node = graph
            .nodes
            .iter()
            .find(|n| n.task_key == key)
            .ok_or_else(|| anyhow::anyhow!("cannot remove completed/in-flight task"))?;
        ensure!(
            contract_digest(node)? == row.get::<String, _>("contract_digest"),
            "cannot edit completed/in-flight contract"
        );
    }
    validate_settled_gates(tx, id, graph, &selection.settled).await?;
    if let Some(v) = sqlx::query(
        "SELECT version,failure_id,manager_task_key FROM campaign_graph_versions WHERE campaign_id=$1 AND digest=$2",
    )
    .bind(id)
    .bind(&digest)
    .fetch_optional(&mut **tx)
    .await?
    {
        ensure!(
            v.get::<Option<String>, _>("failure_id").as_deref() == failure
                && v.get::<Option<String>, _>("manager_task_key").as_deref() == manager,
            "evolution must change the failed graph"
        );
        return Ok(v.get("version"));
    }
    let parent: Option<i64> = state.get("active_version");
    ensure!(!sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_versions WHERE campaign_id=$1 AND status IN ('candidate','validated'))").bind(id).fetch_one(&mut **tx).await?, "another candidate awaits validation or activation");
    if let Some(manager) = manager {
        ensure!(
            failure.is_none() && parent.is_some(),
            "normal progress needs an active parent and its manager owner"
        );
        let owned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN campaign_execution_specs s ON s.campaign_id=t.campaign_id WHERE t.campaign_id=$1 AND t.task_key=$2 AND t.role='manage' AND t.version=s.active_version AND s.manager_enabled AND t.execution_profile->>'goal_digest'=$3)")
            .bind(id).bind(manager).bind(contract_digest(goal)?).fetch_one(&mut **tx).await?;
        ensure!(owned, "normal progress has no current manager owner");
    } else if parent.is_some() {
        let failure =
            failure.ok_or_else(|| anyhow::anyhow!("evolution requires a captured failure"))?;
        ensure!(sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_failures f WHERE f.id=$1 AND f.campaign_id=$2 AND (f.category IN ('code','plan') OR EXISTS(SELECT 1 FROM campaign_graph_reconciliations r WHERE r.failure_id=f.id AND r.campaign_id=f.campaign_id)))").bind(failure).bind(id).fetch_one(&mut **tx).await?,"infrastructure or unknown effects cannot auto-evolve");
    }
    let next: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(version),0)+1 FROM campaign_graph_versions WHERE campaign_id=$1",
    )
    .bind(id)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        next <= i64::from(goal.budget.max_evolutions) + 1,
        "evolution budget exhausted"
    );
    // Completed and in-flight contracts survive a revision. Failed tasks may be
    // replaced with an explicitly different task key; no hidden dispatch retry.
    let mut unpublished = 0_i64;
    for node in &graph.nodes {
        if let Some(old) = sqlx::query(
            "SELECT campaign_id,contract_digest FROM campaign_graph_tasks WHERE task_key=$1",
        )
        .bind(&node.task_key)
        .fetch_optional(&mut **tx)
        .await?
        {
            ensure!(
                old.get::<String, _>("campaign_id") == id
                    && old.get::<String, _>("contract_digest") == contract_digest(node)?,
                "a published task key cannot acquire a new contract"
            );
        } else {
            unpublished += 1;
        }
    }
    let published: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM campaign_graph_tasks WHERE campaign_id=$1")
            .bind(id)
            .fetch_one(&mut **tx)
            .await?;
    // A new candidate needs one independent validation and every unpublished
    // business node. Already published identities consume lifetime usage once.
    let required = published
        + unpublished
        + 1
        + if state.get::<bool, _>("manager_enabled") {
            2
        } else {
            0
        };
    ensure!(
        required <= i64::from(goal.budget.max_dispatches),
        "candidate requires {required} lifetime tasks ({published} published, {unpublished} new business tasks and one independent validation); dispatch limit is {}",
        goal.budget.max_dispatches
    );
    sqlx::query("INSERT INTO campaign_graph_versions(campaign_id,version,parent_version,graph,digest,status,failure_id,manager_task_key) VALUES($1,$2,$3,$4,$5,'candidate',$6,$7)")
        .bind(id).bind(next).bind(parent).bind(serde_json::to_value(graph)?).bind(digest).bind(failure).bind(manager).execute(&mut **tx).await?;
    Ok(next)
}
pub async fn activate(pool: &PgPool, id: &str, version: i64, expected_revision: i64) -> Result<()> {
    let g = goal(pool, id).await?;
    let mut tx = pool.begin().await?;
    let c = lock(&mut tx, id).await?;
    ensure!(
        c.revision == expected_revision,
        "campaign revision conflict"
    );
    let state = sqlx::query(
        "SELECT hold_reason FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    ensure!(
        state.get::<Option<String>, _>("hold_reason").is_none(),
        "campaign held"
    );
    let row=sqlx::query("SELECT graph,status FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2 FOR UPDATE").bind(id).bind(version).fetch_one(&mut *tx).await?;
    ensure!(
        matches!(
            row.get::<String, _>("status").as_str(),
            "validated" | "superseded"
        ),
        "only validated versions can activate"
    );
    let graph: Graph = serde_json::from_value(row.get("graph"))?;
    graph.validate(&g)?;
    let manager: Option<String> = sqlx::query_scalar(
        "SELECT manager_task_key FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2",
    )
    .bind(id)
    .bind(version)
    .fetch_one(&mut *tx)
    .await?;
    if let Some(manager) = manager {
        super::workflow::store::verify_revision_owner(&mut tx, id, &manager).await?;
    }
    let selection = pinned_tasks(&mut tx, id).await?;
    for prior in selection.pinned {
        let key: String = prior.get("task_key");
        let node = graph
            .nodes
            .iter()
            .find(|n| n.task_key == key)
            .ok_or_else(|| anyhow::anyhow!("activation cannot remove completed or running work"))?;
        ensure!(
            contract_digest(node)? == prior.get::<String, _>("contract_digest"),
            "activation cannot change a pinned contract"
        );
    }
    validate_settled_gates(&mut tx, id, &graph, &selection.settled).await?;
    sqlx::query("UPDATE campaign_graph_versions SET status='superseded' WHERE campaign_id=$1 AND status='active'").bind(id).execute(&mut *tx).await?;
    sqlx::query(
        "UPDATE campaign_graph_versions SET status='active' WHERE campaign_id=$1 AND version=$2",
    )
    .bind(id)
    .bind(version)
    .execute(&mut *tx)
    .await?;
    sqlx::query("UPDATE campaign_execution_specs SET active_version=$2 WHERE campaign_id=$1")
        .bind(id)
        .bind(version)
        .execute(&mut *tx)
        .await?;
    let mut nodes = Vec::new();
    for n in &graph.nodes {
        let prior =
            sqlx::query("SELECT card_id,accepted FROM campaign_graph_tasks WHERE task_key=$1")
                .bind(&n.task_key)
                .fetch_optional(&mut *tx)
                .await?;
        nodes.push(serde_json::from_value(json!({"id":n.id,"title":n.title,"stage":format!("{:?}",n.kind).to_lowercase(),"group":g.repo,
            "status":if prior.as_ref().is_some_and(|r|r.get::<bool,_>("accepted")){"completed"}else{"pending"},"round":version,
            "assignee":n.agent,"session_id":prior.as_ref().map(|r|r.get::<String,_>("card_id")),"provider":null,
            "dependencies":n.dependencies,"details":n.details,"acceptance":n.acceptance}))?);
    }
    campaigns::replace_on_tx(
        &mut tx,
        id,
        c.revision,
        CampaignInput {
            title: c.title,
            description: c.description,
            status: c.status,
            round: version as u32,
            nodes,
        },
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub struct Publication<'a> {
    pub key: &'a str,
    pub node: &'a str,
    pub role: &'a str,
    pub version: Option<i64>,
    pub agent: &'a str,
    pub title: &'a str,
    pub prompt: &'a str,
    pub digest: &'a str,
}
pub fn card_id(key: &str) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(key.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash[..16]);
    uuid::Uuid::from_bytes(bytes).to_string()
}
/// A finite queue batch, its card and ownership commit together. Existing task
/// identity is reconciled, never redispatched. This is not another work queue.
pub async fn publish(pool: &PgPool, id: &str, p: &Publication<'_>) -> Result<String> {
    publish_inner(pool, id, p, None, None).await
}
pub async fn publish_planning(
    pool: &PgPool,
    id: &str,
    p: &Publication<'_>,
    authority: Option<&super::learning::ControlAssignment>,
    input: &super::planning_input::Frozen,
) -> Result<String> {
    ensure!(
        matches!(p.role, "plan" | "manage" | "finish"),
        "frozen control input cannot change another task role"
    );
    if let Some(authority) = authority {
        ensure!(
            p.agent == authority.agent_id && authority.purpose == "bootstrap_plan",
            "planning input cannot change bootstrap authority"
        );
        super::learning::policy::eligible_agent(pool, p.agent).await?;
    }
    publish_inner(pool, id, p, authority, Some(input)).await
}
pub async fn publish_control(
    pool: &PgPool,
    id: &str,
    p: &Publication<'_>,
    authority: &super::learning::ControlAssignment,
) -> Result<String> {
    ensure!(
        p.agent == authority.agent_id
            && matches!(
                (p.role, authority.purpose),
                ("plan", "bootstrap_plan") | ("template_review", "template_review")
            ),
        "control assignment does not match publication"
    );
    super::learning::policy::eligible_agent(pool, p.agent).await?;
    publish_inner(pool, id, p, Some(authority), None).await
}
async fn publish_inner(
    pool: &PgPool,
    id: &str,
    p: &Publication<'_>,
    authority: Option<&super::learning::ControlAssignment>,
    input: Option<&super::planning_input::Frozen>,
) -> Result<String> {
    ensure!(
        crate::services::settings::internal_tasks_enabled(pool)
            .await
            .map_err(anyhow::Error::msg)?,
        "internal task admission disabled"
    );
    let mut tx = pool.begin().await?;
    lock(&mut tx, id).await?;
    let g: Goal = serde_json::from_value(
        sqlx::query_scalar::<_, Value>(
            "SELECT spec FROM campaign_execution_specs WHERE campaign_id=$1",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?,
    )?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('campaign-task'),hashtext($1))")
        .bind(p.key)
        .execute(&mut *tx)
        .await?;
    let prompt = admit_settled_publication(&mut tx, id, p).await?;
    if let Some(old) = sqlx::query(
        "SELECT campaign_id,card_id,contract_digest FROM campaign_graph_tasks WHERE task_key=$1",
    )
    .bind(p.key)
    .fetch_optional(&mut *tx)
    .await?
    {
        ensure!(
            old.get::<String, _>("campaign_id") == id
                && old.get::<String, _>("contract_digest") == p.digest,
            "logical task already bound to another contract"
        );
        return Ok(old.get("card_id"));
    }
    let state=sqlx::query("SELECT active_version,hold_reason,manager_enabled FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE").bind(id).fetch_one(&mut *tx).await?;
    ensure!(
        state.get::<Option<String>, _>("hold_reason").is_none(),
        "campaign held"
    );
    if p.role == "work" {
        ensure!(
            state.get::<Option<i64>, _>("active_version") == p.version,
            "stale graph publication"
        );
    }
    if let Some(authority) = authority {
        let member:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM campaign_dag_members WHERE campaign_id=$1 AND family_id=$2)")
            .bind(id).bind(&authority.family_id).fetch_one(&mut *tx).await?;
        ensure!(
            member,
            "superior control assignment belongs to another campaign family"
        );
        let allowed: bool = if authority.purpose == "bootstrap_plan" {
            ensure!(
                p.version.is_none()
                    && p.key == format!("{id}:plan:initial")
                    && authority.input_manifest.is_none(),
                "invalid bootstrap control contract"
            );
            sqlx::query_scalar(
                "SELECT bootstrap_campaign_id=$2 FROM campaign_dag_families WHERE id=$1",
            )
            .bind(&authority.family_id)
            .bind(id)
            .fetch_one(&mut *tx)
            .await?
        } else {
            ensure!(
                authority
                    .input_manifest
                    .as_ref()
                    .is_some_and(Value::is_object),
                "template review requires frozen input evidence"
            );
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM campaign_dag_reviews WHERE task_key=$1 AND family_id=$2 AND owner_campaign_id=$3 AND superior_agent_id=$4 AND status='pending')")
                .bind(p.key).bind(&authority.family_id).bind(id).bind(p.agent).fetch_one(&mut *tx).await?
        };
        ensure!(
            allowed,
            "superior control publication is not authorized by its durable claim"
        );
    }
    if authority.is_some_and(|owner| owner.purpose == "template_review") {
        super::learning::budget::admit_publication(&mut tx, p.key).await?;
    }
    if matches!(p.role, "plan" | "validate" | "manage" | "finish") {
        let frame = super::control_budget::on_connection(&mut tx, id, &g).await?;
        if let Some(reason) = frame.control_wait(p.role) {
            sqlx::query("UPDATE campaign_execution_specs SET hold_reason=$2 WHERE campaign_id=$1 AND hold_reason IS NULL")
                .bind(id).bind(&reason).execute(&mut *tx).await?;
            tx.commit().await?;
            return Err(super::control_budget::Unavailable(reason).into());
        }
    }
    let mut workflow_frontier = None;
    if let Some(input) = input {
        ensure!(
            input.goal_digest == contract_digest(&g)?,
            "planning advice goal changed before publication"
        );
        let file = if matches!(p.role, "manage" | "finish") {
            super::workflow::INPUT_FILE
        } else {
            super::planning_input::INPUT_FILE
        };
        ensure!(
            input.manifest["files"][file].is_string(),
            "control input must contain its bounded canonical file"
        );
        if let Some(digest) = &input.frontier_digest {
            let frontier = super::workflow::store::snapshot_on_connection(&mut tx, id).await?;
            ensure!(
                *digest == frontier.digest()?,
                "workflow frontier changed before publication"
            );
            ensure!(
                p.version == Some(frontier.version),
                "workflow control graph version differs"
            );
            workflow_frontier = Some(frontier);
        }
    }
    if g.schedule.is_none() || !authority.is_some_and(|owner| owner.purpose == "template_review") {
        let count = super::learning::budget::publication_count(&mut tx, id, &g).await?;
        ensure!(
            count < i64::from(g.budget.max_dispatches),
            "campaign dispatch budget exhausted"
        );
    }
    let card = card_id(p.key);
    let run = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO kanban_cards(id,title,description,status,assigned_agent_id,repo_id,metadata) VALUES($1,$2,$3,'ready',$4,$5,$6)")
        .bind(&card).bind(p.title).bind(&prompt).bind(p.agent).bind(&g.repo).bind(json!({"campaign_id":id,"node_id":p.node,"graph_version":p.version,"graph_task_key":p.key})).execute(&mut *tx).await?;
    let provider: String = sqlx::query_scalar("SELECT provider FROM agents WHERE id=$1")
        .bind(p.agent)
        .fetch_one(&mut *tx)
        .await?;
    let template: Option<String> = sqlx::query_scalar(
        "SELECT template_id FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2",
    )
    .bind(id)
    .bind(p.version)
    .fetch_optional(&mut *tx)
    .await?
    .flatten();
    let mut profile = json!({"schema_version":1,"prompt_digest":super::digest(prompt.as_bytes()),
        "goal_digest":contract_digest(&g)?,"result_schema_digest":contract_digest(&super::schema::document()?)?,
        "graph_version":p.version,"template_id":template,"provider_requested":provider});
    if let Some(frontier) = workflow_frontier {
        profile["workflow_goal"] = sqlx::query_scalar::<_, Value>(
            "SELECT spec FROM campaign_execution_specs WHERE campaign_id=$1",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        profile["workflow_graph_digest"] = json!(frontier.graph_digest);
        profile["workflow_authority_state"] = frontier.authority_state();
    }
    if let Some(input) = input {
        profile["control_input_manifest"] = input.manifest.clone();
        profile["control_input_digest"] = json!(contract_digest(&input.manifest)?);
        if let Some(digest) = &input.frontier_digest {
            profile["workflow_snapshot_digest"] = json!(digest);
        }
    }
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,node_id,role,version,contract_digest,card_id,execution_profile) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(p.key).bind(id).bind(p.node).bind(p.role).bind(p.version).bind(p.digest).bind(&card).bind(profile).execute(&mut *tx).await?;
    if let Some(authority) = authority {
        sqlx::query("INSERT INTO campaign_dag_control_assignments(task_key,family_id,agent_id,purpose,input_manifest) VALUES($1,$2,$3,$4,$5)")
            .bind(p.key).bind(&authority.family_id).bind(&authority.agent_id).bind(authority.purpose)
            .bind(&authority.input_manifest).execute(&mut *tx).await?;
    }
    sqlx::query("INSERT INTO auto_queue_runs(id,repo,agent_id,status,ai_model,ai_rationale,max_concurrent_threads,thread_group_count,review_mode) VALUES($1,$2,$3,'generated','campaign-graph-v1','bounded graph contract',1,1,'disabled')")
        .bind(&run).bind(&g.repo).bind(p.agent).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO auto_queue_entries(id,run_id,kanban_card_id,agent_id,priority_rank,thread_group,batch_phase,reason) VALUES($1,$2,$3,$4,1,0,0,'validated graph task')")
        .bind(uuid::Uuid::new_v4().to_string()).bind(run).bind(&card).bind(p.agent).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(card)
}
pub async fn hold(pool: &PgPool, id: &str, reason: &str) -> Result<()> {
    sqlx::query("UPDATE campaign_execution_specs SET hold_reason=$2 WHERE campaign_id=$1")
        .bind(id)
        .bind(reason.chars().take(2000).collect::<String>())
        .execute(pool)
        .await?;
    Ok(())
}

/// Budget admission cannot erase reconciliation or workspace ownership holds.
pub(crate) async fn hold_budget(pool: &PgPool, id: &str) -> Result<()> {
    sqlx::query("UPDATE campaign_execution_specs SET hold_reason=$2 WHERE campaign_id=$1
        AND (hold_reason IS NULL OR hold_reason IN ('Plan evolution budget exhausted',
        'Cumulative execution time or observed tool budget exhausted; retained across restart and graph revisions'))")
        .bind(id).bind("Cumulative execution time or observed tool budget exhausted; retained across restart and graph revisions")
        .execute(pool).await?;
    Ok(())
}
pub fn task_digest(task: &Task) -> Result<String> {
    contract_digest(task)
}

/// Resolve the native contract from canonical graph membership, never a
/// caller-supplied dispatch-context flag that could waive ordinary PR rules.
pub(crate) async fn execution_prompt(pool: &PgPool, dispatch: &str) -> Result<Option<String>> {
    let prompt: Option<String> = sqlx::query_scalar("SELECT k.description FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id JOIN task_dispatches d ON d.kanban_card_id=t.card_id WHERE d.id=$1 AND d.dispatch_type IN ('implementation','rework')")
        .bind(dispatch).fetch_optional(pool).await?;
    if let Some(text) = &prompt {
        ensure!(
            !text.trim().is_empty() && text.len() <= 262144,
            "native graph execution prompt missing or oversized"
        );
    }
    Ok(prompt)
}
