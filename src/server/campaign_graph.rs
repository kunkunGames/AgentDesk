//! Goal-first native graph control on the existing Hub tick and Auto Queue.
use crate::services::campaign_artifacts as artifacts;
pub(crate) mod learning;
mod manager;
mod observation;
mod occurrence_context;
mod planning;
mod reconcile;
#[cfg(test)]
mod tests;
use crate::{
    db::campaigns::{
        self, CampaignStatus,
        graph::{Goal, Graph, contract_digest, store},
    },
    engine::PolicyEngine,
};
use anyhow::Result;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

static IN_FLIGHT: AtomicBool = AtomicBool::new(false);
struct Flight;
impl Drop for Flight {
    fn drop(&mut self) {
        IN_FLIGHT.store(false, Ordering::Release);
    }
}
pub(crate) fn kick(
    pool: PgPool,
    engine: PolicyEngine,
    health_registry: Option<std::sync::Arc<crate::services::discord::health::HealthRegistry>>,
) {
    if IN_FLIGHT.swap(true, Ordering::AcqRel) {
        return;
    }
    tokio::spawn(async move {
        let _flight = Flight;
        let ids = reconciliation_candidates(&pool).await;
        if let Ok(ids) = ids {
            for id in ids {
                if let Err(error) = tick(&pool, &engine, &id, health_registry.clone()).await {
                    tracing::warn!(campaign=%id,%error,"graph reconciliation deferred");
                }
                let _ = sqlx::query("UPDATE campaign_execution_specs SET last_reconciled_at=NOW() WHERE campaign_id=$1").bind(&id).execute(&pool).await;
            }
        }
    });
}
async fn reconciliation_candidates(pool: &PgPool) -> Result<Vec<String>> {
    // A hold fences publication, not durable cleanup. A failed provider can
    // leave a pending retry and occupied slot after its dispatch is terminal.
    let qualified = crate::db::campaigns::graph::learning::samples::VERIFIED_NODES_SQL;
    let review_task = crate::db::campaigns::graph::learning::budget::REVIEW_TASK_SQL;
    let unsettled_review = crate::db::campaigns::graph::learning::budget::UNSETTLED_TASK_SQL;
    let query = format!(
        "SELECT s.campaign_id FROM campaign_execution_specs s JOIN campaigns c ON c.id=s.campaign_id
         WHERE (c.document->>'status'='active' AND s.hold_reason IS NULL)
         OR (c.document->>'status'='completed' AND s.hold_reason IS NULL AND s.active_version IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM campaign_dag_samples sample WHERE sample.campaign_id=s.campaign_id)
             AND EXISTS(SELECT 1 FROM campaign_graph_versions v WHERE v.campaign_id=s.campaign_id
                 AND v.version=s.active_version AND v.status='active' AND {qualified}))
         OR EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN task_dispatches d ON d.kanban_card_id=t.card_id
                   WHERE t.campaign_id=s.campaign_id AND d.dispatch_type IN ('implementation','rework') AND d.status IN ('pending','dispatched','running'))
         OR EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN task_dispatches d ON d.kanban_card_id=t.card_id
                   WHERE t.campaign_id=s.campaign_id AND t.observation IS NULL
                     AND d.dispatch_type IN ('implementation','rework') AND d.status IN ('completed','failed','cancelled'))
          OR (s.hold_reason IS NOT NULL AND EXISTS(
             SELECT 1 FROM campaign_graph_tasks t JOIN auto_queue_entries e ON e.kanban_card_id=t.card_id
              JOIN auto_queue_runs r ON r.id=e.run_id WHERE t.campaign_id=s.campaign_id AND r.status IN ('generated','active','paused')))
          OR EXISTS(SELECT 1 FROM campaign_dag_reviews review WHERE review.owner_campaign_id=s.campaign_id AND review.status='pending')
          OR EXISTS(SELECT 1 FROM campaign_dag_reviews review JOIN campaign_graph_tasks t ON t.task_key=review.task_key
              WHERE review.owner_campaign_id=s.campaign_id AND review.status='failed' AND ({review_task}) AND ({unsettled_review}))
         ORDER BY s.last_reconciled_at NULLS FIRST,s.campaign_id LIMIT 32",
    );
    Ok(sqlx::query_scalar::<_, String>(&query)
        .fetch_all(pool)
        .await?)
}
fn artifact_root() -> Result<PathBuf> {
    Ok(crate::config::runtime_root()
        .ok_or_else(|| anyhow::anyhow!("runtime root unavailable"))?
        .join("artifacts/campaign-graphs"))
}
fn decode(raw: Option<String>) -> Value {
    let v = raw
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .unwrap_or(Value::Null);
    if let Value::String(s) = &v {
        serde_json::from_str(s).unwrap_or(Value::Null)
    } else {
        v
    }
}
fn contract_prompt(goal: &Goal) -> Result<String> {
    let mut prompt = format!(
        "Goal and immutable operator constraints:\n{}\nComplete only this assigned card. No new cards, dispatches, self-delegation, credential access, service deployment or runtime DB writes. Write .agentdesk-campaign-result.json in the assigned workspace with campaign_receipt={{\"outcome\":\"passed|blocked|no_work\",\"summary\":\"observed conclusion\",\"observations\":[{{\"command\":\"actual command\",\"exit_code\":0,\"evidence\":\"bounded observation\"}}],\"failure_category\":\"code|plan|infrastructure|unknown\"}}. Unknown effects must be reconciled, not replayed. Put campaign_graph or graph_validation alongside campaign_receipt in that same JSON object when requested (no result wrapper). The native completion owner reads this file after your turn ends; do not call completion APIs yourself. Commit any tracked repository changes before finishing; untracked declared artifact files may remain. Final prose alone is not a completion receipt. Do not report success merely because the CLI exits.\n",
        serde_json::to_string(goal)?
    );
    prompt.push_str("Execution budget authority: the structured goal.budget fields are the current audited limits for max_dispatches, max_elapsed_secs, node_timeout_secs, max_evolutions and max_observed_tool_calls. A future occurrence may receive a revised policy while historical numeric execution-budget prose remains in its inherited constraints. Use the structured fields for those five execution limits; all business scope, permissions, acceptance and other limits remain binding. Never reset usage or change a limit yourself.\n");
    prompt.push_str("Source-policy boundary: derive acceptance and action gates from the original business requirements and observed evidence. An unavailable private-provider session API is an observability limitation, not by itself a source-mandated approval prerequisite or an observed unknown executed effect. Do not assert that unobserved private work is idle or absent. Require additional ownership reconciliation when the source actually requires it or current evidence indicates a conflicting owner or uncertain prior effect; otherwise retain the limitation and independently verify the permitted current target, source, feedback and native ownership scope. Generated hypotheses are not new immutable business requirements.\n");
    prompt.push_str("Assigned-node outcome is distinct from the reported business outcome. An inspection/report node may return passed only when its own declared acceptance is fulfilled and the original business source permits a truthful failed-check, static-block or left-open report. Preserve the actual command, tested SHA, raw nonzero exit code, unavailable evidence and unreviewed scope in its declared artifacts. Receipt observations must describe actual capture/report-verification commands with their real exit codes; never relabel the failing check as passed, fake its exit code with a wrapper, or claim business approval from report completion. If this node's acceptance requires checks to pass, a failing check means blocked. Unknown external effects remain blocked and require reconciliation. Before attributing a missing generated input to a code regression, inspect the documented install/code-generation/workflow setup at the exact assigned head and perform only setup authorized for this node; record actual setup or unavailable-environment evidence within the original scope and budget.\n");
    prompt.push_str(&crate::db::campaigns::graph::receipt::instructions());
    Ok(prompt)
}
const GRAPH_SCHEMA: &str = r#"Return campaign_graph in the result file with {"nodes":[{"id":"inspect","task_key":"repo:input-sha:operation","title":"Concrete step","agent":"enrolled agent","kind":"inspect|execute|verify","effect":"artifact_only|external","details":"Precise work and code requirements","acceptance":["checkable condition"],"dependencies":[],"inputs":[],"outputs":[{"path":"result.json","format":"json|text|python"}]}]}. Each dependency requires an input {"dependency":"producer-id","source":"declared-output-path","destination":"relative-input-path"}. Every non-verify node requires a directly dependent verify node on the other executor. All nodes must produce real bounded files. Verify nodes must actually run checks/tests and independently inspect the supplied files. Generated Python/code requires syntax and behavioral tests in the verifier, not merely an existence check. Use effect artifact_only only for work confined to disposable workspace outputs with no external business mutation; otherwise use external. Unverified artifact-only producers may be corrected after failed verification; external effects must be preserved and reconciled. Use stable input-specific task keys; changing graph version alone must not repeat a business effect. No cycles, undefined dependencies or ambiguous scope. Even a no-work conclusion requires an inspection artifact and independent verification; do not omit the graph. A non-verify consumer must also depend on the independent verification node of each non-verify producer, with its check report mapped as an input."#;

async fn control(
    pool: &PgPool,
    id: &str,
    g: &Goal,
    role: &str,
    version: Option<i64>,
    suffix: &str,
    prompt: &str,
) -> Result<bool> {
    let key = format!("{id}:{role}:{suffix}");
    // Preserve contracts published by an earlier binary or policy. Do not
    // regenerate a prompt or change the executor of an existing control card.
    if let Some(existing) = sqlx::query(
        "SELECT role,version FROM campaign_graph_tasks WHERE campaign_id=$1 AND task_key=$2",
    )
    .bind(id)
    .bind(&key)
    .fetch_optional(pool)
    .await?
    {
        anyhow::ensure!(
            existing.get::<String, _>("role") == role
                && existing.get::<Option<i64>, _>("version") == version,
            "control task identity differs"
        );
        return Ok(true);
    }
    let authority = if role == "plan" && suffix == "initial" {
        crate::db::campaigns::graph::learning::initial_assignment(pool, id, g).await?
    } else {
        None
    };
    let agent = if let Some(assignment) = &authority {
        assignment.agent_id.clone()
    } else if role == "finish" {
        g.verifier.clone()
    } else if role == "validate" {
        let planner:Option<String>=sqlx::query_scalar("SELECT planner_agent_id FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2")
            .bind(id).bind(version).fetch_optional(pool).await?.flatten();
        if planner.as_deref() == Some(g.verifier.as_str()) {
            g.planner.clone()
        } else {
            g.verifier.clone()
        }
    } else {
        g.planner.clone()
    };
    let title = format!("[{id}] {role} {suffix}");
    let formal = if role == "plan" {
        let managed: bool = sqlx::query_scalar(
            "SELECT manager_enabled FROM campaign_execution_specs WHERE campaign_id=$1",
        )
        .bind(id)
        .fetch_one(pool)
        .await?;
        let compact = if managed {
            "Compact workflow policy: start with 2–4 meaningful nodes including required independent verification. Group related commands inside a node. Plan only the currently justified frontier; the manager can extend or prune unpublished work after new verified results. Reserve lifetime dispatch/time/tool budget for manager decisions, candidate validation and independent final Goal.success assessment. Reuse verified structures as advice, bind current inputs and fresh effect identities, and preserve all published contracts.\n"
        } else {
            ""
        };
        format!(
            "{compact}{}",
            crate::db::campaigns::graph::schema::graph_prompt()?
        )
    } else {
        String::new()
    };
    let graph_references = if matches!(role, "plan" | "manage") {
        crate::db::campaigns::graph::schema::GRAPH_REFERENCE_RULES
    } else {
        ""
    };
    let input = if matches!(role, "manage" | "finish") {
        Some(crate::db::campaigns::graph::workflow::store::freeze(pool, id, g).await?)
    } else if role == "plan" {
        Some(crate::db::campaigns::graph::planning_input::freeze(pool, id, g).await?)
    } else {
        None
    };
    let budget = crate::db::campaigns::graph::control_budget::snapshot(pool, id, g).await?;
    let advice = if matches!(role, "manage" | "finish") {
        "Read the immutable .agentdesk-campaign-workflow-input.json before deciding. Its exact snapshot digest, original goal, current receipts and accepted artifact manifests bind this control. Read current artifact contents and verify hashes; do not invent current business approval from historical files."
    } else if input.is_some() {
        "Read the immutable .agentdesk-campaign-plan-input.json before planning. Its verified structures are advice only; generate fresh identities and current acceptance. Official N-sample superior promotion remains separate. Preserve accepted/running contracts; repair only affected work and use existing evidence without repeating historical validation."
    } else {
        ""
    };
    let prompt = format!(
        "{}\n{}\n{prompt}\n{formal}\n{graph_references}\n{advice}\nControl budget observed at publication (re-read current /execution before concluding; estimates are not minimum runtimes):\n{}",
        occurrence_context::attach(pool, id, contract_prompt(g)?).await?,
        occurrence_context::control_authority(id, g, role, &agent, authority.as_ref())?,
        serde_json::to_string(&budget)?
    );
    let digest = contract_digest(&prompt)?;
    let publication = store::Publication {
        key: &key,
        node: role,
        role,
        version,
        agent: &agent,
        title: &title,
        prompt: &prompt,
        digest: &digest,
    };
    let published = if let Some(input) = &input {
        store::publish_planning(pool, id, &publication, authority.as_ref(), input).await
    } else if let Some(authority) = &authority {
        store::publish_control(pool, id, &publication, authority).await
    } else {
        store::publish(pool, id, &publication).await
    };
    match published {
        Ok(_) => Ok(true),
        Err(error)
            if error
                .downcast_ref::<crate::db::campaigns::graph::control_budget::Unavailable>()
                .is_some() =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}
async fn tick(
    pool: &PgPool,
    engine: &PolicyEngine,
    id: &str,
    health_registry: Option<std::sync::Arc<crate::services::discord::health::HealthRegistry>>,
) -> Result<()> {
    // A finite review records its exact native-stop obligation before general
    // held-queue cleanup can change a live session's database projection.
    let review_ready =
        learning::reconcile_review_budgets(pool, id, health_registry.clone()).await?;
    // A hold fences admission, but terminal receipts still need observation.
    reconcile::cleanup_if_held(pool, id, health_registry.clone()).await?;
    if !crate::services::settings::internal_tasks_enabled(pool)
        .await
        .map_err(anyhow::Error::msg)?
    {
        return Ok(());
    }
    let g = store::goal(pool, id).await?;
    reconcile::observe(pool, id, &g, health_registry.clone()).await?;
    reconcile::reject_failed_candidates(pool, id).await?;
    reconcile::settle_accepted(pool, engine, id).await?;
    planning::settle_superseded_unstarted(pool, id).await?;
    let state = store::status(pool, id).await?;
    if !state["hold_reason"].is_null() {
        reconcile::cleanup_if_held(pool, id, health_registry).await?;
        return Ok(());
    }
    let c = campaigns::get(pool, id).await?;
    if c.status == CampaignStatus::Completed {
        crate::db::campaigns::graph::learning::samples::collect(pool, id, &g).await?;
    }
    if c.status != CampaignStatus::Active {
        return Ok(());
    }
    if !crate::db::campaigns::graph::preflight::check(pool, id, &g).await? {
        return Ok(());
    }
    if !review_ready
        || !learning::reconcile_review_budgets(pool, id, health_registry.clone()).await?
    {
        return Ok(());
    }
    learning::review(pool, id, &g).await?;
    if g.schedule.is_some() {
        crate::db::campaigns::graph::schedule::admit(pool, id, &g, chrono::Utc::now()).await?;
        return activate_runs(pool, engine, id, &g).await;
    }
    // An issued plan owns the control turn until its terminal result is observed.
    // Admission limits apply to the next turn, never to cancellation of this one.
    if planning::pending(&state) {
        return activate_runs(pool, engine, id, &g).await;
    }
    let tasks = state["tasks"].as_array().unwrap();
    if tasks.is_empty() && state["versions"].as_array().is_some_and(Vec::is_empty) {
        if learning::initial(pool, id, &g).await? {
            return Ok(());
        }
        if !control(pool, id, &g, "plan", None, "initial", GRAPH_SCHEMA).await? {
            return Ok(());
        }
    }
    let versions = state["versions"].as_array().unwrap();
    for version in versions {
        let v = version["version"].as_i64().unwrap();
        if version["status"] == "candidate" {
            let prompt = format!(
                "Independently inspect this candidate graph against the immutable goal, source requirements and executable tests. Planning is read-only. Check that the graph reaches the actual business outcome and fits the remaining lifetime dispatch budget including this validation, rather than only inspecting prerequisites or rebuilding unused historical evidence. Previously accepted history remains immutable; test new/changed logic and source-required current checks. Return graph_validation in the result file ={{\"digest\":\"{}\",\"approved\":true|false,\"reason\":\"concrete evidence\"}} plus the completion receipt. Do not approve missing tests or pretend execution occurred. Candidate:\n{}",
                version["digest"].as_str().unwrap(),
                version["graph"]
            );
            if !control(pool, id, &g, "validate", Some(v), &v.to_string(), &prompt).await? {
                return Ok(());
            }
        } else if version["status"] == "validated" {
            store::activate(pool, id, v, c.revision).await?;
            return Ok(());
        }
    }
    if !versions.iter().any(|v| {
        matches!(
            v["status"].as_str(),
            Some("candidate" | "validated" | "active")
        )
    }) {
        let plans = tasks.iter().filter(|t| t["role"] == "plan").count();
        if let Some(f) = state["failures"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["category"] == "plan")
        {
            if plans <= g.budget.max_evolutions as usize {
                let prompt = format!(
                    "Repair the invalid plan using this captured failure: {}. Prior proposals: {}. {}",
                    reconcile::planning_evidence(pool, f).await?,
                    state["versions"],
                    GRAPH_SCHEMA
                );
                if !control(
                    pool,
                    id,
                    &g,
                    "plan",
                    Some(0),
                    f["id"].as_str().unwrap(),
                    &prompt,
                )
                .await?
                {
                    return Ok(());
                }
            } else {
                store::hold(pool, id, "Plan evolution budget exhausted").await?;
                return Ok(());
            }
        }
    }
    if let Some(version) = versions.iter().find(|v| v["status"] == "active") {
        let graph: Graph = serde_json::from_value(version["graph"].clone())?;
        let v = version["version"].as_i64().unwrap();
        // Retire only this goal's obsolete, unstarted batches through the
        // canonical cancellation owner. Activation already fences running work.
        let obsolete:Vec<String>=sqlx::query_scalar("SELECT DISTINCT r.id FROM auto_queue_runs r JOIN auto_queue_entries e ON e.run_id=r.id JOIN campaign_graph_tasks t ON t.card_id=e.kanban_card_id WHERE t.campaign_id=$1 AND t.role='work' AND r.status IN ('generated','active','paused') AND NOT EXISTS(SELECT 1 FROM campaign_graph_versions v,jsonb_array_elements(v.graph->'nodes') n WHERE v.campaign_id=$1 AND v.status='active' AND n->>'task_key'=t.task_key)")
            .bind(id).fetch_all(pool).await?;
        for run in obsolete {
            crate::services::auto_queue::cancel_run::end_run_with_pg(
                health_registry.clone(),
                pool,
                &run,
            )
            .await
            .map_err(anyhow::Error::msg)?;
        }
        let manifests: BTreeMap<String, Value> = graph
            .nodes
            .iter()
            .filter_map(|n| {
                tasks
                    .iter()
                    .find(|t| t["task_key"] == n.task_key && t["accepted"] == true)
                    .map(|t| (n.id.clone(), t["artifact_manifest"].clone()))
            })
            .collect();
        let admission = if state["manager_enabled"] == true {
            manager::admit(pool, id, &g).await?
        } else {
            manager::Admission::Work
        };
        if admission == manager::Admission::Owned {
            return activate_runs(pool, engine, id, &g).await;
        }
        if state["manager_enabled"] != true && manifests.len() == graph.nodes.len() {
            let mut input: campaigns::CampaignInput =
                serde_json::from_value(serde_json::to_value(&c)?)?;
            for node in &mut input.nodes {
                node.status = campaigns::NodeStatus::Completed;
            }
            input.status = CampaignStatus::Completed;
            campaigns::replace(pool, id, c.revision, input).await?;
            crate::db::campaigns::graph::learning::samples::collect(pool, id, &g).await?;
            return Ok(());
        }
        for node in &graph.nodes {
            if admission == manager::Admission::VerifyOnly
                && node.kind != crate::db::campaigns::graph::Kind::Verify
            {
                continue;
            }
            if tasks.iter().any(|t| t["task_key"] == node.task_key)
                || !node.dependencies.iter().all(|d| manifests.contains_key(d))
            {
                continue;
            }
            let repo = g.repo.clone();
            let task = node.clone();
            let available = manifests.clone();
            let root = artifact_root()?;
            let preparation = tokio::task::spawn_blocking(move || -> Result<Value> {
                let repo = crate::services::git::resolve_repo_dir_for_id(Some(&repo))
                    .map_err(anyhow::Error::msg)?
                    .ok_or_else(|| anyhow::anyhow!("repository unavailable"))?;
                let workspace = crate::services::git::worktree_resolver::ensure_worktree_for_card(
                    &repo,
                    &store::card_id(&task.task_key),
                )
                .map_err(anyhow::Error::msg)?;
                artifacts::materialize(
                    &root,
                    std::path::Path::new(&workspace.path),
                    &task,
                    &available,
                )
            });
            let input_manifest = match tokio::time::timeout(
                std::time::Duration::from_secs(30),
                preparation,
            )
            .await
            {
                Ok(Ok(Ok(manifest))) => manifest,
                failure => {
                    // A timed-out filesystem worker may still unwind. Persist a
                    // hold so the next tick cannot start another one or dispatch it.
                    store::hold(pool,id,&format!("Workspace/input preparation unavailable; inspect the owned workspace before recovery: {failure:?}")).await?;
                    return Ok(());
                }
            };
            let prompt =
                occurrence_context::work_prompt(pool, id, &g, node, &input_manifest).await?;
            store::publish(
                pool,
                id,
                &store::Publication {
                    key: &node.task_key,
                    node: &node.id,
                    role: "work",
                    version: Some(v),
                    agent: &node.agent,
                    title: &node.title,
                    prompt: &prompt,
                    digest: &store::task_digest(node)?,
                },
            )
            .await?;
        }
        if !planning::publish_repair(pool, id, &g, &graph, v, &state).await? {
            return Ok(());
        }
    }
    activate_runs(pool, engine, id, &g).await
}
async fn activate_runs(pool: &PgPool, engine: &PolicyEngine, id: &str, g: &Goal) -> Result<()> {
    // Reconcile the exact persisted batches. No direct dispatch creation or
    // follow-up retry/redispatch chain is introduced.
    let runs=sqlx::query("SELECT DISTINCT r.id,r.agent_id FROM auto_queue_runs r JOIN auto_queue_entries e ON e.run_id=r.id JOIN campaign_graph_tasks t ON t.card_id=e.kanban_card_id WHERE t.campaign_id=$1 AND r.status IN ('generated','active') ORDER BY r.id")
        .bind(id).fetch_all(pool).await?;
    for run in runs {
        let body = crate::services::auto_queue::route::ActivateBody {
            run_id: Some(run.get("id")),
            repo: Some(g.repo.clone()),
            agent_id: Some(run.get("agent_id")),
            thread_group: None,
            unified_thread: Some(false),
            active_only: None,
        };
        let _ = crate::services::auto_queue::route::activate_with_bridge_pg(engine.clone(), body)
            .await
            .map_err(|e| anyhow::anyhow!("activation: {e:?}"))?;
    }
    Ok(())
}
