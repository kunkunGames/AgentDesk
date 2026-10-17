use super::super::super::{Effect, Goal, Graph, Kind, contract_digest, digest, learning, result};
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgRow};

#[path = "artifact.rs"]
mod artifact;
#[path = "pregraph_capacity.rs"]
mod pregraph_capacity;
#[path = "pregraph_plan.rs"]
mod pregraph_plan;

pub(super) fn admission(hold: Option<&str>, failure: &PgRow) -> Result<bool> {
    ensure!(
        !failure.get::<bool, _>("accepted"),
        "only an unaccepted failure may request a repair"
    );
    let category: String = failure.get("category");
    let execution_failure = matches!(category.as_str(), "unknown" | "infrastructure");
    if execution_failure {
        ensure!(
            hold == Some("Execution outcome requires reconciliation; automatic replay is disabled")
                || (hold
                    == Some(
                        "The same failure recurred after evolution; reconcile cause before any new attempt"
                    )
                    && failure.get::<Value, _>("evidence")["external_effect_requires_reconciliation"]
                        == false),
            "an explicit execution reconciliation hold is required"
        );
    } else {
        ensure!(
            category == "plan"
                && matches!(
                    failure.get::<String, _>("role").as_str(),
                    "work" | "validate"
                ),
            "only a captured business planning failure or rejected validator may request source correction"
        );
        ensure!(
            hold.is_none() || hold == Some("Plan evolution budget exhausted"),
            "source correction cannot clear an execution or effect reconciliation hold"
        );
    }
    Ok(execution_failure)
}

pub(super) async fn settled(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    failure: &str,
    execution_failure: bool,
) -> Result<()> {
    let outstanding: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM campaign_graph_failures f WHERE f.campaign_id=$1
        AND f.category IN ('unknown','infrastructure') AND NOT EXISTS(
          SELECT 1 FROM campaign_graph_reconciliations r WHERE r.failure_id=f.id)",
    )
    .bind(id)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        outstanding == i64::from(execution_failure)
            && !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
        SELECT 1 FROM campaign_graph_reconciliations WHERE failure_id=$1)"
            )
            .bind(failure)
            .fetch_one(&mut **tx)
            .await?,
        "additional unknown failures need reconciliation"
    );
    ensure!(!sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t
        JOIN task_dispatches d ON d.kanban_card_id=t.card_id WHERE t.campaign_id=$1 AND
        (d.status IN ('pending','dispatched','running') OR EXISTS(SELECT 1 FROM sessions s
          WHERE s.active_dispatch_id=d.id AND s.status IN ('turn_active','awaiting_bg','awaiting_user'))))")
        .bind(id).fetch_one(&mut **tx).await?, "campaign still owns active execution");
    ensure!(!sqlx::query_scalar::<_,bool>("WITH owned AS (SELECT DISTINCT r.id,r.status FROM campaign_graph_tasks t
        JOIN auto_queue_entries e ON e.kanban_card_id=t.card_id JOIN auto_queue_runs r ON r.id=e.run_id
        WHERE t.campaign_id=$1) SELECT EXISTS(SELECT 1 FROM owned WHERE status IN ('generated','active','paused'))
        OR EXISTS(SELECT 1 FROM auto_queue_run_cleanup_tasks WHERE run_ids && ARRAY(SELECT id FROM owned))")
        .bind(id).fetch_one(&mut **tx).await?, "campaign cleanup is incomplete");
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) struct FailureScope<'a> {
    pub active: Option<i64>,
    pub failure: &'a PgRow,
    pub evidence: &'a Value,
    pub unattempted_effect: Option<&'a super::UnattemptedEffect>,
    pub settled_effect: Option<&'a super::SettledExternalEffect>,
    pub goal: &'a Goal,
    pub rejected_validation: Option<&'a super::RejectedValidation>,
    pub cancelled_validation: Option<&'a super::CancelledValidation>,
}

pub(super) async fn failure_scope(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    scope: FailureScope<'_>,
) -> Result<()> {
    let FailureScope {
        active,
        failure,
        evidence,
        unattempted_effect,
        settled_effect,
        goal,
        rejected_validation,
        cancelled_validation,
    } = scope;
    let rejected_plan = failure.get::<String, _>("role") == "validate"
        && failure.get::<String, _>("category") == "plan";
    let pregraph_planner = failure.get::<String, _>("role") == "plan"
        && active.is_none()
        && matches!(failure.get::<Option<i64>, _>("version"), None | Some(0));
    ensure!(
        rejected_plan == rejected_validation.is_some(),
        "only a rejected validator planning failure requires an exact rejected-validation audit"
    );
    ensure!(
        cancelled_validation.is_none()
            || (failure.get::<String, _>("role") == "validate"
                && matches!(
                    failure.get::<String, _>("category").as_str(),
                    "unknown" | "infrastructure"
                )),
        "only an unknown cancelled candidate validator may use a cancelled-validation audit"
    );
    if rejected_plan || cancelled_validation.is_some() || pregraph_planner {
        // Follow the native admission lock order. The card lock fences a later
        // dispatch FK insert; the task lock also fences acceptance drift.
        let owner = sqlx::query(
            "SELECT t.accepted FROM campaign_graph_tasks t
            JOIN kanban_cards k ON k.id=t.card_id WHERE t.campaign_id=$1 AND t.task_key=$2
            FOR UPDATE OF k FOR SHARE OF t",
        )
        .bind(id)
        .bind(failure.get::<String, _>("task_key"))
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            !owner.get::<bool, _>("accepted"),
            "rejected validation was already accepted"
        );
    }
    let dispatch = evidence["dispatch_id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("captured dispatch required"))?;
    let row = sqlx::query("SELECT d.status,d.result::text AS result,d.completed_at IS NOT NULL AS ended,
        d.created_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM task_dispatches unknown
            WHERE unknown.kanban_card_id=d.kanban_card_id AND unknown.created_at IS NULL) AS chronology_known,
        d.dispatch_type,d.to_agent_id AS agent_id,t.card_id,t.observation,
        t.contract_digest AS current_contract_digest,t.version AS current_version,t.role AS current_role,t.accepted
        FROM task_dispatches d
        JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id WHERE d.id=$1 AND t.campaign_id=$2
        AND t.task_key=$3 AND d.id=(SELECT latest.id FROM task_dispatches latest
          WHERE latest.kanban_card_id=t.card_id ORDER BY latest.created_at DESC,latest.id DESC LIMIT 1) FOR SHARE OF d,t")
        .bind(dispatch).bind(id).bind(failure.get::<String,_>("task_key")).fetch_one(&mut **tx).await?;
    let status: Option<String> = row.get("status");
    if pregraph_planner {
        return pregraph_plan::validate(tx, id, scope).await;
    }
    let version: i64 = failure
        .get::<Option<i64>, _>("version")
        .ok_or_else(|| anyhow::anyhow!("versioned failed task required"))?;
    if failure.get::<String, _>("role") == "plan" {
        ensure!(
            unattempted_effect.is_none() && settled_effect.is_none(),
            "a planner cannot claim business effect resolution"
        );
        ensure!(
            active == Some(version)
                && matches!(
                    status.as_deref(),
                    Some("failed" | "completed" | "cancelled")
                ),
            "a terminal planner in the preserved active graph is required"
        );
        ensure!(
            !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM campaign_graph_versions WHERE campaign_id=$1
             AND status IN ('candidate','validated'))",
            )
            .bind(id)
            .fetch_one(&mut **tx)
            .await?,
            "another candidate already awaits QA"
        );
        let latest: String = sqlx::query_scalar(
            "SELECT task_key FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='plan'
             ORDER BY created_at DESC,task_key DESC LIMIT 1",
        )
        .bind(id)
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            latest == failure.get::<String, _>("task_key"),
            "only the latest failed planner may request repair"
        );
        return Ok(());
    }
    let raw: Value = sqlx::query_scalar(
        "SELECT graph FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2",
    )
    .bind(id)
    .bind(version)
    .fetch_one(&mut **tx)
    .await?;
    let graph: Graph = serde_json::from_value(raw)?;
    match failure.get::<String, _>("role").as_str() {
        "work" => {
            ensure!(
                active == Some(version),
                "a failed task in the preserved active graph is required"
            );
            let key: String = failure.get("task_key");
            let node = graph
                .nodes
                .iter()
                .find(|n| n.task_key == key)
                .ok_or_else(|| anyhow::anyhow!("failure is outside the active graph"))?;
            ensure!(
                (node.effect == Effect::ArtifactOnly
                    && unattempted_effect.is_none()
                    && settled_effect.is_none())
                    || (node.effect == Effect::External
                        && (unattempted_effect.is_some() || settled_effect.is_some())),
                "external business effects cannot request replay without a dispatch-pinned effect audit"
            );
            super::super::descendants::require_unpublished(tx, &graph, &node.id).await?;
            ensure!(
                matches!(
                    status.as_deref(),
                    Some("failed" | "completed" | "cancelled")
                ),
                "failed work requires a known terminal dispatch state"
            );
            if status.as_deref() != Some("completed") {
                artifact::terminal(failure, &node.effect, evidence, &row)?;
                return Ok(());
            }
            let raw: Option<String> = row.get("result");
            let mut receipt: Value = serde_json::from_str(raw.as_deref().unwrap_or("null"))?;
            if let Some(encoded) = receipt.as_str() {
                receipt = serde_json::from_str(encoded)?;
            }
            result::validate_dispatch(&receipt)?;
            ensure!(
                receipt["campaign_receipt"]["outcome"] == "blocked",
                "a captured blocked native receipt is required"
            );
            if let Some(audit) = unattempted_effect {
                super::effect::validate(
                    audit,
                    dispatch,
                    evidence,
                    raw.as_deref().unwrap_or("null"),
                    &receipt,
                )?;
            }
            if let Some(audit) = settled_effect {
                super::settled_effect::authenticated_request_proof(
                    tx, id, &key, goal, evidence, audit, true,
                )
                .await?;
            }
        }
        "validate" => {
            ensure!(
                unattempted_effect.is_none() && settled_effect.is_none(),
                "a validator cannot claim business effect resolution"
            );
            ensure!(
                matches!(status.as_deref(), Some("failed" | "completed"))
                    || (status.as_deref() == Some("cancelled") && cancelled_validation.is_some()),
                "candidate validation must be terminal"
            );
            ensure!(
                (status.as_deref() == Some("cancelled")) == cancelled_validation.is_some(),
                "only a cancelled validator requires the cancelled-validation audit"
            );
            if active.is_none() {
                ensure!(!sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks
                    WHERE campaign_id=$1 AND role='work') OR EXISTS(SELECT 1 FROM campaign_graph_occurrences WHERE campaign_id=$1)")
                    .bind(id).fetch_one(&mut **tx).await?, "published business work or occurrences require separate reconciliation");
            }
            ensure!(
                !sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks
                WHERE campaign_id=$1 AND version=$2 AND role='work')"
                )
                .bind(id)
                .bind(version)
                .fetch_one(&mut **tx)
                .await?,
                "candidate has published business work"
            );
            ensure!(
                sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM campaign_graph_versions
                WHERE campaign_id=$1 AND version=$2 AND status=$3 AND version=(
                    SELECT MAX(version) FROM campaign_graph_versions WHERE campaign_id=$1))"
                )
                .bind(id)
                .bind(version)
                .bind(if rejected_plan {
                    "rejected"
                } else {
                    "candidate"
                })
                .fetch_one(&mut **tx)
                .await?,
                "only the latest failed candidate may request planning repair"
            );
            let parent: Option<i64> = sqlx::query_scalar(
                "SELECT parent_version FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2",
            ).bind(id).bind(version).fetch_one(&mut **tx).await?;
            ensure!(
                parent == active && active.is_none_or(|v| version > v),
                "failed candidate parent differs from the preserved active graph"
            );
            if let Some(audit) = rejected_validation {
                rejected_candidate_plan(tx, id, &scope, audit, &row, &graph).await?;
            }
            if let Some(audit) = cancelled_validation {
                super::cancelled_validation::validate(tx, id, &scope, audit, &row, &graph).await?;
            }
            let roots: Vec<_> = graph
                .nodes
                .iter()
                .filter(|n| n.dependencies.is_empty())
                .collect();
            ensure!(
                !roots.is_empty(),
                "recovery roots must observe before external effects"
            );
            for root in roots {
                let already_accepted = active.is_some() && sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE campaign_id=$1 AND task_key=$2 AND accepted AND contract_digest=$3)",
                ).bind(id).bind(&root.task_key).bind(super::super::super::store::task_digest(root)?).fetch_one(&mut **tx).await?;
                // The audited cancelled candidate has no published business work.
                // Retire its artifact root into fresh planning, never replay it.
                ensure!(
                    already_accepted
                        || (root.effect == Effect::ArtifactOnly
                            && (root.kind == Kind::Inspect
                                || (cancelled_validation.is_some() && root.kind == Kind::Execute))),
                    "recovery roots must observe before external effects"
                );
            }
        }
        _ => anyhow::bail!(
            "only failed artifact work or pre-business validation may request replanning"
        ),
    }
    Ok(())
}

async fn rejected_candidate_plan(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    scope: &FailureScope<'_>,
    audit: &super::RejectedValidation,
    dispatch: &PgRow,
    graph: &Graph,
) -> Result<()> {
    let FailureScope {
        active,
        failure,
        evidence,
        ..
    } = *scope;
    let sha = |value: &str| {
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    let version = failure.get::<Option<i64>, _>("version");
    let key: String = failure.get("task_key");
    let contract: String = failure.get("contract_digest");
    ensure!(
        audit.graph_version > 0
            && version == Some(audit.graph_version)
            && !audit.dispatch_id.is_empty()
            && audit.dispatch_id.len() <= 128
            && sha(&audit.graph_digest)
            && sha(&audit.raw_result_sha256)
            && sha(&audit.contract_digest)
            && audit.contract_digest == contract
            && dispatch.get::<String, _>("current_contract_digest") == contract
            && dispatch.get::<Option<i64>, _>("current_version") == version
            && dispatch.get::<String, _>("current_role") == "validate"
            && !dispatch.get::<bool, _>("accepted")
            && key == format!("{id}:validate:{}", audit.graph_version)
            && evidence["dispatch_id"] == audit.dispatch_id
            && evidence["task_key"] == key
            && evidence["external_effect_requires_reconciliation"] == false
            && evidence["runtime"]["role"] == "validate"
            && evidence["runtime"]["graph_version"].as_i64() == version
            && evidence["runtime"]["contract_digest"] == contract
            && evidence["runtime"]["dispatch_status"] == "completed"
            && evidence["failure"]["category"] == "plan"
            && evidence["failure"]["status"] == "completed"
            && dispatch.get::<Option<Value>, _>("observation").as_ref() == Some(evidence),
        "rejected validation audit differs from the captured native task and failure"
    );
    ensure!(
        dispatch.get::<Option<String>, _>("status").as_deref() == Some("completed")
            && dispatch.get::<bool, _>("ended")
            && dispatch.get::<bool, _>("chronology_known")
            && matches!(
                dispatch
                    .get::<Option<String>, _>("dispatch_type")
                    .as_deref(),
                Some("implementation" | "rework")
            ),
        "rejected validation requires the latest completed native dispatch and terminal timestamp"
    );
    let candidate = sqlx::query(
        "SELECT digest,status,parent_version,graph FROM campaign_graph_versions
        WHERE campaign_id=$1 AND version=$2 FOR SHARE",
    )
    .bind(id)
    .bind(audit.graph_version)
    .fetch_one(&mut **tx)
    .await?;
    let current_graph: Graph = serde_json::from_value(candidate.get("graph"))?;
    ensure!(
        candidate.get::<String, _>("status") == "rejected"
            && candidate.get::<String, _>("digest") == audit.graph_digest
            && contract_digest(graph)? == audit.graph_digest
            && contract_digest(&current_graph)? == audit.graph_digest
            && candidate.get::<Option<i64>, _>("parent_version") == active,
        "rejected candidate graph digest or parent changed"
    );
    if let Some(parent) = active {
        ensure!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM campaign_graph_versions
            WHERE campaign_id=$1 AND version=$2 FOR SHARE"
            )
            .bind(id)
            .bind(parent)
            .fetch_one(&mut **tx)
            .await?
                == "active",
            "rejected candidate parent is not the preserved active graph"
        );
    }
    ensure!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM campaign_graph_versions
        WHERE campaign_id=$1 AND status IN ('candidate','validated'))"
        )
        .bind(id)
        .fetch_one(&mut **tx)
        .await?,
        "another candidate already awaits QA"
    );
    let agent = sqlx::query(
        "SELECT k.assigned_agent_id,a.provider FROM kanban_cards k
        LEFT JOIN agents a ON a.id=k.assigned_agent_id WHERE k.id=$1 FOR SHARE OF k",
    )
    .bind(dispatch.get::<String, _>("card_id"))
    .fetch_one(&mut **tx)
    .await?;
    let assigned: Option<String> = agent.get("assigned_agent_id");
    let provider: Option<String> = agent.get("provider");
    ensure!(
        assigned.is_some()
            && assigned == dispatch.get::<Option<String>, _>("agent_id")
            && evidence["runtime"]["agent_id"].as_str() == assigned.as_deref()
            && matches!(provider.as_deref(), Some("codex" | "qwen" | "opencode"))
            && provider.as_deref() == evidence["runtime"]["provider"].as_str(),
        "rejected validator agent or provider changed"
    );
    let raw: Option<String> = dispatch.get("result");
    let raw = raw.ok_or_else(|| anyhow::anyhow!("retained rejected validation result required"))?;
    ensure!(
        digest(raw.as_bytes()) == audit.raw_result_sha256,
        "rejected validation raw result differs from the operator snapshot"
    );
    let mut receipt: Value = serde_json::from_str(&raw)?;
    if let Some(encoded) = receipt.as_str() {
        receipt = serde_json::from_str(encoded)?;
    }
    result::validate_dispatch(&receipt)?;
    ensure!(
        receipt["campaign_receipt"]["outcome"] == "blocked"
            && receipt["campaign_receipt"]["failure_category"] == "plan"
            && receipt["graph_validation"]["approved"] == false
            && receipt["graph_validation"]["digest"] == audit.graph_digest
            && receipt["graph_validation"]["reason"]
                .as_str()
                .is_some_and(|reason| !reason.trim().is_empty()),
        "a native blocked planning receipt must reject the exact candidate digest"
    );
    // Native completion stores JSONB event payloads while dispatch result is
    // TEXT. Compare historical JSON content, never invented historical bytes.
    let captured_event = sqlx::query(
        "SELECT transition_source,payload_json FROM dispatch_events
        WHERE dispatch_id=$1 AND kanban_card_id=$2 AND dispatch_type=$3
          AND from_status IN ('pending','dispatched') AND to_status='completed'
          AND transition_source IN ('campaign_native_receipt','watcher_streaming_final')
        ORDER BY id LIMIT 1 FOR SHARE",
    )
    .bind(&audit.dispatch_id)
    .bind(dispatch.get::<String, _>("card_id"))
    .bind(dispatch.get::<Option<String>, _>("dispatch_type"))
    .fetch_one(&mut **tx)
    .await?;
    let completion_source: String = captured_event.get("transition_source");
    let captured: Value = captured_event.get("payload_json");
    let captured = if let Some(encoded) = captured.as_str() {
        serde_json::from_str(encoded)?
    } else {
        captured
    };
    ensure!(
        captured == receipt,
        "rejected validation content differs from native completion evidence"
    );
    // Older native streaming captures omitted optional source metadata. The
    // exact completed event authenticates those unchanged historical bytes;
    // an explicit source must agree with that event, never be overwritten.
    ensure!(
        receipt["completion_source"].is_null()
            || receipt["completion_source"].as_str() == Some(completion_source.as_str()),
        "rejected validation completion source conflicts with native completion evidence"
    );
    Ok(())
}

pub(super) async fn capacity(
    tx: &mut Transaction<'_, Postgres>,
    _pool: &PgPool,
    id: &str,
    goal: &Goal,
) -> Result<()> {
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*) FILTER(WHERE role='plan'),
        (SELECT COALESCE(MAX(version),0) FROM campaign_graph_versions WHERE campaign_id=$1)
        FROM campaign_graph_tasks WHERE campaign_id=$1",
    )
    .bind(id)
    .fetch_one(&mut **tx)
    .await?;
    let usage = learning::budget::usage_on_connection(tx, id, goal).await?;
    ensure!(
        usage.published_tasks + 4 <= i64::from(goal.budget.max_dispatches)
            && counts.0 < i64::from(goal.budget.max_evolutions) + 1
            && counts.1 < i64::from(goal.budget.max_evolutions) + 1,
        "finite budget must leave room for a planner, independent QA and verified business work"
    );
    ensure!(
        usage.elapsed_dispatch_seconds < f64::from(goal.budget.max_elapsed_secs),
        "elapsed budget exhausted"
    );
    ensure!(
        goal.budget
            .max_observed_tool_calls
            .is_none_or(|limit| usage.observed_tool_calls < i64::from(limit)),
        "observed tool budget exhausted"
    );
    Ok(())
}
