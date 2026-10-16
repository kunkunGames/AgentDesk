//! Retire a pinned internally cancelled QA candidate without accepting or replaying it.
use super::guards::FailureScope;
use crate::db::campaigns::graph::{Graph, contract_digest, digest};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};

/// Seals an operator's current snapshot; it is not evidence of a missing QA result.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CancelledValidation {
    pub dispatch_id: String,
    pub graph_version: i64,
    pub graph_digest: String,
    pub raw_result_sha256: String,
    pub contract_digest: String,
}

pub(super) async fn validate(
    tx: &mut Transaction<'_, Postgres>,
    campaign: &str,
    scope: &FailureScope<'_>,
    audit: &CancelledValidation,
    dispatch: &PgRow,
    graph: &Graph,
) -> Result<()> {
    let FailureScope {
        active,
        failure,
        evidence,
        goal,
        ..
    } = *scope;
    let sha = |value: &str| {
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    let version: Option<i64> = failure.get("version");
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
            && key == format!("{campaign}:validate:{}", audit.graph_version)
            && evidence["dispatch_id"] == audit.dispatch_id
            && evidence["task_key"] == key
            && evidence["external_effect_requires_reconciliation"] == false
            && evidence["runtime"]["role"] == "validate"
            && evidence["runtime"]["graph_version"].as_i64() == version
            && evidence["runtime"]["contract_digest"] == contract
            && evidence["runtime"]["dispatch_status"] == "cancelled"
            && evidence["runtime"]["observed_tool_calls"] == 0
            && evidence["failure"]["status"] == "cancelled"
            && evidence["failure"]["category"].as_str()
                == Some(failure.get::<String, _>("category").as_str())
            && dispatch.get::<Option<Value>, _>("observation").as_ref() == Some(evidence),
        "cancelled validation audit differs from the captured native task and failure"
    );
    ensure!(
        dispatch.get::<Option<String>, _>("status").as_deref() == Some("cancelled")
            && dispatch.get::<bool, _>("chronology_known")
            && matches!(
                dispatch
                    .get::<Option<String>, _>("dispatch_type")
                    .as_deref(),
                Some("implementation" | "rework")
            ),
        "cancelled validation requires the latest known native dispatch"
    );
    let candidate = sqlx::query(
        "SELECT digest,status,parent_version,graph,template_id,planner_agent_id
         FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2 FOR SHARE",
    )
    .bind(campaign)
    .bind(audit.graph_version)
    .fetch_one(&mut **tx)
    .await?;
    let current_graph: Graph = serde_json::from_value(candidate.get("graph"))?;
    ensure!(
        candidate.get::<String, _>("status") == "candidate"
            && candidate.get::<String, _>("digest") == audit.graph_digest
            && contract_digest(graph)? == audit.graph_digest
            && contract_digest(&current_graph)? == audit.graph_digest
            && candidate.get::<Option<i64>, _>("parent_version") == active
            && candidate.get::<Option<String>, _>("template_id").is_none(),
        "cancelled candidate graph digest, parent or template scope changed"
    );
    if let Some(parent) = active {
        ensure!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM campaign_graph_versions
                 WHERE campaign_id=$1 AND version=$2 FOR SHARE",
            )
            .bind(campaign)
            .bind(parent)
            .fetch_one(&mut **tx)
            .await?
                == "active",
            "cancelled candidate parent is not the preserved active graph"
        );
    }
    let task = sqlx::query(
        "SELECT t.node_id,t.execution_profile,k.metadata,k.assigned_agent_id,a.provider,
                d.created_at IS NOT NULL AND d.updated_at>=d.created_at AS terminal_chronology
         FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id
         JOIN task_dispatches d ON d.id=$3 AND d.kanban_card_id=k.id
         LEFT JOIN agents a ON a.id=k.assigned_agent_id
         WHERE t.campaign_id=$1 AND t.task_key=$2 FOR SHARE OF t,k,d",
    )
    .bind(campaign)
    .bind(&key)
    .bind(&audit.dispatch_id)
    .fetch_one(&mut **tx)
    .await?;
    let profile: Value = task.get("execution_profile");
    let metadata: Option<Value> = task.get("metadata");
    let assigned: Option<String> = task.get("assigned_agent_id");
    let provider: Option<String> = task.get("provider");
    let planner: Option<String> = candidate.get("planner_agent_id");
    let planner = planner.as_deref().unwrap_or(&goal.planner);
    let verifier = if planner == goal.verifier {
        &goal.planner
    } else {
        &goal.verifier
    };
    ensure!(
        task.get::<String, _>("node_id") == "validate"
            && task.get::<bool, _>("terminal_chronology")
            && profile["schema_version"] == 1
            && profile["graph_version"].as_i64() == version
            && profile["template_id"].is_null()
            && profile["goal_digest"] == contract_digest(goal)?
            && metadata
                .as_ref()
                .is_some_and(|value| value["campaign_id"] == campaign
                    && value["node_id"] == "validate"
                    && value["graph_version"].as_i64() == version
                    && value["graph_task_key"] == key)
            && assigned.as_deref() == Some(verifier.as_str())
            && assigned.as_deref() != Some(planner)
            && assigned == dispatch.get::<Option<String>, _>("agent_id")
            && evidence["runtime"]["agent_id"].as_str() == assigned.as_deref()
            && matches!(provider.as_deref(), Some("codex" | "qwen" | "opencode"))
            && provider.as_deref() == evidence["runtime"]["provider"].as_str()
            && profile["provider_requested"].as_str() == provider.as_deref(),
        "cancelled validator native role, goal or independent executor changed"
    );
    // Telemetry can be incomplete. Zero samples only narrows this cold-start
    // path; role/candidate ownership and fresh QA, never absence of samples,
    // justify new planning. The missing result remains unknown and unaccepted.
    ensure!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM campaign_graph_progress WHERE dispatch_id=$1)",
        )
        .bind(&audit.dispatch_id)
        .fetch_one(&mut **tx)
        .await?,
        "observed validator execution needs separate reconciliation"
    );
    let raw: Option<String> = dispatch.get("result");
    let raw = raw.ok_or_else(|| anyhow::anyhow!("retained cancellation result required"))?;
    ensure!(
        digest(raw.as_bytes()) == audit.raw_result_sha256,
        "cancelled validation raw result differs from the operator snapshot"
    );
    let captured: Value = serde_json::from_str(&raw)?;
    // This exact reason is the producer's anonymous PreserveSession teardown
    // sentinel (#3169). User stops/watchdogs carry their own source. Provider
    // labels, extra result fields or queue status alone do not authenticate it.
    let anonymous = json!({"reason":"turn_bridge_cancelled"});
    ensure!(
        captured == anonymous,
        "only anonymous internal cancellation may request fresh validator planning"
    );
    let events = sqlx::query(
        "SELECT e.kanban_card_id,e.dispatch_type,e.from_status,e.to_status,
                e.transition_source,e.payload_json,
                e.created_at>=d.created_at AND e.created_at<=d.updated_at AS chronology
         FROM dispatch_events e JOIN task_dispatches d ON d.id=e.dispatch_id
         WHERE e.dispatch_id=$1 AND e.to_status IN ('completed','failed','cancelled')
         ORDER BY e.id FOR SHARE OF e,d",
    )
    .bind(&audit.dispatch_id)
    .fetch_all(&mut **tx)
    .await?;
    ensure!(
        events.len() == 1,
        "one canonical internal cancellation event required"
    );
    let event = &events[0];
    ensure!(
        event.get::<Option<String>, _>("kanban_card_id").as_deref()
            == Some(dispatch.get::<String, _>("card_id").as_str())
            && event.get::<Option<String>, _>("dispatch_type")
                == dispatch.get::<Option<String>, _>("dispatch_type")
            && matches!(
                event.get::<Option<String>, _>("from_status").as_deref(),
                Some("pending" | "dispatched")
            )
            && event.get::<String, _>("to_status") == "cancelled"
            && event.get::<String, _>("transition_source") == "cancel_dispatch"
            && event.get::<Option<Value>, _>("payload_json").as_ref() == Some(&anonymous)
            && event.get::<bool, _>("chronology"),
        "anonymous cancellation lacks matching canonical native provenance"
    );
    Ok(())
}
