//! Exact native provenance and operator snapshot integrity precede recovery.
use super::Recovery;
use crate::db::campaigns::graph::{Goal, Graph, contract_digest, digest, workflow};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{Postgres, Row, Transaction};

fn sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn request_shape(request: &Recovery) -> Result<()> {
    let audit = &request.failed_finish;
    ensure!(
        request.expected_revision > 0
            && !request.request_id.trim().is_empty()
            && request.request_id.len() <= 128
            && !request.failure_id.is_empty()
            && request.failure_id.len() <= 128
            && !request.evidence.trim().is_empty()
            && request.evidence.len() <= 8000
            && !audit.dispatch_id.is_empty()
            && audit.dispatch_id.len() <= 128
            && audit.graph_version > 0
            && sha256(&audit.graph_digest)
            && sha256(&audit.raw_result_sha256)
            && sha256(&audit.contract_digest)
            && sha256(&audit.progress_sha256),
        "bounded evidence, exact native identities and lowercase SHA256 pins required"
    );
    let attestation = &request.operator_attestation;
    ensure!(
        attestation.read_only_execution_verified
            && attestation.no_unknown_effects
            && attestation.provider_cause_repaired,
        "explicit administrator verification of retained read-only commands, effects and provider repair required"
    );
    Ok(())
}

pub(crate) fn quota_result() -> Value {
    // Exact producer presentation, not a substring or provider error label.
    // A quota error does not by itself prove that earlier commands had no effect.
    json!({"error":format!(
        "⚠️ {}\n||**상세**\n```text\nError: Your workspace is out of credits. Add credits to continue.\n```||",
        crate::services::provider_error_transcript::PROVIDER_FAILURE_GUIDANCE
    )})
}

pub(super) async fn captured(
    tx: &mut Transaction<'_, Postgres>,
    campaign: &str,
    goal: &Goal,
    request: &Recovery,
) -> Result<Value> {
    let audit = &request.failed_finish;
    let failure = sqlx::query(
        "SELECT f.task_key,f.category,f.evidence,f.fingerprint,
            f.created_at>=d.updated_at AND f.created_at<=NOW() AS failure_chronology,
            t.role,t.node_id,t.version,t.contract_digest,t.execution_profile,t.accepted,
            t.observation,t.artifact_manifest,k.metadata,k.assigned_agent_id,k.description,
            d.id,d.kanban_card_id,d.to_agent_id,d.dispatch_type,d.status,d.result::text AS result,
            d.created_at IS NOT NULL AND d.updated_at>=d.created_at
                AND d.completed_at IS NULL AS terminal_chronology,
            t.created_at<=d.created_at AND NOT EXISTS(SELECT 1 FROM task_dispatches unknown
                WHERE unknown.kanban_card_id=t.card_id AND unknown.created_at IS NULL) AS known_chronology,
            a.provider
         FROM campaign_graph_failures f JOIN campaign_graph_tasks t ON t.task_key=f.task_key
         JOIN kanban_cards k ON k.id=t.card_id JOIN task_dispatches d ON d.kanban_card_id=k.id
         LEFT JOIN agents a ON a.id=k.assigned_agent_id
         WHERE f.id=$1 AND f.campaign_id=$2 AND d.id=$3
           AND d.id=(SELECT latest.id FROM task_dispatches latest WHERE latest.kanban_card_id=k.id
               ORDER BY latest.created_at DESC,latest.id DESC LIMIT 1)
         FOR UPDATE OF k,d FOR SHARE OF f,t",
    )
    .bind(&request.failure_id)
    .bind(campaign)
    .bind(&audit.dispatch_id)
    .fetch_one(&mut **tx)
    .await?;
    let evidence: Value = failure.get("evidence");
    let key: String = failure.get("task_key");
    let contract: String = failure.get("contract_digest");
    let profile = failure
        .get::<Option<Value>, _>("execution_profile")
        .ok_or_else(|| anyhow::anyhow!("native final verifier execution profile missing"))?;
    let fingerprint = digest(b"unknown:failed:Terminal dispatch has no accepted business evidence");
    ensure!(
        failure.get::<String, _>("category") == "unknown"
            && failure.get::<String, _>("role") == "finish"
            && failure.get::<String, _>("node_id") == "finish"
            && failure.get::<Option<i64>, _>("version") == Some(audit.graph_version)
            && !failure.get::<bool, _>("accepted")
            && failure
                .get::<Option<Value>, _>("artifact_manifest")
                .is_none()
            && failure.get::<String, _>("fingerprint") == fingerprint
            && request.failure_id == digest(format!("{campaign}:{key}:{fingerprint}").as_bytes())
            && failure.get::<Option<Value>, _>("observation").as_ref() == Some(&evidence)
            && contract == audit.contract_digest
            && evidence["task_key"] == key
            && evidence["dispatch_id"] == audit.dispatch_id
            && evidence["external_effect_requires_reconciliation"] == false
            && evidence["runtime"]["role"] == "finish"
            && evidence["runtime"]["graph_version"] == audit.graph_version
            && evidence["runtime"]["contract_digest"] == contract
            && evidence["runtime"]["artifact_manifest"].is_null()
            && evidence["runtime"]["dispatch_status"] == "failed"
            && evidence["failure"]
                == json!({"category":"unknown","status":"failed",
                "summary":"Terminal dispatch has no accepted business evidence","fingerprint":fingerprint}),
        "failed finish differs from the exact retained native observation"
    );
    ensure!(
        failure.get::<Option<String>, _>("status").as_deref() == Some("failed")
            && failure.get::<Option<bool>, _>("terminal_chronology") == Some(true)
            && failure.get::<Option<bool>, _>("known_chronology") == Some(true)
            && failure.get::<Option<bool>, _>("failure_chronology") == Some(true)
            && matches!(
                failure.get::<Option<String>, _>("dispatch_type").as_deref(),
                Some("implementation" | "rework")
            ),
        "latest canonical failed native finish with retained missing completion timestamp required"
    );
    let raw: String = failure
        .get::<Option<String>, _>("result")
        .ok_or_else(|| anyhow::anyhow!("retained native quota result required"))?;
    ensure!(
        digest(raw.as_bytes()) == audit.raw_result_sha256
            && serde_json::from_str::<Value>(&raw)? == quota_result(),
        "only the exact singleton native provider quota result may be recovered"
    );
    let graph = sqlx::query(
        "SELECT graph,digest,status,template_id FROM campaign_graph_versions
         WHERE campaign_id=$1 AND version=$2 FOR SHARE",
    )
    .bind(campaign)
    .bind(audit.graph_version)
    .fetch_one(&mut **tx)
    .await?;
    let published: Graph = serde_json::from_value(graph.get("graph"))?;
    ensure!(
        graph.get::<String, _>("status") == "active"
            && graph.get::<String, _>("digest") == audit.graph_digest
            && contract_digest(&published)? == audit.graph_digest
            && !published.nodes.iter().any(|node| node.task_key == key),
        "failed finish is not an implicit control of the unchanged active graph"
    );
    ensure!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM campaign_graph_versions WHERE campaign_id=$1
                AND (version>$2 OR status IN ('candidate','validated')))
                OR EXISTS(SELECT 1 FROM campaign_graph_occurrences WHERE campaign_id=$1)",
        )
        .bind(campaign)
        .bind(audit.graph_version)
        .fetch_one(&mut **tx)
        .await?,
        "a newer graph, candidate or scheduled occurrence needs separate recovery"
    );
    ensure!(
        sqlx::query_scalar::<_, String>(
            "SELECT task_key FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='finish'
             ORDER BY created_at DESC,task_key DESC LIMIT 1",
        )
        .bind(campaign)
        .fetch_one(&mut **tx)
        .await?
            == key,
        "only the latest failed final verifier may be recovered"
    );
    let assigned: Option<String> = failure.get("assigned_agent_id");
    let provider: Option<String> = failure.get("provider");
    let metadata: Option<Value> = failure.get("metadata");
    let description: Option<String> = failure.get("description");
    let description =
        description.ok_or_else(|| anyhow::anyhow!("native control prompt missing"))?;
    let snapshot = profile["workflow_snapshot_digest"].as_str().unwrap_or("");
    let manifest = &profile["control_input_manifest"];
    ensure!(
        goal.schedule.is_none()
            && goal.planner != goal.verifier
            && assigned.as_deref() == Some(goal.verifier.as_str())
            && assigned == failure.get::<Option<String>, _>("to_agent_id")
            && provider.as_deref() == Some("codex")
            && evidence["runtime"]["provider"] == "codex"
            && evidence["runtime"]["agent_id"].as_str() == assigned.as_deref()
            && key == format!("{campaign}:finish:{snapshot}-goal")
            && sha256(snapshot)
            && contract_digest(&description)? == contract
            && profile["schema_version"] == 1
            && profile["graph_version"] == audit.graph_version
            && profile["template_id"]
                == serde_json::to_value(graph.get::<Option<String>, _>("template_id"))?
            && profile["provider_requested"] == "codex"
            && profile["prompt_digest"] == digest(description.as_bytes())
            && profile["goal_digest"] == contract_digest(goal)?
            && profile["workflow_goal"] == serde_json::to_value(goal)?
            && profile["workflow_graph_digest"] == audit.graph_digest
            && profile["result_schema_digest"]
                == contract_digest(&crate::db::campaigns::graph::schema::document()?)?
            && profile["control_input_digest"] == contract_digest(manifest)?
            && manifest["bundle"].as_str().is_some_and(sha256)
            && manifest["files"]
                .as_object()
                .is_some_and(|files| files.len() == 1)
            && manifest["files"][workflow::INPUT_FILE]
                .as_str()
                .is_some_and(sha256)
            && metadata
                .as_ref()
                .is_some_and(|value| value["campaign_id"] == campaign
                    && value["node_id"] == "finish"
                    && value["graph_version"] == audit.graph_version
                    && value["graph_task_key"] == key),
        "native independent finish executor, goal, prompt or frozen input changed"
    );
    let mut prior = workflow::store::snapshot_on_connection(tx, campaign).await?;
    let current_failures = prior.failures.len();
    prior
        .failures
        .retain(|item| item["id"] != request.failure_id);
    ensure!(
        prior.failures.len() + 1 == current_failures
            && prior.authority_state() == profile["workflow_authority_state"]
            && prior.digest()? == snapshot,
        "business receipts or control authority changed after the failed finish was published"
    );
    let unresolved: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM campaign_graph_failures f WHERE f.campaign_id=$1
         AND f.category IN ('unknown','infrastructure') AND NOT EXISTS(
            SELECT 1 FROM campaign_graph_reconciliations r WHERE r.failure_id=f.id)",
    )
    .bind(campaign)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        unresolved == 1
            && !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM campaign_graph_reconciliations WHERE failure_id=$1)",
            )
            .bind(&request.failure_id)
            .fetch_one(&mut **tx)
            .await?,
        "additional unknown failures or an existing different reconciliation require review"
    );
    let events = sqlx::query(
        "SELECT e.kanban_card_id,e.dispatch_type,e.from_status,e.to_status,e.transition_source,
            e.payload_json,e.created_at>=d.created_at AND e.created_at<=d.updated_at AS chronology
         FROM dispatch_events e JOIN task_dispatches d ON d.id=e.dispatch_id
         WHERE e.dispatch_id=$1 AND e.to_status IN ('completed','failed','cancelled')
         ORDER BY e.id FOR SHARE OF e,d",
    )
    .bind(&audit.dispatch_id)
    .fetch_all(&mut **tx)
    .await?;
    ensure!(
        events.len() == 1,
        "one canonical native provider failure event required"
    );
    let event = &events[0];
    ensure!(
        event.get::<Option<String>, _>("kanban_card_id")
            == failure.get::<Option<String>, _>("kanban_card_id")
            && event.get::<Option<String>, _>("dispatch_type")
                == failure.get::<Option<String>, _>("dispatch_type")
            && matches!(
                event.get::<Option<String>, _>("from_status").as_deref(),
                Some("pending" | "dispatched")
            )
            && event.get::<String, _>("to_status") == "failed"
            && event.get::<String, _>("transition_source") == "turn_bridge_dispatch_failure"
            && event.get::<Option<Value>, _>("payload_json") == Some(quota_result())
            && event.get::<Option<bool>, _>("chronology") == Some(true),
        "quota failure lacks matching canonical native producer provenance"
    );
    let rows = sqlx::query(
        "SELECT p.event_id,p.fingerprint,p.output_digest,
            p.recorded_at>=d.created_at AND p.recorded_at<=d.updated_at AS chronology
         FROM campaign_graph_progress p JOIN task_dispatches d ON d.id=p.dispatch_id
         WHERE p.dispatch_id=$1 ORDER BY p.event_id FOR SHARE OF p,d",
    )
    .bind(&audit.dispatch_id)
    .fetch_all(&mut **tx)
    .await?;
    ensure!(
        rows.len() <= 256,
        "bounded retained final verifier telemetry required"
    );
    let progress: Vec<(String, String, Option<String>)> = rows
        .iter()
        .map(|row| {
            (
                row.get("event_id"),
                row.get("fingerprint"),
                row.get("output_digest"),
            )
        })
        .collect();
    ensure!(
        rows.iter()
            .all(|row| row.get::<Option<bool>, _>("chronology") == Some(true))
            && evidence["runtime"]["observed_tool_calls"].as_u64() == Some(rows.len() as u64)
            && contract_digest(&progress)? == audit.progress_sha256,
        "retained hash-only telemetry differs from the explicit operator audit"
    );
    Ok(evidence)
}
