//! Admit only a canonical capacity refusal before the first provider execution.
//! This read-only proof does not accept the old plan or replay its failed card.
use super::{FailureScope, Goal, contract_digest, digest, learning};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{Postgres, Row, Transaction};

fn capacity_result() -> Value {
    json!({"error":format!(
        "⚠️ {}\n||**상세**\n```text\nError: node execution capacity exhausted; retry when a turn completes\n```||",
        crate::services::provider_error_transcript::PROVIDER_FAILURE_GUIDANCE
    )})
}

fn sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

async fn frozen_input(manifest: &Value, id: &str, goal: &Goal, family: &str) -> Result<()> {
    let input = super::super::super::super::planning_input::INPUT_FILE;
    ensure!(
        manifest.as_object().is_some_and(|fields| fields.len() == 2)
            && manifest["bundle"].as_str().is_some_and(sha256)
            && manifest["files"]
                .as_object()
                .is_some_and(|files| files.len() == 1)
            && manifest["files"][input].as_str().is_some_and(sha256),
        "initial planner requires its exact bounded frozen planning input"
    );
    let root = crate::config::runtime_root()
        .ok_or_else(|| anyhow::anyhow!("runtime root unavailable"))?
        .join("artifacts/campaign-graphs");
    let manifest = manifest.clone();
    let captured: Value = tokio::task::spawn_blocking(move || -> Result<Value> {
        crate::services::campaign_artifacts::verify(&root, &manifest)?;
        let path = root.join(manifest["bundle"].as_str().unwrap()).join(input);
        Ok(serde_json::from_slice(&std::fs::read(path)?)?)
    })
    .await??;
    ensure!(
        captured["schema_version"] == 1
            && captured["purpose"] == "planning_advice_only"
            && captured["campaign_id"] == id
            && captured["family_id"] == family
            && captured["goal_digest"] == contract_digest(goal)?
            && captured["scope_digest"] == learning::scope_digest(goal)?
            && captured["accepted_current_contracts"] == json!([]),
        "initial planner frozen input no longer matches the unchanged goal and empty work frontier"
    );
    Ok(())
}

pub(super) async fn validate(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    scope: FailureScope<'_>,
) -> Result<()> {
    let FailureScope {
        active,
        failure,
        evidence,
        goal,
        unattempted_effect,
        settled_effect,
        rejected_validation,
        cancelled_validation,
    } = scope;
    let key: String = failure.get("task_key");
    ensure!(
        active.is_none()
            && key == format!("{id}:plan:initial")
            && failure.get::<String, _>("category") == "unknown"
            && failure.get::<String, _>("role") == "plan"
            && failure.get::<Option<i64>, _>("version").is_none()
            && unattempted_effect.is_none()
            && settled_effect.is_none()
            && rejected_validation.is_none()
            && cancelled_validation.is_none()
            && goal.schedule.is_none(),
        "only an unversioned initial planner without effect or validation claims may use capacity recovery"
    );
    ensure!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM campaign_graph_versions WHERE campaign_id=$1)
             OR EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE campaign_id=$1 AND task_key<>$2)
             OR (SELECT COUNT(*) FROM campaign_graph_failures WHERE campaign_id=$1)<>1",
        )
        .bind(id)
        .bind(&key)
        .fetch_one(&mut **tx)
        .await?,
        "initial planner recovery requires no graph, other published work or additional failure"
    );
    let dispatch = evidence["dispatch_id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("captured initial planner dispatch required"))?;
    let rows = sqlx::query(
        "SELECT f.id,f.fingerprint,f.evidence,
            f.created_at>=d.updated_at AND f.created_at<=NOW() AS failure_chronology,
            t.node_id,t.role,t.version,t.accepted,t.observation,t.artifact_manifest,
            t.contract_digest,t.execution_profile,k.metadata,k.description,k.assigned_agent_id,k.repo_id,
            d.id AS dispatch_id,d.kanban_card_id,d.dispatch_type,d.to_agent_id,d.status,d.result,
            d.created_at IS NOT NULL AND d.updated_at>=d.created_at
                AND d.completed_at IS NULL AS terminal_chronology,
            t.created_at<=d.created_at AS publication_chronology,
            (SELECT COUNT(*) FROM task_dispatches attempt WHERE attempt.kanban_card_id=k.id)=1 AS sole_attempt
         FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id
         JOIN campaign_graph_failures f ON f.task_key=t.task_key AND f.campaign_id=t.campaign_id
         JOIN task_dispatches d ON d.kanban_card_id=k.id
         WHERE t.campaign_id=$1 AND t.task_key=$2 AND f.evidence=$3 AND d.id=$4
         FOR SHARE OF f,t,d",
    ).bind(id).bind(&key).bind(evidence).bind(dispatch).fetch_all(&mut **tx).await?;
    ensure!(
        rows.len() == 1,
        "one exact native initial planner failure required"
    );
    let row = &rows[0];
    let contract: String = row.get("contract_digest");
    let fingerprint = digest(b"unknown:failed:Terminal dispatch has no accepted business evidence");
    ensure!(
        row.get::<String, _>("id") == digest(format!("{id}:{key}:{fingerprint}").as_bytes())
            && row.get::<String, _>("fingerprint") == fingerprint
            && row.get::<String, _>("node_id") == "plan"
            && row.get::<String, _>("role") == "plan"
            && row.get::<Option<i64>, _>("version").is_none()
            && !row.get::<bool, _>("accepted")
            && row.get::<Option<Value>, _>("artifact_manifest").is_none()
            && row.get::<Option<Value>, _>("observation").as_ref() == Some(evidence)
            && failure.get::<String, _>("contract_digest") == contract
            && evidence["task_key"] == key
            && evidence["dispatch_reference"] == format!("/api/dispatches/{dispatch}")
            && evidence["external_effect_requires_reconciliation"] == false
            && evidence["runtime"]["role"] == "plan"
            && evidence["runtime"].get("graph_version") == Some(&Value::Null)
            && evidence["runtime"]["contract_digest"] == contract
            && evidence["runtime"].get("artifact_manifest") == Some(&Value::Null)
            && evidence["runtime"]["dispatch_status"] == "failed"
            && evidence["runtime"]["observed_tool_calls"].as_u64() == Some(0)
            && evidence["failure"]
                == json!({"category":"unknown","status":"failed",
                "summary":"Terminal dispatch has no accepted business evidence","fingerprint":fingerprint}),
        "initial planner differs from its exact retained native failure observation"
    );
    ensure!(
        row.get::<Option<String>, _>("status").as_deref() == Some("failed")
            && row.get::<Option<String>, _>("dispatch_type").as_deref() == Some("implementation")
            && row.get::<Option<bool>, _>("terminal_chronology") == Some(true)
            && row.get::<Option<bool>, _>("publication_chronology") == Some(true)
            && row.get::<bool, _>("sole_attempt")
            && row.get::<Option<bool>, _>("failure_chronology") == Some(true),
        "initial planner requires its sole failed native attempt with known chronology and missing completion timestamp"
    );
    let raw: Option<String> = row.get("result");
    let raw = raw.ok_or_else(|| anyhow::anyhow!("retained initial planner result required"))?;
    // Decode the native JSON envelope once; never rewrite/hash its re-encoding.
    ensure!(
        serde_json::from_str::<Value>(&raw)? == capacity_result(),
        "only the exact native pre-provider capacity refusal may recover an initial planner"
    );
    let profile = row
        .get::<Option<Value>, _>("execution_profile")
        .ok_or_else(|| anyhow::anyhow!("native initial planner execution profile missing"))?;
    let description = row
        .get::<Option<String>, _>("description")
        .ok_or_else(|| anyhow::anyhow!("native initial planner prompt missing"))?;
    let assigned: Option<String> = row.get("assigned_agent_id");
    let metadata: Option<Value> = row.get("metadata");
    let provider = profile["provider_requested"].as_str();
    let manifest = &profile["control_input_manifest"];
    ensure!(
        assigned.is_some()
            && assigned == row.get::<Option<String>, _>("to_agent_id")
            && evidence["runtime"]["agent_id"].as_str() == assigned.as_deref()
            && provider.is_some_and(|p| matches!(p, "codex" | "qwen" | "opencode"))
            && evidence["runtime"]["provider"].as_str() == provider
            && row.get::<Option<String>, _>("repo_id").as_deref() == Some(goal.repo.as_str())
            && contract_digest(&description)? == contract
            && profile.as_object().is_some_and(|fields| fields.len() == 9)
            && profile["schema_version"] == 1
            && profile.get("graph_version") == Some(&Value::Null)
            && profile.get("template_id") == Some(&Value::Null)
            && profile["goal_digest"] == contract_digest(goal)?
            && profile["prompt_digest"] == digest(description.as_bytes())
            && profile["result_schema_digest"]
                == contract_digest(&super::super::super::super::schema::document()?)?
            && profile["control_input_digest"] == contract_digest(manifest)?
            && metadata.as_ref().is_some_and(|m| m["campaign_id"] == id
                && m["node_id"] == "plan"
                && m["graph_version"].is_null()
                && m["graph_task_key"] == key),
        "native initial planner executor, historical provider, goal, prompt or input profile changed"
    );
    // The superior's current provider may have changed since publication. Its
    // immutable assignment and retained historical profile identify this turn;
    // the new repair uses goal.planner, not today's superior configuration.
    let member = sqlx::query(
        "SELECT m.family_id,f.scope_digest,f.bootstrap_campaign_id
         FROM campaign_dag_members m JOIN campaign_dag_families f ON f.id=m.family_id
         WHERE m.campaign_id=$1 FOR SHARE OF m,f",
    )
    .bind(id)
    .fetch_one(&mut **tx)
    .await?;
    let family: String = member.get("family_id");
    ensure!(
        member.get::<String, _>("scope_digest") == learning::scope_digest(goal)?,
        "initial planner family scope changed"
    );
    let authority = sqlx::query(
        "SELECT family_id,agent_id,purpose,input_manifest FROM campaign_dag_control_assignments
         WHERE task_key=$1 FOR SHARE",
    )
    .bind(&key)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(authority) = authority {
        ensure!(
            authority.get::<String, _>("family_id") == family
                && authority.get::<String, _>("agent_id").as_str() == assigned.as_deref().unwrap()
                && authority.get::<String, _>("purpose") == "bootstrap_plan"
                && authority
                    .get::<Option<Value>, _>("input_manifest")
                    .is_none()
                && member
                    .get::<Option<String>, _>("bootstrap_campaign_id")
                    .as_deref()
                    == Some(id),
            "initial superior planner lacks its exact durable bootstrap assignment"
        );
    } else {
        ensure!(
            assigned.as_deref() == Some(goal.planner.as_str()),
            "unassigned initial control must belong to the enrolled goal planner"
        );
    }
    frozen_input(manifest, id, goal, &family).await?;
    let events = sqlx::query(
        "SELECT e.kanban_card_id,e.dispatch_type,e.from_status,e.to_status,e.transition_source,e.payload_json,
            e.created_at=d.updated_at AND e.created_at>=d.created_at AS chronology
         FROM dispatch_events e JOIN task_dispatches d ON d.id=e.dispatch_id
         WHERE e.dispatch_id=$1 AND e.to_status IN ('completed','failed','cancelled')
         ORDER BY e.id FOR SHARE OF e,d",
    ).bind(dispatch).fetch_all(&mut **tx).await?;
    ensure!(
        events.len() == 1,
        "one canonical native capacity failure event required"
    );
    let event = &events[0];
    ensure!(
        event.get::<Option<String>, _>("kanban_card_id")
            == row.get::<Option<String>, _>("kanban_card_id")
            && event.get::<Option<String>, _>("dispatch_type")
                == row.get::<Option<String>, _>("dispatch_type")
            && matches!(
                event.get::<Option<String>, _>("from_status").as_deref(),
                Some("pending" | "dispatched")
            )
            && event.get::<String, _>("to_status") == "failed"
            && event.get::<String, _>("transition_source") == "turn_bridge_dispatch_failure"
            && event.get::<Option<Value>, _>("payload_json") == Some(capacity_result())
            && event.get::<Option<bool>, _>("chronology") == Some(true),
        "initial capacity refusal lacks matching canonical native producer provenance"
    );
    ensure!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM campaign_graph_progress WHERE dispatch_id=$1)",
        )
        .bind(dispatch)
        .fetch_one(&mut **tx)
        .await?,
        "observed provider tools cannot claim pre-provider initial capacity recovery"
    );
    super::super::settled_effect::require_no_campaign_owners(tx, id).await?;
    ensure!(
        super::super::super::super::workflow::lifecycle::settled(tx, id).await?,
        "initial planner execution, delivery and durable cleanup must settle first"
    );
    Ok(())
}
