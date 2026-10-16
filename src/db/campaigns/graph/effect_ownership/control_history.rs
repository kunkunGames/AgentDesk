//! Exact native control reconciliation is history, never an effect or OS-exit grant.
use super::super::{
    Goal, contract_digest,
    recovery::{finish, replan},
};
use anyhow::{Result, ensure};
use serde::Deserialize;
use serde_json::Value;

// The original observation and all three audit records must describe the same
// native transaction. Implicit controls cannot borrow an explicit work contract.
pub(super) const PROOF_SQL: &str = "(SELECT CASE WHEN octet_length(proof.value::text)<=65536
        THEN proof.value ELSE jsonb_build_object('oversized',true) END
    FROM campaign_graph_tasks t
    JOIN campaign_graph_failures f ON f.campaign_id=t.campaign_id AND f.task_key=t.task_key
    JOIN campaign_graph_reconciliations reconciled ON reconciled.campaign_id=f.campaign_id AND reconciled.failure_id=f.id
    JOIN campaign_graph_failures repair ON repair.campaign_id=f.campaign_id AND repair.task_key=f.task_key
      AND repair.category='plan' AND repair.evidence->>'reconciled_failure_id'=f.id
    JOIN campaign_goal_revisions revision ON revision.campaign_id=f.campaign_id
      AND revision.request_id=COALESCE(repair.evidence->>'operator_replan',repair.evidence->>'operator_finish_recovery')
    CROSS JOIN LATERAL (SELECT jsonb_build_object(
      'campaign_id',t.campaign_id,'role',t.role,'failure_id',f.id,
      'repair_id',repair.id,'repair_fingerprint',repair.fingerprint,'repair',repair.evidence,
      'request_id',revision.request_id,'request_digest',revision.request_digest,
      'expected_revision',reconciled.expected_revision,'campaign_revision',revision.campaign_revision,
      'evidence',reconciled.evidence,'previous_goal',revision.previous_goal,'revised_goal',revision.revised_goal,
      'current_raw_sha256',CASE WHEN t.role='finish' THEN encode(sha256(convert_to(d.result::text,'UTF8')),'hex') END,
      'current_failure_payload',CASE WHEN t.role='finish' THEN (SELECT event.payload_json
        FROM dispatch_events event WHERE event.dispatch_id=d.id AND event.to_status='failed'
        ORDER BY event.id DESC LIMIT 1) END,
      'current_progress',CASE WHEN t.role='finish' THEN (SELECT COALESCE(jsonb_agg(jsonb_build_array(
        p.event_id,p.fingerprint,p.output_digest) ORDER BY p.event_id),'[]'::jsonb)
        FROM campaign_graph_progress p WHERE p.dispatch_id=d.id) END) AS value) proof
    WHERE t.card_id=d.kanban_card_id AND t.role IN ('plan','validate','finish') AND NOT t.accepted
      AND ((t.role IN ('plan','validate') AND repair.evidence ? 'operator_replan')
        OR (t.role='finish' AND repair.evidence ? 'operator_finish_recovery'
          AND repair.evidence->>'finish_recovery_authority'='configured_server_admin_bearer'
          AND d.status='failed' AND d.completed_at IS NULL
          AND d.id=(SELECT latest.id FROM task_dispatches latest WHERE latest.kanban_card_id=t.card_id
            ORDER BY latest.created_at DESC,latest.id DESC LIMIT 1)
          AND (SELECT COUNT(*) FROM dispatch_events event WHERE event.dispatch_id=d.id
            AND event.to_status IN ('completed','failed','cancelled'))=1
          AND EXISTS(SELECT 1 FROM dispatch_events event WHERE event.dispatch_id=d.id
            AND event.kanban_card_id=t.card_id AND event.dispatch_type=d.dispatch_type
            AND event.from_status IN ('pending','dispatched') AND event.to_status='failed'
            AND event.transition_source='turn_bridge_dispatch_failure'
            AND event.created_at>=d.created_at AND event.created_at<=d.updated_at)))
      AND t.observation=f.evidence
      AND ((f.category IN ('unknown','infrastructure')
          AND COALESCE(repair.evidence->'rejected_validation','null'::jsonb)='null'::jsonb)
        OR (f.category='plan' AND t.role='validate'
          AND jsonb_typeof(repair.evidence->'rejected_validation')='object'))
      AND f.evidence->>'dispatch_id'=d.id AND f.evidence->>'task_key'=t.task_key
      AND f.evidence->'external_effect_requires_reconciliation'='false'::jsonb
      AND f.evidence#>>'{runtime,contract_digest}'=t.contract_digest
      AND f.evidence#>>'{runtime,role}'=t.role
      AND f.evidence#>>'{runtime,agent_id}'=d.to_agent_id
      AND f.evidence#>'{runtime,graph_version}'=COALESCE(to_jsonb(t.version),'null'::jsonb)
      AND f.evidence#>>'{runtime,dispatch_status}'=d.status
      AND repair.evidence->'original_failure'=f.evidence
      AND repair.evidence->'external_effect_requires_reconciliation'='false'::jsonb
      AND repair.evidence->>'operator_evidence'=reconciled.evidence AND revision.evidence=reconciled.evidence
      AND reconciled.expected_revision+1=revision.campaign_revision
      AND reconciled.created_at=repair.created_at AND repair.created_at=revision.created_at
      AND reconciled.created_at>=f.created_at AND f.created_at>=d.created_at AND revision.created_at<=NOW()
      AND NOT EXISTS(SELECT 1 FROM campaign_graph_versions v,jsonb_array_elements(v.graph->'nodes') node
        WHERE v.campaign_id=t.campaign_id AND v.version=t.version AND node->>'task_key'=t.task_key)
    ORDER BY revision.campaign_revision DESC,revision.request_id DESC LIMIT 1)";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Proof {
    campaign_id: String,
    role: String,
    failure_id: String,
    repair_id: String,
    repair_fingerprint: String,
    repair: Value,
    request_id: String,
    request_digest: String,
    expected_revision: i64,
    campaign_revision: i64,
    evidence: String,
    previous_goal: Goal,
    revised_goal: Goal,
    #[serde(default)]
    current_raw_sha256: Option<String>,
    #[serde(default)]
    current_failure_payload: Option<Value>,
    #[serde(default)]
    current_progress: Option<Value>,
}

pub(super) fn reconciled(proof: Option<&Value>) -> bool {
    proof.is_some_and(|value| validate(value).is_ok())
}

fn validate(value: &Value) -> Result<()> {
    let proof: Proof = serde_json::from_value(value.clone())?;
    ensure!(
        matches!(proof.role.as_str(), "plan" | "validate" | "finish"),
        "not an implicit control"
    );
    ensure!(
        proof.expected_revision.checked_add(1) == Some(proof.campaign_revision),
        "audit revision mismatch"
    );
    if proof.role == "finish" {
        return validate_finish(&proof);
    }
    let repair_id = contract_digest(&(&proof.campaign_id, "operator-replan", &proof.request_id))?;
    ensure!(
        proof.repair_id == repair_id && proof.repair_fingerprint == repair_id,
        "native repair identity mismatch"
    );
    ensure!(
        proof.repair["unattempted_effect"].is_null() && proof.repair["settled_effect"].is_null(),
        "control history cannot release an effect"
    );
    proof.previous_goal.validate()?;
    proof.revised_goal.validate()?;
    let mut unchanged = proof.previous_goal.clone();
    unchanged.budget = proof.revised_goal.budget.clone();
    ensure!(
        contract_digest(&unchanged)? == contract_digest(&proof.revised_goal)?,
        "operator replan changed business scope"
    );
    let same_budget = contract_digest(&proof.previous_goal.budget)?
        == contract_digest(&proof.revised_goal.budget)?;
    let mut request = replan::Replan {
        expected_revision: proof.expected_revision,
        request_id: proof.request_id,
        failure_id: proof.failure_id,
        evidence: proof.evidence,
        budget: None,
        supersede_candidate: serde_json::from_value(proof.repair["superseded_candidate"].clone())?,
        unattempted_effect: None,
        settled_effect: None,
        rejected_validation: serde_json::from_value(proof.repair["rejected_validation"].clone())?,
        cancelled_validation: serde_json::from_value(proof.repair["cancelled_validation"].clone())?,
    };
    if same_budget && contract_digest(&("operator-replan", &request))? == proof.request_digest {
        return Ok(());
    }
    super::super::revision::budget::validate_increase(
        &proof.previous_goal.budget,
        &proof.revised_goal.budget,
    )?;
    request.budget = Some(proof.revised_goal.budget);
    ensure!(
        contract_digest(&("operator-replan", &request))? == proof.request_digest,
        "native request digest mismatch"
    );
    Ok(())
}

fn validate_finish(proof: &Proof) -> Result<()> {
    ensure!(
        proof.repair["operator_replan"].is_null()
            && proof.repair["finish_recovery_authority"] == finish::AUTHORITY
            && proof.repair["operator_finish_recovery"] == proof.request_id
            && proof.repair["unattempted_effect"].is_null()
            && proof.repair["settled_effect"].is_null()
            && proof.repair["rejected_validation"].is_null()
            && proof.repair["cancelled_validation"].is_null()
            && proof.repair["superseded_candidate"].is_null()
            && proof.repair["reconciled_failure_id"] == proof.failure_id
            && proof.repair_id == proof.repair_fingerprint,
        "finish history cannot borrow a planning or business-effect audit"
    );
    proof.previous_goal.validate()?;
    proof.revised_goal.validate()?;
    ensure!(
        contract_digest(&proof.previous_goal)? == contract_digest(&proof.revised_goal)?,
        "finish recovery changed goal scope or budget"
    );
    let request = finish::Recovery {
        expected_revision: proof.expected_revision,
        request_id: proof.request_id.clone(),
        failure_id: proof.failure_id.clone(),
        evidence: proof.evidence.clone(),
        failed_finish: serde_json::from_value(proof.repair["failed_finish"].clone())?,
        operator_attestation: serde_json::from_value(proof.repair["operator_attestation"].clone())?,
    };
    finish::validate_history_request(
        &proof.campaign_id,
        &request,
        &proof.request_digest,
        &proof.repair_id,
    )?;
    ensure!(
        proof.current_raw_sha256.as_deref()
            == Some(request.failed_finish.raw_result_sha256.as_str())
            && proof.current_failure_payload.as_ref() == Some(&finish::quota_result())
            && proof.current_progress.as_ref().is_some_and(|progress| {
                contract_digest(progress)
                    .is_ok_and(|digest| digest == request.failed_finish.progress_sha256)
            }),
        "retained finish result, canonical failure payload or telemetry changed after recovery"
    );
    Ok(())
}
