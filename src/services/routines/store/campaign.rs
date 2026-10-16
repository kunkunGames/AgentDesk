//! Campaign admission over the shared native run/turn lifecycle. Adopted rows
//! are paused Routine definitions, never independently scheduled Routines.
use super::*;
use crate::db::campaigns::{
    self, Campaign, CampaignInput, CampaignStatus, EvidenceRecord, NodeStatus,
};
use sqlx::types::Json;

#[cfg(test)]
mod tests;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptCampaignOperation {
    pub routine_id: String,
    pub node_id: String,
    pub expected_revision: i64,
    pub expected_routine_updated_at: DateTime<Utc>,
    pub next_due_at: DateTime<Utc>,
}

fn input(c: &Campaign) -> CampaignInput {
    CampaignInput {
        title: c.title.clone(),
        description: c.description.clone(),
        status: c.status,
        round: c.round,
        nodes: c.nodes.iter().map(|n| n.input.clone()).collect(),
        auto_queue: None,
    }
}

async fn locked_campaign(tx: &mut Transaction<'_, Postgres>, id: &str) -> Result<Campaign> {
    Ok(sqlx::query_scalar::<_, Json<Campaign>>(
        "SELECT document FROM campaigns WHERE id=$1 FOR UPDATE",
    )
    .bind(id)
    .fetch_one(&mut **tx)
    .await?
    .0)
}

impl RoutineStore {
    pub async fn adopt_campaign_operation(
        &self,
        id: &str,
        request: AdoptCampaignOperation,
    ) -> Result<Value> {
        let mut tx = self.pool.begin().await?;
        // All claims/adoptions lock source before Campaign, matching run closure.
        let row=sqlx::query("SELECT r.*,a.provider FROM routines r JOIN agents a ON a.id=r.agent_id WHERE r.id=$1 FOR UPDATE OF r")
            .bind(&request.routine_id).fetch_one(&mut *tx).await?;
        anyhow::ensure!(
            row.get::<Option<String>, _>("campaign_id").is_none(),
            "already adopted"
        );
        anyhow::ensure!(
            row.get::<DateTime<Utc>, _>("updated_at") == request.expected_routine_updated_at,
            "source routine changed"
        );
        anyhow::ensure!(
            matches!(row.get::<&str, _>("status"), "enabled" | "paused"),
            "historical detached routines cannot be reactivated by migration"
        );
        anyhow::ensure!(
            row.get::<Option<String>, _>("in_flight_run_id").is_none(),
            "source has an active run"
        );
        let provider: String = row.get("provider");
        let agent: String = row.get("agent_id");
        anyhow::ensure!(
            matches!(provider.as_str(), "qwen" | "opencode"),
            "native operations require Qwen or OpenCode"
        );
        anyhow::ensure!(
            row.get::<&str, _>("execution_strategy") == "fresh"
                && row.get::<i32, _>("max_retries") == 0
                && row.get::<Option<String>, _>("fallback_agent_id").is_none()
                && row
                    .get::<Option<i32>, _>("timeout_secs")
                    .is_some_and(|n| (1..=1800).contains(&n)),
            "Campaign operations require fresh sessions, finite timeouts, no replay and no fallback"
        );
        let c = locked_campaign(&mut tx, id).await?;
        anyhow::ensure!(
            c.revision == request.expected_revision,
            "Campaign revision changed"
        );
        anyhow::ensure!(
            matches!(
                c.status,
                CampaignStatus::Active | CampaignStatus::Paused | CampaignStatus::Planned
            ),
            "Campaign is terminal"
        );
        anyhow::ensure!(
            c.nodes.len() == 1
                && c.nodes[0].input.id == request.node_id
                && c.nodes[0].input.dependencies.is_empty(),
            "recurring operations use one independently pausable goal per Campaign"
        );
        let node = &c.nodes[0].input;
        anyhow::ensure!(
            node.assignee.as_deref() == Some(&agent) && node.provider.as_deref() == Some(&provider),
            "Campaign assignment does not match its native executor"
        );
        anyhow::ensure!(
            node.status == NodeStatus::Pending && node.session_id.is_none(),
            "adoption requires a fresh Campaign node"
        );
        let occupied:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM routines WHERE campaign_id=$1) OR EXISTS(SELECT 1 FROM kanban_cards WHERE metadata->>'campaign_id'=$1)")
            .bind(id).fetch_one(&mut *tx).await?;
        anyhow::ensure!(!occupied, "Campaign already has an execution owner");
        sqlx::query("UPDATE routines SET campaign_id=$2,campaign_node_id=$3,status='paused',pause_reason=NULL,next_due_at=$4,updated_at=NOW() WHERE id=$1")
            .bind(&request.routine_id).bind(id).bind(&request.node_id).bind(request.next_due_at).execute(&mut *tx).await?;
        let mut next = input(&c);
        next.nodes[0].next_action = Some(format!(
            "Campaign admission at {}; native {} executor; {}",
            request.next_due_at,
            provider,
            row.get::<String, _>("script_ref")
        ));
        let adopted = campaigns::replace_on_tx(&mut tx, id, c.revision, next).await?;
        tx.commit().await?;
        Ok(
            json!({"campaign":adopted,"source_routine_id":request.routine_id,"source_status":"paused","execution_profile":"native_operations_v1"}),
        )
    }

    /// Admit at most one native operation globally. A failed/unknown attempt
    /// holds its Campaign; other independent goals can continue on later ticks.
    pub async fn claim_campaign_operation(&self) -> Result<Option<ClaimedRoutineRun>> {
        let mut tx = self.pool.begin().await?;
        let locked: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_xact_lock(hashtext('campaign-native-admission'))",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !locked {
            return Ok(None);
        }
        let running:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM routine_runs WHERE campaign_id IS NOT NULL AND status='running')").fetch_one(&mut *tx).await?;
        if running {
            return Ok(None);
        }
        let ids:Vec<(String,String,String)>=sqlx::query_as("SELECT r.id,r.campaign_id,r.campaign_node_id FROM routines r JOIN campaigns c ON c.id=r.campaign_id WHERE r.status='paused' AND r.in_flight_run_id IS NULL AND (r.next_due_at<=NOW() OR (r.next_due_at IS NULL AND EXISTS(SELECT 1 FROM jsonb_array_elements(c.document->'nodes') n WHERE n->>'status'='pending' AND n->>'session_id' IS NOT NULL))) AND c.document->>'status'='active' ORDER BY r.next_due_at,r.id LIMIT 32 FOR UPDATE OF r SKIP LOCKED")
            .fetch_all(&mut *tx).await?;
        for (rid, cid, nid) in ids {
            let c = locked_campaign(&mut tx, &cid).await?;
            if c.status != CampaignStatus::Active {
                continue;
            }
            let Some(node) = c.nodes.iter().find(|n| n.input.id == nid) else {
                continue;
            };
            if !matches!(
                node.input.status,
                NodeStatus::Pending | NodeStatus::Completed | NodeStatus::Skipped
            ) {
                continue;
            }
            let candidate:RoutineClaimCandidate=sqlx::query_as("SELECT id,agent_id,fallback_agent_id,max_retries,script_ref,name,execution_strategy,checkpoint,discord_thread_id,timeout_secs FROM routines WHERE id=$1")
                .bind(&rid).fetch_one(&mut *tx).await?;
            // Sharing a CLI identity with any native job must not overlap it.
            let busy:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM routine_runs rr JOIN routines r ON r.id=rr.routine_id WHERE rr.status='running' AND r.agent_id=$1)")
                .bind(&candidate.agent_id).fetch_one(&mut *tx).await?;
            if busy {
                continue;
            }
            let latest:Option<(String,String)>=sqlx::query_as("SELECT id,status FROM routine_runs WHERE campaign_id=$1 ORDER BY started_at DESC,id DESC LIMIT 1")
                .bind(&cid).fetch_optional(&mut *tx).await?;
            if let Some((last, status)) = latest {
                if !matches!(status.as_str(), "succeeded" | "skipped")
                    && !(node.input.status == NodeStatus::Pending
                        && node.input.session_id.as_deref() == Some(&last))
                {
                    continue;
                }
            }
            sqlx::query("UPDATE routines SET status='paused',pause_reason=NULL WHERE id=$1")
                .bind(&rid)
                .execute(&mut *tx)
                .await?;
            let claimed = Self::insert_running_run(&mut tx, candidate).await?;
            let mut next = input(&c);
            let n = next.nodes.iter_mut().find(|n| n.id == nid).unwrap();
            n.status = NodeStatus::Running;
            n.session_id = Some(claimed.run_id.clone());
            n.blocker = None;
            n.next_action = Some(format!(
                "Await native run {} and its durable terminal evidence",
                claimed.run_id
            ));
            campaigns::replace_on_tx(&mut tx, &cid, c.revision, next).await?;
            tx.commit().await?;
            return Ok(Some(claimed));
        }
        tx.commit().await?;
        Ok(None)
    }

    pub async fn reconcile_campaign_operations(&self) -> Result<usize> {
        let rows=sqlx::query("SELECT rr.id,rr.campaign_id,rr.campaign_node_id,rr.status,rr.action,rr.result_json,rr.error,rr.turn_id,rr.finished_at,r.next_due_at FROM routine_runs rr JOIN routines r ON r.id=rr.routine_id JOIN campaigns c ON c.id=rr.campaign_id WHERE rr.status<>'running' AND EXISTS(SELECT 1 FROM jsonb_array_elements(c.document->'nodes') n WHERE n->>'session_id'=rr.id AND n->>'status'='running') ORDER BY rr.finished_at LIMIT 32")
            .fetch_all(&*self.pool).await?;
        let mut count = 0;
        for row in rows {
            let cid: String = row.get("campaign_id");
            let nid: String = row.get("campaign_node_id");
            let run: String = row.get("id");
            let mut tx = self.pool.begin().await?;
            let c = locked_campaign(&mut tx, &cid).await?;
            let mut next = input(&c);
            let Some(n) = next.nodes.iter_mut().find(|n| {
                n.id == nid
                    && n.session_id.as_deref() == Some(&run)
                    && n.status == NodeStatus::Running
            }) else {
                continue;
            };
            let status: String = row.get("status");
            let action: Option<String> = row.get("action");
            let result: Value = row
                .get::<Option<Value>, _>("result_json")
                .unwrap_or(Value::Null);
            let native_success = status == "succeeded"
                && (action.as_deref() != Some("agent")
                    || (result["fresh_context_guaranteed"] == true
                        && result["status"] == "completed"
                        && result["turn_terminal_status"] == "completed"
                        && matches!(
                            result["completion_evidence"].as_str(),
                            Some("session_transcripts" | "session_transcripts_no_reply")
                        )));
            n.status = if native_success {
                NodeStatus::Completed
            } else if status == "skipped" {
                NodeStatus::Skipped
            } else {
                NodeStatus::Blocked
            };
            let error: Option<String> = row.get("error");
            n.blocker = if native_success || status == "skipped" {
                None
            } else {
                Some(error.unwrap_or_else(||format!("Native attempt ended {status}; reconcile effects before requesting another attempt")))
            };
            let due: Option<DateTime<Utc>> = row.get("next_due_at");
            n.next_action = Some(if n.blocker.is_some() {
                "Inspect the retained native run and effects. Explicitly set this node pending and Campaign active to retry.".into()
            } else {
                format!(
                    "Recurring operation; next admission {due:?}. Native completion is recorded separately from business effects."
                )
            });
            n.evidence_records.push(EvidenceRecord {summary:format!("Native operation {status}; execution evidence only"),
                command:None,result:Some(json!({"run_id":run,"status":status,"turn_id":row.get::<Option<String>,_>("turn_id"),"completion_evidence":result["completion_evidence"],"provider":result["provider"],"fresh_context_guaranteed":result["fresh_context_guaranteed"]}).to_string()),
                head_sha:None,recorded_at:row.get("finished_at"),references:vec![format!("/api/campaigns/{cid}/operations")]});
            if n.evidence_records.len() > 20 {
                n.evidence_records.drain(..n.evidence_records.len() - 20);
            }
            if n.blocker.is_some() && next.status == CampaignStatus::Active {
                next.status = CampaignStatus::Paused;
            }
            campaigns::replace_on_tx(&mut tx, &cid, c.revision, next).await?;
            tx.commit().await?;
            count += 1;
        }
        Ok(count)
    }

    pub async fn campaign_operations(&self, id: &str) -> Result<Value> {
        let definition:Option<Value>=sqlx::query_scalar("SELECT jsonb_build_object('source_routine_id',id,'node_id',campaign_node_id,'agent_id',agent_id,'script_ref',script_ref,'schedule',schedule,'next_due_at',next_due_at,'in_flight_run_id',in_flight_run_id,'timeout_secs',timeout_secs,'execution_strategy',execution_strategy,'source_status',status) FROM routines WHERE campaign_id=$1")
            .bind(id).fetch_optional(&*self.pool).await?;
        let runs:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('id',id,'status',status,'action',action,'started_at',started_at,'finished_at',finished_at,'turn_id',turn_id,'error',error,'retry_count',retry_count,'completion_evidence',result_json->'completion_evidence','provider',result_json->'provider','fresh_context_guaranteed',result_json->'fresh_context_guaranteed') FROM routine_runs WHERE campaign_id=$1 ORDER BY started_at DESC,id DESC LIMIT 20")
            .bind(id).fetch_all(&*self.pool).await?;
        Ok(
            json!({"profile":"native_operations_v1","definition":definition,"runs":runs,"managed_execution_v1":false}),
        )
    }
}
