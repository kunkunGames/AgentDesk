//! One manager turn per new verified frontier; the existing executor owns work.
use super::*;
use crate::db::campaigns::graph::workflow;

/// Verification can proceed while new business publication remains fenced.
/// Observation and cleanup continue while a manager or final verifier owns control.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Admission {
    Work,
    VerifyOnly,
    Owned,
}
pub(super) async fn admit(pool: &PgPool, id: &str, goal: &Goal) -> Result<Admission> {
    let frontier = workflow::store::snapshot(pool, id).await?;
    if frontier.ready_verification() {
        return Ok(Admission::VerifyOnly);
    }
    if frontier.running() {
        return Ok(Admission::Owned);
    }
    if !frontier.has_results() {
        return Ok(Admission::Work);
    }
    // An independently activated revision already expresses the manager's next
    // work decision. Do not buy another model turn before any new result exists.
    let transition = sqlx::query(
        "SELECT execution_profile FROM campaign_graph_tasks WHERE campaign_id=$1
        AND role='manage' AND accepted AND observation->'candidate_version'=to_jsonb($2::bigint)",
    )
    .bind(id)
    .bind(frontier.version)
    .fetch_optional(pool)
    .await?;
    if let Some(transition) = transition {
        let profile: Value = transition.get("execution_profile");
        let prior = profile["workflow_authority_state"]["tasks"].as_array();
        let current = frontier.authority_state();
        if frontier
            .graph
            .nodes
            .iter()
            .any(|node| frontier.task(&node.task_key).is_none())
            && current["tasks"].as_array().is_some_and(|tasks| {
                tasks.iter().all(|task| {
                    prior.is_some_and(|prior| prior.iter().any(|before| before == task))
                })
            })
        {
            return Ok(Admission::Work);
        }
    }
    let digest = frontier.digest()?;
    let turns = sqlx::query(
        "SELECT task_key,accepted,observation FROM campaign_graph_tasks
        WHERE campaign_id=$1 AND role='manage' AND execution_profile->>'workflow_snapshot_digest'=$2
        ORDER BY created_at,task_key",
    )
    .bind(id)
    .bind(&digest)
    .fetch_all(pool)
    .await?;
    let mut round = turns.len();
    if let Some(turn) = turns.last() {
        let accepted: bool = turn.get("accepted");
        let observation: Option<Value> = turn.get("observation");
        let Some(observation) = observation else {
            return Ok(Admission::Owned);
        };
        if accepted {
            match observation["workflow_decision"]["action"].as_str() {
                Some("continue") => return Ok(Admission::Work),
                Some("revise") => return Ok(Admission::Owned),
                Some("wait") => {
                    let wake = observation["wake_at"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("workflow wake deadline missing"))?;
                    if chrono::DateTime::parse_from_rfc3339(wake)? > chrono::Utc::now() {
                        return Ok(Admission::Owned);
                    }
                }
                Some("complete") => {
                    let suffix = format!("{}-goal", digest);
                    let key = format!("{id}:finish:{suffix}");
                    let finish = sqlx::query("SELECT accepted,observation FROM campaign_graph_tasks WHERE campaign_id=$1 AND task_key=$2")
                        .bind(id).bind(&key).fetch_optional(pool).await?;
                    if let Some(finish) = finish {
                        if finish.get::<bool, _>("accepted") {
                            let assessment: Option<Value> = finish.get("observation");
                            if assessment
                                .as_ref()
                                .is_some_and(|value| value["goal_validation"]["approved"] == true)
                            {
                                workflow::store::complete(pool, id, &key).await?;
                            }
                        }
                    } else {
                        let prompt = format!(
                            "Independently assess every immutable Goal.success criterion against current business targets and accepted artifact contents. Read {} and verify all referenced file hashes before relying on them. Manager completion is a proposal. Return goal_validation alongside campaign_receipt: {{\"snapshot_digest\":\"{digest}\",\"approved\":true|false,\"reason\":\"actual conclusion\",\"criteria\":[{{\"index\":0,\"met\":true|false,\"observation\":0,\"task_key\":\"accepted verify task\",\"path\":\"declared verification output\",\"sha256\":\"accepted file hash\",\"evidence\":\"current evidence for this criterion\"}}]}}. Cover every success index exactly once and reference actual receipt observations and accepted verification artifacts. Set approved=false if any criterion is unmet or unproved. A truthful completed assessment may have a passed report receipt while approved=false; preserve actual failing business checks in its evidence. No external mutations, new work or completion API calls. Do not return campaign_graph, workflow_decision or graph_validation. Formal result schema:\n{}",
                            workflow::INPUT_FILE,
                            serde_json::to_string(
                                &crate::db::campaigns::graph::schema::document()?["result"]
                            )?
                        );
                        control(
                            pool,
                            id,
                            goal,
                            "finish",
                            Some(frontier.version),
                            &suffix,
                            &prompt,
                        )
                        .await?;
                    }
                    return Ok(Admission::Owned);
                }
                _ => anyhow::bail!("accepted workflow action is missing"),
            }
        } else if observation["outcome"] != "superseded" {
            // Known read-only control failures get one causal correction. The
            // existing repeated-failure fence and lifetime budgets still apply.
            if round >= 2 {
                store::hold(pool,id,"Workflow manager repeated a terminal decision failure; reconcile its captured evidence").await?;
                return Ok(Admission::Owned);
            }
        }
    }
    round += 1;
    let suffix = format!("{digest}-{round}");
    let prompt = format!(
        "Act as the single manager of this campaign. Read {} and current accepted artifact contents; use new node results and any unmet goal assessments to decide the smallest remaining workflow. Return workflow_decision alongside campaign_receipt: {{\"snapshot_digest\":\"{digest}\",\"action\":\"continue|revise|wait|complete\",\"reason\":\"causal evidence\"}}. continue admits ready work in the existing graph. revise also returns campaign_graph and changes only unpublished remaining tasks, preserving every published contract and key. For repair of a captured code/plan failure, include its exact failure_id; the existing failure/effect guards still apply. wait also supplies wake_after_secs in 1..3600 and a concrete condition to recheck; its deadline persists across restart. complete proposes independent Goal.success verification and may skip unpublished unnecessary tasks. Start new work in 2–4 meaningful nodes including independent verification; preserve retained nodes within the existing 32-node bound. Do not turn each command or historical check into a node. Reuse artifacts without repeating accepted work or effects. If a prior control failed, inspect its captured failure under /execution and correct the actual cause. Never claim completion from all node receipts or CLI exit alone. Formal result schema:\n{}",
        workflow::INPUT_FILE,
        serde_json::to_string(&crate::db::campaigns::graph::schema::document()?["result"])?
    );
    control(
        pool,
        id,
        goal,
        "manage",
        Some(frontier.version),
        &suffix,
        &prompt,
    )
    .await?;
    Ok(Admission::Owned)
}

pub(super) fn validate_role(role: &str, result: &Value) -> Result<()> {
    anyhow::ensure!(
        role == "manage" || result["workflow_decision"].is_null(),
        "unexpected workflow decision for task role"
    );
    anyhow::ensure!(
        role == "finish" || result["goal_validation"].is_null(),
        "unexpected goal validation for task role"
    );
    if matches!(role, "manage" | "finish") {
        anyhow::ensure!(
            result["graph_validation"].is_null() && result["dag_template_review"].is_null(),
            "workflow control cannot approve graph or template validation"
        );
        if role == "finish" {
            anyhow::ensure!(
                result["campaign_graph"].is_null(),
                "goal verifier cannot revise work"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_authority_is_confined_to_its_assigned_control_role() {
        let decision = serde_json::json!({"workflow_decision":{"action":"complete"}});
        validate_role("manage", &decision).unwrap();
        for role in ["work", "plan", "validate", "template_review", "finish"] {
            assert!(validate_role(role, &decision).is_err(), "{role}");
        }
        let assessment = serde_json::json!({"goal_validation":{"approved":true}});
        validate_role("finish", &assessment).unwrap();
        for role in ["work", "plan", "validate", "template_review", "manage"] {
            assert!(validate_role(role, &assessment).is_err(), "{role}");
        }
    }

    #[test]
    fn workflow_controls_cannot_self_approve_candidates_or_templates() {
        for role in ["manage", "finish"] {
            for field in ["graph_validation", "dag_template_review"] {
                let mut result = serde_json::json!({});
                result[field] = serde_json::json!({"approved":true});
                assert!(validate_role(role, &result).is_err(), "{role}: {field}");
            }
        }
        assert!(validate_role("finish", &serde_json::json!({"campaign_graph":{}})).is_err());
        validate_role("manage", &serde_json::json!({"campaign_graph":{}})).unwrap();
    }
}
