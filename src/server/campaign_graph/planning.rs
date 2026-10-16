//! Durable control ownership and source-correction context for planner turns.
use super::{Goal, Graph, control, reconcile, store};
use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;

pub(super) fn pending(state: &Value) -> bool {
    state["tasks"].as_array().is_some_and(|tasks| {
        tasks.iter().any(|task| {
            matches!(task["role"].as_str(), Some("plan" | "manage" | "finish"))
                && task["accepted"] != true
                && task["observation"].is_null()
        })
    })
}

pub(super) async fn settle_superseded_unstarted(pool: &PgPool, campaign: &str) -> Result<()> {
    crate::db::campaigns::graph::recovery::replan::settle_superseded_unstarted_planners(
        pool, campaign,
    )
    .await?;
    crate::db::campaigns::graph::workflow::retirement::settle_stale_unstarted(pool, campaign)
        .await?;
    Ok(())
}

pub(super) async fn observe_retired_validation(
    pool: &PgPool,
    campaign: &str,
    key: &str,
    dispatch: &str,
    version: i64,
    validation: &Value,
) -> Result<bool> {
    let digest = validation["digest"].as_str().unwrap_or("");
    let retired: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_versions v
         WHERE v.campaign_id=$1 AND v.version=$2 AND v.digest=$3 AND v.status='rejected'
         AND EXISTS(SELECT 1 FROM campaign_graph_failures repair
           WHERE repair.campaign_id=v.campaign_id AND repair.evidence ? 'operator_replan'
             AND repair.evidence->'superseded_candidate'->>'version'=v.version::text
             AND repair.evidence->'superseded_candidate'->>'digest'=v.digest))",
    )
    .bind(campaign)
    .bind(version)
    .bind(digest)
    .fetch_one(pool)
    .await?;
    if !retired {
        return Ok(false);
    }
    // Record the assessment as history, not approval of the retired candidate.
    // Raw provider result, proposal, task identity and lifetime usage stay intact.
    sqlx::query("UPDATE campaign_graph_tasks SET observation=$2
        WHERE campaign_id=$3 AND task_key=$1 AND role='validate' AND version=$4
          AND NOT accepted AND observation IS NULL")
        .bind(key).bind(serde_json::json!({"dispatch_id":dispatch,"outcome":"superseded",
            "graph_version":version,"reason":"Digest-pinned operator source correction retired this unexecuted candidate before result observation"}))
        .bind(campaign).bind(version).execute(pool).await?;
    Ok(true)
}

/// An activated repair owns its exact failure even after a later graph supersedes it.
/// Candidate or rejected proposals are not authority to suppress future planning.
fn activated_repair_owns(versions: &[Value], failure: &Value) -> bool {
    let Some(id) = failure["id"].as_str().filter(|id| !id.is_empty()) else {
        return false;
    };
    versions.iter().any(|version| {
        matches!(version["status"].as_str(), Some("active" | "superseded"))
            && version["failure_id"].as_str() == Some(id)
    })
}

/// Returns false when a persisted budget/admission hold must stop this tick.
/// Reuse the native failure and publication authorities; do not rewrite history.
pub(super) async fn publish_repair(
    pool: &PgPool,
    id: &str,
    goal: &Goal,
    graph: &Graph,
    v: i64,
    state: &Value,
) -> Result<bool> {
    let versions = state["versions"].as_array().unwrap();
    let tasks = state["tasks"].as_array().unwrap();
    // One deterministic planning task per failure; unchanged failures do not
    // create new work on each tick. Infrastructure and unknown effects hold.
    if !versions
        .iter()
        .any(|v| v["status"] == "candidate" || v["status"] == "validated")
    {
        for f in state["failures"].as_array().unwrap() {
            if activated_repair_owns(versions, f) {
                continue;
            }
            let failed_control = tasks.iter().any(|t| {
                t["task_key"] == f["task_key"]
                    && matches!(t["role"].as_str(), Some("plan" | "validate"))
                    && t["version"].as_i64().is_some_and(|fv| fv >= v)
            });
            if !graph.nodes.iter().any(|n| f["task_key"] == n.task_key) && !failed_control {
                continue;
            }
            let repair_key = format!("{id}:plan:{}", f["id"].as_str().unwrap());
            if tasks.iter().any(|t| t["task_key"] == repair_key) {
                continue;
            }
            if f["category"] != "code" && f["category"] != "plan" {
                continue;
            }
            if tasks.iter().filter(|t| t["role"] == "plan").count()
                > goal.budget.max_evolutions as usize
                || versions.len() > goal.budget.max_evolutions as usize
            {
                // A terminal repair failure must remain inspectable when
                // no further evolution is allowed, rather than waiting
                // forever with an active but unfinishable graph.
                store::hold(pool, id, "Plan evolution budget exhausted").await?;
                return Ok(false);
            }
            let prompt = format!(
                "Propose a corrected graph for captured failure {}. Preserve completed/in-flight contracts and their task keys. Change only the failed subgraph and affected inputs. Explicitly explain the causal change in your receipt. Existing graph:\n{}\nFailure evidence:\n{}\n{}",
                f["id"],
                serde_json::to_value(graph)?,
                reconcile::planning_evidence(pool, f).await?,
                super::GRAPH_SCHEMA
            );
            if !control(
                pool,
                id,
                goal,
                "plan",
                Some(v),
                f["id"].as_str().unwrap(),
                &prompt,
            )
            .await?
            {
                return Ok(false);
            }
            break;
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn activated_repair_suppresses_only_its_original_failure() {
        let versions = vec![json!({"status":"active","failure_id":"repaired"})];
        assert!(activated_repair_owns(&versions, &json!({"id":"repaired"})));
        assert!(!activated_repair_owns(
            &versions,
            &json!({"id":"new-failure"})
        ));
        assert!(!activated_repair_owns(&versions, &json!({})));
    }

    #[test]
    fn later_revision_retains_repair_ownership_without_erasing_failure() {
        let versions = vec![
            json!({"status":"superseded","failure_id":"repaired"}),
            json!({"status":"active","failure_id":null}),
        ];
        let failure = json!({"id":"repaired","category":"plan","evidence":"original"});
        let before = failure.clone();
        assert!(activated_repair_owns(&versions, &failure));
        assert_eq!(failure, before);
    }

    #[test]
    fn unactivated_or_malformed_proposals_do_not_own_failures() {
        for status in ["candidate", "validated", "rejected", "invented"] {
            assert!(!activated_repair_owns(
                &[json!({"status":status,"failure_id":"failure"})],
                &json!({"id":"failure"}),
            ));
        }
        for id in [Value::Null, json!(""), json!(5)] {
            assert!(!activated_repair_owns(
                &[json!({"status":"active","failure_id":id})],
                &json!({"id":id}),
            ));
        }
    }
}
