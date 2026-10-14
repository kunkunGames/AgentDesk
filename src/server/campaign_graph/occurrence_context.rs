//! Freeze canonical recurrence identity into each newly published native card.
use crate::db::campaigns::graph::{Goal, Task, learning::ControlAssignment};
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct OccurrenceContext {
    parent_campaign_id: String,
    child_campaign_id: String,
    scheduled_slot: DateTime<Utc>,
    zero_based_absolute_ordinal: u64,
}

async fn load(pool: &PgPool, child: &str) -> Result<Option<OccurrenceContext>> {
    // Migration 135 makes child_id unique and (parent, scheduled_at) the PK.
    // Count retained earlier rows of this owner, never the API's latest 30.
    // Native admission fences historical backfill, so later slots do not move
    // a published child's cursor. The child tie-breaker makes the order explicit.
    let row = sqlx::query(
        "SELECT o.campaign_id,o.child_id,o.scheduled_at,
          (SELECT COUNT(*) FROM campaign_graph_occurrences prior
           WHERE prior.campaign_id=o.campaign_id
             AND (prior.scheduled_at,prior.child_id)<(o.scheduled_at,o.child_id)) AS ordinal
         FROM campaign_graph_occurrences o WHERE o.child_id=$1",
    )
    .bind(child)
    .fetch_optional(pool)
    .await?;
    row.map(|row| {
        let parent: String = row.try_get("campaign_id")?;
        let owner: String = row.try_get("child_id")?;
        ensure!(
            owner == child && parent != owner,
            "native occurrence owner differs"
        );
        Ok(OccurrenceContext {
            parent_campaign_id: parent,
            child_campaign_id: owner,
            scheduled_slot: row.try_get("scheduled_at")?,
            zero_based_absolute_ordinal: u64::try_from(row.try_get::<i64, _>("ordinal")?)?,
        })
    })
    .transpose()
}

fn append(mut prompt: String, context: Option<&OccurrenceContext>) -> Result<String> {
    if let Some(context) = context {
        prompt.push_str(
            "\nFrozen native occurrence context (canonical retained ledger at publication):\n",
        );
        prompt.push_str(&serde_json::to_string(context)?);
        prompt.push_str("\nThis is run context, not a Goal or family-scope revision, a target observation, an approval or a tool result. For original business requirements with a rotating batch cursor, use zero_based_absolute_ordinal as this native parent's batch sequence; its first occurrence is 0. Pin it once for this child and reuse it for the initial and final existing helper. Keep the original one-batch bounds and current-head gates. Do not copy or advance legacy routine checkpoints.\n");
    }
    Ok(prompt)
}

pub(super) async fn attach(pool: &PgPool, child: &str, prompt: String) -> Result<String> {
    append(prompt, load(pool, child).await?.as_ref())
}

pub(super) async fn work_prompt(
    pool: &PgPool,
    child: &str,
    goal: &Goal,
    node: &Task,
    input_manifest: &Value,
) -> Result<String> {
    Ok(format!(
        "{}\nTask contract:\n{}\nPinned input manifest (already materialized):\n{}\nWrite the declared outputs inside your assigned workspace. Do not modify input files. Independent verification reads these actual outputs.",
        attach(pool, child, super::contract_prompt(goal)?).await?,
        serde_json::to_string(node)?,
        input_manifest
    ))
}

pub(super) fn control_authority(
    campaign: &str,
    goal: &Goal,
    role: &str,
    agent: &str,
    authority: Option<&ControlAssignment>,
) -> Result<String> {
    if let Some(authority) = authority {
        ensure!(
            role == "plan" && authority.purpose == "bootstrap_plan" && authority.agent_id == agent,
            "native bootstrap explanation differs from publication authority"
        );
    }
    let facts = json!({"campaign_id":campaign,"task_role":role,"assigned_agent_id":agent,
        "bootstrap_authority":authority.map(|a|json!({"family_id":a.family_id,
            "agent_id":a.agent_id,"purpose":a.purpose})),
        "ongoing_goal_planner":goal.planner,"business_executors":goal.agents,
        "independent_goal_verifier":goal.verifier});
    let mut prompt = format!(
        "Native control publication assignment (bootstrap authority is persisted atomically with this card):\n{}\n",
        serde_json::to_string(&facts)?
    );
    if authority.is_some() {
        prompt.push_str("This card is the durable bootstrap_plan assignment for the configured superior to author the first DAG of this family. Goal.planner names the ongoing planner and business executor, not a replacement for this first-family authority. Author the initial DAG here; do not perform its business work. Keep business nodes on Goal.agents and candidate validation independent of the actual plan author. Subsequent planning and work retain the enrolled Goal roles.\n");
    } else {
        prompt.push_str("Perform only this card's assigned control role as assigned_agent_id. Goal.planner remains the ongoing planning/business role; candidate validation uses the separately assigned independent executor. This assignment is not permission to add executors, change Goal scope, approve your own plan or perform unassigned business work.\n");
    }
    Ok(prompt)
}

#[cfg(test)]
#[path = "occurrence_context/tests.rs"]
mod tests;
