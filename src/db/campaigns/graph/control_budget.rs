//! Admission reserves are explicit policy estimates, not minimum task runtimes.
//! An unstarted control can wait without cancelling a running owner or losing receipts.
use super::{Budget, Goal, learning::budget::OrdinaryUsage};
use anyhow::Result;
use serde::Serialize;
use sqlx::{PgConnection, PgPool, Row};

const HOLD_PREFIX: &str = "Control budget requires audited recovery before a new";

pub fn held_role(reason: Option<&str>) -> Option<&'static str> {
    let reason = reason?;
    ["plan", "validate", "manage", "finish"]
        .into_iter()
        .find(|role| reason.starts_with(&format!("{HOLD_PREFIX} {role}:")))
}

#[derive(Clone, Debug, Serialize)]
pub struct Frame {
    pub schema_version: u32,
    pub observed_at: chrono::DateTime<chrono::Utc>,
    pub limits: Budget,
    pub usage: OrdinaryUsage,
    pub remaining_seconds: f64,
    pub remaining_publications: i64,
    pub remaining_observed_tool_calls: Option<i64>,
    pub role_reserves_seconds: std::collections::BTreeMap<String, f64>,
    pub reserve_basis: &'static str,
}

#[derive(Debug)]
pub struct Unavailable(pub String);
impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Unavailable {}

impl Frame {
    pub fn control_wait(&self, role: &str) -> Option<String> {
        let (roles, publications): (&[&str], i64) = match role {
            // Leave room for the planner, independent candidate validation and
            // at least one fresh business verification. Work may need more.
            "plan" => (&["plan", "validate", "work"], 3),
            "validate" => (&["validate", "work"], 2),
            "manage" => (&["manage", "finish"], 2),
            "finish" => (&["finish"], 1),
            _ => return None,
        };
        let reserve: f64 = roles
            .iter()
            .map(|role| self.role_reserves_seconds[*role])
            .sum();
        if self.remaining_publications < publications
            || self.remaining_seconds < reserve
            || self.remaining_observed_tool_calls.is_some_and(|n| n <= 0)
        {
            Some(format!(
                "{HOLD_PREFIX} {role}: {:.1}s and {} publications remain; {:.1}s and {publications} publications reserved for control and fresh verification. No new control was started; accumulated usage and unknown outcomes are retained",
                self.remaining_seconds, self.remaining_publications, reserve
            ))
        } else {
            None
        }
    }
}

pub async fn snapshot(pool: &PgPool, id: &str, goal: &Goal) -> Result<Frame> {
    let mut connection = pool.acquire().await?;
    on_connection(&mut connection, id, goal).await
}

pub async fn on_connection(connection: &mut PgConnection, id: &str, goal: &Goal) -> Result<Frame> {
    let usage = super::learning::budget::usage_on_connection(connection, id, goal).await?;
    // Only accepted controls/work in the same pinned family are references.
    // Failed, cancelled and incomplete turns never inflate the estimate.
    let rows = sqlx::query(
        "SELECT role,percentile_cont(0.5) WITHIN GROUP(ORDER BY seconds) AS reserve
         FROM (SELECT t.role,GREATEST(0,EXTRACT(EPOCH FROM(d.completed_at-d.created_at)))::float8 AS seconds
               FROM campaign_graph_tasks t JOIN task_dispatches d ON d.id=t.observation->>'dispatch_id'
               JOIN campaign_dag_members m ON m.campaign_id=t.campaign_id
               WHERE m.family_id=(SELECT family_id FROM campaign_dag_members WHERE campaign_id=$1)
                 AND t.accepted AND d.status='completed' AND d.completed_at IS NOT NULL
                 AND t.role IN ('plan','validate','work','manage','finish')
               ORDER BY d.completed_at DESC LIMIT 48) recent GROUP BY role",
    )
    .bind(id)
    .fetch_all(&mut *connection)
    .await?;
    // This finite finish allowance is policy, not a claim that any task must
    // take 30 seconds. Shorter node deadlines retain their configured ceiling.
    let floor = f64::from(goal.budget.node_timeout_secs.min(30));
    let mut reserves: std::collections::BTreeMap<String, f64> =
        ["plan", "validate", "work", "manage", "finish"]
            .into_iter()
            .map(|r| (r.into(), floor))
            .collect();
    for row in rows {
        let value: f64 = row.get("reserve");
        if value.is_finite() {
            reserves.insert(
                row.get("role"),
                value
                    .max(floor)
                    .min(f64::from(goal.budget.node_timeout_secs)),
            );
        }
    }
    Ok(Frame {
        schema_version: 1,
        observed_at: chrono::Utc::now(),
        remaining_seconds: (f64::from(goal.budget.max_elapsed_secs)
            - usage.elapsed_dispatch_seconds)
            .max(0.0),
        remaining_publications: (i64::from(goal.budget.max_dispatches) - usage.published_tasks)
            .max(0),
        remaining_observed_tool_calls: goal
            .budget
            .max_observed_tool_calls
            .map(|n| (i64::from(n) - usage.observed_tool_calls).max(0)),
        limits: goal.budget.clone(),
        usage,
        role_reserves_seconds: reserves,
        reserve_basis: "same-family accepted dispatch-lifetime median, capped by node deadline; finite finish allowance when unavailable; estimate, not a runtime guarantee",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frame(remaining: f64) -> Frame {
        Frame {
            schema_version: 1,
            observed_at: chrono::Utc::now(),
            limits: Budget {
                max_dispatches: 20,
                max_elapsed_secs: 1800,
                node_timeout_secs: 600,
                max_evolutions: 2,
                max_observed_tool_calls: Some(300),
            },
            usage: OrdinaryUsage {
                scope: "campaign_lifetime",
                published_tasks: 6,
                elapsed_dispatch_seconds: 1800.0 - remaining,
                observed_tool_calls: 100,
            },
            remaining_seconds: remaining,
            remaining_publications: 14,
            remaining_observed_tool_calls: Some(200),
            role_reserves_seconds: [
                ("plan".into(), 30.0),
                ("validate".into(), 30.0),
                ("work".into(), 30.0),
                ("manage".into(), 30.0),
                ("finish".into(), 30.0),
            ]
            .into(),
            reserve_basis: "fixture policy",
        }
    }
    #[test]
    fn captured_56_second_repair_waits_before_another_planner_is_issued() {
        assert!(frame(56.3).control_wait("plan").is_some());
        assert!(frame(90.0).control_wait("plan").is_none());
        assert!(frame(60.0).control_wait("validate").is_none());
        assert!(frame(0.0).control_wait("work").is_none());
        let mut f = frame(1800.0);
        f.remaining_publications = 2;
        assert!(f.control_wait("plan").is_some());
        f.remaining_publications = 20;
        f.remaining_observed_tool_calls = Some(0);
        assert!(f.control_wait("validate").is_some());
    }

    #[test]
    fn manager_reserves_an_independent_goal_assessment_before_spending_its_last_turn() {
        let mut f = frame(60.0);
        f.remaining_publications = 2;
        assert!(f.control_wait("manage").is_none());
        f.remaining_publications = 1;
        assert!(f.control_wait("manage").is_some());
        assert!(f.control_wait("finish").is_none());
        f.remaining_seconds = 29.0;
        assert!(f.control_wait("finish").is_some());
        assert_eq!(
            held_role(Some(
                "Control budget requires audited recovery before a new manage: budget"
            )),
            Some("manage")
        );
        assert_eq!(
            held_role(Some(
                "Control budget requires audited recovery before a new finish: budget"
            )),
            Some("finish")
        );
    }
}
