//! Schedule admission belongs to the Hub tick. No second daemon or RoutineRun.
pub(crate) mod activation;
pub(crate) mod budget;
mod rollover;
use super::{Goal, digest};
use crate::db::campaigns::{self, CampaignInput, CampaignStatus};
use anyhow::{Result, ensure};
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schedule {
    pub timezone: String,
    /// Minutes after local midnight. Ambiguous/nonexistent DST slots are skipped.
    pub daily_minutes: BTreeSet<u16>,
}
impl Schedule {
    pub fn validate(&self) -> Result<()> {
        self.timezone.parse::<chrono_tz::Tz>()?;
        ensure!(
            !self.daily_minutes.is_empty()
                && self.daily_minutes.len() <= 48
                && self.daily_minutes.iter().all(|n| *n < 1440),
            "invalid bounded daily schedule"
        );
        Ok(())
    }
    pub fn latest(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
        self.validate()?;
        let zone = self.timezone.parse::<chrono_tz::Tz>()?;
        let today = now.with_timezone(&zone).date_naive();
        Ok([0, 1]
            .into_iter()
            .flat_map(|offset| {
                let day = today - Duration::days(offset);
                self.daily_minutes.iter().filter_map(move |m| {
                    let local = day.and_hms_opt(u32::from(*m / 60), u32::from(*m % 60), 0)?;
                    zone.from_local_datetime(&local)
                        .single()
                        .map(|d| d.with_timezone(&Utc))
                })
            })
            .filter(|t| *t <= now)
            .max())
    }
}

/// Coalesce missed slots. A settled, exhausted earlier occurrence may retire;
/// live execution and operator pauses still fence the next occurrence.
pub async fn admit(
    pool: &PgPool,
    id: &str,
    goal: &Goal,
    now: DateTime<Utc>,
) -> Result<Option<String>> {
    let family = super::learning::family(pool, id, goal).await?;
    let schedule = goal
        .schedule
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("goal has no schedule"))?;
    let Some(slot) = schedule.latest(now)? else {
        return Ok(None);
    };
    let mut tx = pool.begin().await?;
    let parent = super::store::lock(&mut tx, id).await?;
    let state=sqlx::query("SELECT spec,occurrence_budget,manager_enabled,COALESCE(schedule_admission_started_at,created_at) AS created_at,hold_reason FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE")
        .bind(id).fetch_one(&mut *tx).await?;
    let current_goal: Goal = serde_json::from_value(state.get("spec"))?;
    // A tick may have read the goal before an audited pause/revision/resume.
    if super::contract_digest(&current_goal)? != super::contract_digest(goal)? {
        return Ok(None);
    }
    if state.get::<Option<String>, _>("hold_reason").is_some()
        || slot < state.get::<DateTime<Utc>, _>("created_at")
    {
        return Ok(None);
    }
    if let Some(child) = sqlx::query_scalar::<_, String>(
        "SELECT child_id FROM campaign_graph_occurrences WHERE campaign_id=$1 AND scheduled_at=$2",
    )
    .bind(id)
    .bind(slot)
    .fetch_optional(&mut *tx)
    .await?
    {
        return Ok(Some(child));
    }
    if !rollover::settle_previous(&mut tx, id, slot).await? {
        return Ok(None);
    }
    let child_id = format!(
        "occ-{}",
        &digest(format!("{id}:{}", slot.to_rfc3339()).as_bytes())[..32]
    );
    let mut child_goal = goal.clone();
    child_goal.schedule = None;
    child_goal.budget = budget::resolve(goal, state.get("occurrence_budget"))?;
    // A time window is run context, not a different reusable business scope.
    child_goal.observation_window = Some(slot.to_rfc3339());
    child_goal.validate()?;
    campaigns::create_on_tx(
        &mut tx,
        child_id.clone(),
        CampaignInput {
            title: format!(
                "{} · {}",
                parent.title.chars().take(400).collect::<String>(),
                slot.to_rfc3339()
            ),
            description: format!("Recurring goal: {id}\nScheduled at: {}", slot.to_rfc3339()),
            status: CampaignStatus::Active,
            round: 1,
            nodes: vec![],
            auto_queue: Some(false),
        },
    )
    .await?;
    sqlx::query(
        "INSERT INTO campaign_execution_specs(campaign_id,spec,manager_enabled) VALUES($1,$2,$3)",
    )
    .bind(&child_id)
    .bind(serde_json::to_value(child_goal)?)
    .bind(state.get::<bool, _>("manager_enabled"))
    .execute(&mut *tx)
    .await?;
    sqlx::query("INSERT INTO campaign_graph_occurrences(campaign_id,scheduled_at,child_id) VALUES($1,$2,$3)")
        .bind(id).bind(slot).bind(&child_id).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO campaign_dag_members(campaign_id,family_id) VALUES($1,$2)")
        .bind(&child_id)
        .bind(family)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(child_id))
}
