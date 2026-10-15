//! Recurring work has a persisted finite policy independent of parent reviews.
use super::super::{Budget, Goal};
use anyhow::{Result, ensure};
use serde_json::Value;

/// Each scheduled child gets an initial plan and up to eight fresh replans.
/// This is independent of the parent's lifetime control budget.
pub(crate) const MAX_OCCURRENCE_EVOLUTIONS: u32 = 8;

/// Called under the campaign lock by every audited budget mutation path.
/// Historical children are read without changing their recorded limits.
pub(crate) async fn validate_revision(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: &str,
    budget: &Budget,
) -> Result<()> {
    if budget.max_evolutions > MAX_OCCURRENCE_EVOLUTIONS {
        ensure!(
            !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM campaign_graph_occurrences WHERE child_id=$1)"
            )
            .bind(id)
            .fetch_one(&mut **tx)
            .await?,
            "scheduled children allow at most eight replans; await a fresh occurrence"
        );
    }
    Ok(())
}

pub(crate) fn resolve(goal: &Goal, stored: Option<Value>) -> Result<Budget> {
    let budget = match stored {
        Some(value) => serde_json::from_value(value)?,
        None => goal.budget.clone(),
    };
    let mut occurrence = goal.clone();
    occurrence.schedule = None;
    occurrence.budget = budget;
    occurrence.validate()?;
    ensure!(
        occurrence.budget.max_evolutions <= MAX_OCCURRENCE_EVOLUTIONS,
        "occurrence policy allows at most eight replans per scheduled child"
    );
    Ok(occurrence.budget)
}
