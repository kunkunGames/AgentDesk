use anyhow::{Result, anyhow};
use sqlx::PgPool;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MementoFeedbackTurnStat {
    pub turn_id: String,
    pub stat_date: String,
    pub agent_id: String,
    pub provider: String,
    pub recall_count: i64,
    pub manual_tool_feedback_count: i64,
    pub manual_covered_recall_count: i64,
    pub auto_tool_feedback_count: i64,
    pub covered_recall_count: i64,
}

pub async fn upsert_turn_stat_pg(pool: &PgPool, stat: &MementoFeedbackTurnStat) -> Result<()> {
    sqlx::query(
        "INSERT INTO memento_feedback_turn_stats (
            turn_id, stat_date, agent_id, provider,
            recall_count, manual_tool_feedback_count,
            manual_covered_recall_count, auto_tool_feedback_count,
            covered_recall_count
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (turn_id) DO UPDATE SET
            stat_date = EXCLUDED.stat_date,
            agent_id = EXCLUDED.agent_id,
            provider = EXCLUDED.provider,
            recall_count = EXCLUDED.recall_count,
            manual_tool_feedback_count = EXCLUDED.manual_tool_feedback_count,
            manual_covered_recall_count = EXCLUDED.manual_covered_recall_count,
            auto_tool_feedback_count = EXCLUDED.auto_tool_feedback_count,
            covered_recall_count = EXCLUDED.covered_recall_count",
    )
    .bind(&stat.turn_id)
    .bind(&stat.stat_date)
    .bind(&stat.agent_id)
    .bind(&stat.provider)
    .bind(stat.recall_count)
    .bind(stat.manual_tool_feedback_count)
    .bind(stat.manual_covered_recall_count)
    .bind(stat.auto_tool_feedback_count)
    .bind(stat.covered_recall_count)
    .execute(pool)
    .await
    .map_err(|e| anyhow!("persist postgres memento feedback stats failed: {e}"))?;

    Ok(())
}
