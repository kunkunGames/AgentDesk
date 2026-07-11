use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

/// Agent-mode deliveries still awaiting transcript evidence, joined with the
/// parent fields the poller needs. Extends the lease of everything returned.
///
/// Only rows whose external launch was confirmed qualify. A recorded `turn_id`
/// with NULL `turn_started_at` is merely a durable launch intent; renewing it
/// here would turn a pre-launch process crash into a 30-minute phantom turn.
/// Intent-only crashes are owned by lease expiry +
/// [`recover_expired_leases_pg`].
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RunningAgentDelivery {
    pub delivery_id: String,
    pub scheduled_message_id: String,
    pub claim_token: String,
    pub fire_scheduled_at: DateTime<Utc>,
    pub turn_id: Option<String>,
    pub started_at: DateTime<Utc>,
    pub retry_count: i32,
    pub content: String,
    pub target_channel_id: Option<String>,
    pub bot: String,
    pub agent_id: Option<String>,
    pub on_agent_failure: String,
    pub schedule: Option<String>,
    pub timezone: String,
    pub scheduled_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

pub async fn list_running_agent_deliveries_pg(
    pool: &PgPool,
    lease_secs: i64,
    limit: i64,
) -> Result<Vec<RunningAgentDelivery>, sqlx::Error> {
    sqlx::query_as::<_, RunningAgentDelivery>(
        "UPDATE scheduled_message_deliveries d
         SET lease_expires_at = NOW() + ($1::bigint * INTERVAL '1 second'),
             updated_at = NOW()
         FROM scheduled_messages m
         WHERE d.id IN (
             SELECT id FROM scheduled_message_deliveries
             WHERE status = 'running' AND delivery_kind = 'agent'
               AND turn_id IS NOT NULL AND turn_started_at IS NOT NULL
             ORDER BY created_at
             LIMIT $2
             FOR UPDATE SKIP LOCKED)
           AND m.id = d.scheduled_message_id
         RETURNING d.id AS delivery_id, d.scheduled_message_id, d.claim_token,
                   d.fire_scheduled_at, d.turn_id, d.turn_started_at AS started_at,
                   d.retry_count,
                   m.content, m.target_channel_id, m.bot, m.agent_id,
                   m.on_agent_failure, m.schedule, m.timezone,
                   m.scheduled_at, m.expires_at",
    )
    .bind(lease_secs)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// Boot/lease recovery: expired running deliveries become `interrupted` and
/// their parents return to `scheduled` so the due scan can re-arm the slot
/// (bounded by the retry cap enforced at claim time).
pub async fn recover_expired_leases_pg(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "SELECT m.id AS scheduled_message_id,
                d.id AS delivery_id,
                d.fire_scheduled_at
         FROM scheduled_messages m
         JOIN scheduled_message_deliveries d ON d.id = m.in_flight_delivery_id
         WHERE m.status = 'firing'
           AND d.status = 'running'
           AND d.lease_expires_at IS NOT NULL
           AND d.lease_expires_at < NOW()
         ORDER BY d.lease_expires_at, m.id
         FOR UPDATE OF m SKIP LOCKED",
    )
    .fetch_all(&mut *tx)
    .await?;
    let mut recovered = 0_u64;
    for row in rows {
        let delivery_id: String = row.try_get("delivery_id")?;
        let message_id: String = row.try_get("scheduled_message_id")?;
        let fire_scheduled_at: DateTime<Utc> = row.try_get("fire_scheduled_at")?;
        // The parent lock is held before this child update. Re-check the lease
        // cutoff so a concurrent token-guarded turn-start renewal wins safely.
        let delivery_updated = sqlx::query(
            "UPDATE scheduled_message_deliveries
             SET status = 'interrupted', error = 'delivery lease expired',
                 finished_at = NOW(), updated_at = NOW()
             WHERE id = $1 AND status = 'running'
               AND lease_expires_at IS NOT NULL AND lease_expires_at < NOW()",
        )
        .bind(&delivery_id)
        .execute(&mut *tx)
        .await?;
        if delivery_updated.rows_affected() == 0 {
            continue;
        }
        let parent_updated = sqlx::query(
            "UPDATE scheduled_messages
             SET status = 'scheduled', scheduled_at = $3,
                 in_flight_delivery_id = NULL, updated_at = NOW()
             WHERE id = $1 AND in_flight_delivery_id = $2 AND status = 'firing'",
        )
        .bind(&message_id)
        .bind(&delivery_id)
        .bind(fire_scheduled_at)
        .execute(&mut *tx)
        .await?;
        recovered = recovered.saturating_add(parent_updated.rows_affected());
    }
    tx.commit().await?;
    Ok(recovered)
}
