use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Agent-mode deliveries still awaiting transcript evidence, joined with the
/// parent fields the poller needs. Extends the lease of everything returned.
///
/// Only runtime-confirmed rows qualify. A recorded `turn_id` with NULL
/// `turn_started_at` is a durable launch intent; renewing it here would turn a
/// pre-launch crash into a phantom live turn. Intent-only crashes are owned by
/// lease expiry + [`recover_expired_leases_pg`].
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RunningAgentDelivery {
    pub delivery_id: String,
    pub scheduled_message_id: String,
    pub claim_token: String,
    pub fire_scheduled_at: DateTime<Utc>,
    pub turn_id: Option<String>,
    pub started_at: DateTime<Utc>,
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
    claim_owner: &str,
    lease_secs: i64,
    limit: i64,
) -> Result<Vec<RunningAgentDelivery>, sqlx::Error> {
    let takeover_token = format!("smpoll_{}", Uuid::new_v4());
    sqlx::query_as::<_, RunningAgentDelivery>(
        "WITH candidates AS MATERIALIZED (
             SELECT candidate.id
             FROM scheduled_message_deliveries AS candidate
             WHERE candidate.status = 'running'
               AND candidate.delivery_kind = 'agent'
               AND candidate.turn_id IS NOT NULL
               AND candidate.turn_started_at IS NOT NULL
               AND (candidate.claim_owner = $1 OR candidate.claim_owner IS NULL
                    OR candidate.lease_expires_at IS NULL
                    OR candidate.lease_expires_at <= NOW())
             ORDER BY candidate.lease_expires_at,
                      candidate.created_at, candidate.id
             LIMIT $4
             FOR UPDATE SKIP LOCKED
         )
         UPDATE scheduled_message_deliveries d
         SET claim_owner = $1,
             claim_token = CASE
                 WHEN d.claim_owner = $1 THEN d.claim_token
                 ELSE $2
             END,
             lease_expires_at = NOW() + ($3::bigint * INTERVAL '1 second'),
             updated_at = NOW()
         FROM scheduled_messages m, candidates
         WHERE d.id = candidates.id
           AND m.id = d.scheduled_message_id
           AND m.status = 'firing' AND m.in_flight_delivery_id = d.id
           AND d.status = 'running' AND d.delivery_kind = 'agent'
           AND d.turn_id IS NOT NULL
           AND d.turn_started_at IS NOT NULL
         RETURNING d.id AS delivery_id, d.scheduled_message_id, d.claim_token,
                   d.fire_scheduled_at, d.turn_id, d.turn_started_at AS started_at,
                   m.content, m.target_channel_id, m.bot, m.agent_id,
                   m.on_agent_failure, m.schedule, m.timezone,
                   d.resume_scheduled_at AS scheduled_at, m.expires_at",
    )
    .bind(claim_owner)
    .bind(takeover_token)
    .bind(lease_secs)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// Return an agent claim to `scheduled` without consuming its retry budget
/// when a process-wide prerequisite (the Discord runtime) is unavailable.
pub async fn defer_delivery_without_retry_pg(
    pool: &PgPool,
    delivery_id: &str,
    claim_token: &str,
    message_id: &str,
    resume_scheduled_at: DateTime<Utc>,
    reason: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    if !super::lock_active_delivery_tx(&mut tx, message_id, delivery_id, claim_token).await? {
        return Ok(false);
    }
    let deleted = sqlx::query(
        "DELETE FROM scheduled_message_deliveries
         WHERE id = $1 AND claim_token = $2 AND status = 'running'",
    )
    .bind(delivery_id)
    .bind(claim_token)
    .execute(&mut *tx)
    .await?;
    if deleted.rows_affected() == 0 {
        return Ok(false);
    }
    sqlx::query(
        "UPDATE scheduled_messages
         SET status = 'scheduled', scheduled_at = $3,
             in_flight_delivery_id = NULL, last_error = $4, updated_at = NOW()
         WHERE id = $1 AND in_flight_delivery_id = $2 AND status = 'firing'",
    )
    .bind(message_id)
    .bind(delivery_id)
    .bind(resume_scheduled_at)
    .bind(reason)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// Boot/lease recovery: expired pre-launch deliveries become `interrupted` and
/// their parents return to `scheduled` so the due scan can re-arm the slot.
/// Once `turn_started_at` confirms a runtime launch, the durable turn is
/// adopted by the regular poller instead of restarted; claim fencing cannot
/// prevent an old turn from relaying a duplicate after a replacement starts.
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
           AND d.turn_started_at IS NULL
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
