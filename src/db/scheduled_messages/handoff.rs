use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction};

/// Lock and validate the active parent/child claim in the global
/// parent-before-delivery order. Callers keep this transaction open through
/// the irreversible handoff so operator cancellation either wins first or
/// observes the recorded handoff afterwards.
pub(crate) async fn lock_active_delivery_claim_tx(
    tx: &mut Transaction<'_, Postgres>,
    message_id: &str,
    delivery_id: &str,
    claim_token: &str,
) -> Result<bool, sqlx::Error> {
    if !super::lock_active_parent_tx(tx, message_id, delivery_id).await? {
        return Ok(false);
    }
    let locked = sqlx::query_scalar::<_, i32>(
        "SELECT 1
         FROM scheduled_message_deliveries
         WHERE id = $1 AND claim_token = $2 AND status = 'running'
         FOR UPDATE",
    )
    .bind(delivery_id)
    .bind(claim_token)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(locked.is_some())
}

pub(crate) async fn record_delivery_outbox_handoff_tx(
    tx: &mut Transaction<'_, Postgres>,
    delivery_id: &str,
    claim_token: &str,
    outbox_id: i64,
    fallback: bool,
) -> Result<bool, sqlx::Error> {
    let updated = if fallback {
        sqlx::query(
            "UPDATE scheduled_message_deliveries
             SET fallback_outbox_id = $3, updated_at = NOW()
             WHERE id = $1 AND claim_token = $2 AND status = 'running'",
        )
        .bind(delivery_id)
        .bind(claim_token)
        .bind(outbox_id)
        .execute(&mut **tx)
        .await?
    } else {
        sqlx::query(
            "UPDATE scheduled_message_deliveries
             SET outbox_id = $3, updated_at = NOW()
             WHERE id = $1 AND claim_token = $2 AND status = 'running'",
        )
        .bind(delivery_id)
        .bind(claim_token)
        .bind(outbox_id)
        .execute(&mut **tx)
        .await?
    };
    Ok(updated.rows_affected() > 0)
}

/// Return a claim to `scheduled` without consuming a retry attempt. This is
/// reserved for prerequisites that are absent for the whole process (for
/// example, an agent delivery when no Discord runtime was bootstrapped).
pub(crate) async fn defer_delivery_without_retry_pg(
    pool: &PgPool,
    delivery_id: &str,
    claim_token: &str,
    message_id: &str,
    fire_scheduled_at: DateTime<Utc>,
    reason: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    if !lock_active_delivery_claim_tx(&mut tx, message_id, delivery_id, claim_token).await? {
        return Ok(false);
    }
    let resume_scheduled_at = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
        "DELETE FROM scheduled_message_deliveries
         WHERE id = $1 AND claim_token = $2 AND status = 'running'
         RETURNING resume_scheduled_at",
    )
    .bind(delivery_id)
    .bind(claim_token)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(resume_scheduled_at) = resume_scheduled_at else {
        return Ok(false);
    };
    let resume_at = resume_scheduled_at.unwrap_or(fire_scheduled_at);
    sqlx::query(
        "UPDATE scheduled_messages
         SET status = 'scheduled', scheduled_at = $3,
             in_flight_delivery_id = NULL, last_error = $4, updated_at = NOW()
         WHERE id = $1 AND in_flight_delivery_id = $2 AND status = 'firing'",
    )
    .bind(message_id)
    .bind(delivery_id)
    .bind(resume_at)
    .bind(reason)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// Persist the reserved turn identity while the claim fence is held, without
/// declaring that the external launch has happened. Intent-only rows are not
/// polled or lease-renewed, so a crash before launch is recovered normally.
pub(crate) async fn record_delivery_agent_turn_intent_pg(
    pool: &PgPool,
    message_id: &str,
    delivery_id: &str,
    claim_token: &str,
    turn_id: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    if !lock_active_delivery_claim_tx(&mut tx, message_id, delivery_id, claim_token).await? {
        return Ok(false);
    }
    let updated = sqlx::query(
        "UPDATE scheduled_message_deliveries
         SET turn_id = $3,
             turn_started_at = NULL,
             updated_at = NOW()
         WHERE id = $1 AND claim_token = $2 AND status = 'running'",
    )
    .bind(delivery_id)
    .bind(claim_token)
    .bind(turn_id)
    .execute(&mut *tx)
    .await?;
    let recorded = updated.rows_affected() > 0;
    tx.commit().await?;
    Ok(recorded)
}

/// Last-moment cancellation fence for the gap between persisting a turn intent
/// and calling the external headless runtime.
pub(crate) async fn agent_turn_intent_is_active_pg(
    pool: &PgPool,
    message_id: &str,
    delivery_id: &str,
    claim_token: &str,
    turn_id: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    if !lock_active_delivery_claim_tx(&mut tx, message_id, delivery_id, claim_token).await? {
        return Ok(false);
    }
    let active = sqlx::query_scalar::<_, i32>(
        "SELECT 1
         FROM scheduled_message_deliveries
         WHERE id = $1 AND claim_token = $2 AND turn_id = $3
           AND status = 'running'",
    )
    .bind(delivery_id)
    .bind(claim_token)
    .bind(turn_id)
    .fetch_optional(&mut *tx)
    .await?
    .is_some();
    tx.commit().await?;
    Ok(active)
}

/// Confirm an agent turn only after the headless runtime reports `started`.
/// The intent's turn id is part of the predicate so a stale or mismatched
/// launch can never graduate another attempt.
pub(crate) async fn mark_delivery_agent_turn_started_pg(
    pool: &PgPool,
    message_id: &str,
    delivery_id: &str,
    claim_token: &str,
    turn_id: &str,
    lease_secs: i64,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    if !lock_active_delivery_claim_tx(&mut tx, message_id, delivery_id, claim_token).await? {
        return Ok(false);
    }
    let updated = sqlx::query(
        "UPDATE scheduled_message_deliveries
         SET turn_started_at = NOW(),
             started_at = NOW(),
             lease_expires_at = NOW() + ($4::bigint * INTERVAL '1 second'),
             updated_at = NOW()
         WHERE id = $1 AND claim_token = $2 AND turn_id = $3
           AND status = 'running'",
    )
    .bind(delivery_id)
    .bind(claim_token)
    .bind(turn_id)
    .bind(lease_secs)
    .execute(&mut *tx)
    .await?;
    let recorded = updated.rows_affected() > 0;
    tx.commit().await?;
    Ok(recorded)
}
