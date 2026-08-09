//! Atomic push handoff shared by Discord-only and provider-fan-out fires.

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::db::scheduled_messages as db;
use crate::db::scheduled_messages::ClaimedFire;
use crate::services::external_share_outbox::{
    NewExternalShareOutbox, enqueue_external_share_outbox_tx,
};
use crate::services::message_outbox::{
    OutboxMessage, enqueue_outbox_pg_returning_id_with_persistent_dedupe_on_tx,
};

use super::timing::compute_resume;

pub(super) async fn commit_push_handoff(
    pool: &PgPool,
    fire: &ClaimedFire,
    message: OutboxMessage<'_>,
    external_share: Option<&NewExternalShareOutbox>,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    if !db::lock_active_delivery_tx(
        &mut tx,
        &fire.message.id,
        &fire.delivery_id,
        &fire.claim_token,
    )
    .await?
    {
        return Ok(false);
    }
    let outbox_id =
        enqueue_outbox_pg_returning_id_with_persistent_dedupe_on_tx(&mut tx, message).await?;
    if let Some(external_share) = external_share {
        enqueue_external_share_outbox_tx(&mut tx, external_share).await?;
    }
    let (next, forced_terminal) = compute_resume(
        fire.message.schedule.as_deref(),
        &fire.message.timezone,
        fire.message.scheduled_at,
        fire.message.expires_at,
        now,
    );
    let terminal_status = forced_terminal.unwrap_or(db::STATUS_SENT);
    let next = forced_terminal.is_none().then_some(next).flatten();
    let transitioned = db::finish_locked_delivery_and_finalize_parent_tx(
        &mut tx,
        &fire.delivery_id,
        &fire.claim_token,
        db::DELIVERY_SENT,
        None,
        Some(outbox_id),
        None,
        &fire.message.id,
        true,
        terminal_status,
        next,
    )
    .await?;
    if !transitioned {
        return Ok(false);
    }
    tx.commit().await?;
    Ok(true)
}
