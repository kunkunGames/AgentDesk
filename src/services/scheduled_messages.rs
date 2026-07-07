//! Fire executor + scheduler loop for the scheduled-message reservation pool.
//!
//! Design: docs/design/scheduled-messages.md.
//!
//! Ownership boundaries (deliberately narrow):
//!   * push fires hand off to `message_outbox` and finish immediately —
//!     retry/final delivery state belongs to `message_outbox_loop`, never
//!     re-polled here.
//!   * agent fires start a headless turn in the target channel (the relayed
//!     assistant reply IS the delivered message) and stay `running` until
//!     transcript evidence, terminal turn error, or timeout — the same
//!     completion-evidence model as `RoutineAgentExecutor`.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::sync::Arc;

use crate::db::scheduled_messages as db;
use crate::db::scheduled_messages::{ClaimedFire, RunningAgentDelivery, ScheduledMessageRow};
use crate::services::discord::health::{
    HealthRegistry, reserve_headless_agent_turn,
    start_reserved_headless_agent_turn_with_owner_channel,
};
use crate::services::message_outbox::{OutboxMessage, enqueue_outbox_pg_returning_id_with_ttl};

const CLAIM_BATCH: i64 = 10;
const AGENT_POLL_BATCH: i64 = 20;
pub(crate) const LEASE_SECS: i64 = 120;
/// A fire slot is retried this many times after interruptions before the
/// definition is failed outright (claim-time cap; slot retry_count counts
/// re-arms of the same fire slot).
const MAX_FIRE_RETRIES: i32 = 3;
/// Agent turns that produced no transcript evidence within this window fall
/// back per on_agent_failure. Matches the routines agent-completion default.
const AGENT_COMPLETION_TIMEOUT_SECS: i64 = 1800;
/// Outbox dedupe TTL for a fire slot; long enough that a crashed node retrying
/// the same slot cannot double-post, short enough not to swallow the next
/// recurrence (minimum practical recurrence is minutes, not hours).
const OUTBOX_DEDUPE_TTL_SECS: i64 = 3600;
const OUTBOX_SOURCE: &str = "scheduled_message";

// ── Scheduler loop ──────────────────────────────────────────────────────────

pub async fn scheduled_message_loop(
    pg_pool: Arc<PgPool>,
    health_registry: Option<Arc<HealthRegistry>>,
) {
    use std::time::Duration;

    // Give Discord runtime bootstrap a brief head start (message_outbox_loop
    // pattern) — agent fires need a live runtime to start turns.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let claim_owner = format!(
        "scheduled-messages:{}:{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "local".to_string()),
        std::process::id()
    );
    tracing::info!("[smsg] scheduled message worker started (adaptive backoff 500ms-5s)");

    let mut poll_interval = Duration::from_millis(500);
    let max_interval = Duration::from_secs(5);
    loop {
        tokio::time::sleep(poll_interval).await;
        let did_work = tick_once(&pg_pool, health_registry.as_deref(), &claim_owner).await;
        if did_work {
            poll_interval = Duration::from_millis(500);
        } else {
            poll_interval = (poll_interval.mul_f64(1.5)).min(max_interval);
        }
    }
}

/// One scheduler pass: lease recovery, due-claim + fire, agent-turn polling.
/// Returns true when any row moved (drives the adaptive backoff).
async fn tick_once(
    pool: &PgPool,
    health_registry: Option<&HealthRegistry>,
    claim_owner: &str,
) -> bool {
    let mut did_work = false;

    match db::recover_expired_leases_pg(pool).await {
        Ok(recovered) if recovered > 0 => {
            tracing::warn!(recovered, "[smsg] rewound expired delivery leases");
            did_work = true;
        }
        Ok(_) => {}
        Err(error) => tracing::warn!("[smsg] lease recovery failed: {error}"),
    }

    let now = Utc::now();
    match db::claim_due_fires_pg(pool, claim_owner, CLAIM_BATCH, LEASE_SECS, now).await {
        Ok(claimed) => {
            for fire in claimed {
                did_work = true;
                fire_claimed(pool, health_registry, fire, now).await;
            }
        }
        Err(error) => tracing::warn!("[smsg] due claim failed: {error}"),
    }

    match db::list_running_agent_deliveries_pg(pool, LEASE_SECS, AGENT_POLL_BATCH).await {
        Ok(running) => {
            for delivery in running {
                if poll_agent_delivery(pool, delivery).await {
                    did_work = true;
                }
            }
        }
        Err(error) => tracing::warn!("[smsg] agent delivery poll failed: {error}"),
    }

    did_work
}

// ── Fire execution ──────────────────────────────────────────────────────────

/// Execute one armed fire slot. Every branch leaves the delivery and parent in
/// a consistent state; errors degrade to `interrupted` so the bounded re-arm
/// path (claim-time retry cap) owns the retry policy.
pub async fn fire_claimed(
    pool: &PgPool,
    health_registry: Option<&HealthRegistry>,
    fire: ClaimedFire,
    now: DateTime<Utc>,
) {
    let message = &fire.message;

    if fire.retry_count > MAX_FIRE_RETRIES {
        let error = format!("fire retry budget exhausted after {MAX_FIRE_RETRIES} re-arms");
        finish_and_finalize(
            pool,
            &fire,
            db::DELIVERY_FAILED,
            Some(&error),
            None,
            None,
            false,
            now,
        )
        .await;
        return;
    }

    if let Some(expires_at) = message.expires_at {
        if expires_at <= fire.fire_scheduled_at {
            if let Err(error) = db::mark_expired_pg(pool, &message.id, &fire.delivery_id).await {
                tracing::warn!(id = message.id, "[smsg] expire transition failed: {error}");
            }
            return;
        }
    }

    match message.delivery_kind.as_str() {
        db::KIND_PUSH => fire_push(pool, &fire, now).await,
        db::KIND_AGENT => fire_agent(pool, health_registry, &fire).await,
        other => {
            let error = format!("unknown delivery_kind '{other}'");
            finish_and_finalize(
                pool,
                &fire,
                db::DELIVERY_FAILED,
                Some(&error),
                None,
                None,
                false,
                now,
            )
            .await;
        }
    }
}

async fn fire_push(pool: &PgPool, fire: &ClaimedFire, now: DateTime<Utc>) {
    let message = &fire.message;
    let Some(channel_id) = message.target_channel_id.as_deref() else {
        // chk_smsg_push_target_required makes this unreachable; degrade safely.
        finish_and_finalize(
            pool,
            fire,
            db::DELIVERY_FAILED,
            Some("push delivery has no target channel"),
            None,
            None,
            false,
            now,
        )
        .await;
        return;
    };
    let target = format!("channel:{channel_id}");
    // reason_code carries the fire-slot identity so the outbox dedupe key is
    // per-slot: a crashed node re-firing the same slot is suppressed, while
    // the next recurrence (different slot) passes.
    let reason_code = format!(
        "scheduled_message:v1:{}:{}",
        message.id,
        fire.fire_scheduled_at.timestamp()
    );
    match enqueue_outbox_pg_returning_id_with_ttl(
        pool,
        OutboxMessage {
            target: &target,
            content: &message.content,
            bot: &message.bot,
            source: OUTBOX_SOURCE,
            reason_code: Some(&reason_code),
            session_key: None,
        },
        OUTBOX_DEDUPE_TTL_SECS,
    )
    .await
    {
        // Ok(None) = dedupe suppressed: an earlier attempt for this slot
        // already handed off — treat as sent (at-most-once wins).
        Ok(outbox_id) => {
            finish_and_finalize(
                pool,
                fire,
                db::DELIVERY_SENT,
                None,
                outbox_id,
                None,
                true,
                now,
            )
            .await;
        }
        Err(error) => {
            let error = format!("outbox enqueue failed: {error}");
            tracing::warn!(id = message.id, "[smsg] {error}");
            interrupt_for_retry(pool, fire, &error).await;
        }
    }
}

async fn fire_agent(pool: &PgPool, health_registry: Option<&HealthRegistry>, fire: &ClaimedFire) {
    let message = &fire.message;
    match start_agent_turn(pool, health_registry, message).await {
        Ok(turn_id) => {
            if let Err(error) =
                db::mark_delivery_agent_turn_started_pg(pool, &fire.delivery_id, &turn_id).await
            {
                tracing::warn!(
                    id = message.id,
                    turn_id,
                    "[smsg] failed to record started turn: {error}"
                );
            }
            // Delivery stays running; poll_agent_delivery owns completion.
        }
        Err(error) => {
            tracing::warn!(id = message.id, "[smsg] agent turn start failed: {error}");
            interrupt_for_retry(pool, fire, &format!("agent turn start failed: {error}")).await;
        }
    }
}

/// Start a headless agent turn whose relayed reply delivers the message.
/// Mirrors `RoutineAgentExecutor::start_turn` minus routine-thread routing:
/// the turn runs directly in the target channel (or the agent's primary
/// channel when no target was pinned).
async fn start_agent_turn(
    pool: &PgPool,
    health_registry: Option<&HealthRegistry>,
    message: &ScheduledMessageRow,
) -> anyhow::Result<String> {
    use anyhow::anyhow;

    let registry =
        health_registry.ok_or_else(|| anyhow!("discord runtime health registry unavailable"))?;
    let agent_id = message
        .agent_id
        .as_deref()
        .ok_or_else(|| anyhow!("agent delivery requires agent_id"))?;
    let bindings = crate::db::agents::load_agent_channel_bindings_pg(pool, agent_id)
        .await
        .map_err(|error| anyhow!("load agent bindings for {agent_id}: {error}"))?
        .ok_or_else(|| anyhow!("agent {agent_id} not found"))?;
    let provider = bindings
        .resolved_primary_provider_kind()
        .ok_or_else(|| anyhow!("agent {agent_id} primary provider is not configured"))?;
    let primary_channel = bindings
        .primary_channel()
        .ok_or_else(|| anyhow!("agent {agent_id} primary channel is not configured"))?;
    let resolve_channel = |value: &str| {
        crate::services::dispatches::outbox_route::resolve_channel_alias_pub(value)
            .or_else(|| value.parse::<u64>().ok())
    };
    let owner_channel_num = resolve_channel(&primary_channel)
        .ok_or_else(|| anyhow!("agent {agent_id} primary channel is invalid: {primary_channel}"))?;
    let turn_channel_num = match message.target_channel_id.as_deref() {
        Some(target) => {
            resolve_channel(target).ok_or_else(|| anyhow!("target channel is invalid: {target}"))?
        }
        None => owner_channel_num,
    };
    let owner_channel = poise::serenity_prelude::ChannelId::new(owner_channel_num);
    let turn_channel = poise::serenity_prelude::ChannelId::new(turn_channel_num);

    let prompt = build_agent_prompt(message);
    let reservation = reserve_headless_agent_turn(turn_channel);
    let turn_id = reservation.turn_id().to_string();
    let metadata = Some(serde_json::json!({
        "agent_id": agent_id,
        "scheduled_message_id": message.id,
        "turn_id": turn_id,
        "target_channel_id": message.target_channel_id,
        "parent_channel_id": owner_channel_num.to_string(),
    }));

    let outcome = start_reserved_headless_agent_turn_with_owner_channel(
        registry,
        owner_channel,
        turn_channel,
        provider,
        prompt,
        Some(OUTBOX_SOURCE.to_string()),
        metadata,
        Some(primary_channel.clone()),
        None,
        reservation,
    )
    .await
    .map_err(|error| anyhow!("start scheduled message turn for {agent_id}: {error}"))?;

    if outcome.turn_id != turn_id {
        return Err(anyhow!(
            "reserved scheduled message turn id mismatch: expected {turn_id} but started {}",
            outcome.turn_id
        ));
    }
    Ok(turn_id)
}

pub(crate) fn build_agent_prompt(message: &ScheduledMessageRow) -> String {
    let mut prompt = String::new();
    prompt.push_str(
        "예약 메시지 전달 요청입니다. 아래 예약 메시지를 이 채널의 독자에게 전달하세요. \
         당신의 답변이 그대로 채널에 게시됩니다.\n\n",
    );
    if let Some(instruction) = message
        .agent_instruction
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        prompt.push_str("[전달 지침]\n");
        prompt.push_str(instruction.trim());
        prompt.push_str("\n\n");
    }
    if let Some(title) = message.title.as_deref().filter(|v| !v.trim().is_empty()) {
        prompt.push_str("[제목]\n");
        prompt.push_str(title.trim());
        prompt.push_str("\n\n");
    }
    prompt.push_str("[예약 메시지 원문]\n");
    prompt.push_str(&message.content);
    prompt
}

// ── Agent-turn completion polling ───────────────────────────────────────────

/// Check one running agent delivery for completion evidence. Returns true
/// when the delivery transitioned.
async fn poll_agent_delivery(pool: &PgPool, delivery: RunningAgentDelivery) -> bool {
    let Some(turn_id) = delivery.turn_id.as_deref() else {
        // Crash window between arming and recording the turn id: no turn to
        // watch, so rewind for the bounded re-arm path.
        interrupt_delivery(
            pool,
            &delivery.delivery_id,
            &delivery.scheduled_message_id,
            delivery.scheduled_at,
            "agent delivery has no recorded turn",
        )
        .await;
        return true;
    };

    match find_turn_delivery_evidence(pool, turn_id, delivery.started_at).await {
        Ok(Some(TurnEvidence::Delivered)) => {
            finalize_agent_delivery(pool, &delivery, db::DELIVERY_SENT, None, None, true).await;
            true
        }
        Ok(Some(TurnEvidence::TerminalFailure(reason))) => {
            apply_agent_failure(pool, &delivery, &reason).await;
            true
        }
        Ok(None) => {
            let deadline =
                delivery.started_at + chrono::Duration::seconds(AGENT_COMPLETION_TIMEOUT_SECS);
            if Utc::now() >= deadline {
                let reason = format!(
                    "agent turn produced no transcript within {AGENT_COMPLETION_TIMEOUT_SECS}s"
                );
                apply_agent_failure(pool, &delivery, &reason).await;
                return true;
            }
            false
        }
        Err(error) => {
            tracing::warn!(
                delivery_id = delivery.delivery_id,
                "[smsg] completion evidence lookup failed: {error}"
            );
            false
        }
    }
}

enum TurnEvidence {
    Delivered,
    TerminalFailure(String),
}

/// Transcript-based completion evidence, same sources as
/// `RoutineAgentExecutor::find_turn_completion`: a non-empty assistant
/// transcript proves relay delivery; an `empty_response` terminal quality
/// event proves the turn died without output.
async fn find_turn_delivery_evidence(
    pool: &PgPool,
    turn_id: &str,
    started_at: DateTime<Utc>,
) -> Result<Option<TurnEvidence>, sqlx::Error> {
    let delivered: Option<i32> = sqlx::query_scalar(
        "SELECT 1
         FROM session_transcripts
         WHERE turn_id = $1
           AND created_at >= $2
           AND BTRIM(assistant_message) <> ''
         LIMIT 1",
    )
    .bind(turn_id)
    .bind(started_at)
    .fetch_optional(pool)
    .await?;
    if delivered.is_some() {
        return Ok(Some(TurnEvidence::Delivered));
    }

    let terminal: Option<String> = sqlx::query_scalar(
        "SELECT event_type::text
         FROM agent_quality_event
         WHERE correlation_id = $1
           AND source_event_id = $1
           AND created_at >= $2
           AND event_type = 'turn_error'::agent_quality_event_type
           AND payload #>> '{details,outcome}' = 'empty_response'
         LIMIT 1",
    )
    .bind(turn_id)
    .bind(started_at)
    .fetch_optional(pool)
    .await?;
    Ok(terminal.map(|_| {
        TurnEvidence::TerminalFailure("agent turn ended with an empty response".to_string())
    }))
}

/// Terminal agent failure: honor on_agent_failure. `push_raw` demotes the
/// original content to a direct outbox push so must-deliver announcements
/// still go out; `fail` records the failure.
async fn apply_agent_failure(pool: &PgPool, delivery: &RunningAgentDelivery, reason: &str) {
    if delivery.on_agent_failure == "push_raw" {
        // Prefer the pinned target channel; otherwise the agent-target form
        // ("agent:<id>") lands the raw content on the agent's own channel —
        // the same place the failed turn would have replied.
        let target = match (
            delivery.target_channel_id.as_deref(),
            delivery.agent_id.as_deref(),
        ) {
            (Some(channel_id), _) => Some(format!("channel:{channel_id}")),
            (None, Some(agent_id)) => Some(format!("agent:{agent_id}")),
            (None, None) => None,
        };
        if let Some(target) = target {
            let reason_code = format!(
                "scheduled_message:v1:{}:fallback:{}",
                delivery.scheduled_message_id,
                delivery.scheduled_at.timestamp()
            );
            match enqueue_outbox_pg_returning_id_with_ttl(
                pool,
                OutboxMessage {
                    target: &target,
                    content: &delivery.content,
                    bot: &delivery.bot,
                    source: OUTBOX_SOURCE,
                    reason_code: Some(&reason_code),
                    session_key: None,
                },
                OUTBOX_DEDUPE_TTL_SECS,
            )
            .await
            {
                Ok(fallback_outbox_id) => {
                    let error = format!("{reason}; fell back to raw push");
                    finalize_agent_delivery(
                        pool,
                        delivery,
                        db::DELIVERY_SENT,
                        Some(&error),
                        fallback_outbox_id,
                        true,
                    )
                    .await;
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        delivery_id = delivery.delivery_id,
                        "[smsg] push_raw fallback enqueue failed: {error}"
                    );
                }
            }
        }
    }
    finalize_agent_delivery(
        pool,
        delivery,
        db::DELIVERY_FAILED,
        Some(reason),
        None,
        false,
    )
    .await;
}

// ── Shared transitions ──────────────────────────────────────────────────────

/// Recurrence: a live future slot (manual trigger-now case) resumes as-is;
/// otherwise the next occurrence comes from the routine schedule grammar.
/// Recurrences past expires_at end the definition as `expired`.
fn compute_resume(
    message_schedule: Option<&str>,
    timezone: &str,
    current_scheduled_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> (Option<DateTime<Utc>>, Option<&'static str>) {
    let Some(schedule) = message_schedule.filter(|value| !value.trim().is_empty()) else {
        return (None, None);
    };
    let next = if current_scheduled_at > now {
        current_scheduled_at
    } else {
        match crate::services::routines::next_due_after(schedule, timezone, now) {
            Ok(next) => next,
            Err(error) => {
                tracing::warn!("[smsg] recurrence computation failed: {error}");
                return (None, Some(db::STATUS_FAILED));
            }
        }
    };
    if let Some(expires_at) = expires_at {
        if next >= expires_at {
            return (None, Some(db::STATUS_EXPIRED));
        }
    }
    (Some(next), None)
}

#[allow(clippy::too_many_arguments)]
async fn finish_and_finalize(
    pool: &PgPool,
    fire: &ClaimedFire,
    delivery_status: &str,
    error: Option<&str>,
    outbox_id: Option<i64>,
    fallback_outbox_id: Option<i64>,
    fired: bool,
    now: DateTime<Utc>,
) {
    let message = &fire.message;
    if let Err(db_error) = db::finish_delivery_pg(
        pool,
        &fire.delivery_id,
        delivery_status,
        error,
        outbox_id,
        fallback_outbox_id,
    )
    .await
    {
        tracing::warn!(id = message.id, "[smsg] delivery finish failed: {db_error}");
        return;
    }
    let (next, forced_terminal) = compute_resume(
        message.schedule.as_deref(),
        &message.timezone,
        message.scheduled_at,
        message.expires_at,
        now,
    );
    let terminal_status = forced_terminal.unwrap_or(if delivery_status == db::DELIVERY_SENT {
        db::STATUS_SENT
    } else {
        db::STATUS_FAILED
    });
    let next = forced_terminal.is_none().then_some(next).flatten();
    if let Err(db_error) = db::finalize_parent_pg(
        pool,
        &message.id,
        &fire.delivery_id,
        fired,
        terminal_status,
        error,
        next,
    )
    .await
    {
        tracing::warn!(id = message.id, "[smsg] parent finalize failed: {db_error}");
    }
}

async fn finalize_agent_delivery(
    pool: &PgPool,
    delivery: &RunningAgentDelivery,
    delivery_status: &str,
    error: Option<&str>,
    fallback_outbox_id: Option<i64>,
    fired: bool,
) {
    if let Err(db_error) = db::finish_delivery_pg(
        pool,
        &delivery.delivery_id,
        delivery_status,
        error,
        None,
        fallback_outbox_id,
    )
    .await
    {
        tracing::warn!(
            delivery_id = delivery.delivery_id,
            "[smsg] delivery finish failed: {db_error}"
        );
        return;
    }
    let now = Utc::now();
    let (next, forced_terminal) = compute_resume(
        delivery.schedule.as_deref(),
        &delivery.timezone,
        delivery.scheduled_at,
        delivery.expires_at,
        now,
    );
    let terminal_status = forced_terminal.unwrap_or(if delivery_status == db::DELIVERY_SENT {
        db::STATUS_SENT
    } else {
        db::STATUS_FAILED
    });
    let next = forced_terminal.is_none().then_some(next).flatten();
    if let Err(db_error) = db::finalize_parent_pg(
        pool,
        &delivery.scheduled_message_id,
        &delivery.delivery_id,
        fired,
        terminal_status,
        error,
        next,
    )
    .await
    {
        tracing::warn!(
            id = delivery.scheduled_message_id,
            "[smsg] parent finalize failed: {db_error}"
        );
    }
}

/// Transient failure: mark the delivery interrupted and rewind the parent to
/// its fire slot so the due scan re-arms it (bounded by MAX_FIRE_RETRIES).
async fn interrupt_for_retry(pool: &PgPool, fire: &ClaimedFire, error: &str) {
    interrupt_delivery(
        pool,
        &fire.delivery_id,
        &fire.message.id,
        fire.fire_scheduled_at,
        error,
    )
    .await;
}

async fn interrupt_delivery(
    pool: &PgPool,
    delivery_id: &str,
    message_id: &str,
    fire_scheduled_at: DateTime<Utc>,
    error: &str,
) {
    if let Err(db_error) = db::finish_delivery_pg(
        pool,
        delivery_id,
        db::DELIVERY_INTERRUPTED,
        Some(error),
        None,
        None,
    )
    .await
    {
        tracing::warn!(
            id = message_id,
            "[smsg] interrupt transition failed: {db_error}"
        );
        return;
    }
    if let Err(db_error) = sqlx::query(
        "UPDATE scheduled_messages
         SET status = 'scheduled', scheduled_at = $3,
             in_flight_delivery_id = NULL, last_error = $4, updated_at = NOW()
         WHERE id = $1 AND in_flight_delivery_id = $2 AND status = 'firing'",
    )
    .bind(message_id)
    .bind(delivery_id)
    .bind(fire_scheduled_at)
    .bind(error)
    .execute(pool)
    .await
    {
        tracing::warn!(id = message_id, "[smsg] parent rewind failed: {db_error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn message_with(
        schedule: Option<&str>,
        scheduled_at: DateTime<Utc>,
        expires_at: Option<DateTime<Utc>>,
    ) -> (Option<String>, String, DateTime<Utc>, Option<DateTime<Utc>>) {
        (
            schedule.map(str::to_string),
            "Asia/Seoul".to_string(),
            scheduled_at,
            expires_at,
        )
    }

    #[test]
    fn one_shot_has_no_resume() {
        let now = Utc.with_ymd_and_hms(2026, 7, 8, 0, 0, 0).unwrap();
        let (schedule, tz, at, exp) = message_with(None, now, None);
        let (next, terminal) = compute_resume(schedule.as_deref(), &tz, at, exp, now);
        assert_eq!(next, None);
        assert_eq!(terminal, None);
    }

    #[test]
    fn recurring_advances_past_now() {
        let now = Utc.with_ymd_and_hms(2026, 7, 8, 0, 0, 0).unwrap();
        let (schedule, tz, at, exp) = message_with(Some("@every 10m"), now, None);
        let (next, terminal) = compute_resume(schedule.as_deref(), &tz, at, exp, now);
        assert_eq!(terminal, None);
        assert!(next.expect("next slot") > now);
    }

    #[test]
    fn manual_fire_resumes_original_future_slot() {
        let now = Utc.with_ymd_and_hms(2026, 7, 8, 0, 0, 0).unwrap();
        let original = Utc.with_ymd_and_hms(2026, 7, 9, 9, 0, 0).unwrap();
        let (schedule, tz, at, exp) = message_with(Some("@every 24h"), original, None);
        let (next, terminal) = compute_resume(schedule.as_deref(), &tz, at, exp, now);
        assert_eq!(terminal, None);
        assert_eq!(next, Some(original));
    }

    #[test]
    fn recurrence_past_expiry_ends_expired() {
        let now = Utc.with_ymd_and_hms(2026, 7, 8, 0, 0, 0).unwrap();
        let expires = Utc.with_ymd_and_hms(2026, 7, 8, 0, 5, 0).unwrap();
        let (schedule, tz, at, exp) = message_with(Some("@every 10m"), now, Some(expires));
        let (next, terminal) = compute_resume(schedule.as_deref(), &tz, at, exp, now);
        assert_eq!(next, None);
        assert_eq!(terminal, Some(db::STATUS_EXPIRED));
    }

    #[test]
    fn invalid_schedule_fails_terminal() {
        let now = Utc.with_ymd_and_hms(2026, 7, 8, 0, 0, 0).unwrap();
        let (schedule, tz, at, exp) = message_with(Some("not a schedule"), now, None);
        let (next, terminal) = compute_resume(schedule.as_deref(), &tz, at, exp, now);
        assert_eq!(next, None);
        assert_eq!(terminal, Some(db::STATUS_FAILED));
    }

    #[test]
    fn agent_prompt_includes_instruction_and_content() {
        let message = ScheduledMessageRow {
            id: "smsg_test".to_string(),
            content: "내일 배포 예정".to_string(),
            title: Some("배포 공지".to_string()),
            target_channel_id: Some("123".to_string()),
            bot: "announce".to_string(),
            delivery_kind: "agent".to_string(),
            agent_id: Some("coder".to_string()),
            agent_instruction: Some("3줄로 요약".to_string()),
            on_agent_failure: "fail".to_string(),
            scheduled_at: Utc::now(),
            schedule: None,
            timezone: "Asia/Seoul".to_string(),
            expires_at: None,
            status: "firing".to_string(),
            in_flight_delivery_id: None,
            fire_count: 0,
            last_fired_at: None,
            last_error: None,
            source: "api".to_string(),
            created_by: None,
            dedupe_key: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let prompt = build_agent_prompt(&message);
        assert!(prompt.contains("3줄로 요약"));
        assert!(prompt.contains("내일 배포 예정"));
        assert!(prompt.contains("배포 공지"));
    }
}
