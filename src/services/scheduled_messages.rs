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
use crate::services::message_outbox::{
    OutboxEnqueueError, OutboxMessage, enqueue_outbox_tx_returning_id_with_persistent_dedupe,
};

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
    match db::claim_due_fires_pg(
        pool,
        claim_owner,
        health_registry.is_some(),
        CLAIM_BATCH,
        LEASE_SECS,
        now,
    )
    .await
    {
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

    // Compare against the claim time, not the fire slot: a worker that wakes
    // up late must not deliver a message whose expiry has already passed.
    if let Some(expires_at) = message.expires_at {
        if expires_at <= now {
            if let Err(error) =
                db::mark_expired_pg(pool, &message.id, &fire.delivery_id, &fire.claim_token).await
            {
                tracing::warn!(id = message.id, "[smsg] expire transition failed: {error}");
            }
            return;
        }
    }

    if fire.retry_count > MAX_FIRE_RETRIES {
        let error = format!("fire retry budget exhausted after {MAX_FIRE_RETRIES} re-arms");
        if message.delivery_kind == db::KIND_AGENT && message.on_agent_failure == "push_raw" {
            finish_exhausted_agent_with_raw_fallback(pool, &fire, &error).await;
        } else {
            finish_terminal_failure(pool, &fire, &error).await;
        }
        return;
    }

    match message.delivery_kind.as_str() {
        db::KIND_PUSH => fire_push(pool, &fire, now).await,
        db::KIND_AGENT => fire_agent(pool, health_registry, &fire).await,
        other => {
            let error = format!("unknown delivery_kind '{other}'");
            finish_terminal_failure(pool, &fire, &error).await;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimHandoff {
    Enqueued(i64),
    ClaimLost,
}

async fn enqueue_outbox_for_active_claim(
    pool: &PgPool,
    message_id: &str,
    delivery_id: &str,
    claim_token: &str,
    message: OutboxMessage<'_>,
    fallback: bool,
) -> Result<ClaimHandoff, OutboxEnqueueError> {
    let mut tx = pool.begin().await?;
    if !db::lock_active_delivery_claim_tx(&mut tx, message_id, delivery_id, claim_token).await? {
        tx.rollback().await?;
        return Ok(ClaimHandoff::ClaimLost);
    }

    let outbox_id = enqueue_outbox_tx_returning_id_with_persistent_dedupe(&mut tx, message).await?;
    if !db::record_delivery_outbox_handoff_tx(
        &mut tx,
        delivery_id,
        claim_token,
        outbox_id,
        fallback,
    )
    .await?
    {
        tx.rollback().await?;
        return Ok(ClaimHandoff::ClaimLost);
    }
    tx.commit().await?;
    Ok(ClaimHandoff::Enqueued(outbox_id))
}

async fn fire_push(pool: &PgPool, fire: &ClaimedFire, now: DateTime<Utc>) {
    let message = &fire.message;
    let Some(channel_id) = message.target_channel_id.as_deref() else {
        // chk_smsg_push_target_required makes this unreachable; degrade safely.
        finish_terminal_failure(pool, fire, "push delivery has no target channel").await;
        return;
    };
    let target = format!("channel:{channel_id}");
    // reason_code carries the fire-slot identity so the outbox dedupe key is
    // per-slot: a crashed node re-firing the same slot is suppressed, while
    // the next recurrence (different slot) passes.
    let reason_code = format!(
        "scheduled_message:v1:{}:{}",
        message.id,
        fire.fire_scheduled_at.timestamp_micros()
    );
    match enqueue_outbox_for_active_claim(
        pool,
        &message.id,
        &fire.delivery_id,
        &fire.claim_token,
        OutboxMessage {
            target: &target,
            content: &message.content,
            bot: &message.bot,
            source: OUTBOX_SOURCE,
            reason_code: Some(&reason_code),
            session_key: None,
        },
        false,
    )
    .await
    {
        Ok(ClaimHandoff::Enqueued(outbox_id)) => {
            finish_and_finalize(
                pool,
                fire,
                db::DELIVERY_SENT,
                None,
                Some(outbox_id),
                None,
                true,
                now,
            )
            .await;
        }
        Ok(ClaimHandoff::ClaimLost) => {
            tracing::info!(
                id = message.id,
                delivery_id = fire.delivery_id,
                "[smsg] push handoff skipped after claim cancellation"
            );
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
    let Some(health_registry) = health_registry else {
        let reason = "discord runtime health registry unavailable";
        if let Err(error) = db::defer_delivery_without_retry_pg(
            pool,
            &fire.delivery_id,
            &fire.claim_token,
            &message.id,
            fire.fire_scheduled_at,
            reason,
        )
        .await
        {
            tracing::warn!(
                id = message.id,
                "[smsg] runtime-unavailable defer failed: {error}"
            );
        }
        return;
    };
    match start_agent_turn(pool, health_registry, fire).await {
        Ok(_) => {
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
    health_registry: &HealthRegistry,
    fire: &ClaimedFire,
) -> anyhow::Result<String> {
    use anyhow::anyhow;

    let message = &fire.message;

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
    let recorded = db::mark_delivery_agent_turn_started_pg(
        pool,
        &message.id,
        &fire.delivery_id,
        &fire.claim_token,
        &turn_id,
        LEASE_SECS,
    )
    .await
    .map_err(|error| anyhow!("record scheduled message turn {turn_id}: {error}"))?;
    if !recorded {
        return Err(anyhow!(
            "scheduled message claim was lost before turn {turn_id} could start"
        ));
    }
    let metadata = Some(serde_json::json!({
        "agent_id": agent_id,
        "scheduled_message_id": message.id,
        "turn_id": turn_id,
        "target_channel_id": message.target_channel_id,
        "parent_channel_id": owner_channel_num.to_string(),
    }));

    let outcome = start_reserved_headless_agent_turn_with_owner_channel(
        health_registry,
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
    if outcome.status.as_str() != "started" {
        return Err(anyhow!(
            "scheduled message turn {turn_id} was not started (status: {})",
            outcome.status.as_str()
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
        // Unreachable: the listing query only returns rows with a recorded
        // turn_id. Never interrupt here — a missing turn id means the start
        // call is still in flight (trigger-now fires outside the scheduler
        // tick), and rewinding would race it into a duplicate delivery.
        // Pre-turn crashes are owned by lease expiry recovery.
        return false;
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

#[derive(Debug, PartialEq, Eq)]
enum TurnEvidence {
    Delivered,
    TerminalFailure(String),
}

fn transcript_delivery_evidence(assistant_message: &str) -> TurnEvidence {
    if assistant_message.trim().eq_ignore_ascii_case("NO_REPLY") {
        TurnEvidence::TerminalFailure("agent turn returned NO_REPLY".to_string())
    } else {
        TurnEvidence::Delivered
    }
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
    let delivered: Option<String> = sqlx::query_scalar(
        "SELECT assistant_message
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
    if let Some(assistant_message) = delivered {
        return Ok(Some(transcript_delivery_evidence(&assistant_message)));
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

fn raw_fallback_target(target_channel_id: Option<&str>, agent_id: Option<&str>) -> Option<String> {
    match (target_channel_id, agent_id) {
        (Some(channel_id), _) => Some(format!("channel:{channel_id}")),
        (None, Some(agent_id)) => Some(format!("agent:{agent_id}")),
        (None, None) => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawFallbackHandoff {
    Enqueued(i64),
    NoTarget,
    ClaimLost,
}

async fn enqueue_raw_fallback(
    pool: &PgPool,
    scheduled_message_id: &str,
    delivery_id: &str,
    claim_token: &str,
    fire_scheduled_at: DateTime<Utc>,
    target_channel_id: Option<&str>,
    agent_id: Option<&str>,
    content: &str,
    bot: &str,
) -> Result<RawFallbackHandoff, OutboxEnqueueError> {
    let Some(target) = raw_fallback_target(target_channel_id, agent_id) else {
        return Ok(RawFallbackHandoff::NoTarget);
    };
    let reason_code = format!(
        "scheduled_message:v1:{scheduled_message_id}:fallback:{}",
        fire_scheduled_at.timestamp_micros()
    );
    match enqueue_outbox_for_active_claim(
        pool,
        scheduled_message_id,
        delivery_id,
        claim_token,
        OutboxMessage {
            target: &target,
            content,
            bot,
            source: OUTBOX_SOURCE,
            reason_code: Some(&reason_code),
            session_key: None,
        },
        true,
    )
    .await
    {
        Ok(ClaimHandoff::Enqueued(outbox_id)) => Ok(RawFallbackHandoff::Enqueued(outbox_id)),
        Ok(ClaimHandoff::ClaimLost) => Ok(RawFallbackHandoff::ClaimLost),
        Err(error) => Err(error),
    }
}

/// Terminal agent failure: honor on_agent_failure. `push_raw` demotes the
/// original content to a direct outbox push so must-deliver announcements
/// still go out; `fail` records the failure.
async fn apply_agent_failure(pool: &PgPool, delivery: &RunningAgentDelivery, reason: &str) {
    if delivery
        .expires_at
        .is_some_and(|expires_at| expires_at <= Utc::now())
    {
        if let Err(error) = db::mark_expired_pg(
            pool,
            &delivery.scheduled_message_id,
            &delivery.delivery_id,
            &delivery.claim_token,
        )
        .await
        {
            tracing::warn!(
                delivery_id = delivery.delivery_id,
                "[smsg] expired agent delivery transition failed: {error}"
            );
        }
        return;
    }
    if delivery.on_agent_failure == "push_raw" {
        match enqueue_raw_fallback(
            pool,
            &delivery.scheduled_message_id,
            &delivery.delivery_id,
            &delivery.claim_token,
            delivery.fire_scheduled_at,
            delivery.target_channel_id.as_deref(),
            delivery.agent_id.as_deref(),
            &delivery.content,
            &delivery.bot,
        )
        .await
        {
            Ok(RawFallbackHandoff::Enqueued(fallback_outbox_id)) => {
                let error = format!("{reason}; fell back to raw push");
                finalize_agent_delivery(
                    pool,
                    delivery,
                    db::DELIVERY_SENT,
                    Some(&error),
                    Some(fallback_outbox_id),
                    true,
                )
                .await;
                return;
            }
            Ok(RawFallbackHandoff::ClaimLost) => return,
            Ok(RawFallbackHandoff::NoTarget) => {}
            Err(error) => tracing::warn!(
                delivery_id = delivery.delivery_id,
                "[smsg] push_raw fallback enqueue failed: {error}"
            ),
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

async fn finish_exhausted_agent_with_raw_fallback(pool: &PgPool, fire: &ClaimedFire, reason: &str) {
    let message = &fire.message;
    match enqueue_raw_fallback(
        pool,
        &message.id,
        &fire.delivery_id,
        &fire.claim_token,
        fire.fire_scheduled_at,
        message.target_channel_id.as_deref(),
        message.agent_id.as_deref(),
        &message.content,
        &message.bot,
    )
    .await
    {
        Ok(RawFallbackHandoff::Enqueued(fallback_outbox_id)) => {
            let error = format!("{reason}; fell back to raw push");
            finish_terminal(
                pool,
                fire,
                db::DELIVERY_SENT,
                Some(&error),
                None,
                Some(fallback_outbox_id),
                true,
            )
            .await;
        }
        Ok(RawFallbackHandoff::NoTarget) => {
            finish_terminal_failure(pool, fire, &format!("{reason}; no fallback target")).await;
        }
        Ok(RawFallbackHandoff::ClaimLost) => {}
        Err(error) => {
            finish_terminal_failure(
                pool,
                fire,
                &format!("{reason}; push_raw fallback enqueue failed: {error}"),
            )
            .await;
        }
    }
}

// ── Shared transitions ──────────────────────────────────────────────────────

async fn finish_terminal_failure(pool: &PgPool, fire: &ClaimedFire, error: &str) {
    finish_terminal(
        pool,
        fire,
        db::DELIVERY_FAILED,
        Some(error),
        None,
        None,
        false,
    )
    .await;
}

/// Exhausting a fire slot is terminal for the definition, even when the
/// agent failure policy successfully hands this final attempt to raw push.
/// The delivery can record that fallback as sent, but the recurring parent
/// stays failed and requires an operator decision instead of silently moving
/// on to its next occurrence.
#[allow(clippy::too_many_arguments)]
async fn finish_terminal(
    pool: &PgPool,
    fire: &ClaimedFire,
    delivery_status: &str,
    error: Option<&str>,
    outbox_id: Option<i64>,
    fallback_outbox_id: Option<i64>,
    fired: bool,
) {
    if let Err(db_error) = db::finish_delivery_and_finalize_parent_pg(
        pool,
        &fire.delivery_id,
        &fire.claim_token,
        delivery_status,
        error,
        outbox_id,
        fallback_outbox_id,
        &fire.message.id,
        fired,
        db::STATUS_FAILED,
        None,
    )
    .await
    {
        tracing::warn!(
            id = fire.message.id,
            "[smsg] terminal delivery failure transition failed: {db_error}"
        );
    }
}

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
        match crate::services::routines::next_due_after_anchor(
            schedule,
            timezone,
            current_scheduled_at,
            now,
        ) {
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
    if let Err(db_error) = db::finish_delivery_and_finalize_parent_pg(
        pool,
        &fire.delivery_id,
        &fire.claim_token,
        delivery_status,
        error,
        outbox_id,
        fallback_outbox_id,
        &message.id,
        fired,
        terminal_status,
        next,
    )
    .await
    {
        tracing::warn!(
            id = message.id,
            "[smsg] delivery finalize failed: {db_error}"
        );
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
    if let Err(db_error) = db::finish_delivery_and_finalize_parent_pg(
        pool,
        &delivery.delivery_id,
        &delivery.claim_token,
        delivery_status,
        error,
        None,
        fallback_outbox_id,
        &delivery.scheduled_message_id,
        fired,
        terminal_status,
        next,
    )
    .await
    {
        tracing::warn!(
            id = delivery.scheduled_message_id,
            "[smsg] delivery finalize failed: {db_error}"
        );
    }
}

/// Transient failure: mark the delivery interrupted and rewind the parent to
/// its fire slot so the due scan re-arms it (bounded by MAX_FIRE_RETRIES).
async fn interrupt_for_retry(pool: &PgPool, fire: &ClaimedFire, error: &str) {
    interrupt_delivery(
        pool,
        &fire.delivery_id,
        &fire.claim_token,
        &fire.message.id,
        fire.fire_scheduled_at,
        error,
    )
    .await;
}

async fn interrupt_delivery(
    pool: &PgPool,
    delivery_id: &str,
    claim_token: &str,
    message_id: &str,
    fire_scheduled_at: DateTime<Utc>,
    error: &str,
) {
    if let Err(db_error) = db::interrupt_delivery_and_rewind_pg(
        pool,
        delivery_id,
        claim_token,
        message_id,
        fire_scheduled_at,
        error,
    )
    .await
    {
        tracing::warn!(
            id = message_id,
            "[smsg] interrupt transition failed: {db_error}"
        );
    }
}

#[cfg(test)]
mod postgres_tests;

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
        assert_eq!(
            next,
            Some(Utc.with_ymd_and_hms(2026, 7, 8, 0, 10, 0).unwrap())
        );
    }

    #[test]
    fn recurring_interval_stays_anchored_when_completion_is_late() {
        let slot = Utc.with_ymd_and_hms(2026, 7, 8, 9, 0, 0).unwrap();
        let completed = Utc.with_ymd_and_hms(2026, 7, 8, 9, 5, 0).unwrap();
        let (schedule, tz, at, exp) = message_with(Some("@every 24h"), slot, None);
        let (next, terminal) = compute_resume(schedule.as_deref(), &tz, at, exp, completed);
        assert_eq!(terminal, None);
        assert_eq!(
            next,
            Some(Utc.with_ymd_and_hms(2026, 7, 9, 9, 0, 0).unwrap())
        );
    }

    #[test]
    fn recurring_interval_skips_missed_slots_to_first_future_anchor() {
        let slot = Utc.with_ymd_and_hms(2026, 7, 8, 0, 0, 0).unwrap();
        let completed = Utc.with_ymd_and_hms(2026, 7, 8, 0, 27, 0).unwrap();
        let (schedule, tz, at, exp) = message_with(Some("@every 10m"), slot, None);
        let (next, terminal) = compute_resume(schedule.as_deref(), &tz, at, exp, completed);
        assert_eq!(terminal, None);
        assert_eq!(
            next,
            Some(Utc.with_ymd_and_hms(2026, 7, 8, 0, 30, 0).unwrap())
        );
    }

    #[test]
    fn no_reply_transcripts_are_terminal_failures() {
        for message in ["NO_REPLY", " no_reply ", "No_RePlY\n"] {
            assert_eq!(
                transcript_delivery_evidence(message),
                TurnEvidence::TerminalFailure("agent turn returned NO_REPLY".to_string())
            );
        }
        assert_eq!(
            transcript_delivery_evidence("예약 내용을 전달했습니다."),
            TurnEvidence::Delivered
        );
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
