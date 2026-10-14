use super::*;
use crate::db::turns::PersistTurnOwned;

pub(super) fn pending_delivery_outcome(
    primary_outcome: &'static str,
    signal: BridgeCompletionSignal,
    preserve: bool,
    held_by_other: bool,
) -> &'static str {
    if !matches!(
        primary_outcome,
        "completed" | "tmux_handoff" | "watcher_relay" | "standby_relay"
    ) {
        return primary_outcome;
    }
    match signal {
        BridgeCompletionSignal::DeferredToCustody | BridgeCompletionSignal::DeferredToOwner => {
            "delivery_pending"
        }
        _ if preserve && held_by_other => "delivery_pending",
        BridgeCompletionSignal::Unresolved if preserve => "delivery_unresolved",
        _ => primary_outcome,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_turn_quality_event(
    provider: &ProviderKind,
    channel_id: ChannelId,
    dispatch_id: Option<&str>,
    session_key: Option<&str>,
    turn_id: &str,
    role_binding: Option<&RoleBinding>,
    event_type: &str,
    payload: serde_json::Value,
) {
    crate::services::observability::emit_agent_quality_event(
        crate::services::observability::AgentQualityEvent {
            source_event_id: Some(turn_id.to_string()),
            correlation_id: dispatch_id
                .map(str::to_string)
                .or_else(|| Some(turn_id.to_string())),
            agent_id: role_binding.map(|binding| binding.role_id.clone()),
            provider: Some(provider.as_str().to_string()),
            channel_id: Some(channel_id.get().to_string()),
            card_id: None,
            dispatch_id: dispatch_id.map(str::to_string),
            event_type: event_type.to_string(),
            payload: serde_json::json!({
                "turn_id": turn_id,
                "session_key": session_key,
                "details": payload,
            }),
        },
    );
}

pub(super) fn turn_duration_ms(started_at: std::time::Instant) -> i64 {
    i64::try_from(started_at.elapsed().as_millis()).unwrap_or(i64::MAX)
}

fn analytics_started_at_utc<Tz: chrono::TimeZone>(
    value: &str,
    legacy_timezone: &Tz,
) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Ok(value) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(value.with_timezone(&chrono::Utc));
    }
    // Inflight state still uses its existing host-local clock encoding for
    // lease identity and recovery. Resolve that encoding at its producer,
    // rather than letting the database connection supply a different zone.
    let value = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").ok()?;
    legacy_timezone
        .from_local_datetime(&value)
        .single()
        .map(|value| value.with_timezone(&chrono::Utc))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn record_turn_bridge_invariant(
    condition: bool,
    provider: &ProviderKind,
    channel_id: ChannelId,
    dispatch_id: Option<&str>,
    session_key: Option<&str>,
    turn_id: Option<&str>,
    invariant: &'static str,
    code_location: &'static str,
    message: &'static str,
    details: serde_json::Value,
) -> bool {
    crate::services::observability::record_invariant_check(
        condition,
        crate::services::observability::InvariantViolation {
            provider: Some(provider.as_str()),
            channel_id: Some(channel_id.get()),
            dispatch_id,
            session_key,
            turn_id,
            invariant,
            code_location,
            message,
            details,
        },
    )
}

pub(super) fn discord_turn_id(
    provider: &ProviderKind,
    channel_id: ChannelId,
    user_msg_id: Option<MessageId>,
    session_key: Option<&str>,
    // PER-TURN discriminator for the message-less fallback (user_msg_id == 0):
    // the turn's JSONL start offset, distinct per turn within a session. Keying
    // only on `session_key` (stable across the whole tmux/Discord session) would
    // make repeated message-less turns upsert-overwrite each other's transcript
    // rows (Codex P2). Ignored when a real `user_msg_id` is present.
    turn_start_offset: Option<u64>,
) -> String {
    // A recovery turn with no anchored Discord user message (user_msg_id == 0,
    // e.g. a TUI-direct turn) cannot key its turn id on a message id —
    // `discord:<channel>:0` is the bogus form the invariant guards against.
    // Fall back to the session key plus a per-turn discriminator, and skip the
    // non-zero-message-id invariant, which does not apply here.
    let Some(user_msg_id) = user_msg_id else {
        let turn_discriminator = match turn_start_offset {
            Some(offset) => format!("off{offset}"),
            None => "recovery".to_string(),
        };
        return match session_key {
            Some(key) => format!(
                "discord:{}:session:{key}:{turn_discriminator}",
                channel_id.get()
            ),
            None => format!("discord:{}:recovery:{turn_discriminator}", channel_id.get()),
        };
    };
    let turn_id = format!("discord:{}:{}", channel_id.get(), user_msg_id.get());
    let nonzero_components = channel_id.get() != 0 && user_msg_id.get() != 0;
    record_turn_bridge_invariant(
        nonzero_components,
        provider,
        channel_id,
        None,
        session_key,
        Some(turn_id.as_str()),
        "turn_id_unique_within_session",
        "src/services/discord/turn_bridge/turn_analytics.rs:discord_turn_id",
        "turn_id must be built from non-zero Discord channel/message ids",
        serde_json::json!({
            "channel_id": channel_id.get(),
            "user_msg_id": user_msg_id.get(),
            "turn_id": turn_id.as_str(),
        }),
    );
    debug_assert!(
        nonzero_components,
        "turn_id requires non-zero Discord channel/message ids"
    );
    turn_id
}

#[allow(clippy::too_many_arguments)]
pub(super) fn assert_response_sent_offset_progress(
    provider: &ProviderKind,
    channel_id: ChannelId,
    dispatch_id: Option<&str>,
    session_key: Option<&str>,
    turn_id: &str,
    previous: usize,
    next: usize,
    full_response: &str,
    code_location: &'static str,
) {
    let monotonic = next >= previous;
    record_turn_bridge_invariant(
        monotonic,
        provider,
        channel_id,
        dispatch_id,
        session_key,
        Some(turn_id),
        "response_sent_offset_monotonic",
        code_location,
        "turn_bridge response_sent_offset must not move backwards",
        serde_json::json!({
            "previous": previous,
            "next": next,
            "full_response_len": full_response.len(),
        }),
    );
    debug_assert!(
        monotonic,
        "turn_bridge response_sent_offset must not move backwards"
    );

    let in_bounds = next <= full_response.len() && full_response.is_char_boundary(next);
    record_turn_bridge_invariant(
        in_bounds,
        provider,
        channel_id,
        dispatch_id,
        session_key,
        Some(turn_id),
        "response_sent_offset_in_bounds",
        code_location,
        "turn_bridge response_sent_offset must stay on a full_response boundary",
        serde_json::json!({
            "next": next,
            "full_response_len": full_response.len(),
        }),
    );
    debug_assert!(
        in_bounds,
        "turn_bridge response_sent_offset must stay on a full_response boundary"
    );
}

pub(super) fn total_model_input_tokens(
    input_tokens: u64,
    cache_create_tokens: u64,
    cache_read_tokens: u64,
) -> u64 {
    input_tokens
        .saturating_add(cache_create_tokens)
        .saturating_add(cache_read_tokens)
}

struct TurnAnalyticsSnapshot {
    output_path: Option<String>,
    output_start_offset: u64,
    output_end_offset: Option<u64>,
    fallback_session_id: Option<String>,
    fallback_token_usage: TurnTokenUsage,
    inflight_session_id: Option<String>,
}

impl TurnAnalyticsSnapshot {
    fn capture(
        inflight_state: &InflightTurnState,
        fallback_session_id: Option<&str>,
        fallback_token_usage: TurnTokenUsage,
    ) -> Self {
        Self {
            output_path: inflight_state.output_path.clone(),
            output_start_offset: inflight_state
                .turn_start_offset
                .unwrap_or(inflight_state.last_offset),
            output_end_offset: inflight_state
                .output_path
                .as_ref()
                .map(|_| inflight_state.last_offset),
            fallback_session_id: fallback_session_id.map(str::to_string),
            fallback_token_usage,
            inflight_session_id: inflight_state.session_id.clone(),
        }
    }
}

fn resolve_output_analytics_snapshot(
    snapshot: &TurnAnalyticsSnapshot,
) -> (Option<String>, TurnTokenUsage) {
    let (output_session_id, output_token_usage) = snapshot
        .output_path
        .as_deref()
        .map(|path| {
            crate::services::session_backend::extract_turn_analytics_from_output_range(
                path,
                snapshot.output_start_offset,
                snapshot.output_end_offset,
            )
        })
        .unwrap_or((None, None));

    (
        output_session_id
            .or_else(|| snapshot.fallback_session_id.clone())
            .or_else(|| snapshot.inflight_session_id.clone()),
        output_token_usage.unwrap_or(snapshot.fallback_token_usage),
    )
}

/// #2849: resolve the EXACT final context-occupancy token usage for the
/// completed status panel, or `None` if no exact usage exists anywhere.
///
/// Prefers the live accumulated snapshot (occupancy > 0). If the live
/// `StatusUpdate`s never carried token data — common for silent/background
/// turns — it re-parses the output JSONL range exactly as persisted analytics
/// does via [`resolve_output_analytics_snapshot`]. Returns `None` when neither
/// source yields non-zero occupancy, so callers never fabricate numbers and
/// never reuse stale prior-turn usage.
pub(super) fn resolve_exact_completion_usage(
    inflight_state: &InflightTurnState,
    fallback_session_id: Option<&str>,
    accumulated: TurnTokenUsage,
) -> Option<TurnTokenUsage> {
    if accumulated.context_occupancy_input_tokens() > 0 {
        return Some(accumulated);
    }
    let snapshot = TurnAnalyticsSnapshot::capture(inflight_state, fallback_session_id, accumulated);
    let (_session_id, usage) = resolve_output_analytics_snapshot(&snapshot);
    (usage.context_occupancy_input_tokens() > 0).then_some(usage)
}

#[allow(clippy::too_many_arguments)]
pub(in crate::services::discord) fn persist_turn_analytics_row_with_handles(
    pg_pool: Option<&sqlx::PgPool>,
    provider: &ProviderKind,
    channel_id: ChannelId,
    user_msg_id: MessageId,
    role_binding: Option<&RoleBinding>,
    dispatch_id: Option<&str>,
    session_key: Option<&str>,
    session_id: Option<&str>,
    inflight_state: &InflightTurnState,
    token_usage: TurnTokenUsage,
    duration_ms: i64,
) {
    let thread_id = inflight_state
        .thread_id
        .map(|value| value.to_string())
        .or_else(|| {
            inflight_state
                .channel_name
                .as_deref()
                .and_then(crate::services::discord::adk_session::parse_thread_channel_id_from_name)
                .map(|value| value.to_string())
        });
    let turn_id = discord_turn_id(provider, channel_id, Some(user_msg_id), session_key, None);
    let session_key = session_key.map(str::to_string);
    let thread_title = inflight_state.thread_title.clone();
    let persisted_channel_id = inflight_state
        .logical_channel_id
        .unwrap_or(channel_id.get())
        .to_string();
    let agent_id = role_binding.map(|binding| binding.role_id.clone());
    let provider_name = provider.as_str().to_string();
    let dispatch_id = dispatch_id
        .map(str::to_string)
        .or_else(|| inflight_state.dispatch_id.clone());
    let Some(started_at) = analytics_started_at_utc(&inflight_state.started_at, &chrono::Local)
    else {
        tracing::warn!(%turn_id, "turn analytics start instant is invalid or ambiguous; row not persisted");
        return;
    };
    let analytics_snapshot =
        TurnAnalyticsSnapshot::capture(inflight_state, session_id, token_usage);
    let (resolved_session_id, resolved_token_usage) =
        resolve_output_analytics_snapshot(&analytics_snapshot);
    let entry = PersistTurnOwned {
        turn_id,
        session_key,
        thread_id,
        thread_title,
        channel_id: persisted_channel_id,
        agent_id,
        provider: Some(provider_name),
        session_id: resolved_session_id,
        dispatch_id,
        started_at: Some(started_at),
        finished_at: Some(chrono::Utc::now()),
        duration_ms: Some(duration_ms),
        token_usage: resolved_token_usage,
    };
    let pg_pool = pg_pool.cloned();
    let persist_pg = move |pg_pool: sqlx::PgPool, entry: PersistTurnOwned| async move {
        if let Err(error) = crate::db::turns::upsert_turn_owned_db(Some(&pg_pool), &entry).await {
            let ts = chrono::Local::now().format("%H:%M:%S");
            tracing::warn!("  [{ts}] ⚠ failed to persist turn analytics row: {error}");
        }
    };

    let Some(pg_pool) = pg_pool else {
        return;
    };
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        let _ = runtime.spawn(persist_pg(pg_pool, entry));
        return;
    }
    match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => {
            runtime.block_on(persist_pg(pg_pool, entry));
        }
        Err(error) => {
            let ts = chrono::Local::now().format("%H:%M:%S");
            tracing::warn!(
                "  [{ts}] ⚠ failed to create runtime for turn analytics persistence: {error}"
            );
        }
    }
}

#[cfg(test)]
mod timestamp_tests {
    use super::{
        ChannelId, InflightTurnState, MessageId, ProviderKind, analytics_started_at_utc,
        discord_turn_id, persist_turn_analytics_row_with_handles,
    };
    use chrono::{DateTime, FixedOffset, Utc};

    #[test]
    fn analytics_start_preserves_explicit_offsets_and_legacy_host_local_instants() {
        let legacy_timezone = FixedOffset::east_opt(9 * 3600).unwrap();
        let expected = DateTime::parse_from_rfc3339("2026-10-02T19:55:40Z")
            .unwrap()
            .with_timezone(&Utc);
        for value in [
            "2026-10-02T19:55:40Z",
            "2026-10-03T04:55:40+09:00",
            "2026-10-03 04:55:40",
        ] {
            assert_eq!(
                analytics_started_at_utc(value, &legacy_timezone),
                Some(expected)
            );
        }
        assert!(analytics_started_at_utc("invalid", &legacy_timezone).is_none());
        assert!(analytics_started_at_utc("2026-02-30 04:55:40", &legacy_timezone).is_none());
    }

    #[tokio::test]
    async fn turn_analytics_production_writer_preserves_utc_instants_and_duration_pg() {
        let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
        let pool = fixture.connect_and_migrate_with_max_connections(1).await;
        sqlx::query("SET TIME ZONE 'UTC'")
            .execute(&pool)
            .await
            .unwrap();
        let provider = ProviderKind::Codex;
        let channel = ChannelId::new(987654);
        let message = MessageId::new(456789);
        let inflight = InflightTurnState::new(
            provider.clone(),
            channel.get(),
            None,
            123,
            message.get(),
            message.get(),
            "timestamp regression".into(),
            None,
            None,
            None,
            None,
            0,
        );
        let expected_start =
            analytics_started_at_utc(&inflight.started_at, &chrono::Local).unwrap();
        let before = Utc::now();
        persist_turn_analytics_row_with_handles(
            Some(&pool),
            &provider,
            channel,
            message,
            None,
            None,
            None,
            None,
            &inflight,
            crate::db::turns::TurnTokenUsage::default(),
            306_632,
        );
        let turn_id = discord_turn_id(&provider, channel, Some(message), None, None);
        let row = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(row) = sqlx::query_as::<_, (DateTime<Utc>, DateTime<Utc>, i64)>(
                    "SELECT started_at, finished_at, duration_ms FROM turns WHERE turn_id = $1",
                )
                .bind(&turn_id)
                .fetch_optional(&pool)
                .await
                .unwrap()
                {
                    break row;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("production analytics row persisted within deadline");
        assert_eq!(row.0, expected_start);
        assert!(row.1 >= before && row.1 <= Utc::now());
        assert_eq!(row.2, 306_632);
        pool.close().await;
        fixture.drop().await;
    }
}

#[cfg(test)]
mod discord_turn_id_no_user_message_tests {
    //! Regression coverage for the msgid-0 panic class. A recovery turn with no
    //! anchored Discord user message (user_msg_id == 0, e.g. a TUI-direct turn)
    //! must key its turn id on the session instead of `discord:<channel>:0`,
    //! and must not panic via `MessageId::new(0)`.
    use super::discord_turn_id;
    use crate::services::provider::ProviderKind;
    use poise::serenity_prelude::{ChannelId, MessageId};

    #[test]
    fn none_user_msg_id_falls_back_to_session_plus_per_turn_offset() {
        let turn_id = discord_turn_id(
            &ProviderKind::Claude,
            ChannelId::new(4243),
            None,
            Some("session-key-abc"),
            Some(4096),
        );
        assert_eq!(turn_id, "discord:4243:session:session-key-abc:off4096");
        assert!(
            !turn_id.ends_with(":0"),
            "must not produce the bogus discord:<channel>:0 form"
        );
    }

    #[test]
    fn message_less_turns_in_same_session_get_distinct_ids() {
        // Two message-less turns sharing a session must NOT collide (Codex P2):
        // the per-turn JSONL start offset keeps their transcript turn ids apart.
        let first = discord_turn_id(
            &ProviderKind::Claude,
            ChannelId::new(4243),
            None,
            Some("session-key-abc"),
            Some(1000),
        );
        let second = discord_turn_id(
            &ProviderKind::Claude,
            ChannelId::new(4243),
            None,
            Some("session-key-abc"),
            Some(2000),
        );
        assert_ne!(first, second);
    }

    #[test]
    fn none_user_msg_id_and_no_session_uses_recovery_marker() {
        let turn_id = discord_turn_id(
            &ProviderKind::Claude,
            ChannelId::new(4243),
            None,
            None,
            Some(512),
        );
        assert_eq!(turn_id, "discord:4243:recovery:off512");
    }

    #[test]
    fn some_user_msg_id_keeps_legacy_message_keyed_form() {
        let turn_id = discord_turn_id(
            &ProviderKind::Claude,
            ChannelId::new(4243),
            Some(MessageId::new(99)),
            Some("session-key-abc"),
            None,
        );
        assert_eq!(turn_id, "discord:4243:99");
    }
}

#[cfg(test)]
mod pending_delivery_outcome_tests {
    use super::{BridgeCompletionSignal as Signal, pending_delivery_outcome};

    #[test]
    fn pending_delivery_preserves_primary_failure_causes() {
        for primary in [
            "cancelled",
            "recovery_retry",
            "prompt_too_long",
            "transport_error",
            "empty_response",
        ] {
            for (signal, preserve, held_by_other) in [
                (Signal::Unresolved, true, true), // cancel/stop hit a real lease Skip
                (Signal::Unresolved, true, false),
                (Signal::DeferredToCustody, true, false),
                (Signal::DeferredToOwner, false, true),
                (Signal::Finalized, false, false),
            ] {
                assert_eq!(
                    pending_delivery_outcome(primary, signal, preserve, held_by_other),
                    primary
                );
            }
        }
    }

    #[test]
    fn pending_delivery_replaces_only_neutral_success_labels() {
        for primary in [
            "completed",
            "tmux_handoff",
            "watcher_relay",
            "standby_relay",
        ] {
            for (signal, preserve, held_by_other, expected) in [
                (Signal::Unresolved, true, false, "delivery_unresolved"),
                (Signal::Unresolved, true, true, "delivery_pending"),
                (Signal::DeferredToCustody, false, false, "delivery_pending"),
                (Signal::DeferredToOwner, false, false, "delivery_pending"),
                (Signal::Finalized, false, false, primary),
            ] {
                assert_eq!(
                    pending_delivery_outcome(primary, signal, preserve, held_by_other),
                    expected
                );
            }
        }
    }
}
