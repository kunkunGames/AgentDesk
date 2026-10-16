use crate::services::discord::SharedData;
use crate::services::provider::ProviderKind;
use poise::serenity_prelude as serenity;

/// Finalize only if the mailbox and recorded runtime identities still match the absent probe.
pub(super) fn stale_busy_turn_is_still_reapable(
    observed_user_msg_id: serenity::MessageId,
    current_user_msg_id: Option<serenity::MessageId>,
    has_session: bool,
    observed_tmux_session_name: &str,
    current_tmux_session_name: Option<&str>,
) -> bool {
    !has_session
        && current_user_msg_id == Some(observed_user_msg_id)
        && current_tmux_session_name == Some(observed_tmux_session_name)
}

/// Use the recorded runtime name so a Fresh turn cannot be confused with its primary session.
pub(super) fn recorded_inflight_tmux_session_name(
    provider: &ProviderKind,
    channel_id: serenity::ChannelId,
    user_msg_id: serenity::MessageId,
    turn_nonce: Option<&str>,
) -> Option<String> {
    crate::services::discord::inflight::load_inflight_state(provider, channel_id.get())
        .filter(|state| {
            state.user_msg_id == user_msg_id.get() && state.turn_nonce.as_deref() == turn_nonce
        })
        .and_then(|state| state.tmux_session_name)
}

// Keep the same finite startup window as watcher recovery
// (`watchers/lifecycle/restore.rs`). An intake/headless turn records its intended
// tmux name before provider admission and pane creation, so absence during this
// window is not evidence that a previously running session disappeared.
pub(super) const STALE_BUSY_START_GRACE: std::time::Duration = std::time::Duration::from_secs(60);

pub(super) fn stale_busy_start_grace_elapsed(
    monotonic_start: Option<std::time::Instant>,
    wall_start: Option<chrono::DateTime<chrono::Utc>>,
    monotonic_now: std::time::Instant,
    wall_now: chrono::DateTime<chrono::Utc>,
) -> bool {
    match monotonic_start {
        Some(start) => monotonic_now
            .checked_duration_since(start)
            .is_some_and(|age| age >= STALE_BUSY_START_GRACE),
        // The actor publishes its start before intake can publish the monotonic
        // map entry. Preserve that claim-to-publication window as well. A missing
        // or future origin does not prove a stale start.
        None => wall_start
            .and_then(|start| (wall_now - start).to_std().ok())
            .is_some_and(|age| age >= STALE_BUSY_START_GRACE),
    }
}

pub(super) fn stale_busy_mailbox_start_is_expired(
    shared: &SharedData,
    channel_id: serenity::ChannelId,
    snapshot: &crate::services::turn_orchestrator::ChannelMailboxSnapshot,
) -> bool {
    let monotonic_start = shared
        .turn_start_times
        .get(&channel_id)
        .map(|entry| *entry.value())
        .into_iter()
        .chain(snapshot.recovery_started_at)
        .max();
    stale_busy_start_grace_elapsed(
        monotonic_start,
        snapshot.turn_started_at,
        std::time::Instant::now(),
        chrono::Utc::now(),
    )
}
