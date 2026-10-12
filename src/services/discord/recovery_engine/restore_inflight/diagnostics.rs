//! Recovery-snapshot and attempt diagnostics, emitted in the scan's original order.
use super::{ChannelId, ProviderKind, emit_recovery_quality_event, inflight, resolve_role_binding};

pub(super) fn observe_restore_inflight_snapshot(
    provider: &ProviderKind,
    states: &[inflight::InflightTurnState],
    boot_elapsed: std::time::Duration,
) {
    let current_generation = super::super::runtime_store::process_generation();
    tracing::info!(provider = %provider.as_str(), snapshot_rows = states.len(), boot_elapsed_ms = boot_elapsed.as_millis(), "restore_inflight snapshot loaded");
    for state in states
        .iter()
        .filter(|state| state.born_generation != 0 && state.born_generation == current_generation)
    {
        tracing::warn!(provider = %provider.as_str(), channel_id = state.channel_id, user_msg_id = state.user_msg_id, born_generation = state.born_generation, current_generation, boot_elapsed_ms = boot_elapsed.as_millis(), "restore_inflight snapshot contains a row authored by the running process");
    }
}

pub(super) fn emit_recovery_fired(
    provider: &ProviderKind,
    state: &inflight::InflightTurnState,
    channel_id: ChannelId,
    dispatch_id: Option<&str>,
    reason: &str,
) {
    let recovery_turn_id = super::super::analytics_transcript::recovered_transcript_turn_id(
        state.channel_id,
        state.user_msg_id,
        state.session_key.as_deref(),
        state.turn_start_offset,
        &state.started_at,
    );
    let recovery_agent_id = resolve_role_binding(channel_id, state.channel_name.as_deref())
        .map(|binding| binding.role_id);
    crate::services::observability::emit_recovery_fired(
        provider.as_str(),
        state.channel_id,
        dispatch_id,
        state.session_key.as_deref(),
        Some(recovery_turn_id.as_str()),
        recovery_agent_id.as_deref(),
        reason,
    );
    emit_recovery_quality_event(
        provider,
        state.channel_id,
        dispatch_id,
        state.session_key.as_deref(),
        Some(recovery_turn_id.as_str()),
        recovery_agent_id.as_deref(),
        reason,
    );
}
