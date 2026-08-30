use super::inflight::load_inflight_state_read_only;
use super::relay_health::{RelayHealthSnapshot, RelayStallState};
use crate::services::agent_recovery::{self, DetectorSignal, ObserveInput};
use crate::services::provider::ProviderKind;

pub(in crate::services::discord) fn observe_classified_stall(
    snapshot: &RelayHealthSnapshot,
    stall: RelayStallState,
) {
    let channel_id = snapshot.channel_id.to_string();
    let turn_id = snapshot
        .mailbox_active_user_msg_id
        .unwrap_or(snapshot.channel_id)
        .to_string();
    let elapsed_secs = snapshot
        .mailbox_turn_age_secs
        .unwrap_or(0)
        .min(u64::from(u32::MAX)) as u32;
    agent_recovery::observe_mailbox_stall(
        &channel_id,
        &turn_id,
        stall.as_str(),
        elapsed_secs,
        snapshot.mailbox_has_cancel_token,
    );
    if snapshot.tmux_alive == Some(false) && snapshot.mailbox_has_cancel_token {
        agent_recovery::observe(ObserveInput {
            channel_id: channel_id.clone(),
            primary_turn_id: turn_id,
            signal: DetectorSignal::TmuxSessionDead,
        });
    }
    let Some(provider) = ProviderKind::from_str(&snapshot.provider) else {
        return;
    };
    let owner_healthy = snapshot.tmux_alive == Some(true)
        && matches!(
            stall,
            RelayStallState::Healthy
                | RelayStallState::ActiveForegroundStream
                | RelayStallState::ExplicitBackgroundWork
        );
    let fallback_inflight =
        agent_recovery::fallback_provider(&channel_id).is_some_and(|fallback| {
            load_inflight_state_read_only(&fallback, snapshot.channel_id).is_some()
        });
    let _ =
        agent_recovery::try_restore_owner(&channel_id, &provider, owner_healthy, fallback_inflight);
}
