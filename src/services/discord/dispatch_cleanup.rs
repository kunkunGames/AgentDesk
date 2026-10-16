//! Finite dispatch cancellation must stop its CLI, not merely recycle a session.
use super::{health::HealthRegistry, inflight};
use crate::services::provider::ProviderKind;
use crate::services::turn_lifecycle::{
    TurnLifecycleTarget, force_kill_dispatch_with_verdict, force_kill_verdict,
};

/// Local inflight identity is required: a database thread alone never authorizes
/// killing a successor turn or a runtime owned by another machine.
pub(crate) async fn stop_owned_turn(
    registry: Option<&HealthRegistry>,
    provider: &ProviderKind,
    channel: u64,
    dispatch: &str,
) -> Result<(), String> {
    // A completed or replaced dispatch remains an idempotent no-op even when
    // its old runtime is no longer registered. This read grants no kill authority.
    let initial = inflight::load_inflight_state_read_only_result(provider, channel)?;
    if !initial
        .as_ref()
        .is_some_and(|state| owns_dispatch(state.dispatch_id.as_deref(), dispatch))
    {
        return Ok(());
    }
    let registry =
        registry.ok_or_else(|| "campaign cancellation requires an owned runtime".to_string())?;
    let channel_id = poise::serenity_prelude::ChannelId::new(channel);
    let shared = registry
        .shared_for_provider_on_channel(provider, channel_id)
        .await
        .ok_or_else(|| "campaign cancellation runtime unavailable".to_string())?;
    // Intake, queue kickoff, injection and reset use this same boundary. Re-read
    // the dispatch only after acquiring it, and keep it through the final cleanup.
    let _transition = shared
        .acquire_session_transition(channel_id)
        .await
        .map_err(|_| "campaign cancellation deferred during session transition".to_string())?;
    let Some(state) = inflight::load_inflight_state_read_only_result(provider, channel)? else {
        return Ok(());
    };
    if !owns_dispatch(state.dispatch_id.as_deref(), dispatch) {
        return Ok(());
    }
    let snapshot = super::mailbox_snapshot(&shared, channel_id).await;
    let token = snapshot.cancel_token.as_ref();
    if state.turn_nonce.as_deref().is_none_or(str::is_empty)
        || snapshot.active_user_message_id.map(|id| id.get()) != Some(state.user_msg_id)
        || snapshot.active_turn_nonce != state.turn_nonce
        || token.and_then(|token| token.turn_nonce()) != state.turn_nonce.as_deref()
    {
        return Err("campaign cancellation deferred: inflight and actor ownership differ".into());
    }
    let target = TurnLifecycleTarget {
        provider: Some(provider.clone()),
        channel_id: Some(channel_id),
        tmux_name: state.tmux_session_name.unwrap_or_default(),
    };
    #[cfg(all(test, unix))]
    transition_tests::assert_successor_waits(&shared, channel_id).await;
    let verdict = force_kill_verdict(Some(registry), &target, None).await;
    if verdict.kept() || !verdict.uses_runtime(&shared) {
        return Err("campaign cancellation deferred: runtime or host ownership changed".into());
    }
    let result = force_kill_dispatch_with_verdict(registry, verdict).await;
    #[cfg(all(test, unix))]
    transition_tests::assert_successor_waits(&shared, channel_id).await;
    if result.host_guard_kept() {
        return Err(format!(
            "Campaign dispatch {dispatch}: {}",
            result.refusal_reason()
        ));
    }
    if result.queue_preserved != Some(true) || result.mailbox_foreground_free == Some(false) {
        return Err(format!(
            "Campaign dispatch {dispatch}: runtime cleanup did not preserve queue/free mailbox"
        ));
    }
    if inflight::load_inflight_state_read_only_result(provider, channel)?
        .is_some_and(|s| owns_dispatch(s.dispatch_id.as_deref(), dispatch))
    {
        return Err(format!(
            "Campaign dispatch {dispatch}: inflight survived cancellation"
        ));
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod transition_tests;

fn owns_dispatch(observed: Option<&str>, expected: &str) -> bool {
    !expected.is_empty() && observed == Some(expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cancellation_requires_positive_dispatch_identity() {
        assert!(owns_dispatch(Some("owned"), "owned"));
        assert!(!owns_dispatch(Some("successor"), "owned"));
        assert!(!owns_dispatch(None, "owned"));
        assert!(!owns_dispatch(Some(""), ""));
    }

    #[tokio::test]
    async fn absent_or_replaced_dispatch_cleanup_does_not_require_a_runtime() {
        let _root = crate::config::TestRuntimeRootGuard::new();
        let channel = 1_479_671_301_388_059_801;
        stop_owned_turn(None, &ProviderKind::Claude, channel, "old")
            .await
            .unwrap();
        let mut row = inflight::InflightTurnState::new(
            ProviderKind::Claude,
            channel,
            None,
            1,
            channel + 1,
            channel + 2,
            "successor".into(),
            None,
            None,
            None,
            None,
            0,
        );
        row.dispatch_id = Some("new".into());
        inflight::save_inflight_state_create_new(&row).unwrap();
        stop_owned_turn(None, &ProviderKind::Claude, channel, "old")
            .await
            .unwrap();
        assert!(
            stop_owned_turn(None, &ProviderKind::Claude, channel, "new")
                .await
                .is_err()
        );
        assert_eq!(
            inflight::load_inflight_state_read_only_result(&ProviderKind::Claude, channel)
                .unwrap()
                .unwrap()
                .dispatch_id
                .as_deref(),
            Some("new")
        );
    }
}
