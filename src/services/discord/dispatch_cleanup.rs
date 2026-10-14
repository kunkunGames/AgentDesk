//! Finite dispatch cancellation must stop its CLI, not merely recycle a session.
use super::{health::HealthRegistry, inflight};
use crate::services::provider::ProviderKind;
use crate::services::turn_lifecycle::{TurnLifecycleTarget, force_kill_turn_without_cancel_event};

/// Local inflight identity is required: a database thread alone never authorizes
/// killing a successor turn or a runtime owned by another machine.
pub(crate) async fn stop_owned_turn(
    registry: Option<&HealthRegistry>,
    provider: &ProviderKind,
    channel: u64,
    dispatch: &str,
) -> Result<(), String> {
    let Some(state) = inflight::load_inflight_state_read_only_result(provider, channel)? else {
        return Ok(());
    };
    if !owns_dispatch(state.dispatch_id.as_deref(), dispatch) {
        return Ok(());
    }
    let result = force_kill_turn_without_cancel_event(
        registry,
        &TurnLifecycleTarget {
            provider: Some(provider.clone()),
            channel_id: Some(poise::serenity_prelude::ChannelId::new(channel)),
            tmux_name: state.tmux_session_name.unwrap_or_default(),
        },
        "Cancelled finite campaign dispatch",
        "campaign_dispatch_cancelled",
    )
    .await;
    if !result.queue_preserved || result.mailbox_foreground_free == Some(false) {
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
}
