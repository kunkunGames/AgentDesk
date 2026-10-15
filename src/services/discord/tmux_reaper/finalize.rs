use std::sync::Arc;

use poise::serenity_prelude as serenity;

use crate::services::discord::{SharedData, mailbox_snapshot, turn_finalizer};
use crate::services::provider::ProviderKind;

pub(super) async fn finalize_stale_busy_turn(
    shared: &Arc<SharedData>,
    provider: &ProviderKind,
    channel_id: serenity::ChannelId,
    observed_user_msg_id: serenity::MessageId,
    observed_actor: Arc<crate::services::provider::CancelToken>,
    tmux_session_name: &str,
    trigger: &'static str,
) -> bool {
    let Some(row) =
        crate::services::discord::inflight::load_inflight_state(provider, channel_id.get()).filter(
            |state| {
                state.user_msg_id == observed_user_msg_id.get()
                    && state.turn_nonce.as_deref() == observed_actor.turn_nonce()
                    && state.tmux_session_name.as_deref() == Some(tmux_session_name)
            },
        )
    else {
        return false;
    };
    let mut claim_snapshot = turn_finalizer::SyntheticClaimSnapshot::from_row(&row);
    // RecoveryKickoff may retain the persisted message ID and nonce while
    // installing a fresh actor. Keep the actor observed before either probe so
    // finalizer transport and its yielding claim cannot adopt that recovery.
    // The existing actor CAS also checks its start cutoff at the release point.
    claim_snapshot.recovery_actor = Some(Arc::downgrade(&observed_actor));
    let ts = chrono::Local::now().format("%H:%M:%S");
    tracing::warn!(
        "  [{ts}] stale-busy self-heal: finalizing turn {} in channel {} after tmux session {} disappeared (trigger={trigger})",
        observed_user_msg_id.get(),
        channel_id.get(),
        tmux_session_name,
    );
    let outcome = shared
        .turn_finalizer
        .submit_terminal_with_claim_snapshot(
            turn_finalizer::TurnKey::new(
                channel_id,
                observed_user_msg_id.get(),
                shared.restart.current_generation,
            ),
            provider.clone(),
            turn_finalizer::TerminalEvent::Cancel,
            turn_finalizer::FinalizeContext::stale_busy_mailbox(),
            Some(claim_snapshot),
            shared.clone(),
        )
        .await;

    let finalized_matching_turn = matches!(
        outcome,
        turn_finalizer::FinalizeOutcome::Finalized {
            removed_token: Some(_),
            ..
        }
    );
    let released = finalized_matching_turn
        && mailbox_snapshot(shared, channel_id)
            .await
            .active_user_message_id
            != Some(observed_user_msg_id);
    tracing::info!(
        channel_id = channel_id.get(),
        user_msg_id = observed_user_msg_id.get(),
        trigger,
        finalized = finalized_matching_turn,
        released,
        "stale-busy self-heal finalizer result"
    );
    released
}
