//! Mailbox actor execution and ordered turn finalization.
use super::{
    Arc, ChannelId, ChannelMailboxHandle, ChannelMailboxState, FinishTurnResult, InterventionMode,
    QueuePersistenceContext, RecoveryDoneSignal, claim_observation, has_soft_intervention,
    input_mailbox_step, mpsc, persist_queue_or_restore, release_active_turn_anchor, remint_fence,
};

pub(super) fn finalize_turn_state(
    state: &mut ChannelMailboxState,
    channel_id: ChannelId,
    persistence: Option<&QueuePersistenceContext>,
    preserve_queue: bool,
) -> FinishTurnResult {
    let removed_token = release_active_turn_anchor(
        state,
        channel_id,
        persistence.map(|context| &context.provider),
    );
    if preserve_queue {
        return FinishTurnResult {
            removed_token,
            has_pending: state
                .intervention_queue
                .iter()
                .any(|item| item.mode == InterventionMode::Soft),
            mailbox_online: true,
            queue_exit_events: Vec::new(),
            persistence_error: None,
        };
    }
    let previous_len = state.intervention_queue.len();
    let previous_queue = state.intervention_queue.clone();
    let pending_result = has_soft_intervention(&mut state.intervention_queue);
    if let Some(persistence) = persistence {
        if state.intervention_queue.len() != previous_len || !state.intervention_queue.is_empty() {
            if let Err(error) = persist_queue_or_restore(
                state,
                channel_id,
                persistence,
                previous_queue,
                "finish_turn",
            ) {
                return FinishTurnResult {
                    removed_token,
                    has_pending: state
                        .intervention_queue
                        .iter()
                        .any(|item| item.mode == InterventionMode::Soft),
                    mailbox_online: true,
                    queue_exit_events: Vec::new(),
                    persistence_error: Some(error),
                };
            }
        }
    }
    FinishTurnResult {
        removed_token,
        has_pending: pending_result.has_pending,
        mailbox_online: true,
        queue_exit_events: pending_result.queue_exit_events,
        persistence_error: None,
    }
}

pub(super) fn spawn_channel_mailbox(
    channel_id: ChannelId,
    fence: remint_fence::FenceCell,
    recovery_done: Arc<RecoveryDoneSignal>,
) -> ChannelMailboxHandle {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let own_recovery_done = recovery_done.clone();
    tokio::spawn(async move {
        let mut state = ChannelMailboxState {
            claim_log: claim_observation::ClaimLog::spawned(channel_id),
            remint_fence: fence,
            ..Default::default()
        };
        while let Some((msg, input_permit)) = rx.recv().await {
            if crate::services::discord::input_runtime::fence::channel_gate(channel_id.get())
                .is_some()
            {
                let signal = own_recovery_done.clone();
                state = tokio::task::spawn_blocking(move || {
                    crate::services::discord::input_runtime::fence::effect::synchronous(
                        input_permit,
                        || {
                            crate::services::discord::input_runtime::fence::blocking(|| {
                                input_mailbox_step(state, channel_id, msg, &signal)
                            })
                        },
                    )
                })
                .await
                .expect("input mailbox step panicked");
            } else {
                state = crate::services::discord::input_runtime::fence::effect::synchronous(
                    input_permit,
                    || input_mailbox_step(state, channel_id, msg, &own_recovery_done),
                );
            }
        }
    });
    ChannelMailboxHandle {
        sender: tx,
        recovery_done,
    }
}
