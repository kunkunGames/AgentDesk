//! Restored and continued watcher render seed selection before streaming starts.

use super::{
    CollectedTurnStream, RestoredWatcherTurn, TurnStreamCollectorContext, WatcherStreamSeed,
    watcher_stream_seed_after_restored_seed_discard,
};
use crate::services::discord::inflight::InflightTurnIdentity;

pub(super) fn select_stream_seed(
    ctx: &TurnStreamCollectorContext,
    continuation: &Option<CollectedTurnStream>,
    restored_turn_seed: Option<RestoredWatcherTurn>,
    watcher_turn_identity: Option<&InflightTurnIdentity>,
) -> WatcherStreamSeed {
    let watcher_provider = &ctx.watcher_provider;
    let tmux_session_name = &ctx.tmux_session_name;
    let channel_id = ctx.channel_id;
    let prompt_anchor_for_seed_discard =
        crate::services::tui_prompt_dedupe::prompt_anchor_for_response(
            watcher_provider.as_str(),
            &tmux_session_name,
            channel_id.get(),
        );
    let seed_disposition = watcher_stream_seed_after_restored_seed_discard(
        restored_turn_seed,
        watcher_turn_identity,
        prompt_anchor_for_seed_discard.map(|anchor| anchor.message_id),
    );
    if !seed_disposition.discard_restored_seed
        && seed_disposition.prompt_anchor_present
        && seed_disposition.restored_seed_undelivered_body_len > 0
    {
        tracing::info!(
            channel_id = channel_id.get(),
            body_len = seed_disposition.restored_seed_undelivered_body_len,
            tmux_session = %tmux_session_name,
            "watcher: preserving restored stream seed with undelivered body for idle SSH-direct prompt"
        );
    }
    if seed_disposition.discard_restored_seed {
        let ts = chrono::Local::now().format("%H:%M:%S");
        tracing::info!(
            "  [{ts}] 👁 watcher: discarding restored stream seed for idle SSH-direct prompt on channel {} (tmux={}, cross_turn={})",
            channel_id.get(),
            tmux_session_name,
            seed_disposition.seed_reassigned_to_different_turn
        );
    }
    continuation
        .as_ref()
        .map(|turn| WatcherStreamSeed {
            full_response: turn.full_response.clone(),
            placeholder_msg_id: turn.placeholder_msg_id,
            status_panel_msg_id: turn.status_panel_msg_id,
            last_edit_text: turn.last_edit_text.clone(),
            response_sent_offset: turn.response_sent_offset,
            streaming_rollover_frozen_msg_ids: turn
                .watcher_streaming_rollover_frozen_msg_ids
                .clone(),
            task_notification_kind: turn.task_notification_kind,
            finish_mailbox_on_completion: turn.finish_mailbox_on_completion,
        })
        .unwrap_or(seed_disposition.stream_seed)
}
