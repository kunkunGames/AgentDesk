//! Serialized cleanup of merged queue cards and their persisted ownership.
use super::super::{
    QueuedCardDisposition, QueuedCardTeardown, SharedData, queued_card_gate,
    queued_placeholders_store,
};
use super::{ChannelId, MessageId};

/// codex review P2 (#1332 follow-up): drain the `queued_placeholders` /
/// `placeholder_controller` bookkeeping for every non-head source message id
/// of a merged intervention. The dispatch path uses `intervention.message_id`
/// (the merged tail) as the Active card, so the head id's mapping must be
/// preserved here — only the *other* source ids leak. Returns the placeholder
/// Discord message ids whose visible cards the caller should delete (kept as
/// a return value to keep the helper independent of `serenity::Http` so the
/// test harness can invoke it without a real Discord client).
///
/// #5035 (A4/A5): a non-head source id losing its mapping does not make the card
/// unowned — a rollback can leave a *surviving* entry owning it, so each drained
/// card is gated and only released ones come back, as teardown tokens.
pub(in crate::services::discord) async fn drain_merged_queued_placeholders(
    shared: &SharedData,
    channel_id: ChannelId,
    head_message_id: MessageId,
    source_message_ids: &[MessageId],
) -> Vec<QueuedCardTeardown> {
    // codex review round-4 P2 + round-5 P2: serialize the merged-source
    // drain with every other `queued_placeholders` mutation on the same
    // channel via the per-channel async persistence mutex. Otherwise an
    // `insert_queued_placeholder` for the head id could race this drain and
    // let the older snapshot overwrite the newer disk file, resurrecting
    // non-head source mappings on restart. The lock is async so this helper
    // can be safely awaited from both the live dispatch path and the
    // restart-induced kickoff path (round-5 P2 finding 3) without blocking
    // the runtime runner.
    let persist_lock = shared.queued_placeholders_persist_lock(channel_id);
    let _persist_guard = persist_lock.lock().await;
    let mut to_delete = Vec::new();
    let mut mutated = false;
    // #5035: complete departing hint (head ∪ sources) — re-key ordering only.
    let departing: Vec<MessageId> = std::iter::once(head_message_id)
        .chain(source_message_ids.iter().copied())
        .collect();
    for message_id in source_message_ids {
        if *message_id == head_message_id {
            continue;
        }
        if let Some((_, placeholder_msg_id)) = shared
            .queued
            .queued_placeholders
            .remove(&(channel_id, *message_id))
        {
            mutated = true;
            if let QueuedCardDisposition::Released(teardown) =
                queued_card_gate::release_or_rekey_locked(
                    shared,
                    channel_id,
                    placeholder_msg_id,
                    &departing,
                    &_persist_guard,
                )
                .await
            {
                to_delete.push(teardown);
            }
        }
    }
    // codex review round-3 P2: persist the write-through after the batch
    // drain so a restart sees the same state as memory.
    if mutated {
        queued_placeholders_store::persist_channel_from_map(
            &shared.queued.queued_placeholders,
            &shared.provider,
            &shared.token_hash,
            channel_id,
        );
    }
    to_delete
}
