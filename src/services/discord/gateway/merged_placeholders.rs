//! Serialized cleanup of merged queue cards and their persisted ownership.
use super::super::{
    QueuedCardDisposition, QueuedCardTeardown, SharedData, queued_card_gate,
    queued_placeholders_store,
};
use super::{ChannelId, MessageId};

/// Preserve the active head mapping and drain other merged-source mappings.
/// A surviving entry may still own a drained card after rollback; return teardown
/// tokens only for cards released by the ownership gate. The caller deletes them.
pub(in crate::services::discord) async fn drain_merged_queued_placeholders(
    shared: &SharedData,
    channel_id: ChannelId,
    head_message_id: MessageId,
    source_message_ids: &[MessageId],
) -> Vec<QueuedCardTeardown> {
    // Serialize map edits and persistence so an older snapshot cannot overwrite
    // a concurrent head insertion and resurrect drained mappings on restart.
    let persist_lock = shared.queued_placeholders_persist_lock(channel_id);
    let _persist_guard = persist_lock.lock().await;
    let mut to_delete = Vec::new();
    let mut mutated = false;
    // Include the head in the departing hint used for re-key ordering.
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
    // Persist the batch before releasing the channel lock.
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
