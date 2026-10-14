//! Counter updates paired with mailbox activation and release.

use super::SharedData;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(in crate::services::discord) fn saturating_decrement_counter(counter: &AtomicUsize) -> bool {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_sub(1)
        })
        .is_ok()
}

/// Decrement `global_active` without allowing a stale/restored cleanup path
/// to wrap the counter from 0 to `usize::MAX`.
pub(in crate::services::discord) fn saturating_decrement_global_active(
    shared: &SharedData,
) -> bool {
    saturating_decrement_counter(shared.restart.global_active.as_ref())
}

/// Increment only when mailbox start or recovery kickoff activates a slot.
/// Pair this with [`saturating_decrement_global_active`] when finish or clear
/// removes the token. The reason records the activation site.
pub(in crate::services::discord) fn increment_global_active(
    shared: &SharedData,
    reason: &str,
) -> usize {
    increment_counter(shared.restart.global_active.as_ref(), reason)
}

pub(super) fn increment_counter(counter: &AtomicUsize, reason: &str) -> usize {
    let next = counter.fetch_add(1, Ordering::Relaxed) + 1;
    tracing::debug!(
        target: "agentdesk::global_active",
        reason,
        global_active = next,
        "global_active increment"
    );
    next
}
