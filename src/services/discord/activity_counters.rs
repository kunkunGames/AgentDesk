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

/// Single authoritative writer for the INCREMENT side of `global_active`,
/// mirroring [`saturating_decrement_global_active`] on the decrement side
/// (#3019, sub-issue of #3016).
///
/// INVARIANT: `global_active` == number of mailbox slots currently in the
/// started-not-yet-finished state. This helper MUST be called +1 IFF a mailbox
/// `try_start_turn` / `recovery_kickoff` actually activated a slot
/// (`started` / `activated_turn()`); the matching -1 happens IFF a
/// mailbox finish/clear actually removed it (`removed_token.is_some()`). Keeping
/// increment/decrement 1:1 with the real mailbox state transition — NEVER caller
/// intent — is what prevents the drift/underflow seen in #2934.
///
/// Callers are responsible for the mailbox-activation gate; this helper does NOT
/// change WHEN the counter moves, only funnels HOW it moves so increment is
/// single-authority/single-helper exactly like decrement. `reason` is recorded
/// for observability so every increment is attributable to its activation site.
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
