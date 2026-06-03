//! EPIC #3078 PR-3 — bounded, log-once guard for the SHADOW status-panel parity
//! mismatch `warn!`s.
//!
//! The sweeper/orphan-store parity gates run on every sweep, so a persistently
//! diverging row would log-flood without a bound. [`ParityWarnOnce`] records each
//! distinct mismatch *shape* (a small hashable key) and reports `true` only the
//! FIRST time a shape is seen, so the caller logs at most once per shape. It is
//! capacity-bounded (a `VecDeque` evicting the oldest shape) so the set itself
//! cannot grow without limit, mirroring the bounded one-shot collections elsewhere
//! in this module (e.g. `RECENT_WATCHER_REATTACH_OFFSETS`). Evicted shapes may
//! re-log once, which is the intended soft bound.

use std::collections::VecDeque;
use std::hash::Hash;
use std::sync::Mutex;

/// Default capacity for a parity warn-once guard.
pub(in crate::services::discord) const DEFAULT_CAPACITY: usize = 64;

/// Sweeper reclaim-target mismatch shape: `(channel, controller_target, legacy_target)`.
pub(in crate::services::discord) type SweeperShape = (u64, Option<u64>, Option<u64>);
/// Orphan-gate ownership mismatch shape: `(channel, panel, controller_owns, legacy_owns)`.
pub(in crate::services::discord) type OrphanGateShape = (u64, u64, bool, bool);

/// A capacity-bounded "log this shape at most once" guard, keyed on an arbitrary
/// hashable+equatable shape `K` (e.g. `(channel, controller, legacy)`).
pub(in crate::services::discord) struct ParityWarnOnce<K> {
    seen: Mutex<VecDeque<K>>,
    capacity: usize,
}

impl<K: Eq + Hash + Clone> ParityWarnOnce<K> {
    /// Create a guard with [`DEFAULT_CAPACITY`].
    pub(in crate::services::discord) const fn new() -> Self {
        Self {
            seen: Mutex::new(VecDeque::new()),
            capacity: DEFAULT_CAPACITY,
        }
    }

    /// Record `shape`; return `true` the FIRST time it is seen (caller should
    /// `warn!`), `false` for repeats (suppress the flood).
    pub(in crate::services::discord) fn should_warn(&self, shape: K) -> bool {
        let mut seen = match self.seen.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if seen.contains(&shape) {
            return false;
        }
        while seen.len() >= self.capacity {
            seen.pop_front();
        }
        seen.push_back(shape);
        true
    }
}

/// One-shot bound for the PR-3 sweeper parity-mismatch `warn!`: a high-frequency
/// sweep over a persistently-diverging row must not log-flood, so each distinct
/// mismatch shape logs at most once.
static SWEEPER_PARITY_WARNED: ParityWarnOnce<SweeperShape> = ParityWarnOnce::new();

/// EPIC #3078 PR-3 — parity gate between the legacy sweeper reclaim target
/// (`panel_reclaim_target` + `clear_status_panel_if_current`) and the id the
/// `StatusPanelController` chooses for the same row; they must agree or routing
/// the IO through it later would change behaviour. `debug_assert` (test/dev fail
/// loud) + bounded, log-once `warn!` in release (never `panic!`, so an unseen
/// orphan shape cannot crash a prod sweep over the still-executing legacy path).
pub(in crate::services::discord) fn assert_sweeper_reclaim_parity(
    controller_target: Option<u64>,
    legacy_target: Option<u64>,
    channel_id: u64,
) {
    if controller_target == legacy_target {
        return;
    }
    debug_assert_eq!(
        controller_target, legacy_target,
        "#3078 PR-3 sweeper status-panel reclaim parity mismatch (channel {channel_id}): controller chose {controller_target:?}, legacy chose {legacy_target:?}"
    );
    if !SWEEPER_PARITY_WARNED.should_warn((channel_id, controller_target, legacy_target)) {
        return;
    }
    tracing::warn!(
        channel = channel_id,
        controller_target = ?controller_target,
        legacy_target = ?legacy_target,
        "#3078 PR-3 sweeper status-panel reclaim parity mismatch — StatusPanelController chose a different reclaim target than the legacy sweeper; legacy path executed (no behaviour change), divergence logged once for the later controller-executes cutover"
    );
}

#[cfg(test)]
pub(in crate::services::discord) fn sweeper_parity_should_warn(shape: SweeperShape) -> bool {
    SWEEPER_PARITY_WARNED.should_warn(shape)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warns_once_per_distinct_shape() {
        let guard: ParityWarnOnce<(u64, u64)> = ParityWarnOnce::new();
        // First sighting logs; repeats suppressed.
        assert!(guard.should_warn((1, 2)));
        assert!(!guard.should_warn((1, 2)));
        assert!(!guard.should_warn((1, 2)));
        // Distinct shapes each log once.
        assert!(guard.should_warn((1, 3)));
        assert!(guard.should_warn((2, 2)));
        // Original stays suppressed.
        assert!(!guard.should_warn((1, 2)));
    }

    #[test]
    fn capacity_bound_evicts_oldest() {
        let guard: ParityWarnOnce<u64> = ParityWarnOnce::new();
        // Fill exactly to capacity with distinct shapes.
        for i in 0..DEFAULT_CAPACITY as u64 {
            assert!(guard.should_warn(i));
        }
        // A new shape evicts the oldest (0); 0 re-logs once (soft bound), and the
        // set never exceeds capacity.
        assert!(guard.should_warn(DEFAULT_CAPACITY as u64));
        assert!(guard.should_warn(0));
    }
}
