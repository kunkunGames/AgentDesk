//! Lightweight atomic observability counters for channel × provider.
//!
//! Introduced by issue #1070 (Epic #905 Phase 1) as a foundation layer on top
//! of the existing, heavier `observability` module. The existing module writes
//! structured events into SQLite/Postgres; this module maintains a pure
//! in-memory atomic counter table for O(1) hot-path updates and cheap
//! snapshotting by the `/api/analytics/observability` endpoint.
//!
//! Design goals:
//! - Hot-path writes must be lock-free (DashMap + AtomicU64).
//! - Counter keys are `(channel_id, provider)` tuples; `channel_id = 0` means
//!   "aggregate / unknown channel" (so callers that only know the provider can
//!   still record).
//! - Snapshot returns a `Vec<CounterSnapshotRow>` suitable for serde
//!   serialization without holding any lock.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;
use serde::Serialize;

/// Key for the counters table. `provider` is lowercased for stability.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CounterKey {
    pub channel_id: u64,
    pub provider: String,
}

impl CounterKey {
    pub fn new(channel_id: u64, provider: &str) -> Self {
        Self {
            channel_id,
            provider: provider.trim().to_ascii_lowercase(),
        }
    }
}

/// Atomic counters per `(channel_id, provider)`. All fields use `AtomicU64`.
#[derive(Debug, Default)]
pub struct AtomicCounters {
    pub attempts: AtomicU64,
    pub guard_fires: AtomicU64,
    pub watcher_replacements: AtomicU64,
    pub success: AtomicU64,
    pub fail: AtomicU64,
}

impl AtomicCounters {
    fn snapshot(&self) -> AtomicCountersSnapshot {
        AtomicCountersSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            guard_fires: self.guard_fires.load(Ordering::Relaxed),
            watcher_replacements: self.watcher_replacements.load(Ordering::Relaxed),
            success: self.success.load(Ordering::Relaxed),
            fail: self.fail.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct AtomicCountersSnapshot {
    pub attempts: u64,
    pub guard_fires: u64,
    pub watcher_replacements: u64,
    pub success: u64,
    pub fail: u64,
}

/// One row emitted by `ObservabilityCounters::snapshot()`.
#[derive(Debug, Clone, Serialize)]
pub struct CounterSnapshotRow {
    pub channel_id: u64,
    pub provider: String,
    pub attempts: u64,
    pub guard_fires: u64,
    pub watcher_replacements: u64,
    pub success: u64,
    pub fail: u64,
    pub success_rate: f64,
}

/// In-process registry of `(channel_id, provider) -> AtomicCounters`.
#[derive(Debug, Default)]
pub struct ObservabilityCounters {
    table: DashMap<CounterKey, Arc<AtomicCounters>>,
}

impl ObservabilityCounters {
    pub fn new() -> Self {
        Self {
            table: DashMap::new(),
        }
    }

    fn slot(&self, channel_id: u64, provider: &str) -> Arc<AtomicCounters> {
        let key = CounterKey::new(channel_id, provider);
        if let Some(existing) = self.table.get(&key) {
            return existing.clone();
        }
        let fresh = Arc::new(AtomicCounters::default());
        self.table
            .entry(key)
            .or_insert_with(|| fresh.clone())
            .clone()
    }

    pub fn record_attempt(&self, channel_id: u64, provider: &str) {
        self.slot(channel_id, provider)
            .attempts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_guard_fire(&self, channel_id: u64, provider: &str) {
        self.slot(channel_id, provider)
            .guard_fires
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_watcher_replacement(&self, channel_id: u64, provider: &str) {
        self.slot(channel_id, provider)
            .watcher_replacements
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_success(&self, channel_id: u64, provider: &str) {
        self.slot(channel_id, provider)
            .success
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_fail(&self, channel_id: u64, provider: &str) {
        self.slot(channel_id, provider)
            .fail
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Serde-friendly snapshot. Does not clear counters.
    pub fn snapshot(&self) -> Vec<CounterSnapshotRow> {
        let mut rows: Vec<CounterSnapshotRow> = self
            .table
            .iter()
            .map(|entry| {
                let key = entry.key().clone();
                let snap = entry.value().snapshot();
                let denom = snap.success + snap.fail;
                let rate = if denom == 0 {
                    0.0
                } else {
                    snap.success as f64 / denom as f64
                };
                CounterSnapshotRow {
                    channel_id: key.channel_id,
                    provider: key.provider,
                    attempts: snap.attempts,
                    guard_fires: snap.guard_fires,
                    watcher_replacements: snap.watcher_replacements,
                    success: snap.success,
                    fail: snap.fail,
                    success_rate: rate,
                }
            })
            .collect();
        rows.sort_by(|a, b| {
            b.attempts
                .cmp(&a.attempts)
                .then_with(|| a.provider.cmp(&b.provider))
                .then_with(|| a.channel_id.cmp(&b.channel_id))
        });
        rows
    }

    #[cfg(test)]
    pub fn reset(&self) {
        self.table.clear();
    }
}

static GLOBAL_COUNTERS: OnceLock<Arc<ObservabilityCounters>> = OnceLock::new();

pub fn global() -> Arc<ObservabilityCounters> {
    GLOBAL_COUNTERS
        .get_or_init(|| Arc::new(ObservabilityCounters::new()))
        .clone()
}

/// Convenience wrappers so call-sites don't have to pull `global()` each time.
pub fn record_attempt(channel_id: u64, provider: &str) {
    global().record_attempt(channel_id, provider);
}

pub fn record_guard_fire(channel_id: u64, provider: &str) {
    global().record_guard_fire(channel_id, provider);
}

pub fn record_watcher_replacement(channel_id: u64, provider: &str) {
    global().record_watcher_replacement(channel_id, provider);
}

pub fn record_success(channel_id: u64, provider: &str) {
    global().record_success(channel_id, provider);
}

pub fn record_fail(channel_id: u64, provider: &str) {
    global().record_fail(channel_id, provider);
}

pub fn snapshot() -> Vec<CounterSnapshotRow> {
    global().snapshot()
}

#[cfg(test)]
pub fn reset_for_tests() {
    global().reset();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn counter_increments_are_thread_safe() {
        let counters = Arc::new(ObservabilityCounters::new());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let c = counters.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    c.record_attempt(42, "codex");
                    c.record_success(42, "codex");
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let snap = counters.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].attempts, 8 * 1000);
        assert_eq!(snap[0].success, 8 * 1000);
        assert_eq!(snap[0].fail, 0);
        assert!((snap[0].success_rate - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn different_keys_get_separate_rows() {
        let c = ObservabilityCounters::new();
        c.record_attempt(1, "codex");
        c.record_attempt(2, "claude");
        c.record_fail(1, "codex");
        let snap = c.snapshot();
        assert_eq!(snap.len(), 2);
        // ordered by attempts desc then provider asc
        let (codex_row, claude_row) = if snap[0].provider == "codex" {
            (&snap[0], &snap[1])
        } else {
            (&snap[1], &snap[0])
        };
        assert_eq!(codex_row.attempts, 1);
        assert_eq!(codex_row.fail, 1);
        assert!((codex_row.success_rate - 0.0).abs() < f64::EPSILON);
        assert_eq!(claude_row.attempts, 1);
    }

    #[test]
    fn provider_is_normalized_lowercase() {
        let c = ObservabilityCounters::new();
        c.record_attempt(10, "Codex");
        c.record_attempt(10, "CODEX");
        c.record_attempt(10, " codex ");
        let snap = c.snapshot();
        assert_eq!(snap.len(), 1, "expected a single normalized row");
        assert_eq!(snap[0].provider, "codex");
        assert_eq!(snap[0].attempts, 3);
    }
}
