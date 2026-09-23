//! Runtime status, lifecycle counters, and hub-epoch observation.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use super::{
    ClusterRuntime, RunnerExecutionScope, RunnerLocalTerminalReason, RunnerSpec, ServerRunnerId,
};

pub(super) static HUB_ONLY_RUNNERS_STARTED: AtomicBool = AtomicBool::new(false);
pub(super) static HUB_ONLY_RUNNER_ACTIVE_COUNT: AtomicUsize = AtomicUsize::new(0);
pub(super) static HUB_ONLY_RUNNER_LAST_SPAWN_UNIX_MS: AtomicI64 = AtomicI64::new(0);
pub(super) static RATE_LIMIT_SYNC_ACTIVE: AtomicBool = AtomicBool::new(false);
pub(super) static RUNNER_LOCAL_TERMINAL_SIGNAL_COUNT: AtomicUsize = AtomicUsize::new(0);
pub(super) static RUNNER_LOCAL_UNEXPECTED_TERMINAL_SIGNAL_COUNT: AtomicUsize = AtomicUsize::new(0);
pub(super) static RUNNER_LOCAL_LAST_TERMINAL_SIGNAL: LazyLock<
    Mutex<Option<RunnerLocalTerminalSignal>>,
> = LazyLock::new(|| Mutex::new(None));
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RunnerLocalTerminalSignal {
    runner: &'static str,
    reason: &'static str,
    expected_shutdown: bool,
    observed_unix_ms: i64,
}

pub(crate) fn hub_only_runner_status_json() -> serde_json::Value {
    let last_spawn_unix_ms = HUB_ONLY_RUNNER_LAST_SPAWN_UNIX_MS.load(Ordering::Acquire);
    let last_runner_local_signal = runner_local_terminal_signal_snapshot().map(|signal| {
        serde_json::json!({
            "runner": signal.runner,
            "reason": signal.reason,
            "expected_shutdown": signal.expected_shutdown,
            "observed_at": chrono::DateTime::<chrono::Utc>::from_timestamp_millis(signal.observed_unix_ms),
        })
    });
    serde_json::json!({
        "hub_only_runners_started": HUB_ONLY_RUNNERS_STARTED.load(Ordering::Acquire),
        "hub_only_runners_active_count": HUB_ONLY_RUNNER_ACTIVE_COUNT.load(Ordering::Acquire),
        "last_hub_only_runner_spawn_at": if last_spawn_unix_ms > 0 {
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(last_spawn_unix_ms)
        } else {
            None
        },
        "runner_local_terminal_signal_count": RUNNER_LOCAL_TERMINAL_SIGNAL_COUNT.load(Ordering::Acquire),
        "runner_local_unexpected_terminal_signal_count": RUNNER_LOCAL_UNEXPECTED_TERMINAL_SIGNAL_COUNT.load(Ordering::Acquire),
        "last_runner_local_terminal_signal": last_runner_local_signal,
        // #4515 PR2: runner-local restart/exhaustion recovery counters.
        "runner_local_recovery": super::super::runner_recovery::recovery_runtime_json(),
        // Backward-compatible aliases for clients deployed before #4515.
        "runner_local_loop_owned_terminal_signal_count": RUNNER_LOCAL_TERMINAL_SIGNAL_COUNT.load(Ordering::Acquire),
        "runner_local_loop_owned_unexpected_terminal_signal_count": RUNNER_LOCAL_UNEXPECTED_TERMINAL_SIGNAL_COUNT.load(Ordering::Acquire),
        "last_runner_local_loop_owned_terminal_signal": last_runner_local_signal,
    })
}

pub(crate) fn rate_limit_sync_active() -> bool {
    RATE_LIMIT_SYNC_ACTIVE.load(Ordering::Acquire)
}

pub(super) fn runner_local_terminal_signal_snapshot() -> Option<RunnerLocalTerminalSignal> {
    *RUNNER_LOCAL_LAST_TERMINAL_SIGNAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) fn record_runner_local_terminal_signal(
    spec: RunnerSpec,
    reason: RunnerLocalTerminalReason,
    expected_shutdown: bool,
    auto_restart: bool,
    restart_attempt: usize,
) {
    if spec.execution_scope != RunnerExecutionScope::RunnerLocal {
        return;
    }

    let reason = reason.as_doc_str();
    let signal = RunnerLocalTerminalSignal {
        runner: spec.name,
        reason,
        expected_shutdown,
        observed_unix_ms: chrono::Utc::now().timestamp_millis(),
    };
    RUNNER_LOCAL_TERMINAL_SIGNAL_COUNT.fetch_add(1, Ordering::AcqRel);
    if !expected_shutdown {
        RUNNER_LOCAL_UNEXPECTED_TERMINAL_SIGNAL_COUNT.fetch_add(1, Ordering::AcqRel);
    }
    *RUNNER_LOCAL_LAST_TERMINAL_SIGNAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(signal);

    if expected_shutdown {
        tracing::info!(
            runner = spec.name,
            target = spec.target,
            observability_target = spec.target,
            kind = spec.kind.as_doc_str(),
            stage = spec.start_stage.as_doc_str(),
            order = spec.start_order,
            restart = spec.restart_policy.as_doc_str(),
            shutdown = spec.shutdown_policy.as_doc_str(),
            execution_scope = spec.execution_scope.as_doc_str(),
            owner = spec.owner,
            health = spec.health_owner,
            responsibility = spec.responsibility,
            notes = spec.notes,
            reason,
            auto_restart,
            restart_attempt,
            "runner-local loop-owned runner future exited after shutdown"
        );
    } else if reason == RunnerLocalTerminalReason::Panicked.as_doc_str() {
        tracing::error!(
            runner = spec.name,
            target = spec.target,
            observability_target = spec.target,
            kind = spec.kind.as_doc_str(),
            stage = spec.start_stage.as_doc_str(),
            order = spec.start_order,
            restart = spec.restart_policy.as_doc_str(),
            shutdown = spec.shutdown_policy.as_doc_str(),
            execution_scope = spec.execution_scope.as_doc_str(),
            owner = spec.owner,
            health = spec.health_owner,
            responsibility = spec.responsibility,
            notes = spec.notes,
            reason,
            auto_restart,
            restart_attempt,
            "runner-local loop-owned runner future panicked"
        );
    } else {
        tracing::warn!(
            runner = spec.name,
            target = spec.target,
            observability_target = spec.target,
            kind = spec.kind.as_doc_str(),
            stage = spec.start_stage.as_doc_str(),
            order = spec.start_order,
            restart = spec.restart_policy.as_doc_str(),
            shutdown = spec.shutdown_policy.as_doc_str(),
            execution_scope = spec.execution_scope.as_doc_str(),
            owner = spec.owner,
            health = spec.health_owner,
            responsibility = spec.responsibility,
            notes = spec.notes,
            reason,
            auto_restart,
            restart_attempt,
            "runner-local loop-owned runner future exited unexpectedly"
        );
    }
}

pub(super) fn record_hub_only_runner_started(spec: RunnerSpec) {
    HUB_ONLY_RUNNERS_STARTED.store(true, Ordering::Release);
    HUB_ONLY_RUNNER_ACTIVE_COUNT.fetch_add(1, Ordering::AcqRel);
    if spec.id == ServerRunnerId::RateLimitSync {
        RATE_LIMIT_SYNC_ACTIVE.store(true, Ordering::Release);
    }
    HUB_ONLY_RUNNER_LAST_SPAWN_UNIX_MS
        .store(chrono::Utc::now().timestamp_millis(), Ordering::Release);
    tracing::info!(
        runner = spec.name,
        target = spec.target,
        observability_target = spec.target,
        kind = spec.kind.as_doc_str(),
        stage = spec.start_stage.as_doc_str(),
        order = spec.start_order,
        restart = spec.restart_policy.as_doc_str(),
        shutdown = spec.shutdown_policy.as_doc_str(),
        execution_scope = spec.execution_scope.as_doc_str(),
        owner = spec.owner,
        health = spec.health_owner,
        responsibility = spec.responsibility,
        notes = spec.notes,
        "hub-only runner epoch started"
    );
}

pub(super) fn record_hub_only_runner_stopped(spec: RunnerSpec, reason: &str) {
    let _ =
        HUB_ONLY_RUNNER_ACTIVE_COUNT.fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            Some(count.saturating_sub(1))
        });
    if spec.id == ServerRunnerId::RateLimitSync {
        RATE_LIMIT_SYNC_ACTIVE.store(false, Ordering::Release);
    }
    tracing::warn!(
        runner = spec.name,
        target = spec.target,
        observability_target = spec.target,
        kind = spec.kind.as_doc_str(),
        stage = spec.start_stage.as_doc_str(),
        order = spec.start_order,
        restart = spec.restart_policy.as_doc_str(),
        shutdown = spec.shutdown_policy.as_doc_str(),
        execution_scope = spec.execution_scope.as_doc_str(),
        owner = spec.owner,
        health = spec.health_owner,
        responsibility = spec.responsibility,
        notes = spec.notes,
        reason,
        "hub-only runner epoch stopped"
    );
}

pub(super) struct HubOnlyRunnerEpoch {
    spec: RunnerSpec,
}

impl HubOnlyRunnerEpoch {
    pub(super) fn start(spec: RunnerSpec) -> Self {
        record_hub_only_runner_started(spec);
        Self { spec }
    }
}

impl Drop for HubOnlyRunnerEpoch {
    fn drop(&mut self) {
        record_hub_only_runner_stopped(self.spec, "hub runner epoch ended");
    }
}

pub(super) async fn wait_until_shutdown(shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Acquire) {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

pub(super) async fn wait_until_hub_or_shutdown(
    cluster_runtime: &ClusterRuntime,
    shutdown: Arc<AtomicBool>,
) -> bool {
    loop {
        if shutdown.load(Ordering::Acquire) {
            return false;
        }
        if cluster_runtime.is_hub() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
