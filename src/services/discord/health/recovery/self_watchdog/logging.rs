//! Best-effort diagnostics cannot block the watchdog's deadline or exit.

use super::driver::Sample;
use super::policy::{Decision, WatchdogState};
use super::{MAX_FAILURES, WatchdogMode};
use std::io;
use std::sync::mpsc::{self, SyncSender};
use std::time::{Duration, Instant};

struct Record {
    state: WatchdogState,
    decision: Decision,
    sample: Option<Sample>,
    at: Instant,
}

pub(super) struct PreparedLog(SyncSender<Record>);

impl PreparedLog {
    pub(super) fn spawn(mode: WatchdogMode) -> io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<Record>(16);
        let (ready, readiness) = mpsc::sync_channel(1);
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        std::thread::Builder::new()
            .name("watchdog-log".into())
            .spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    if ready.send(()).is_ok() {
                        for record in receiver {
                            emit(mode, record);
                        }
                    }
                });
            })?;
        // Initialize outside the monitored window, like the dump helper.
        readiness
            .recv_timeout(Duration::from_secs(5))
            .map_err(|error| {
                io::Error::other(format!("diagnostic helper did not initialize ({error})"))
            })?;
        Ok(Self(sender))
    }

    pub(super) fn record(
        &self,
        state: WatchdogState,
        decision: Decision,
        sample: Option<Sample>,
        at: Instant,
    ) {
        // A stalled sink has bounded memory and never backpressures recovery.
        let _ = self.0.try_send(Record {
            state,
            decision,
            sample,
            at,
        });
    }
}

fn emit(mode: WatchdogMode, record: Record) {
    let Record {
        state,
        decision,
        sample,
        at,
    } = record;
    let reason = decision.reason().label();
    let action = decision.action();
    let response_blackout_ms = state.blackout_at(at).as_millis();
    if let Some(sample) = sample {
        let outcome = &sample.observation.0;
        let verdict = crate::services::hang_forensics::verdict(outcome, &sample.current);
        let db_progress = sample.db_progress;
        let probe = outcome.render();
        let crumbs = sample.current.render();
        if outcome.is_ok() && matches!(decision, Decision::Continue(_)) {
            if sample.previous_failures > 0 {
                tracing::info!(
                    "watchdog: health recovered after {} failure(s) action={action} reason={reason} mode={mode:?} {probe} {crumbs}",
                    sample.previous_failures
                );
            }
        } else {
            tracing::warn!(
                "watchdog: verdict={verdict} action={action} reason={reason} mode={mode:?} failure_streak={} exit_streak={}/{MAX_FAILURES} response_blackout_ms={response_blackout_ms} db_progress={db_progress} {probe} {crumbs}",
                state.observed_failures,
                state.fast_failures
            );
        }
    } else {
        tracing::warn!(
            "watchdog: action={action} reason={reason} mode={mode:?} failure_streak={} exit_streak={}/{MAX_FAILURES} response_blackout_ms={response_blackout_ms}",
            state.observed_failures,
            state.fast_failures
        );
    }
}

#[cfg(test)]
mod tests {
    use super::super::policy::Reason;
    use super::super::{driver, policy::WatchdogPolicy};
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Condvar, Mutex};

    #[derive(Clone)]
    struct BlockedWriter {
        entered: SyncSender<()>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl Write for BlockedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let _ = self.entered.try_send(());
            let (lock, changed) = &*self.gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct Release(Arc<(Mutex<bool>, Condvar)>);

    impl Drop for Release {
        fn drop(&mut self) {
            let (lock, changed) = &*self.0;
            *lock.lock().unwrap() = true;
            changed.notify_all();
        }
    }

    #[test]
    fn blocked_diagnostic_writer_cannot_delay_blackout_exit_sink() {
        let (entered, entry) = mpsc::sync_channel(1);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Release(gate.clone());
        let writer = BlockedWriter { entered, gate };
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let logger = tracing::dispatcher::with_default(&dispatch, || {
            PreparedLog::spawn(WatchdogMode::Enforce).unwrap()
        });
        logger.record(
            WatchdogState::new(Instant::now()),
            Decision::Warn(Reason::ListenerGone),
            None,
            Instant::now(),
        );
        entry.recv_timeout(Duration::from_secs(2)).unwrap();
        let (exited, exit) = mpsc::channel();
        let (armed, arm) = mpsc::channel();
        let watchdog = std::thread::spawn(move || {
            let policy = WatchdogPolicy {
                blackout_limit: Duration::from_millis(600),
                ..WatchdogPolicy::DEFAULT
            };
            let started = Instant::now();
            let state = WatchdogState::new(started);
            let deadline = state.deadline(&policy);
            armed.send(deadline).unwrap();
            driver::run(
                &policy,
                WatchdogMode::Enforce,
                state,
                deadline,
                driver::Hooks {
                    probe: |_| panic!("expired deadline must skip probing"),
                    snapshot: || panic!("expired deadline must skip snapshots"),
                    dump: None,
                    log: move |state: &WatchdogState,
                               decision: &Decision,
                               sample: Option<&Sample>,
                               at| {
                        // Fill the bounded queue while its consumer is blocked.
                        for _ in 0..32 {
                            logger.record(*state, *decision, sample.cloned(), at);
                        }
                    },
                    exit: move |reason, _| {
                        exited.send((reason, Instant::now())).unwrap();
                    },
                },
            );
        });
        let deadline = arm.recv_timeout(Duration::from_secs(2)).unwrap();
        let result = exit.recv_timeout(Duration::from_secs(2));
        // Always release a regressed blocking sender before joining its thread.
        drop(release);
        watchdog.join().unwrap();
        let (reason, exited_at) = result.expect("blocked diagnostics must not hold the exit sink");
        assert_eq!(reason, Reason::HealthResponseBlackoutLimit);
        assert!(exited_at >= deadline);
        assert!(exited_at <= deadline + Duration::from_millis(120));
    }
}
