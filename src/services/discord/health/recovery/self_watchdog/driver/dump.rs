use std::fmt;
use std::io;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

/// Prepare the OS thread before starting the watchdog's blackout clock.
pub(crate) struct PreparedDump {
    request: SyncSender<()>,
    result: Receiver<Result<(), String>>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DumpError {
    Unavailable,
    TimedOut,
    Capture(String),
}

impl fmt::Display for DumpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("dump wait ended (channel disconnected)"),
            Self::TimedOut => formatter.write_str("dump wait ended (timed out waiting on channel)"),
            Self::Capture(error) => write!(formatter, "dump capture failed ({error})"),
        }
    }
}

impl PreparedDump {
    pub(crate) fn spawn(
        capture: impl FnOnce() -> Result<(), String> + Send + 'static,
    ) -> io::Result<Self> {
        let (request, requests) = mpsc::sync_channel(1);
        let (finished, result) = mpsc::sync_channel(1);
        let (ready, readiness) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("watchdog-dump".into())
            .spawn(move || {
                if ready.send(()).is_ok() && requests.recv().is_ok() {
                    let _ = finished.send(capture());
                }
            })?;
        // Initialization is bounded, but occurs outside the monitored window.
        readiness
            .recv_timeout(Duration::from_secs(5))
            .map_err(|error| {
                io::Error::other(format!("dump helper did not initialize ({error})"))
            })?;
        Ok(Self { request, result })
    }

    pub(crate) fn wait(self, limit: Duration, deadline: Instant) -> Result<(), DumpError> {
        let started = Instant::now();
        let dump_deadline = (started + limit).min(deadline);
        if started >= dump_deadline {
            return Ok(());
        }
        // A single buffered request never waits for capture or OS thread creation.
        self.request
            .try_send(())
            .map_err(|_| DumpError::Unavailable)?;
        match self
            .result
            .recv_timeout(dump_deadline.saturating_duration_since(Instant::now()))
        {
            Ok(result) => result.map_err(DumpError::Capture),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(DumpError::TimedOut),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(DumpError::Unavailable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Released(mpsc::Sender<()>);

    impl Drop for Released {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[test]
    fn unused_prepared_captures_are_released_without_work() {
        for expired in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let recorded = calls.clone();
            let (released, release) = mpsc::channel();
            let owner = Released(released);
            let dump = PreparedDump::spawn(move || {
                let _owner = owner;
                recorded.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            if expired {
                assert_eq!(
                    dump.wait(
                        Duration::from_secs(5),
                        Instant::now() - Duration::from_millis(1)
                    ),
                    Ok(())
                );
            } else {
                drop(dump);
            }
            release.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn requested_capture_reports_its_failure_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let recorded = calls.clone();
        let dump = PreparedDump::spawn(move || {
            recorded.fetch_add(1, Ordering::SeqCst);
            Err("capture marker".into())
        })
        .unwrap();
        assert_eq!(
            dump.wait(
                Duration::from_secs(1),
                Instant::now() + Duration::from_secs(2)
            ),
            Err(DumpError::Capture("capture marker".into()))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn remaining_deadline_caps_a_blocked_capture() {
        let (entered, entry) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let release_on_drop = Released(release);
        let (closed, closure_closed) = mpsc::channel();
        let owner = Released(closed);
        let dump = PreparedDump::spawn(move || {
            let _owner = owner;
            let _ = entered.send(());
            let _ = released.recv();
            Ok(())
        })
        .unwrap();
        let (finished, completion) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let result = dump.wait(
                Duration::from_secs(5),
                Instant::now() + Duration::from_millis(100),
            );
            let _ = finished.send(result);
        });
        let entered = entry.recv_timeout(Duration::from_secs(2));
        let result = completion.recv_timeout(Duration::from_secs(1));
        // Release before assertions/join, including when a wait-budget regression fails.
        drop(release_on_drop);
        waiter.join().unwrap();
        closure_closed.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(entered.is_ok());
        assert_eq!(result.unwrap(), Err(DumpError::TimedOut));
    }
}
