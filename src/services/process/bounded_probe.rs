//! Bounded observation of fixed read-only native commands.
//!
//! A stalled spawn, pipe or reap keeps its lane occupied. Callers still return
//! at the deadline, and another observation cannot accumulate helpers in that
//! lane. The worker retains ownership until its exact child and reader finish.

use std::io::Read;
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct ProbeLane {
    occupied: Arc<AtomicBool>,
}

struct Permit {
    occupied: Arc<AtomicBool>,
    cleanup_complete: bool,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if self.cleanup_complete {
            self.occupied.store(false, Ordering::Release);
        }
    }
}

impl ProbeLane {
    pub(super) fn output(
        &self,
        command: Command,
        timeout: Duration,
        output_limit: usize,
    ) -> Option<Output> {
        self.output_before_receive(command, timeout, output_limit, || {}, |_| {})
    }

    fn output_before_receive(
        &self,
        mut command: Command,
        timeout: Duration,
        output_limit: usize,
        before_receive: impl FnOnce(),
        on_queued: impl FnOnce(bool) + Send + 'static,
    ) -> Option<Output> {
        let read_limit = output_limit.checked_add(1)?;
        let deadline = Instant::now().checked_add(timeout)?;
        loop {
            if Instant::now() >= deadline {
                return None;
            }
            if self
                .occupied
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
            // Normal concurrent cleanup waits for the complete census instead
            // of treating a short busy lane as an empty descendant tree.
            std::thread::sleep(
                Duration::from_millis(5).min(deadline.saturating_duration_since(Instant::now())),
            );
        }
        let mut permit = Permit {
            occupied: Arc::clone(&self.occupied),
            cleanup_complete: false,
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = Arc::clone(&cancelled);
        let (tx, rx) = mpsc::channel();
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        // Spawn/cleanup also stay off the caller: these OS calls can stall.
        if std::thread::Builder::new()
            .name("native-readonly-probe".into())
            .spawn(move || {
                let result = collect(
                    command,
                    deadline,
                    &worker_cancelled,
                    read_limit,
                    output_limit,
                    &mut permit,
                );
                let complete = result.is_some();
                drop(permit);
                let sent = tx.send(result).is_ok();
                on_queued(sent && complete);
            })
            .is_err()
        {
            // A failed thread spawn never started collect or created a child.
            self.occupied.store(false, Ordering::Release);
            return None;
        }
        before_receive();
        match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(result) if Instant::now() < deadline && !cancelled.load(Ordering::Acquire) => result,
            Ok(_) | Err(_) => {
                cancelled.store(true, Ordering::Release);
                None
            }
        }
    }
}

fn reap_owned(child: &mut Child) -> bool {
    // The unreaped Child reserves this PID. Never rediscover ownership by PID
    // or call kill_pid_tree: this observer is used by identity-based cleanup.
    let _ = child.kill();
    child.wait().is_ok()
}

fn collect(
    mut command: Command,
    deadline: Instant,
    cancelled: &AtomicBool,
    read_limit: usize,
    output_limit: usize,
    permit: &mut Permit,
) -> Option<Output> {
    if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
        permit.cleanup_complete = true;
        return None;
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => {
            permit.cleanup_complete = true;
            return None;
        }
    };
    let Some(stdout) = child.stdout.take() else {
        permit.cleanup_complete = reap_owned(&mut child);
        return None;
    };
    let (tx, rx) = mpsc::channel();
    let reader = match std::thread::Builder::new()
        .name("native-readonly-probe-output".into())
        .spawn(move || {
            let mut bytes = Vec::new();
            let result = stdout
                .take(read_limit as u64)
                .read_to_end(&mut bytes)
                .map(|_| bytes);
            let _ = tx.send(result);
        }) {
        Ok(reader) => reader,
        Err(_) => {
            permit.cleanup_complete = reap_owned(&mut child);
            return None;
        }
    };
    let (status, reaped) = loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                break (Some(status), true);
            }
            Ok(None) if !cancelled.load(Ordering::Acquire) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            _ => {
                break (None, reap_owned(&mut child));
            }
        }
    };
    // Waiting here retains the lane if an escaped writer holds the pipe.
    // The caller's independent deadline already returned an unknown result.
    let bytes = rx.recv().ok();
    let reader_finished = reader.join().is_ok();
    permit.cleanup_complete = reaped && reader_finished;
    let bytes = bytes?.ok()?;
    let status = status?;
    if !status.success()
        || bytes.len() > output_limit
        || cancelled.load(Ordering::Acquire)
        || Instant::now() >= deadline
    {
        return None;
    }
    Some(Output {
        status,
        stdout: bytes,
        stderr: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid_command() -> Command {
        let mut command = Command::new("/bin/ps");
        command.args(["-p", &std::process::id().to_string(), "-o", "pid="]);
        command
    }

    fn await_lane(lane: &ProbeLane) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while lane.occupied.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            !lane.occupied.load(Ordering::Acquire),
            "owned probe/reader did not finish"
        );
    }

    #[test]
    fn native_probe_collects_complete_bounded_output() {
        let lane = ProbeLane::default();
        let result = lane
            .output(pid_command(), Duration::from_secs(2), 64)
            .expect("native ps");
        assert_eq!(
            String::from_utf8(result.stdout).unwrap().trim(),
            std::process::id().to_string()
        );
        assert!(!lane.occupied.load(Ordering::Acquire));
    }

    #[test]
    fn timeout_reaps_owned_leaf_and_allows_fresh_observation() {
        let lane = ProbeLane::default();
        let mut command = Command::new("/bin/sleep");
        command.arg("30");
        let started = Instant::now();
        assert!(
            lane.output(command, Duration::from_millis(50), 64)
                .is_none()
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        await_lane(&lane);
        assert!(
            lane.output(pid_command(), Duration::from_secs(2), 64)
                .is_some()
        );
    }

    #[test]
    fn inherited_pipe_timeout_retains_lane_and_does_not_spawn_again() {
        let lane = ProbeLane::default();
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "sleep 1 & exit 0"]);
        let started = Instant::now();
        assert!(
            lane.output(command, Duration::from_millis(50), 64)
                .is_none()
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(lane.occupied.load(Ordering::Acquire));
        assert!(
            lane.output(pid_command(), Duration::from_millis(50), 64)
                .is_none()
        );
        await_lane(&lane);
        assert!(
            lane.output(pid_command(), Duration::from_secs(2), 64)
                .is_some()
        );
    }

    #[test]
    fn excessive_output_is_incomplete_observation() {
        let lane = ProbeLane::default();
        assert!(
            lane.output(pid_command(), Duration::from_secs(2), 0)
                .is_none()
        );
        await_lane(&lane);
    }

    #[test]
    fn uncertain_cleanup_retains_occupancy_on_unwind() {
        let occupied = Arc::new(AtomicBool::new(true));
        drop(Permit {
            occupied: Arc::clone(&occupied),
            cleanup_complete: false,
        });
        assert!(occupied.load(Ordering::Acquire));
        drop(Permit {
            occupied: Arc::clone(&occupied),
            cleanup_complete: true,
        });
        assert!(!occupied.load(Ordering::Acquire));
    }

    #[test]
    fn failed_command_spawn_does_not_occupy_lane() {
        let lane = ProbeLane::default();
        let command = Command::new("/agentdesk-probe-nonexistent-executable");
        assert!(lane.output(command, Duration::from_secs(2), 64).is_none());
        assert!(!lane.occupied.load(Ordering::Acquire));
    }

    #[test]
    fn queued_complete_output_received_after_deadline_is_unknown() {
        let lane = ProbeLane::default();
        let produced = Arc::new(AtomicBool::new(false));
        let witness = Arc::clone(&produced);
        assert!(
            lane.output_before_receive(
                pid_command(),
                Duration::from_millis(500),
                64,
                || {
                    let witness_deadline = Instant::now() + Duration::from_secs(2);
                    while !produced.load(Ordering::Acquire) {
                        assert!(
                            Instant::now() < witness_deadline,
                            "complete result was not queued"
                        );
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    std::thread::sleep(Duration::from_secs(1));
                },
                move |complete| witness.store(complete, Ordering::Release)
            )
            .is_none()
        );
        assert!(
            produced.load(Ordering::Acquire),
            "native complete result was not queued before delay"
        );
        await_lane(&lane);
    }

    #[test]
    fn concurrent_complete_native_observations_wait_for_the_same_lane() {
        let lane = Arc::new(ProbeLane::default());
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let callers: Vec<_> = (0..4)
            .map(|_| {
                let lane = Arc::clone(&lane);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    let result = lane
                        .output(pid_command(), Duration::from_secs(2), 64)
                        .expect("complete concurrent probe");
                    assert_eq!(
                        String::from_utf8(result.stdout).unwrap().trim(),
                        std::process::id().to_string()
                    );
                })
            })
            .collect();
        for caller in callers {
            caller.join().unwrap();
        }
        await_lane(&lane);
    }
}
