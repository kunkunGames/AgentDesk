//! Cancellation during both prompt admission and streaming.
use super::*;

/// Poll cancellation while `read_line` waits for an SSE chunk (#2091).
const CANCEL_WATCHDOG_POLL: Duration = Duration::from_millis(250);

/// Allow abort RTT and stream draining before retiring an exclusive server.
const WATCHDOG_KILL_GRACE: Duration = Duration::from_millis(500);

struct StopOnDrop(Arc<AtomicBool>);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

pub(super) fn retire_exclusive(server: &Arc<OpenCodeWarmServer>) {
    if server.try_begin_exclusive_hard_kill().is_ok() {
        server.kill_owned_tree();
        evict_warm_server_from_pool(server);
    } else {
        tracing::warn!(
            "Skipping OpenCode hard-kill for shared or retiring warm server {} with {} active sessions",
            server.base_url,
            server.active_sessions.load(Ordering::SeqCst)
        );
    }
}

pub(super) fn run<T>(
    session_id: &str,
    cancel_token: Option<&CancelToken>,
    base_url: &str,
    auth: &str,
    warm_server: Option<Arc<OpenCodeWarmServer>>,
    operation: impl FnOnce() -> T,
) -> T {
    // Abort unblocks SSE reads; an exclusively owned server bounds fallback
    // latency to poll + abort timeout + kill grace instead of the read timeout.
    let watchdog_done = Arc::new(AtomicBool::new(false));
    thread::scope(|scope| {
        let _stop_on_unwind = StopOnDrop(Arc::clone(&watchdog_done));
        if let Some(cancel) = cancel_token {
            let stop = watchdog_done.clone();
            let watchdog_base_url = base_url.to_string();
            let watchdog_auth = auth.to_string();
            let watchdog_session_id = session_id.to_string();
            let watchdog_server = warm_server.clone();
            scope.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if cancel_requested(Some(cancel)) {
                        // The abort request is bounded by ABORT_SESSION_TIMEOUT.
                        abort_session(&watchdog_base_url, &watchdog_auth, &watchdog_session_id);
                        let kill_deadline = Instant::now() + WATCHDOG_KILL_GRACE;
                        while Instant::now() < kill_deadline && !stop.load(Ordering::Relaxed) {
                            thread::sleep(Duration::from_millis(50));
                        }
                        // Shared servers retain unrelated turns; only exclusive
                        // ownership permits tree retirement. Others use read timeout.
                        if !stop.load(Ordering::Relaxed)
                            && let Some(server) = watchdog_server.as_ref()
                        {
                            retire_exclusive(server);
                        }
                        return;
                    }
                    thread::sleep(CANCEL_WATCHDOG_POLL);
                }
            });
        }
        let result = operation();
        // Signal before the scope joins, including unwind via StopOnDrop.
        watchdog_done.store(true, Ordering::Relaxed);
        result
    })
}
