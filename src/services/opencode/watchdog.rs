//! Cancellation during both prompt admission and streaming.
use super::*;

/// Poll interval for the cancel-watchdog thread spawned by [`run`].
///
/// `BufReader::lines()` blocks on `read_line` until the SSE peer emits a
/// chunk or the OS read returns. The in-loop `is_cancelled` check only
/// fires on the *next* line, so without a watchdog a cancel signal could
/// take up to `SSE_READ_TIMEOUT` (120 s) to be observed — issue #2091.
/// 250 ms is short enough to give snappy cancel UX while keeping the
/// watchdog cost negligible (4 polls/sec, atomic load + sleep).
const CANCEL_WATCHDOG_POLL: Duration = Duration::from_millis(250);

/// Grace window between `abort_session` (graceful close) and the hard
/// `kill_pid_tree` fallback. If the SSE socket doesn't EOF within this
/// window, the watchdog kills the opencode server process so the TCP
/// connection drops and `read_line` returns. Sized to cover a healthy
/// server's abort RTT (~tens of ms) plus the main thread's tail
/// processing without dragging the worst-case cancel latency too high.
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
    // Watchdog: when the caller's `CancelToken` fires while we're parked
    // inside `reader.lines()`, the in-loop poll wouldn't notice until the
    // peer emits the next chunk. The scoped watchdog thread fires
    // `abort_session` the moment cancel is observed, which closes the
    // upstream SSE connection and unblocks the blocking `read_line` —
    // dropping observed latency from `≤ SSE_READ_TIMEOUT` to `≤
    // CANCEL_WATCHDOG_POLL + abort_session RTT`.
    //
    // Hard fallback: if `abort_session` times out / 5xx's and the SSE
    // socket still hasn't EOF'd within `WATCHDOG_KILL_GRACE`, the
    // watchdog kills the registered opencode server PID tree. The local
    // process exit forces the SSE TCP connection closed regardless of
    // server-side abort behaviour, capping the worst-case cancel
    // observation at `CANCEL_WATCHDOG_POLL + ABORT_SESSION_TIMEOUT +
    // WATCHDOG_KILL_GRACE` instead of `SSE_READ_TIMEOUT`.
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
                        // Graceful: ask opencode to close the SSE stream.
                        // Bounded by `ABORT_SESSION_TIMEOUT` (3 s).
                        abort_session(&watchdog_base_url, &watchdog_auth, &watchdog_session_id);
                        // Give the main thread a brief moment to observe
                        // the peer-closed connection and exit `read_line`.
                        let kill_deadline = Instant::now() + WATCHDOG_KILL_GRACE;
                        while Instant::now() < kill_deadline && !stop.load(Ordering::Relaxed) {
                            thread::sleep(Duration::from_millis(50));
                        }
                        // Hard fallback: if this warm server is not
                        // shared by another active turn, kill the server
                        // process tree so the SSE socket drops and
                        // `read_line` returns. When multiple OpenCode
                        // turns share the same warm server, killing the
                        // server would interrupt unrelated sessions, so
                        // skip the hard-kill fallback and let the bounded
                        // read timeout surface the cancel/error instead.
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
        // Tell the watchdog to exit so `thread::scope` joins promptly even
        // on the happy path. Done *before* the scope's implicit join.
        watchdog_done.store(true, Ordering::Relaxed);
        result
    })
}
