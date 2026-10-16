//! Async adapter and wait policy for the turn bridge's blocking stream receiver.

use super::*;

pub(in crate::services::discord) struct StreamMessageReceiverAdapter {
    rx: tokio::sync::mpsc::UnboundedReceiver<StreamMessage>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl StreamMessageReceiverAdapter {
    // #2426: timeout is only a safety wake; handoff frames wake immediately.
    pub(super) async fn queue_next_frame(
        &mut self,
        pending: &mut std::collections::VecDeque<StreamMessage>,
        done: bool,
        terminal_control_drain_until: Option<std::time::Instant>,
    ) {
        let stream_wait = turn_bridge_stream_wait_duration(
            done,
            terminal_control_drain_until,
            std::time::Instant::now(),
        );
        if stream_wait.is_zero() {
            if let Ok(msg) = self.try_recv() {
                pending.push_back(msg);
            }
        } else if let Ok(Some(msg)) = tokio::time::timeout(stream_wait, self.recv()).await {
            pending.push_back(msg);
        }
    }

    pub(super) async fn recv(&mut self) -> Option<StreamMessage> {
        self.rx.recv().await
    }

    pub(super) fn try_recv(
        &mut self,
    ) -> Result<StreamMessage, tokio::sync::mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }
}

impl Drop for StreamMessageReceiverAdapter {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
    }
}

pub(in crate::services::discord) fn spawn_stream_message_receiver_adapter(
    rx: mpsc::Receiver<StreamMessage>,
) -> StreamMessageReceiverAdapter {
    let (tx, async_rx) = tokio::sync::mpsc::unbounded_channel();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_worker = stop.clone();
    tokio::task::spawn_blocking(move || {
        while !stop_worker.load(std::sync::atomic::Ordering::Acquire) {
            match rx.recv_timeout(std::time::Duration::from_millis(10)) {
                Ok(message) => {
                    if stop_worker.load(std::sync::atomic::Ordering::Acquire)
                        || tx.send(message).is_err()
                    {
                        break;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    });
    StreamMessageReceiverAdapter { rx: async_rx, stop }
}

pub(in crate::services::discord) fn turn_bridge_stream_wait_duration(
    done: bool,
    terminal_control_drain_until: Option<std::time::Instant>,
    now: std::time::Instant,
) -> std::time::Duration {
    if done {
        return terminal_control_drain_until
            .map(|deadline| deadline.saturating_duration_since(now))
            .unwrap_or_else(|| std::time::Duration::from_millis(0));
    }
    std::time::Duration::from_secs(1)
}

#[cfg(test)]
#[path = "stream_receiver/tests.rs"]
mod ready_drain_unit_tests;
