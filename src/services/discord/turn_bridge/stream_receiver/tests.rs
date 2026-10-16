use super::*;

#[test]
fn done_wait_uses_remaining_drain_window_as_safety_wake() {
    let now = std::time::Instant::now();
    assert_eq!(
        turn_bridge_stream_wait_duration(
            true,
            Some(now + std::time::Duration::from_millis(123)),
            now,
        ),
        std::time::Duration::from_millis(123)
    );
    assert_eq!(
        turn_bridge_stream_wait_duration(true, None, now),
        std::time::Duration::from_millis(0)
    );
    assert_eq!(
        turn_bridge_stream_wait_duration(false, None, now),
        std::time::Duration::from_secs(1)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_receiver_adapter_wakes_on_ready_frame() {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut async_rx = spawn_stream_message_receiver_adapter(rx);
    tx.send(StreamMessage::TmuxReady {
        output_path: "/tmp/out.jsonl".to_string(),
        input_fifo_path: "/tmp/in.fifo".to_string(),
        tmux_session_name: "adk-test".to_string(),
        last_offset: 12,
    })
    .expect("send ready frame");

    let received = tokio::time::timeout(std::time::Duration::from_millis(50), async_rx.recv())
        .await
        .expect("ready frame should wake without a poll tick")
        .expect("adapter should forward ready frame");

    assert!(matches!(received, StreamMessage::TmuxReady { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_next_frame_wakes_before_drain_deadline_and_preserves_pending_order() {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut receiver = spawn_stream_message_receiver_adapter(rx);
    let mut pending = std::collections::VecDeque::from([StreamMessage::OutputOffset { offset: 7 }]);
    tx.send(StreamMessage::OutputOffset { offset: 11 })
        .expect("send handoff offset");

    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        receiver.queue_next_frame(
            &mut pending,
            true,
            Some(std::time::Instant::now() + std::time::Duration::from_secs(5)),
        ),
    )
    .await
    .expect("queued handoff must wake before the safety deadline");

    assert_eq!(pending.len(), 2);
    assert!(matches!(
        pending.pop_front(),
        Some(StreamMessage::OutputOffset { offset: 7 })
    ));
    assert!(matches!(
        pending.pop_front(),
        Some(StreamMessage::OutputOffset { offset: 11 })
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn queue_next_frame_after_drain_expiry_is_nonblocking_and_receives_one_frame() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut receiver = StreamMessageReceiverAdapter {
        rx,
        stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };
    let mut pending = std::collections::VecDeque::new();
    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        receiver.queue_next_frame(&mut pending, true, None),
    )
    .await
    .expect("expired drain must not wait for an absent frame");
    assert!(pending.is_empty());

    for offset in [11, 17] {
        tx.send(StreamMessage::OutputOffset { offset })
            .expect("queue handoff offsets");
    }
    receiver.queue_next_frame(&mut pending, true, None).await;
    assert_eq!(pending.len(), 1);
    assert!(matches!(
        pending.pop_front(),
        Some(StreamMessage::OutputOffset { offset: 11 })
    ));
    assert!(matches!(
        receiver.try_recv(),
        Ok(StreamMessage::OutputOffset { offset: 17 })
    ));
}
