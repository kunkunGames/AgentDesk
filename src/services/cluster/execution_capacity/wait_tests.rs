use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

struct Policy {
    deadline: Instant,
    valid: AtomicBool,
    calls: AtomicUsize,
    refuse_call: usize,
}

impl Policy {
    fn new() -> Self {
        Self {
            deadline: Instant::now() + Duration::from_secs(60),
            valid: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
            refuse_call: usize::MAX,
        }
    }
}

impl AdmissionCheck for Policy {
    fn deadline(&self) -> Instant {
        self.deadline
    }

    fn verify(&self) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if !self.valid.load(Ordering::SeqCst) || call >= self.refuse_call {
                Err("canonical owner or policy changed".into())
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test(start_paused = true)]
async fn campaign_wait_keeps_fifo_across_periodic_revalidation() {
    let slots = Arc::new(Semaphore::new(1));
    let busy = slots.clone().acquire_owned().await.unwrap();
    let token = CancelToken::new();
    let policy = Policy::new();
    let first = wait::permit(slots.clone(), &token, Some(&policy));
    let second = wait::permit(slots.clone(), &token, Some(&policy));
    tokio::pin!(first, second);
    assert!(futures::poll!(&mut first).is_pending());
    assert!(futures::poll!(&mut second).is_pending());
    tokio::time::advance(Duration::from_millis(300)).await;
    assert!(futures::poll!(&mut first).is_pending());
    assert!(futures::poll!(&mut second).is_pending());
    assert!(policy.calls.load(Ordering::SeqCst) >= 6);
    drop(busy);
    assert!(futures::poll!(&mut second).is_pending());
    let first = first.await.unwrap();
    assert_eq!(slots.available_permits(), 0);
    drop(first);
    drop(second.await.unwrap());
    assert_eq!(slots.available_permits(), 1);
}

#[tokio::test]
async fn campaign_wait_cancellation_and_owner_change_win_the_free_slot_race() {
    for reason in ["cancel", "complete", "owner"] {
        let slots = Arc::new(Semaphore::new(1));
        let busy = slots.clone().acquire_owned().await.unwrap();
        let token = CancelToken::new();
        let policy = Policy::new();
        let pending = wait::permit(slots.clone(), &token, Some(&policy));
        tokio::pin!(pending);
        assert!(futures::poll!(&mut pending).is_pending());
        match reason {
            "cancel" => token.cancelled.store(true, Ordering::SeqCst),
            "complete" => token.mark_completion_cleanup(),
            _ => policy.valid.store(false, Ordering::SeqCst),
        }
        drop(busy);
        assert!(pending.await.is_err(), "{reason}");
        assert_eq!(slots.available_permits(), 1, "{reason}");
    }
}

#[tokio::test]
async fn campaign_wait_original_deadline_expires_without_replaying_or_leaking() {
    let slots = Arc::new(Semaphore::new(1));
    let busy = slots.clone().acquire_owned().await.unwrap();
    let token = CancelToken::new();
    let mut policy = Policy::new();
    policy.deadline = Instant::now() + Duration::from_millis(80);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        wait::permit(slots.clone(), &token, Some(&policy)),
    )
    .await
    .unwrap();
    assert!(result.unwrap_err().contains("deadline"));
    drop(busy);
    assert_eq!(slots.available_permits(), 1);
    assert_eq!(wait::io_budget(Some(&policy)), Duration::ZERO);
}

#[tokio::test]
async fn campaign_wait_immediate_permit_still_checks_current_authority() {
    let slots = Arc::new(Semaphore::new(1));
    let token = CancelToken::new();
    let mut policy = Policy::new();
    policy.refuse_call = 2;
    let result = wait::permit(slots.clone(), &token, Some(&policy)).await;
    assert!(result.unwrap_err().contains("owner or policy changed"));
    assert_eq!(policy.calls.load(Ordering::SeqCst), 2);
    assert_eq!(slots.available_permits(), 1);
}

#[tokio::test]
async fn campaign_wait_lookup_errors_are_terminal_and_legacy_capacity_stays_fail_fast() {
    let slots = Arc::new(Semaphore::new(0));
    let token = CancelToken::new();
    let mut policy = Policy::new();
    policy.refuse_call = 1;
    assert!(
        wait::permit(slots.clone(), &token, Some(&policy))
            .await
            .is_err()
    );
    assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        wait::permit(slots.clone(), &token, None).await.unwrap_err(),
        "node execution capacity exhausted; retry when a turn completes"
    );
    slots.close();
    assert_eq!(
        wait::permit(slots.clone(), &token, None).await.unwrap_err(),
        "node execution capacity is closed"
    );
    assert_eq!(
        wait::permit(slots, &token, Some(&Policy::new()))
            .await
            .unwrap_err(),
        "node execution capacity is closed"
    );
}
