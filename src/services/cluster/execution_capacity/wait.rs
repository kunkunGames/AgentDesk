//! A campaign may wait for one local slot only within its original admission window.
//! The caller owns its durable authorization; this module never reconstructs it from a channel.
use super::*;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tokio::sync::TryAcquireError;

pub(crate) trait AdmissionCheck: Sync {
    fn deadline(&self) -> Instant;
    fn verify(&self) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;
}

fn live(cancel: &CancelToken, policy: Option<&dyn AdmissionCheck>) -> Result<(), String> {
    if crate::services::provider::cancel_requested(Some(cancel)) || cancel.is_completion_cleanup() {
        return Err("execution admission cancelled or completing".into());
    }
    if policy.is_some_and(|p| Instant::now() >= p.deadline()) {
        return Err("campaign execution admission deadline elapsed".into());
    }
    Ok(())
}

pub(super) fn io_budget(policy: Option<&dyn AdmissionCheck>) -> Duration {
    policy.map_or(LEASE_IO_TIMEOUT, |p| {
        LEASE_IO_TIMEOUT.min(p.deadline().saturating_duration_since(Instant::now()))
    })
}

pub(super) async fn verify(
    cancel: &CancelToken,
    policy: Option<&dyn AdmissionCheck>,
) -> Result<(), String> {
    live(cancel, policy)?;
    if let Some(policy) = policy {
        tokio::time::timeout(io_budget(Some(policy)), policy.verify())
            .await
            .map_err(|_| "campaign admission verification timed out".to_string())??;
    }
    live(cancel, policy)
}

pub(super) async fn permit(
    semaphore: Arc<Semaphore>,
    cancel: &CancelToken,
    policy: Option<&dyn AdmissionCheck>,
) -> Result<OwnedSemaphorePermit, String> {
    verify(cancel, policy).await?;
    match semaphore.clone().try_acquire_owned() {
        Ok(permit) => {
            verify(cancel, policy).await?;
            return Ok(permit);
        }
        Err(TryAcquireError::Closed) => return Err("node execution capacity is closed".into()),
        Err(TryAcquireError::NoPermits) => {}
    }
    let Some(policy) = policy else {
        return Err("node execution capacity exhausted; retry when a turn completes".into());
    };
    // Keep a single queued future. Recreating it on each ownership/cancel check
    // would lose FIFO position and could starve a recurring campaign forever.
    let waiting = semaphore.acquire_owned();
    tokio::pin!(waiting);
    let deadline = tokio::time::Instant::from_std(policy.deadline());
    let mut check = tokio::time::interval(Duration::from_millis(250));
    check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => {
                return Err("campaign execution admission deadline elapsed".into());
            }
            result = &mut waiting => {
                let permit = result.map_err(|_| "node execution capacity is closed".to_string())?;
                // Cancellation, terminal dispatch, replacement ownership or an
                // elapsed deadline always wins over a simultaneously free slot.
                verify(cancel, Some(policy)).await?;
                return Ok(permit);
            }
            _ = check.tick() => verify(cancel, Some(policy)).await?,
        }
    }
}
