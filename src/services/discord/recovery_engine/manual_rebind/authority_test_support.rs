use super::super::{RebindError, RebindOutcome};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::task::JoinHandle;

const HANDOFF_TIMEOUT: Duration = Duration::from_secs(20);
const CANCELLATION_TIMEOUT: Duration = Duration::from_secs(3);

type RebindResult = Result<RebindOutcome, RebindError>;

pub(super) struct OwnedTmuxSession(String);

impl OwnedTmuxSession {
    pub(super) fn start(name: &str) -> Self {
        assert!(
            !crate::services::platform::tmux::has_session(name),
            "authority fixture must own a fresh session"
        );
        let created = crate::services::platform::tmux::create_session(name, None, "sleep 60")
            .expect("create owned authority fixture session");
        assert!(created.status.success(), "create authority fixture session");
        Self(name.to_owned())
    }
}

impl Drop for OwnedTmuxSession {
    fn drop(&mut self) {
        let _ = crate::services::platform::tmux::kill_session(
            &self.0,
            "exact-episode authority fixture cleanup",
        );
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct RebindTaskFailure {
    phase: &'static str,
    kind: &'static str,
    early_result: Option<&'static str>,
}

fn result_kind(result: &RebindResult) -> &'static str {
    match result {
        Ok(_) => "RebindSucceeded",
        Err(RebindError::TmuxNotAlive { .. }) => "TmuxNotAlive",
        Err(RebindError::ChannelIdZero) => "ChannelIdZero",
        Err(RebindError::InflightAlreadyExists) => "InflightAlreadyExists",
        Err(RebindError::InflightEpisodeChanged) => "InflightEpisodeChanged",
        Err(RebindError::StaleOutputPath { .. }) => "StaleOutputPath",
        Err(RebindError::ChannelNotBound) => "ChannelNotBound",
        Err(RebindError::RuntimeBindingUnavailable { .. }) => "RuntimeBindingUnavailable",
        Err(RebindError::ChannelNameMissing) => "ChannelNameMissing",
        Err(RebindError::WatcherWithheld { .. }) => "WatcherWithheld",
        Err(RebindError::InputFenced(_)) => "InputFenced",
        Err(RebindError::Internal(_)) => "Internal",
    }
}

pub(super) struct RebindTask {
    task: JoinHandle<RebindResult>,
    consumed: bool,
    timeout: Duration,
}

impl RebindTask {
    pub(super) fn new(task: JoinHandle<RebindResult>) -> Self {
        Self {
            task,
            consumed: false,
            timeout: HANDOFF_TIMEOUT,
        }
    }

    pub(super) async fn checkpoint(
        &mut self,
        reached: &Arc<Barrier>,
        phase: &'static str,
    ) -> Result<(), RebindTaskFailure> {
        let observation = tokio::time::timeout(self.timeout, async {
            tokio::select! {
                biased;
                _ = reached.wait() => Ok(()),
                result = &mut self.task => {
                    self.consumed = true;
                    Err(RebindTaskFailure {
                        phase,
                        kind: "finished_before_checkpoint",
                        early_result: Some(match &result {
                            Ok(result) => result_kind(result),
                            Err(error) if error.is_cancelled() => "JoinCancelled",
                            Err(_) => "JoinPanicked",
                        }),
                    })
                }
            }
        })
        .await;
        match observation {
            Ok(result) => result,
            Err(_) => {
                self.cancel_and_observe(phase).await?;
                Err(RebindTaskFailure {
                    phase,
                    kind: "checkpoint_timed_out",
                    early_result: None,
                })
            }
        }
    }

    pub(super) async fn finish(&mut self) -> Result<RebindResult, RebindTaskFailure> {
        match tokio::time::timeout(self.timeout, &mut self.task).await {
            Ok(result) => {
                self.consumed = true;
                result.map_err(|error| RebindTaskFailure {
                    phase: "handoff_completion",
                    kind: if error.is_cancelled() {
                        "join_cancelled"
                    } else {
                        "join_panicked"
                    },
                    early_result: None,
                })
            }
            Err(_) => {
                self.cancel_and_observe("handoff_completion").await?;
                Err(RebindTaskFailure {
                    phase: "handoff_completion",
                    kind: "completion_timed_out",
                    early_result: None,
                })
            }
        }
    }

    async fn cancel_and_observe(&mut self, phase: &'static str) -> Result<(), RebindTaskFailure> {
        self.task.abort();
        match tokio::time::timeout(CANCELLATION_TIMEOUT, &mut self.task).await {
            Ok(_) => {
                self.consumed = true;
                Ok(())
            }
            Err(_) => Err(RebindTaskFailure {
                phase,
                kind: "cancellation_not_observed",
                early_result: None,
            }),
        }
    }
}

impl Drop for RebindTask {
    fn drop(&mut self) {
        if !self.consumed {
            self.task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn early_rebind_failure_is_reported_without_waiting_for_checkpoint() {
        let mut task = RebindTask::new(tokio::spawn(async { Err(RebindError::ChannelNotBound) }));
        let reached = Arc::new(Barrier::new(2));
        assert_eq!(
            task.checkpoint(&reached, "before_adoption").await,
            Err(RebindTaskFailure {
                phase: "before_adoption",
                kind: "finished_before_checkpoint",
                early_result: Some("ChannelNotBound"),
            })
        );
    }

    #[tokio::test]
    async fn checkpoint_timeout_observes_task_cancellation_and_guard_drop() {
        let dropped = Arc::new(AtomicBool::new(false));
        let started = Arc::new(Barrier::new(2));
        let handle = {
            let dropped = dropped.clone();
            let started = started.clone();
            tokio::spawn(async move {
                let _guard = DropFlag(dropped);
                started.wait().await;
                std::future::pending::<RebindResult>().await
            })
        };
        started.wait().await;
        let mut task = RebindTask::new(handle);
        task.timeout = Duration::from_millis(25);
        assert_eq!(
            task.checkpoint(&Arc::new(Barrier::new(2)), "before_adoption")
                .await,
            Err(RebindTaskFailure {
                phase: "before_adoption",
                kind: "checkpoint_timed_out",
                early_result: None,
            })
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert!(task.consumed);
    }

    #[tokio::test]
    async fn completion_timeout_observes_task_cancellation_after_checkpoint() {
        let dropped = Arc::new(AtomicBool::new(false));
        let reached = Arc::new(Barrier::new(2));
        let mut task = RebindTask::new({
            let reached = reached.clone();
            let dropped = dropped.clone();
            tokio::spawn(async move {
                let _guard = DropFlag(dropped);
                reached.wait().await;
                std::future::pending::<RebindResult>().await
            })
        });
        task.timeout = Duration::from_millis(25);
        task.checkpoint(&reached, "authority_held").await.unwrap();
        let result = task.finish().await;
        assert!(matches!(
            result,
            Err(RebindTaskFailure {
                kind: "completion_timed_out",
                ..
            })
        ));
        assert!(dropped.load(Ordering::SeqCst));
        assert!(task.consumed);
    }
}
