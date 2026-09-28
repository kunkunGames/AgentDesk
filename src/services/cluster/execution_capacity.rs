//! One budget for admitted intake and actual provider execution. Outbox rows
//! reserve capacity; fenced renewable leases cover execution after delivery.

use crate::services::provider::CancelToken;
use serde_json::{Value, json};
use sqlx::PgPool;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

mod admission;
mod managed;
pub(crate) mod recovery;
mod renewal;
mod store;
pub(crate) use managed::{ExecutionLimits, ManagedAttempt};
use managed::{LeaseAdmission, LeaseIdentity};
#[cfg(test)]
pub(crate) mod managed_tests;
#[cfg(test)]
mod tests;

static LOCAL: OnceLock<(u32, Arc<Semaphore>)> = OnceLock::new();
const LEASE_SECONDS: i64 = 30;
// Renew three times per lease period; each DB operation must finish sooner.
const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(LEASE_SECONDS as u64 / 3);
const LEASE_IO_TIMEOUT: Duration = Duration::from_secs(5);
// Match the execution_slots admission bound when comparing advertised occupancy.
const MAX_RANKED_SLOTS: u64 = 1024;
pub(crate) const EXHAUSTED: &str = "node_execution_capacity_exhausted";
pub(crate) fn is_exhausted(error: &sqlx::Error) -> bool {
    error.as_database_error().and_then(|e| e.constraint())
        == Some("node_execution_capacity_available")
}

pub(crate) fn publish(caps: &mut serde_json::Map<String, Value>) {
    let cluster = crate::config::load_graceful().cluster;
    caps.remove("execution_capacity");
    if let Some(slots) = LOCAL
        .get()
        .map(|v| v.0)
        .or(cluster.execution_slots.filter(|_| cluster.enabled))
    {
        // Capacity changes require restart, so advertisement and local guard
        // can never disagree after a config hot reload.
        let (slots, _) = LOCAL.get_or_init(|| (slots, Arc::new(Semaphore::new(slots as usize))));
        let stable_identity = super::node_registry::identity::require_stable(
            &cluster,
            &super::node_registry::resolve_self_instance_id_without_config(),
        )
        .is_ok();
        let managed_providers: serde_json::Map<String, Value> =
            crate::services::provider::provider_registry()
                .iter()
                .filter_map(|entry| {
                    if !stable_identity {
                        return None;
                    }
                    entry
                        .execution_adapter
                        .managed_execution_protocol()
                        .map(|version| (entry.id.to_string(), json!(version)))
                })
                .collect();
        caps.insert(
            "execution_capacity".into(),
            json!({"version":1,"slots":slots,"managed_providers":managed_providers}),
        );
    }
}

pub(crate) fn automatic_enabled() -> bool {
    crate::config_live_reload::current()
        .map(|config| config.cluster.intake_routing.capacity_aware)
        .unwrap_or_else(|| {
            crate::config::load_graceful()
                .cluster
                .intake_routing
                .capacity_aware
        })
}

/// Order available bounded candidates by occupancy ratio, oldest assignment,
/// then identity. Legacy/unbounded nodes never absorb automatic overflow.
pub(crate) fn rank(nodes: &mut Vec<Value>) {
    nodes.retain(|node| {
        node.pointer("/capabilities/execution_capacity/version") == Some(&json!(1))
            && node
                .pointer("/capabilities/execution_capacity/slots")
                .and_then(Value::as_u64)
                .is_some_and(|slots| {
                    slots > node["execution_occupied"].as_u64().unwrap_or(u64::MAX)
                })
    });
    nodes.sort_by(|a, b| {
        let slots = |n: &Value| {
            n.pointer("/capabilities/execution_capacity/slots")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .min(MAX_RANKED_SLOTS)
        };
        let used = |n: &Value| {
            n["execution_occupied"]
                .as_u64()
                .unwrap_or(0)
                .min(MAX_RANKED_SLOTS)
        };
        (used(a) * slots(b))
            .cmp(&(used(b) * slots(a)))
            .then_with(|| {
                a["last_execution_assignment_at"]
                    .as_str()
                    .cmp(&b["last_execution_assignment_at"].as_str())
            })
            .then_with(|| a["instance_id"].as_str().cmp(&b["instance_id"].as_str()))
    });
    for (rank, node) in nodes.iter_mut().enumerate() {
        node["capacity_rank"] = json!(rank);
    }
}

pub(crate) struct ExecutionGuard {
    stop: Option<oneshot::Sender<()>>,
    released: Option<oneshot::Receiver<()>>,
    runtime: tokio::runtime::Handle,
    _permit: OwnedSemaphorePermit,
}

impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        // execute() owns this guard on a blocking thread. Complete release before
        // the next queued turn can acquire the same provider/channel identity.
        if let Some(released) = self.released.take() {
            self.runtime.block_on(async {
                let _ = tokio::time::timeout(LEASE_IO_TIMEOUT, released).await;
            });
        }
    }
}

/// Called inside the provider's blocking task, before it starts any process.
pub(crate) fn acquire(
    pool: Option<&PgPool>,
    provider: &str,
    channel: u64,
    working_dir: &str,
    remote: bool,
    cancel: Arc<CancelToken>,
) -> Result<Option<ExecutionGuard>, String> {
    let config = crate::config::load_graceful();
    let instance = super::node_registry::resolve_self_instance_id_without_config();
    let channel = channel.to_string();
    let limits = if let Some(pool) = pool {
        admission::limits(pool, provider, &channel)?
    } else {
        None
    };
    if limits.is_some() {
        super::node_registry::identity::require_stable(&config.cluster, &instance)?;
        let protocol = crate::services::provider::ProviderKind::from_str(provider)
            .and_then(|kind| kind.execution_adapter())
            .and_then(|adapter| adapter.managed_execution_protocol());
        if protocol != Some(1) {
            return Err(format!(
                "provider {provider} has no verified managed admission protocol"
            ));
        }
        if cancel.managed_execution.get().is_some() {
            return Err("managed execution requires a fresh admitted attempt".into());
        }
    }
    let resource_key = admission::resource(working_dir, remote, limits.is_some())?;
    if LOCAL.get().is_none()
        && (!config.cluster.enabled || config.cluster.execution_slots.is_none())
    {
        if limits.is_some() {
            return Err("managed execution requires enabled PostgreSQL execution capacity".into());
        }
        if let Some(pool) = pool {
            tokio::runtime::Handle::current().block_on(async {
                tokio::time::timeout(
                    LEASE_IO_TIMEOUT,
                    store::check_retained(pool, &instance, &channel, resource_key.as_deref()),
                )
                .await
                .map_err(|_| "retained execution lookup timed out".to_string())?
            })?;
        }
        return Ok(None);
    }
    let slots = config
        .cluster
        .execution_slots
        .ok_or("execution_slots changed; restart the node")?;
    let (configured, semaphore) =
        LOCAL.get_or_init(|| (slots, Arc::new(Semaphore::new(slots as usize))));
    if *configured != slots {
        return Err("execution_slots changed; restart the node before new execution".into());
    }
    let permit = semaphore.clone().try_acquire_owned().map_err(|_| {
        "node execution capacity exhausted; retry when a turn completes".to_string()
    })?;
    let pool = pool
        .ok_or("execution capacity requires PostgreSQL")?
        .clone();
    let provider = provider.to_string();
    acquire_guard(
        pool,
        instance,
        provider,
        channel,
        cancel,
        permit,
        resource_key.map(|resource_key| LeaseAdmission {
            resource_key,
            limits,
        }),
    )
    .map(Some)
}

fn acquire_guard(
    pool: PgPool,
    instance: String,
    provider: String,
    channel: String,
    cancel: Arc<CancelToken>,
    permit: OwnedSemaphorePermit,
    admission: Option<LeaseAdmission>,
) -> Result<ExecutionGuard, String> {
    let managed = admission.as_ref().is_some_and(|a| a.limits.is_some());
    let nonce = if managed {
        uuid::Uuid::parse_str(
            cancel
                .turn_nonce()
                .ok_or("managed attempt nonce is missing")?,
        )
        .map_err(|_| "managed attempt nonce is invalid")?
    } else {
        uuid::Uuid::new_v4()
    };
    let handle = tokio::runtime::Handle::current();
    let admission_started = std::time::Instant::now();
    let deadline = handle.block_on(async {
        tokio::time::timeout(
            LEASE_IO_TIMEOUT,
            store::acquire_scoped(
                &pool,
                &instance,
                &provider,
                &channel,
                nonce,
                admission.as_ref(),
            ),
        )
        .await
        .map_err(|_| "execution lease admission timed out".to_string())?
        .map_err(|e| format!("execution lease admission: {e}"))
    })?;
    let managed_attempt = deadline.map(|deadline| {
        Arc::new(ManagedAttempt::new(
            pool.clone(),
            LeaseIdentity {
                node: instance.clone(),
                provider: provider.clone(),
                channel: channel.clone(),
                nonce,
            },
            deadline,
        ))
    });
    if let Some(attempt) = &managed_attempt {
        // Never replace an attempt's clock/budget with a fresh session budget.
        if cancel.managed_execution.set(Arc::clone(attempt)).is_err() {
            handle
                .block_on(store::release(&pool, &instance, &provider, &channel, nonce))
                .map_err(|error| format!("release duplicate managed admission: {error}"))?;
            return Err("managed attempt is already bound".into());
        }
    }
    let (stop, mut stopped) = oneshot::channel();
    let (released_tx, released) = oneshot::channel();
    handle.spawn(async move {
        let mut renewal = renewal::RenewalWindow::admitted(admission_started);
        let deadline = async {
            if let Some(attempt) = &managed_attempt {
                tokio::time::sleep(attempt.remaining()).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                _ = &mut deadline => {
                    if managed_attempt.as_ref().is_some_and(|attempt| attempt.is_expired()) {
                        cancel.publish_cancel_if_source_absent("managed_attempt_watchdog_timeout");
                        let cancel = Arc::clone(&cancel);
                        let _ = tokio::task::spawn_blocking(move || cancel.cancel_with_tmux_cleanup()).await;
                    }
                    break;
                }
                renewed = async {
                    tokio::time::sleep(renewal.delay(std::time::Instant::now())).await;
                    let started = std::time::Instant::now();
                    let budget = renewal.io_budget(started);
                    let result = if budget.is_zero() {
                        None
                    } else {
                        Some(tokio::time::timeout(budget, store::renew(&pool,&instance,&provider,&channel,nonce)).await)
                    };
                    (started, result)
                } => {
                    let (started, result) = renewed;
                    let cause = match result {
                        Some(Ok(Ok(true))) => {
                            renewal.renewed(started);
                            continue;
                        }
                        Some(Ok(Ok(false))) => "identity_or_expiry_rejected",
                        Some(Ok(Err(ref error))) => {
                            if renewal.retry_transport_error(std::time::Instant::now()) {
                                tracing::warn!(%instance,%provider,%channel,%error,"execution lease transport error; retrying within confirmed lease");
                                continue;
                            }
                            "database_error_after_lease_window"
                        }
                        Some(Err(_)) => {
                            if renewal.retry_transport_error(std::time::Instant::now()) {
                                tracing::warn!(%instance,%provider,%channel,"execution lease I/O timed out; retrying within confirmed lease");
                                continue;
                            }
                            "io_timeout_after_lease_window"
                        }
                        None => "confirmed_lease_window_elapsed",
                    };
                        tracing::error!(%instance,%provider,%channel,cause,"execution lease lost; cancelling provider");
                        cancel.publish_cancel_if_source_absent("execution_lease_lost");
                        let cancel = Arc::clone(&cancel);
                        let _ = tokio::task::spawn_blocking(move || cancel.cancel_with_tmux_cleanup()).await;
                        // Keep the local permit until actual execute() returns.
                        break;
                }
            }
        }
        let released = if let Some(attempt) = &managed_attempt {
            attempt.finish().await
        } else {
            store::release(&pool,&instance,&provider,&channel,nonce).await
        };
        if let Err(error) = released {
            tracing::warn!(%error,"execution lease finalization failed; managed rows retain admission protection");
        }
        let _ = released_tx.send(());
    });
    Ok(ExecutionGuard {
        stop: Some(stop),
        released: Some(released),
        runtime: handle,
        _permit: permit,
    })
}
