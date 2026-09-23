use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::config::{ClusterConfig, ClusterRole, Config};
use crate::db::postgres::AdvisoryLockLease;
use crate::services::cluster::session_routing::{
    cluster_capabilities_with_runner_api, runner_api_base_url_from_capabilities,
};

pub(crate) const CLUSTER_HUB_ADVISORY_LOCK_ID: i64 = 7_801_100;

pub(crate) use super::capability_routing::{
    CapabilityRouteCandidate, CapabilityRouteDecision, explain_capability_match,
    select_capability_route,
};
use super::intake_runner_capabilities::capabilities_with_runtime_state;
pub(crate) use super::intake_runner_capabilities::{
    deregister_gateway_waiter, node_awaits_gateway, node_supports_intake_provider,
    node_supports_intake_request, refresh_runner_node_runtime_capabilities,
    register_gateway_waiter, register_intake_runner_provider,
};

#[derive(Clone, Debug)]
pub(crate) struct ClusterRuntime {
    enabled: bool,
    instance_id: String,
    configured_role: ClusterRole,
    effective_role: ClusterRole,
    hub_active: Arc<AtomicBool>,
}

impl ClusterRuntime {
    pub(crate) fn single_node() -> Self {
        // Cache the synthetic id so the intake-routing hub hook
        // sees a stable answer in single-node mode too.
        let _ = SELF_INSTANCE_ID.set("single-node".to_string());
        Self {
            enabled: false,
            instance_id: "single-node".to_string(),
            configured_role: ClusterRole::Hub,
            effective_role: ClusterRole::Hub,
            hub_active: Arc::new(AtomicBool::new(true)),
        }
    }

    pub(crate) fn is_hub(&self) -> bool {
        !self.enabled || self.hub_active.load(Ordering::Acquire)
    }

    /// Test-only constructor that builds an `enabled=true` runtime backed by a
    /// caller-provided `hub_active` flag. Lets tests flip hub ownership at will
    /// to exercise supervised runners across lease takeovers without standing
    /// up a real cluster. See `runner_registry::tests`.
    #[cfg(test)]
    pub(crate) fn for_test_with_hub(hub_active: Arc<AtomicBool>) -> Self {
        Self {
            enabled: true,
            instance_id: "test-node".to_string(),
            configured_role: ClusterRole::Auto,
            effective_role: ClusterRole::Runner,
            hub_active,
        }
    }

    pub(crate) fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub(crate) async fn wait_until_not_hub(&self) {
        if !self.enabled {
            std::future::pending::<()>().await;
            return;
        }
        loop {
            if !self.is_hub() {
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    // reason: cluster-runtime hub ownership-wait API exercised directly by the
    // `#[cfg(test)]` hub ownership-transition test; the production supervisor path
    // now blocks via `wait_until_hub_or_shutdown`, so the lib build sees no
    // caller. See #3034.
    #[allow(dead_code)]
    pub(crate) async fn wait_until_hub(&self) {
        if !self.enabled {
            return;
        }
        loop {
            if self.is_hub() {
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    pub(crate) fn describe_for_log(&self) -> serde_json::Value {
        serde_json::json!({
            "enabled": self.enabled,
            "instance_id": self.instance_id,
            "configured_role": self.configured_role.as_str(),
            "effective_role": self.effective_role.as_str(),
            "is_hub": self.is_hub(),
        })
    }
}

fn auto_node_can_attempt_hub_ownership(config: &Config) -> bool {
    config.discord.bots.values().any(|bot| {
        bot.token
            .as_deref()
            .is_some_and(|token| !token.trim().is_empty())
    })
}

pub(crate) async fn bootstrap(config: &Config, pg_pool: Option<PgPool>) -> ClusterRuntime {
    if !config.cluster.enabled {
        tracing::info!("[cluster] disabled; running in standalone hub mode");
        return ClusterRuntime::single_node();
    }

    let Some(pool) = pg_pool else {
        tracing::warn!("[cluster] enabled but PostgreSQL pool is unavailable; disabling cluster");
        return ClusterRuntime::single_node();
    };

    let instance_id = resolve_instance_id(&config.cluster);
    // Phase 4 of intake-node-routing: cache the resolved instance_id
    // so the intake-routing hub hook (`services::cluster::intake_router_hook`)
    // sees the same id we register with `cluster_nodes`. The OnceLock
    // ignores subsequent sets; if bootstrap is called twice in tests,
    // the first wins.
    let _ = SELF_INSTANCE_ID.set(instance_id.clone());
    let hostname = crate::services::platform::hostname_short();
    let configured_role = config.cluster.role;
    let auto_hub_eligible =
        configured_role != ClusterRole::Auto || auto_node_can_attempt_hub_ownership(config);
    let mut hub_lease = match configured_role {
        ClusterRole::Runner => None,
        ClusterRole::Auto if !auto_hub_eligible => {
            tracing::info!(
                instance_id,
                "[cluster] auto node has no configured Discord gateway token; registering as runner standby"
            );
            None
        }
        ClusterRole::Hub | ClusterRole::Auto => {
            match AdvisoryLockLease::try_acquire(&pool, CLUSTER_HUB_ADVISORY_LOCK_ID, "cluster-hub")
                .await
            {
                Ok(lease) => lease,
                Err(error) => {
                    tracing::warn!("[cluster] hub lease acquisition failed: {error}");
                    None
                }
            }
        }
    };
    let effective_role = if hub_lease.is_some() {
        ClusterRole::Hub
    } else {
        ClusterRole::Runner
    };
    let hub_active = Arc::new(AtomicBool::new(hub_lease.is_some()));
    let labels = serde_json::Value::Array(
        config
            .cluster
            .labels
            .iter()
            .map(|label| serde_json::Value::String(label.clone()))
            .collect(),
    );
    let base_capabilities = cluster_capabilities_with_runner_api(&config.cluster);
    super::readiness::spawn_probe(config.clone());
    super::machine_resources::spawn(config.cluster.heartbeat_interval_secs);
    super::attachment_transfer::temporary::spawn_cleanup();
    crate::services::session_forwarding::probe::spawn(
        config.clone(),
        pool.clone(),
        instance_id.clone(),
    );
    let capabilities = capabilities_with_runtime_state(&base_capabilities);
    let pid = std::process::id() as i32;

    if let Err(error) = upsert_runner_node(
        &pool,
        &instance_id,
        &hostname,
        pid,
        configured_role,
        effective_role,
        &labels,
        &capabilities,
    )
    .await
    {
        tracing::warn!("[cluster] runner node registration failed: {error}");
    }
    if let Err(error) = upsert_node_mcp_endpoints(&pool, &instance_id, &capabilities).await {
        tracing::warn!("[cluster] runner MCP endpoint registration failed: {error}");
    }
    if should_wake_wait_queue_after_node_join(&hub_active) {
        crate::services::dispatches::wait_queue::spawn_wait_queue_wake_pg(
            pool.clone(),
            config.cluster.clone(),
            "node_join",
            "cluster_node_join",
            None,
        );
    }

    let stale_reassignment_pool = pool.clone();
    let stale_reassignment_config = config.cluster.clone();
    spawn_stale_claim_owner_reassignment_loop(
        stale_reassignment_pool,
        stale_reassignment_config,
        hub_active.clone(),
    );

    spawn_heartbeat_loop(
        pool,
        instance_id.clone(),
        hostname,
        pid,
        configured_role,
        labels,
        base_capabilities,
        config.cluster.heartbeat_interval_secs,
        config.cluster.lease_ttl_secs,
        hub_active.clone(),
        hub_lease.take(),
        auto_hub_eligible,
    );

    let runtime = ClusterRuntime {
        enabled: true,
        instance_id,
        configured_role,
        effective_role,
        hub_active,
    };
    tracing::info!(cluster = %runtime.describe_for_log(), "[cluster] runtime bootstrapped");
    runtime
}

fn should_wake_wait_queue_after_node_join(hub_active: &AtomicBool) -> bool {
    hub_active.load(Ordering::Acquire)
}

pub(crate) async fn run_hub_intake_retry_maintenance_once(
    pool: &PgPool,
    instance_id: &str,
    stale_threshold_secs: u64,
    lease_ttl_secs: u64,
    retry: impl FnOnce() -> Option<(u32, u64)>,
) -> Result<Option<crate::db::intake_outbox::FailedPreAcceptSweepOutcome>, String> {
    mark_stale_cluster_nodes_offline(pool, stale_threshold_secs, instance_id).await?;
    super::attachment_transfer::store::cleanup(pool)
        .await
        .map_err(|e| format!("attachment cleanup: {e}"))?;
    let Some((max_attempts, retry_authorization_secs)) = retry() else {
        return Ok(None);
    };
    crate::db::intake_outbox::sweep_failed_pre_accept_once(
        pool,
        instance_id,
        max_attempts,
        lease_ttl_secs,
        Some(retry_authorization_secs),
    )
    .await
    .map(Some)
    .map_err(|error| format!("sweep failed pre-accept intake routes: {error}"))
}

fn current_intake_retry_config() -> Option<(u32, u64)> {
    let config = crate::services::cluster::intake_routing_config::effective_intake_routing_config();
    config.mode_is_enforce().then(|| {
        (
            config.max_attempts_per_message,
            config.retry_authorization_secs,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn spawn_heartbeat_loop(
    pool: PgPool,
    instance_id: String,
    hostname: String,
    pid: i32,
    configured_role: ClusterRole,
    labels: serde_json::Value,
    base_capabilities: serde_json::Value,
    heartbeat_interval_secs: u64,
    lease_ttl_secs: u64,
    hub_active: Arc<AtomicBool>,
    mut hub_lease: Option<AdvisoryLockLease>,
    hub_eligible: bool,
) {
    let interval_secs = heartbeat_interval_secs.max(1);
    let stale_threshold_secs = lease_ttl_secs.max(interval_secs * 3);
    let hub_eligible =
        hub_eligible && matches!(configured_role, ClusterRole::Hub | ClusterRole::Auto);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.tick().await;
        // #3651: throttle for the hub-only stale-GC backpressure yield log.
        let mut last_pressure_log = Instant::now() - crate::db::postgres::BACKPRESSURE_LOG_THROTTLE;
        loop {
            interval.tick().await;
            if let Some(lease) = hub_lease.as_mut()
                && let Err(error) = lease.keepalive().await
            {
                tracing::warn!("[cluster] hub lease keepalive failed: {error}");
                hub_active.store(false, Ordering::Release);
                hub_lease = None;
            }
            // Live failover: if this node is eligible to lead and currently is
            // not hub, retry the advisory lock. Picks up hub ownership when the
            // previous hub's session is gone (Postgres releases the lock on
            // session disconnect), without waiting for a dcserver restart.
            if hub_eligible && hub_lease.is_none() && !hub_active.load(Ordering::Acquire) {
                match AdvisoryLockLease::try_acquire(
                    &pool,
                    CLUSTER_HUB_ADVISORY_LOCK_ID,
                    "cluster-hub",
                )
                .await
                {
                    Ok(Some(new_lease)) => {
                        tracing::info!(
                            instance_id,
                            "[cluster] acquired hub advisory lock via failover"
                        );
                        hub_lease = Some(new_lease);
                        hub_active.store(true, Ordering::Release);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!("[cluster] hub lease retry failed: {error}");
                    }
                }
            }
            let current_effective_role = if hub_active.load(Ordering::Acquire) {
                ClusterRole::Hub
            } else {
                ClusterRole::Runner
            };
            let capabilities = capabilities_with_runtime_state(&base_capabilities);
            if let Err(error) = upsert_runner_node(
                &pool,
                &instance_id,
                &hostname,
                pid,
                configured_role,
                current_effective_role,
                &labels,
                &capabilities,
            )
            .await
            {
                tracing::warn!("[cluster] heartbeat failed: {error}");
            }
            if let Err(error) = upsert_node_mcp_endpoints(&pool, &instance_id, &capabilities).await
            {
                tracing::warn!("[cluster] heartbeat MCP endpoint sync failed: {error}");
            }
            // Stale-row GC: hub-only sweep that flips
            // cluster_nodes.status='offline' when a peer's last_heartbeat_at is
            // beyond stale_threshold_secs. Without this, dead nodes keep
            // status='online' and split-brain diagnostics remain unreliable.
            //
            // #3651: the heartbeat upsert above is NEVER gated (liveness /
            // hub-lease signal — gating it would risk false failover). Only
            // this deferrable GC backs off under pool pressure; it self-heals on
            // the next heartbeat tick once pressure clears.
            if hub_active.load(Ordering::Acquire) {
                let throttle = crate::db::postgres::BACKPRESSURE_LOG_THROTTLE;
                if crate::db::postgres::background_should_yield(&pool) {
                    if last_pressure_log.elapsed() >= throttle {
                        tracing::debug!("[cluster] stale GC yielding under pool pressure");
                        last_pressure_log = Instant::now();
                    }
                } else {
                    match run_hub_intake_retry_maintenance_once(
                        &pool,
                        &instance_id,
                        stale_threshold_secs,
                        lease_ttl_secs,
                        current_intake_retry_config,
                    )
                    .await
                    {
                        Ok(
                            None
                            | Some(crate::db::intake_outbox::FailedPreAcceptSweepOutcome::Empty),
                        ) => {}
                        Ok(Some(outcome)) => tracing::debug!(
                            ?outcome,
                            "[cluster] swept failed pre-accept intake route"
                        ),
                        Err(error) => {
                            tracing::warn!("[cluster] hub intake retry maintenance failed: {error}")
                        }
                    }
                }
            }
        }
    });
}

fn spawn_stale_claim_owner_reassignment_loop(
    pool: PgPool,
    cluster_config: ClusterConfig,
    hub_active: Arc<AtomicBool>,
) {
    let interval_secs = cluster_config.heartbeat_interval_secs.max(1);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // #3651: throttle for the backpressure yield log.
        let mut last_pressure_log = Instant::now() - crate::db::postgres::BACKPRESSURE_LOG_THROTTLE;
        loop {
            interval.tick().await;
            if !hub_active.load(Ordering::Acquire) {
                continue;
            }
            // #3651: this hub-only loop holds the longest-lived background tx
            // (reassignment + routing CPU), so it yields first under foreground
            // pool pressure. Skipping is safe — stale claims are reassigned on
            // the next tick once pressure clears.
            if crate::db::postgres::background_should_yield(&pool) {
                if last_pressure_log.elapsed() >= crate::db::postgres::BACKPRESSURE_LOG_THROTTLE {
                    tracing::warn!(
                        "[cluster] yielding stale dispatch_outbox claim-owner reassignment to foreground under pool pressure"
                    );
                    last_pressure_log = Instant::now();
                }
                continue;
            }
            match crate::services::dispatches::outbox_claiming::reassign_stale_dispatch_outbox_claim_owners_with_cluster_config_pg(
                &pool,
                &cluster_config,
            )
            .await
            {
                Ok(0) => {}
                Ok(count) => {
                    tracing::info!(
                        count,
                        "[cluster] reassigned stale dispatch_outbox claim owners"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        error,
                        "[cluster] stale dispatch_outbox claim-owner reassignment failed"
                    );
                }
            }
        }
    });
}

async fn mark_stale_cluster_nodes_offline(
    pool: &PgPool,
    stale_threshold_secs: u64,
    self_instance_id: &str,
) -> Result<u64, String> {
    let result = sqlx::query(
        "UPDATE cluster_nodes
            SET status = 'offline'
          WHERE status = 'online'
            AND instance_id <> $2
            AND last_heartbeat_at < NOW() - ($1::BIGINT * INTERVAL '1 second')",
    )
    .bind(stale_threshold_secs.max(1) as i64)
    .bind(self_instance_id)
    .execute(pool)
    .await
    .map_err(|error| format!("mark stale cluster_nodes offline: {error}"))?;
    let affected = result.rows_affected();
    if affected > 0 {
        tracing::info!(
            stale_threshold_secs,
            affected,
            "[cluster] flipped stale cluster_nodes to offline"
        );
    }
    Ok(affected)
}

#[allow(clippy::too_many_arguments)]
async fn upsert_runner_node(
    pool: &PgPool,
    instance_id: &str,
    hostname: &str,
    pid: i32,
    configured_role: ClusterRole,
    effective_role: ClusterRole,
    labels: &serde_json::Value,
    capabilities: &serde_json::Value,
) -> Result<(), String> {
    sqlx::query(
        r#"
        INSERT INTO cluster_nodes (
            instance_id, hostname, process_id, role, effective_role, status,
            labels, capabilities, last_heartbeat_at, started_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, 'online', $6, $7, NOW(), NOW(), NOW())
        ON CONFLICT (instance_id) DO UPDATE SET
            hostname = EXCLUDED.hostname,
            process_id = EXCLUDED.process_id,
            role = EXCLUDED.role,
            effective_role = EXCLUDED.effective_role,
            status = 'online',
            labels = EXCLUDED.labels,
            capabilities = EXCLUDED.capabilities,
            last_heartbeat_at = NOW(),
            updated_at = NOW()
        "#,
    )
    .bind(instance_id)
    .bind(hostname)
    .bind(pid)
    .bind(configured_role.as_str())
    .bind(effective_role.as_str())
    .bind(labels)
    .bind(capabilities)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| format!("upsert cluster_nodes: {error}"))
}

async fn upsert_node_mcp_endpoints(
    pool: &PgPool,
    instance_id: &str,
    capabilities: &Value,
) -> Result<(), String> {
    let mut endpoint_names = Vec::new();
    let Some(mcp) = capabilities.get("mcp") else {
        sqlx::query("DELETE FROM node_mcp_endpoints WHERE instance_id = $1")
            .bind(instance_id)
            .execute(pool)
            .await
            .map_err(|error| format!("clear node_mcp_endpoints: {error}"))?;
        return Ok(());
    };

    match mcp {
        Value::Object(map) => {
            for (name, metadata) in map {
                if name.trim().is_empty() {
                    continue;
                }
                endpoint_names.push(name.clone());
                let healthy = metadata
                    .get("healthy")
                    .and_then(|value| value.as_bool())
                    .or_else(|| metadata.as_bool());
                sqlx::query(
                    r#"
                    INSERT INTO node_mcp_endpoints (
                        instance_id, endpoint_name, healthy, metadata, last_checked_at, updated_at
                    )
                    VALUES ($1, $2, $3, $4, NOW(), NOW())
                    ON CONFLICT (instance_id, endpoint_name) DO UPDATE SET
                        healthy = EXCLUDED.healthy,
                        metadata = EXCLUDED.metadata,
                        last_checked_at = NOW(),
                        updated_at = NOW()
                    "#,
                )
                .bind(instance_id)
                .bind(name)
                .bind(healthy)
                .bind(metadata)
                .execute(pool)
                .await
                .map_err(|error| format!("upsert node_mcp_endpoints: {error}"))?;
            }
        }
        Value::Array(names) => {
            for endpoint in names.iter().filter_map(|value| value.as_str()) {
                if endpoint.trim().is_empty() {
                    continue;
                }
                endpoint_names.push(endpoint.to_string());
                sqlx::query(
                    r#"
                    INSERT INTO node_mcp_endpoints (
                        instance_id, endpoint_name, healthy, metadata, last_checked_at, updated_at
                    )
                    VALUES ($1, $2, NULL, '{}'::jsonb, NOW(), NOW())
                    ON CONFLICT (instance_id, endpoint_name) DO UPDATE SET
                        healthy = EXCLUDED.healthy,
                        metadata = EXCLUDED.metadata,
                        last_checked_at = NOW(),
                        updated_at = NOW()
                    "#,
                )
                .bind(instance_id)
                .bind(endpoint)
                .execute(pool)
                .await
                .map_err(|error| format!("upsert node_mcp_endpoints: {error}"))?;
            }
        }
        _ => {}
    }

    sqlx::query(
        "DELETE FROM node_mcp_endpoints
          WHERE instance_id = $1
            AND NOT (endpoint_name = ANY($2))",
    )
    .bind(instance_id)
    .bind(endpoint_names)
    .execute(pool)
    .await
    .map_err(|error| format!("prune node_mcp_endpoints: {error}"))?;
    Ok(())
}

/// Process-global cache of the resolved self `instance_id`. Set once
/// during `bootstrap()` from `ClusterRuntime.instance_id()` so callers
/// (e.g. the intake-routing hub hook in `services::cluster::intake_router_hook`)
/// see the SAME id the cluster bootstrap registered with `cluster_nodes`,
/// even when the id was supplied via `ClusterConfig.instance_id` rather
/// than env or hostname.
pub(crate) static SELF_INSTANCE_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Resolve the self instance_id, preferring the value the cluster
/// bootstrap registered (config-driven if present), falling back to
/// the env-var/hostname pair only when the OnceLock has not yet been
/// initialised (e.g. unit tests, early startup before bootstrap).
///
/// Phase 4 codex blocker fix #1: a config-driven id must be reachable
/// from the intake hook so `pick_intake_target` can correctly
/// classify the hub as self. Otherwise the hook can route a
/// message to hub's own `instance_id` and the gate then skips
/// local execution, leaving the row unconsumed.
pub(crate) fn resolve_self_instance_id_without_config() -> String {
    if let Some(value) = SELF_INSTANCE_ID.get() {
        return value.clone();
    }
    if let Ok(value) = std::env::var("AGENTDESK_INSTANCE_ID")
        && !value.trim().is_empty()
    {
        return value.trim().to_string();
    }
    format!(
        "{}-{}",
        crate::services::platform::hostname_short(),
        std::process::id()
    )
}

/// Wait until `cluster::bootstrap` has populated `SELF_INSTANCE_ID`, then
/// return its value. Used by callers (Phase 5.1 intake_runner spawn) that
/// race with cluster bootstrap and would otherwise pick up the
/// hostname+PID fallback. Times out after `max_wait` and falls back to
/// `resolve_self_instance_id_without_config()` so the caller never blocks
/// forever in degraded boots.
pub(crate) async fn wait_for_self_instance_id(max_wait: std::time::Duration) -> String {
    let start = std::time::Instant::now();
    while SELF_INSTANCE_ID.get().is_none() {
        if start.elapsed() >= max_wait {
            tracing::warn!(
                elapsed_ms = start.elapsed().as_millis() as u64,
                "[cluster] wait_for_self_instance_id timed out — falling back to hostname/PID"
            );
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    resolve_self_instance_id_without_config()
}

fn resolve_instance_id(config: &ClusterConfig) -> String {
    if let Some(value) = config
        .instance_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return value.to_string();
    }
    if let Ok(value) = std::env::var("AGENTDESK_INSTANCE_ID")
        && !value.trim().is_empty()
    {
        return value.trim().to_string();
    }
    format!(
        "{}-{}",
        crate::services::platform::hostname_short(),
        std::process::id()
    )
}

pub(crate) async fn list_cluster_nodes(
    pool: &PgPool,
    lease_ttl_secs: u64,
) -> Result<Vec<serde_json::Value>, String> {
    let rows = sqlx::query(
        r#"
        SELECT
            cluster_nodes.instance_id,
            hostname,
            process_id,
            role,
            effective_role,
            CASE
                WHEN last_heartbeat_at < NOW() - ($1::BIGINT * INTERVAL '1 second') THEN 'offline'
                ELSE status
            END AS computed_status,
            labels,
            capabilities,
            COALESCE(active_dispatches.active_dispatch_count, 0)::BIGINT AS active_dispatch_count,
            execution_assignments.last_execution_assignment_at,
            (SELECT count(*) FROM node_execution_occupancy(cluster_nodes.instance_id)) AS execution_occupied,
            (SELECT count(*) FROM node_execution_leases l WHERE l.instance_id=cluster_nodes.instance_id AND l.expires_at>NOW()) AS execution_active,
            last_heartbeat_at,
            started_at,
            updated_at
        FROM cluster_nodes
        LEFT JOIN node_execution_assignments execution_assignments ON execution_assignments.instance_id=cluster_nodes.instance_id
        LEFT JOIN (
            SELECT claim_owner, COUNT(*)::BIGINT AS active_dispatch_count
              FROM dispatch_outbox
             WHERE status IN ('claimed', 'processing')
               AND claim_owner IS NOT NULL
             GROUP BY claim_owner
        ) active_dispatches ON active_dispatches.claim_owner = cluster_nodes.instance_id
        ORDER BY last_heartbeat_at DESC, cluster_nodes.instance_id ASC
        "#,
    )
    .bind(lease_ttl_secs.max(1) as i64)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("query cluster_nodes: {error}"))?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let capabilities = row
                .try_get::<Option<serde_json::Value>, _>("capabilities")
                .ok()
                .flatten()
                .unwrap_or_else(|| serde_json::json!({}));
            let api_base_url = runner_api_base_url_from_capabilities(&capabilities);
            let session_api_routable = api_base_url.is_some();
            serde_json::json!({
                "instance_id": row.try_get::<String, _>("instance_id").ok(),
                "hostname": row.try_get::<Option<String>, _>("hostname").ok().flatten(),
                "process_id": row.try_get::<Option<i32>, _>("process_id").ok().flatten(),
                "role": row.try_get::<Option<String>, _>("role").ok().flatten(),
                "effective_role": row.try_get::<Option<String>, _>("effective_role").ok().flatten(),
                "status": row.try_get::<Option<String>, _>("computed_status").ok().flatten(),
                "labels": row
                    .try_get::<Option<serde_json::Value>, _>("labels")
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| serde_json::json!([])),
                "capabilities": capabilities,
                "active_dispatch_count": row
                    .try_get::<Option<i64>, _>("active_dispatch_count")
                    .ok()
                    .flatten()
                    .unwrap_or(0),
                "execution_occupied": row.try_get::<i64,_>("execution_occupied").ok(),
                "execution_active": row.try_get::<i64,_>("execution_active").ok(),
                "last_execution_assignment_at": row.try_get::<Option<chrono::DateTime<chrono::Utc>>,_>("last_execution_assignment_at").ok().flatten(),
                "api_base_url": api_base_url,
                "session_api_routable": session_api_routable,
                "last_heartbeat_at": row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_heartbeat_at").ok().flatten(),
                "started_at": row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("started_at").ok().flatten(),
                "updated_at": row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("updated_at").ok().flatten(),
            })
        })
        .collect())
}
#[cfg(test)]
mod tests {
    use super::{
        ClusterRole, ClusterRuntime, auto_node_can_attempt_hub_ownership,
        current_intake_retry_config, explain_capability_match, resolve_instance_id,
        select_capability_route, should_wake_wait_queue_after_node_join,
    };
    use crate::config::{ClusterConfig, Config};
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    #[test]
    fn intake_retry_tick_reads_each_hot_reload_snapshot() {
        let mut config = Config::default();
        config.cluster.intake_routing.enabled = true;
        config.cluster.intake_routing.mode = crate::config::ClusterIntakeRoutingMode::Enforce;
        for (mode, expected) in [
            (crate::config::ClusterIntakeRoutingMode::Enforce, true),
            (crate::config::ClusterIntakeRoutingMode::Observe, false),
            (crate::config::ClusterIntakeRoutingMode::Enforce, true),
        ] {
            config.cluster.intake_routing.mode = mode;
            crate::config_live_reload::install(config.clone());
            assert_eq!(current_intake_retry_config().is_some(), expected);
        }
    }

    #[test]
    fn configured_instance_id_wins() {
        let config = ClusterConfig {
            instance_id: Some("mac-mini-release".to_string()),
            ..ClusterConfig::default()
        };
        assert_eq!(resolve_instance_id(&config), "mac-mini-release");
    }

    #[test]
    fn node_join_wake_runs_only_on_hub() {
        let hub = AtomicBool::new(true);
        assert!(should_wake_wait_queue_after_node_join(&hub));

        hub.store(false, Ordering::Release);
        assert!(!should_wake_wait_queue_after_node_join(&hub));
    }

    #[test]
    fn auto_node_hub_ownership_requires_configured_gateway_token() {
        let mut config = Config::default();
        config.cluster.enabled = true;
        config.cluster.role = ClusterRole::Auto;
        config.discord.bots.clear();

        assert!(!auto_node_can_attempt_hub_ownership(&config));

        config.discord.bots.insert(
            "codex".to_string(),
            crate::config::BotConfig {
                token: Some("   ".to_string()),
                provider: Some("codex".to_string()),
                ..crate::config::BotConfig::default()
            },
        );
        assert!(!auto_node_can_attempt_hub_ownership(&config));

        config.discord.bots.get_mut("codex").unwrap().token = Some("token".to_string());
        assert!(auto_node_can_attempt_hub_ownership(&config));
    }

    #[tokio::test]
    async fn wait_until_hub_follows_late_hub_ownership_transition() {
        let hub_active = Arc::new(AtomicBool::new(false));
        let runtime = ClusterRuntime {
            enabled: true,
            instance_id: "test-node".to_string(),
            configured_role: ClusterRole::Auto,
            effective_role: ClusterRole::Runner,
            hub_active: hub_active.clone(),
        };
        let wait = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.wait_until_hub().await }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!wait.is_finished());
        hub_active.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .expect("wait_until_hub should observe hub ownership")
            .expect("wait task should not panic");
    }

    #[test]
    fn capability_match_accepts_labels_providers_and_healthy_mcp() {
        let node = json!({
            "instance_id": "mac-book-release",
            "labels": ["mac-book"],
            "capabilities": {
                "providers": ["codex"],
                "mcp": {"filesystem": {"healthy": true}}
            }
        });
        let required = json!({
            "labels": ["mac-book"],
            "providers": ["codex"],
            "mcp": {"filesystem": {"healthy": true}}
        });

        let decision = explain_capability_match(&node, &required);
        assert!(decision.eligible, "{:?}", decision.reasons);
    }

    #[test]
    fn capability_match_reports_exclusion_reasons() {
        let node = json!({
            "instance_id": "mac-mini-release",
            "labels": ["mac-mini"],
            "capabilities": {
                "providers": ["claude"],
                "mcp": {"filesystem": {"healthy": false}}
            }
        });
        let required = json!({
            "labels": ["mac-book"],
            "providers": ["codex"],
            "mcp": {"filesystem": {"healthy": true}, "unreal": true}
        });

        let decision = explain_capability_match(&node, &required);
        assert!(!decision.eligible);
        assert!(
            decision
                .reasons
                .iter()
                .any(|reason| reason.contains("mac-book"))
        );
        assert!(
            decision
                .reasons
                .iter()
                .any(|reason| reason.contains("codex"))
        );
        assert!(
            decision
                .reasons
                .iter()
                .any(|reason| reason.contains("filesystem"))
        );
        assert!(
            decision
                .reasons
                .iter()
                .any(|reason| reason.contains("unreal"))
        );
    }

    #[test]
    fn required_namespace_remains_hard_and_preferred_namespace_is_soft() {
        let mac_mini = json!({
            "instance_id": "mac-mini-release",
            "status": "online",
            "labels": ["mac-mini"],
            "capabilities": {"providers": ["codex"]},
            "last_heartbeat_at": "2026-05-03T00:00:00Z"
        });
        let mac_book = json!({
            "instance_id": "mac-book-release",
            "status": "online",
            "labels": ["mac-book"],
            "capabilities": {"providers": ["claude"]},
            "last_heartbeat_at": "2026-05-03T00:00:01Z"
        });
        let route = json!({
            "required": {"providers": ["codex"]},
            "preferred": {"labels": ["mac-book"]}
        });

        let candidates = select_capability_route(&[mac_mini, mac_book], &route);
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].decision.instance_id.as_deref(),
            Some("mac-mini-release")
        );
        assert_eq!(candidates[0].score, 0);
    }

    #[test]
    fn preferred_label_ranks_online_match_but_falls_back_to_online_candidate() {
        let offline_preferred = json!({
            "instance_id": "mac-book-release",
            "status": "offline",
            "labels": ["mac-book"],
            "capabilities": {"providers": ["codex"]},
            "last_heartbeat_at": "2026-05-03T00:00:02Z"
        });
        let fallback = json!({
            "instance_id": "mac-mini-release",
            "status": "online",
            "labels": ["mac-mini"],
            "capabilities": {"providers": ["codex"]},
            "last_heartbeat_at": "2026-05-03T00:00:01Z"
        });
        let preferred_online = json!({
            "instance_id": "mac-book-release",
            "status": "online",
            "labels": ["mac-book"],
            "capabilities": {"providers": ["codex"]},
            "last_heartbeat_at": "2026-05-03T00:00:02Z"
        });
        let route = json!({"preferred": {"labels": ["mac-book"]}});

        let fallback_candidates =
            select_capability_route(&[offline_preferred, fallback.clone()], &route);
        assert_eq!(
            fallback_candidates[0].decision.instance_id.as_deref(),
            Some("mac-mini-release")
        );
        assert_eq!(fallback_candidates[0].score, 0);

        let preferred_candidates = select_capability_route(&[fallback, preferred_online], &route);
        assert_eq!(
            preferred_candidates[0].decision.instance_id.as_deref(),
            Some("mac-book-release")
        );
        assert_eq!(preferred_candidates[0].score, 1);
    }

    #[test]
    fn preferred_label_order_beats_newer_heartbeat() {
        let first_label = json!({
            "instance_id": "mac-book-release",
            "status": "online",
            "labels": ["mac-book"],
            "capabilities": {"providers": ["codex"]},
            "last_heartbeat_at": "2026-05-03T00:00:01Z"
        });
        let second_label_newer = json!({
            "instance_id": "mac-mini-release",
            "status": "online",
            "labels": ["mac-mini"],
            "capabilities": {"providers": ["codex"]},
            "last_heartbeat_at": "2026-05-03T00:00:02Z"
        });
        let route = json!({"preferred": {"labels": ["mac-book", "mac-mini"]}});

        let candidates = select_capability_route(&[second_label_newer, first_label], &route);
        assert_eq!(
            candidates[0].decision.instance_id.as_deref(),
            Some("mac-book-release")
        );
        assert!(candidates[0].score > candidates[1].score);
    }

    #[test]
    fn equally_preferred_candidates_tie_break_by_latest_heartbeat() {
        let stale = json!({
            "instance_id": "mac-mini-release",
            "status": "online",
            "labels": ["mac"],
            "capabilities": {"providers": ["codex"]},
            "last_heartbeat_at": "2026-05-03T00:00:01Z"
        });
        let fresh = json!({
            "instance_id": "mac-book-release",
            "status": "online",
            "labels": ["mac"],
            "capabilities": {"providers": ["codex"]},
            "last_heartbeat_at": "2026-05-03T00:00:02Z"
        });
        let route = json!({"preferred": {"labels": ["mac"]}});

        let candidates = select_capability_route(&[stale, fresh], &route);
        assert_eq!(
            candidates[0].decision.instance_id.as_deref(),
            Some("mac-book-release")
        );
    }
}
