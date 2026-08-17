use axum::{
    Router,
    routing::{get, post},
};

use super::super::{ApiRouter, AppState, cluster, protected_api_domain};

// Category: cluster

pub(crate) fn router(state: AppState) -> ApiRouter {
    protected_api_domain(
        Router::new()
            .route("/cluster/nodes", get(cluster::list_nodes))
            .route("/cluster/sessions", get(cluster::list_sessions))
            .route(
                "/cluster/routing-diagnostics",
                get(cluster::routing_diagnostics),
            )
            .route("/cluster/resource-locks", get(cluster::list_resource_locks))
            .route(
                "/cluster/resource-locks/acquire",
                post(cluster::acquire_resource_lock),
            )
            .route(
                "/cluster/resource-locks/heartbeat",
                post(cluster::heartbeat_resource_lock),
            )
            .route(
                "/cluster/resource-locks/release",
                post(cluster::release_resource_lock),
            )
            .route(
                "/cluster/resource-locks/reclaim-expired",
                post(cluster::reclaim_expired_resource_locks),
            )
            .route(
                "/cluster/test-phase-runs",
                get(cluster::list_test_phase_runs),
            )
            .route(
                "/cluster/test-phase-runs/upsert",
                post(cluster::upsert_test_phase_run),
            )
            .route(
                "/cluster/test-phase-runs/start",
                post(cluster::start_test_phase_run),
            )
            .route(
                "/cluster/test-phase-runs/complete",
                post(cluster::complete_test_phase_run),
            )
            .route(
                "/cluster/test-phase-runs/evidence",
                get(cluster::latest_test_phase_evidence),
            )
            .route(
                "/cluster/task-dispatches/claim",
                post(cluster::claim_task_dispatches),
            ),
        state,
    )
}
