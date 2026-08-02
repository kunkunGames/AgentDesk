use axum::{
    Router,
    routing::{get, post},
};

use super::super::{ApiRouter, AppState, protected_api_domain, routines};

// Category: routines

pub(crate) fn router(state: AppState) -> ApiRouter {
    protected_api_domain(
        Router::new()
            .route(
                "/routines",
                get(routines::list_routines).post(routines::attach_routine),
            )
            .route("/routines/metrics", get(routines::routine_metrics))
            .route(
                "/routines/runs/search",
                get(routines::search_routine_run_results),
            )
            .route(
                "/routines/{id}",
                get(routines::get_routine)
                    .patch(routines::patch_routine)
                    .delete(routines::delete_routine),
            )
            .route("/routines/{id}/runs", get(routines::list_routine_runs))
            .route("/routines/{id}/pause", post(routines::pause_routine))
            .route("/routines/{id}/resume", post(routines::resume_routine))
            .route("/routines/{id}/detach", post(routines::detach_routine))
            .route("/routines/{id}/run-now", post(routines::run_routine_now))
            .route(
                "/routines/{id}/session/reset",
                post(routines::reset_routine_session),
            )
            .route(
                "/routines/{id}/session/kill",
                post(routines::kill_routine_session),
            ),
        state,
    )
}
