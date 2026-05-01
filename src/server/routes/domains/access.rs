use axum::{
    Router,
    routing::{get, post},
};

use super::super::{ApiRouter, auth, health_api, public_api_domain};

// Category: ops and integrations

pub(crate) fn router() -> ApiRouter {
    public_api_domain(
        Router::new()
            .route("/health", get(health_api::health_handler))
            .route("/discord/send", post(health_api::send_handler))
            .route(
                "/discord/send-to-agent",
                post(health_api::send_to_agent_handler),
            )
            .route("/discord/send-dm", post(health_api::senddm_handler))
            .route(
                "/inflight/rebind",
                post(health_api::rebind_inflight_handler),
            )
            .route("/auth/session", get(auth::get_session)),
    )
}
