use axum::{Router, routing::get};

use super::super::{ApiRouter, auth, health_api, kakao, public_api_domain};

// Category: access

pub(crate) fn router() -> ApiRouter {
    public_api_domain(
        Router::new()
            .route("/health", get(health_api::health_handler))
            .route("/auth/session", get(auth::get_session))
            .route("/kakao/oauth/callback", get(kakao::oauth_callback)),
    )
}
