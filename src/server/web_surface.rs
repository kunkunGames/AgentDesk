//! Browser entry points for both runtime profiles. Runner redirects to the Hub
//! without provisioning or serving the full dashboard assets.

use std::path::Path;

use axum::{
    Router,
    extract::{OriginalUri, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
};
use tower_http::services::{ServeDir, ServeFile};

use super::{dashboard_auth::DashboardAccess, routes, ws};

mod hub_redirect;

pub(super) fn router(
    state: routes::AppState,
    dashboard_dir: &Path,
    include_hook_receiver: bool,
) -> Router {
    let dashboard_enabled = state.config.cluster.runtime_profile.modules().dashboard;
    let access = DashboardAccess::new(&state.config);
    let mut app = Router::new();
    if dashboard_enabled {
        app = app.route(
            "/ws",
            get(ws::ws_handler).with_state((state.broadcast_tx.clone(), access.clone())),
        );
    }
    app = app.nest(
        "/api",
        routes::api_router_with_dashboard_access(state.clone(), access),
    );
    if include_hook_receiver {
        app = app.merge(
            crate::services::claude_tui::hook_server::hook_receiver_router().route_layer(
                axum::middleware::from_fn_with_state(state.clone(), routes::auth::auth_middleware),
            ),
        );
    }
    // The event-driven TUI wait path is needed even before hook publication.
    // Apply auth only to matched routes so unknown paths retain their 404.
    app = app.merge(
        crate::services::claude_tui::tui_relay::router().route_layer(
            axum::middleware::from_fn_with_state(state.clone(), routes::auth::auth_middleware),
        ),
    );
    if dashboard_enabled {
        return app.fallback_service(
            ServeDir::new(dashboard_dir)
                .append_index_html_on_directories(true)
                .fallback(ServeFile::new(dashboard_dir.join("index.html"))),
        );
    }
    app.route("/", get(runner_entry).with_state(state.clone()))
        .route("/settings", get(runner_entry).with_state(state))
}

async fn runner_entry(
    State(state): State<routes::AppState>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let target = if let Some(pool) = state.pg_pool.as_ref() {
        // An unavailable database must not leave address-bar navigation hanging.
        match tokio::time::timeout(
            std::time::Duration::from_secs(2),
            hub_redirect::online_hubs(pool, state.config.cluster.lease_ttl_secs),
        )
        .await
        {
            Ok(Ok(nodes)) => hub_redirect::destination(
                &state.config.cluster,
                state.cluster_instance_id.as_deref(),
                &nodes,
                &uri,
            ),
            _ => None,
        }
    } else {
        None
    };
    runner_response(target.as_deref())
}

fn runner_response(target: Option<&str>) -> Response {
    let response = match target {
        Some(target) => Redirect::temporary(target).into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Html(include_str!("web_surface/runner.html")),
        )
            .into_response(),
    };
    (
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        response,
    )
        .into_response()
}

#[cfg(test)]
mod tests;
