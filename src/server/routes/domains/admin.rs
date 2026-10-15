use axum::{
    Router,
    extract::DefaultBodyLimit,
    middleware::map_response,
    routing::{get, patch, post},
};

use super::super::{
    ApiRouter, AppState, analytics, campaigns, departments, escalation, home_metrics,
    protected_api_domain, settings, stats, voice_config,
};

// Category: admin

/// The ledger PUT carries the whole DAG, so only these routes raise the body limit.
fn campaign_router() -> ApiRouter {
    Router::new()
        .route("/campaigns", get(campaigns::list).post(campaigns::create))
        .route("/campaigns/graph-schema", get(campaigns::graph_schema))
        .route(
            "/campaigns/{id}",
            get(campaigns::get).put(campaigns::replace),
        )
        .route("/campaigns/{id}/history", get(campaigns::history))
        .route(
            "/campaigns/{id}/execution",
            get(campaigns::execution).post(campaigns::enroll_execution),
        )
        .route(
            "/campaigns/{id}/execution/effect-ownership",
            get(campaigns::effect_ownership),
        )
        .route(
            "/campaigns/{id}/execution/activate",
            post(campaigns::activate_graph),
        )
        .route(
            "/campaigns/{id}/execution/manager",
            post(campaigns::configure_manager),
        )
        .route(
            "/campaigns/{id}/execution/reconcile",
            post(campaigns::reconcile_graph),
        )
        .route(
            "/campaigns/{id}/execution/replan",
            post(campaigns::request_replan),
        )
        .route(
            "/campaigns/{id}/execution/recover-finish",
            post(campaigns::recover_finish),
        )
        .route(
            "/campaigns/{id}/execution/schedule",
            post(campaigns::activate_schedule),
        )
        .route(
            "/campaigns/{id}/execution/revise",
            post(campaigns::revise_goal),
        )
        .route(
            "/campaigns/{id}/execution/budget",
            post(campaigns::extend_budget),
        )
        .route(
            "/campaigns/{id}/operations",
            get(campaigns::operations).post(campaigns::adopt_operation),
        )
        .layer(DefaultBodyLimit::max(campaigns::LEDGER_BODY_LIMIT_BYTES))
        .layer(map_response(campaigns::body_limit_envelope))
}

pub(crate) fn router(state: AppState) -> ApiRouter {
    protected_api_domain(
        Router::new()
            .merge(campaign_router())
            .route(
                "/managed-checks/{id}",
                get(super::super::managed_checks::inspect),
            )
            .route(
                "/managed-checks/{id}/enroll",
                post(super::super::managed_checks::enroll),
            )
            .route(
                "/managed-checks/{id}/approve",
                post(super::super::managed_checks::approve),
            )
            .route(
                "/departments",
                get(departments::list_departments).post(departments::create_department),
            )
            .route(
                "/departments/reorder",
                patch(departments::reorder_departments),
            )
            .route(
                "/departments/{id}",
                patch(departments::update_department).delete(departments::delete_department),
            )
            .route("/stats", get(stats::get_stats))
            .route("/stats/memento", get(stats::get_memento_stats))
            .route(
                "/settings",
                get(settings::get_settings).put(settings::put_settings),
            )
            .route(
                "/settings/config",
                get(settings::get_config_entries).patch(settings::patch_config_entries),
            )
            .route(
                "/settings/runtime-config",
                get(settings::get_runtime_config).put(settings::put_runtime_config),
            )
            .route(
                "/settings/operator-connectors",
                get(settings::get_operator_connectors),
            )
            .route(
                "/settings/escalation",
                get(escalation::get_escalation_settings).put(escalation::put_escalation_settings),
            )
            .route(
                "/voice/config",
                get(voice_config::get_voice_config).put(voice_config::put_voice_config),
            )
            .route(
                "/internal/escalation/emit",
                post(escalation::emit_escalation),
            ),
        state,
    )
}
