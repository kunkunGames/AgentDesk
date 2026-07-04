use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;

use super::AppState;

// ── Body types ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SkillUsageBody {
    pub skill_id: String,
    pub agent_id: Option<String>,
    pub role_id: Option<String>,
    pub session_key: Option<String>,
}

// ── Handlers ─────────────────────────────────────────────────

/// POST /api/hook/reset-status
pub async fn reset_status(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let Some(pool) = state.pg_pool_ref() else {
        return pg_unavailable();
    };
    match reset_status_pg(pool).await {
        Ok(updated) => (
            StatusCode::OK,
            Json(json!({"ok": true, "updated": updated})),
        ),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{error}")})),
        ),
    }
}

/// POST /api/hook/skill-usage
pub async fn skill_usage(
    State(state): State<AppState>,
    Json(body): Json<SkillUsageBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let Some(pool) = state.pg_pool_ref() else {
        return pg_unavailable();
    };
    match skill_usage_pg(pool, &body).await {
        Ok(id) => (StatusCode::OK, Json(json!({"ok": true, "id": id}))),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{error}")})),
        ),
    }
}

/// DELETE /api/hook/session/{sessionKey}
pub async fn disconnect_session(
    State(state): State<AppState>,
    Path(session_key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let Some(pool) = state.pg_pool_ref() else {
        return pg_unavailable();
    };
    match disconnect_session_pg(pool, &session_key).await {
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "session not found"})),
        ),
        Ok(true) => (
            StatusCode::OK,
            Json(json!({"ok": true, "session_key": session_key})),
        ),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{error}")})),
        ),
    }
}

fn pg_unavailable() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "postgres pool unavailable"})),
    )
}

async fn reset_status_pg(pool: &PgPool) -> Result<u64, sqlx::Error> {
    sqlx::query("UPDATE agents SET status = 'idle' WHERE status = 'working'")
        .execute(pool)
        .await
        .map(|result| result.rows_affected())
}

async fn skill_usage_pg(pool: &PgPool, body: &SkillUsageBody) -> Result<i64, sqlx::Error> {
    let agent_id = match body.agent_id.as_deref() {
        Some(agent_id) => Some(agent_id.to_string()),
        None => {
            if let Some(role_id) = body.role_id.as_deref() {
                sqlx::query_scalar("SELECT id FROM agents WHERE id = $1")
                    .bind(role_id)
                    .fetch_optional(pool)
                    .await?
            } else {
                None
            }
        }
    };

    sqlx::query_scalar(
        "INSERT INTO skill_usage (skill_id, agent_id, session_key, used_at)
         VALUES ($1, $2, $3, NOW())
         RETURNING id::BIGINT",
    )
    .bind(body.skill_id.as_str())
    .bind(agent_id.as_deref())
    .bind(body.session_key.as_deref())
    .fetch_one(pool)
    .await
}

async fn disconnect_session_pg(pool: &PgPool, session_key: &str) -> Result<bool, sqlx::Error> {
    sqlx::query("UPDATE sessions SET status = 'disconnected' WHERE session_key = $1")
        .bind(session_key)
        .execute(pool)
        .await
        .map(|result| result.rows_affected() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::routes::AppState;
    use crate::services::PolicyEngine;
    use axum::http::{Request, StatusCode};
    use axum::{Router, body::Body};
    use std::sync::Arc;
    use tower::ServiceExt;

    fn setup_test_app(pool: PgPool) -> Router {
        let state = AppState {
            pg_pool: Some(pool),
            engine: PolicyEngine::default(),
            config: Arc::new(crate::config::Config::default()),
            broadcast_tx: crate::server::ws::BroadcastTx::new(),
            batch_buffer: crate::server::ws::BatchBuffer::new(),
            health_registry: None,
            cluster_instance_id: None,
        };

        Router::new()
            .route("/api/hook/reset-status", axum::routing::post(reset_status))
            .route("/api/hook/skill-usage", axum::routing::post(skill_usage))
            .route("/api/hook/session/:sessionKey", axum::routing::delete(disconnect_session))
            .with_state(state)
    }

    #[sqlx::test]
    async fn test_reset_status_ok(pool: PgPool) {
        let app = setup_test_app(pool.clone());

        sqlx::query("INSERT INTO agents (id, status) VALUES ('agent_1', 'working')")
            .execute(&pool)
            .await
            .unwrap();

        let req = Request::builder()
            .method("POST")
            .uri("/api/hook/reset-status")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let status: String = sqlx::query_scalar("SELECT status FROM agents WHERE id = 'agent_1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "idle");
    }

    #[sqlx::test]
    async fn test_skill_usage_ok(pool: PgPool) {
        let app = setup_test_app(pool.clone());

        let req = Request::builder()
            .method("POST")
            .uri("/api/hook/skill-usage")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"skill_id": "test_skill"}"#))
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM skill_usage WHERE skill_id = 'test_skill'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[sqlx::test]
    async fn test_disconnect_session_ok(pool: PgPool) {
        let app = setup_test_app(pool.clone());

        sqlx::query("INSERT INTO sessions (session_key, status) VALUES ('sess_1', 'connected')")
            .execute(&pool)
            .await
            .unwrap();

        let req = Request::builder()
            .method("DELETE")
            .uri("/api/hook/session/sess_1")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let status: String = sqlx::query_scalar("SELECT status FROM sessions WHERE session_key = 'sess_1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "disconnected");
    }

    #[sqlx::test]
    async fn test_disconnect_session_not_found(pool: PgPool) {
        let app = setup_test_app(pool);
        let req = Request::builder()
            .method("DELETE")
            .uri("/api/hook/session/nonexistent")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
