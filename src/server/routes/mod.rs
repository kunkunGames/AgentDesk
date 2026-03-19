use axum::{
    Router,
    extract::{Path, State},
    routing::get,
    Json,
};
use serde_json::json;

use crate::db::Db;
use crate::engine::PolicyEngine;

/// Shared application state passed to all route handlers.
#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub engine: PolicyEngine,
}

pub fn api_router(db: Db, engine: PolicyEngine) -> Router {
    let state = AppState { db, engine };

    Router::new()
        .route("/health", get(health))
        .route("/agents", get(list_agents))
        .route("/agents/{id}", get(get_agent))
        .route("/sessions", get(list_sessions))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    let db_ok = state
        .db
        .lock()
        .map(|conn| conn.execute_batch("SELECT 1").is_ok())
        .unwrap_or(false);

    Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        "db": db_ok
    }))
}

async fn list_agents(State(state): State<AppState>) -> Json<serde_json::Value> {
    let agents = match state.db.lock() {
        Ok(conn) => {
            let mut stmt = match conn.prepare(
                "SELECT id, name, name_ko, provider, department, avatar_emoji,
                        discord_channel_id, discord_channel_alt, status, xp
                 FROM agents ORDER BY id",
            ) {
                Ok(s) => s,
                Err(e) => {
                    return Json(json!({ "error": format!("query prepare failed: {e}") }));
                }
            };

            let rows = stmt
                .query_map([], |row| {
                    Ok(json!({
                        "id": row.get::<_, String>(0)?,
                        "name": row.get::<_, String>(1)?,
                        "name_ko": row.get::<_, Option<String>>(2)?,
                        "provider": row.get::<_, Option<String>>(3)?,
                        "department": row.get::<_, Option<String>>(4)?,
                        "avatar_emoji": row.get::<_, Option<String>>(5)?,
                        "discord_channel_id": row.get::<_, Option<String>>(6)?,
                        "discord_channel_alt": row.get::<_, Option<String>>(7)?,
                        "status": row.get::<_, Option<String>>(8)?,
                        "xp": row.get::<_, i64>(9)?,
                    }))
                })
                .ok();

            match rows {
                Some(iter) => iter.filter_map(|r| r.ok()).collect::<Vec<_>>(),
                None => Vec::new(),
            }
        }
        Err(_) => Vec::new(),
    };

    Json(json!({ "agents": agents }))
}

async fn get_agent(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    match state.db.lock() {
        Ok(conn) => {
            let result = conn.query_row(
                "SELECT id, name, name_ko, provider, department, avatar_emoji,
                        discord_channel_id, discord_channel_alt, status, xp
                 FROM agents WHERE id = ?1",
                [&id],
                |row| {
                    Ok(json!({
                        "id": row.get::<_, String>(0)?,
                        "name": row.get::<_, String>(1)?,
                        "name_ko": row.get::<_, Option<String>>(2)?,
                        "provider": row.get::<_, Option<String>>(3)?,
                        "department": row.get::<_, Option<String>>(4)?,
                        "avatar_emoji": row.get::<_, Option<String>>(5)?,
                        "discord_channel_id": row.get::<_, Option<String>>(6)?,
                        "discord_channel_alt": row.get::<_, Option<String>>(7)?,
                        "status": row.get::<_, Option<String>>(8)?,
                        "xp": row.get::<_, i64>(9)?,
                    }))
                },
            );

            match result {
                Ok(agent) => Json(json!({ "agent": agent })),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    Json(json!({ "error": "agent not found" }))
                }
                Err(e) => Json(json!({ "error": format!("query failed: {e}") })),
            }
        }
        Err(_) => Json(json!({ "error": "db lock failed" })),
    }
}

async fn list_sessions(State(state): State<AppState>) -> Json<serde_json::Value> {
    let sessions = match state.db.lock() {
        Ok(conn) => {
            let mut stmt = match conn.prepare(
                "SELECT id, session_key, agent_id, provider, status, active_dispatch_id,
                        model, tokens, cwd, last_heartbeat
                 FROM sessions
                 WHERE status IN ('connected', 'working', 'idle')
                 ORDER BY id",
            ) {
                Ok(s) => s,
                Err(e) => {
                    return Json(json!({ "error": format!("query prepare failed: {e}") }));
                }
            };

            let rows = stmt
                .query_map([], |row| {
                    Ok(json!({
                        "id": row.get::<_, i64>(0)?,
                        "session_key": row.get::<_, Option<String>>(1)?,
                        "agent_id": row.get::<_, Option<String>>(2)?,
                        "provider": row.get::<_, Option<String>>(3)?,
                        "status": row.get::<_, Option<String>>(4)?,
                        "active_dispatch_id": row.get::<_, Option<String>>(5)?,
                        "model": row.get::<_, Option<String>>(6)?,
                        "tokens": row.get::<_, i64>(7)?,
                        "cwd": row.get::<_, Option<String>>(8)?,
                        "last_heartbeat": row.get::<_, Option<String>>(9)?,
                    }))
                })
                .ok();

            match rows {
                Some(iter) => iter.filter_map(|r| r.ok()).collect::<Vec<_>>(),
                None => Vec::new(),
            }
        }
        Err(_) => Vec::new(),
    };

    Json(json!({ "sessions": sessions }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;

    fn test_db() -> Db {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        crate::db::schema::migrate(&conn).unwrap();
        Arc::new(Mutex::new(conn))
    }

    fn test_engine(db: &Db) -> PolicyEngine {
        let config = crate::config::Config::default();
        PolicyEngine::new(&config, db.clone()).unwrap()
    }

    #[tokio::test]
    async fn health_returns_ok_with_db_status() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], true);
        assert_eq!(json["db"], true);
        assert_eq!(json["version"], env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn agents_empty_list() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(Request::builder().uri("/agents").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["agents"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn agents_returns_synced_agents() {
        let db = test_db();
        let engine = test_engine(&db);

        // Insert an agent
        {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO agents (id, name, provider, status, xp) VALUES ('a1', 'Agent1', 'claude', 'idle', 0)",
                [],
            )
            .unwrap();
        }

        let app = api_router(db, engine);
        let response = app
            .oneshot(Request::builder().uri("/agents").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let agents = json["agents"].as_array().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0]["id"], "a1");
        assert_eq!(agents[0]["name"], "Agent1");
    }

    #[tokio::test]
    async fn get_agent_found() {
        let db = test_db();
        let engine = test_engine(&db);

        {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO agents (id, name, provider, status, xp) VALUES ('a1', 'Agent1', 'claude', 'idle', 0)",
                [],
            )
            .unwrap();
        }

        let app = api_router(db, engine);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/agents/a1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["agent"]["id"], "a1");
    }

    #[tokio::test]
    async fn get_agent_not_found() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/agents/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "agent not found");
    }

    #[tokio::test]
    async fn sessions_empty_list() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["sessions"].as_array().unwrap().is_empty());
    }
}
