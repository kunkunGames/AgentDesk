pub mod kanban;
pub mod dispatches;
pub mod pipeline;
pub mod github;

use axum::{
    Router,
    extract::{Path, State},
    routing::{get, post, delete},
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
        .route("/policies", get(list_policies))
        // Kanban
        .route("/kanban-cards", get(kanban::list_cards).post(kanban::create_card))
        .route("/kanban-cards/{id}", get(kanban::get_card).patch(kanban::update_card))
        .route("/kanban-cards/{id}/assign", post(kanban::assign_card))
        // Dispatches
        .route("/dispatches", get(dispatches::list_dispatches).post(dispatches::create_dispatch))
        .route("/dispatches/{id}", get(dispatches::get_dispatch).patch(dispatches::update_dispatch))
        // Pipeline stages
        .route("/pipeline-stages", get(pipeline::list_stages).post(pipeline::create_stage))
        .route("/pipeline-stages/{id}", delete(pipeline::delete_stage))
        // GitHub repos
        .route("/github/repos", get(github::list_repos).post(github::register_repo))
        .route("/github/repos/{owner}/{repo}/sync", post(github::sync_repo))
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
                        "xp": row.get::<_, f64>(9).unwrap_or(0.0) as i64,
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
                        "xp": row.get::<_, f64>(9).unwrap_or(0.0) as i64,
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

async fn list_policies(State(state): State<AppState>) -> Json<serde_json::Value> {
    let policies = state.engine.list_policies();
    let items: Vec<serde_json::Value> = policies
        .into_iter()
        .map(|p| {
            json!({
                "name": p.name,
                "file": p.file,
                "priority": p.priority,
                "hooks": p.hooks,
            })
        })
        .collect();
    Json(json!({ "policies": items }))
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

    // ── Kanban CRUD tests ──────────────────────────────────────────

    #[tokio::test]
    async fn kanban_create_card() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/kanban-cards")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"title":"Test Card","priority":"high"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["card"]["title"], "Test Card");
        assert_eq!(json["card"]["priority"], "high");
        assert_eq!(json["card"]["status"], "backlog");
        assert!(json["card"]["id"].as_str().unwrap().len() > 10); // UUID
    }

    #[tokio::test]
    async fn kanban_list_cards_empty() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(Request::builder().uri("/kanban-cards").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["cards"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn kanban_list_cards_with_filter() {
        let db = test_db();
        let engine = test_engine(&db);

        {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO kanban_cards (id, title, status, priority, created_at, updated_at) VALUES ('c1', 'Card1', 'backlog', 'medium', datetime('now'), datetime('now'))",
                [],
            ).unwrap();
            conn.execute(
                "INSERT INTO kanban_cards (id, title, status, priority, created_at, updated_at) VALUES ('c2', 'Card2', 'ready', 'high', datetime('now'), datetime('now'))",
                [],
            ).unwrap();
        }

        let app = api_router(db, engine);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/kanban-cards?status=ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let cards = json["cards"].as_array().unwrap();
        assert_eq!(cards.len(), 1);
        assert_eq!(cards[0]["id"], "c2");
    }

    #[tokio::test]
    async fn kanban_get_card() {
        let db = test_db();
        let engine = test_engine(&db);

        {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO kanban_cards (id, title, status, priority, created_at, updated_at) VALUES ('c1', 'Card1', 'backlog', 'medium', datetime('now'), datetime('now'))",
                [],
            ).unwrap();
        }

        let app = api_router(db, engine);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/kanban-cards/c1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["card"]["id"], "c1");
        assert_eq!(json["card"]["title"], "Card1");
    }

    #[tokio::test]
    async fn kanban_get_card_not_found() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/kanban-cards/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn kanban_update_card_status() {
        let db = test_db();
        let engine = test_engine(&db);

        {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO kanban_cards (id, title, status, priority, created_at, updated_at) VALUES ('c1', 'Card1', 'backlog', 'medium', datetime('now'), datetime('now'))",
                [],
            ).unwrap();
        }

        let app = api_router(db, engine);
        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/kanban-cards/c1")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"status":"ready"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["card"]["status"], "ready");
    }

    #[tokio::test]
    async fn kanban_update_card_not_found() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/kanban-cards/nonexistent")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"status":"ready"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn kanban_assign_card() {
        let db = test_db();
        let engine = test_engine(&db);

        {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO agents (id, name, provider, status, xp) VALUES ('ch-td', 'Agent TD', 'claude', 'idle', 0)",
                [],
            ).unwrap();
            conn.execute(
                "INSERT INTO kanban_cards (id, title, status, priority, created_at, updated_at) VALUES ('c1', 'Card1', 'backlog', 'medium', datetime('now'), datetime('now'))",
                [],
            ).unwrap();
        }

        let app = api_router(db, engine);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/kanban-cards/c1/assign")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"agent_id":"ch-td"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["card"]["status"], "ready");
        assert_eq!(json["card"]["assigned_agent_id"], "ch-td");
    }

    #[tokio::test]
    async fn kanban_assign_card_not_found() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/kanban-cards/nonexistent/assign")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"agent_id":"ch-td"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ── Dispatch API tests ─────────────────────────────────────────

    #[tokio::test]
    async fn dispatch_list_empty() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(Request::builder().uri("/dispatches").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["dispatches"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dispatch_create_and_get() {
        let db = test_db();
        let engine = test_engine(&db);

        {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO kanban_cards (id, title, status, priority, created_at, updated_at) VALUES ('c1', 'Card1', 'ready', 'medium', datetime('now'), datetime('now'))",
                [],
            ).unwrap();
        }

        let app = api_router(db.clone(), engine.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/dispatches")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"kanban_card_id":"c1","to_agent_id":"ch-td","title":"Do it"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let dispatch_id = json["dispatch"]["id"].as_str().unwrap().to_string();
        assert_eq!(json["dispatch"]["status"], "pending");
        assert_eq!(json["dispatch"]["kanban_card_id"], "c1");

        // Card should be "requested"
        let conn = db.lock().unwrap();
        let card_status: String = conn
            .query_row("SELECT status FROM kanban_cards WHERE id = 'c1'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(card_status, "requested");
        drop(conn);

        // GET single dispatch
        let app2 = api_router(db, engine);
        let response2 = app2
            .oneshot(
                Request::builder()
                    .uri(&format!("/dispatches/{dispatch_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response2.status(), StatusCode::OK);
        let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await.unwrap();
        let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
        assert_eq!(json2["dispatch"]["id"], dispatch_id);
    }

    #[tokio::test]
    async fn dispatch_create_card_not_found() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/dispatches")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"kanban_card_id":"nope","to_agent_id":"ch-td","title":"Do it"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn dispatch_complete() {
        let db = test_db();
        let engine = test_engine(&db);

        {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO kanban_cards (id, title, status, priority, created_at, updated_at) VALUES ('c1', 'Card1', 'ready', 'medium', datetime('now'), datetime('now'))",
                [],
            ).unwrap();
        }

        // Create dispatch
        let app = api_router(db.clone(), engine.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/dispatches")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"kanban_card_id":"c1","to_agent_id":"ch-td","title":"Do it"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let dispatch_id = json["dispatch"]["id"].as_str().unwrap().to_string();

        // Complete dispatch
        let app2 = api_router(db, engine);
        let response2 = app2
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(&format!("/dispatches/{dispatch_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"status":"completed","result":{"ok":true}}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response2.status(), StatusCode::OK);
        let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await.unwrap();
        let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
        assert_eq!(json2["dispatch"]["status"], "completed");
    }

    #[tokio::test]
    async fn dispatch_get_not_found() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dispatches/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ── Policy hook firing tests ───────────────────────────────────

    #[tokio::test]
    async fn kanban_terminal_status_fires_hook() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("test-hooks.js"),
            r#"
            var p = {
                name: "test-hooks",
                priority: 1,
                onCardTransition: function(payload) {
                    agentdesk.db.execute(
                        "INSERT OR REPLACE INTO kv_meta (key, value) VALUES ('transition', '" + payload.from + "->" + payload.to + "')",
                        []
                    );
                },
                onCardTerminal: function(payload) {
                    agentdesk.db.execute(
                        "INSERT OR REPLACE INTO kv_meta (key, value) VALUES ('terminal', '" + payload.card_id + ":" + payload.status + "')",
                        []
                    );
                }
            };
            agentdesk.registerPolicy(p);
            "#,
        ).unwrap();

        let db = test_db();
        let config = crate::config::Config {
            policies: crate::config::PoliciesConfig {
                dir: dir.path().to_path_buf(),
                hot_reload: false,
            },
            ..crate::config::Config::default()
        };
        let engine = PolicyEngine::new(&config, db.clone()).unwrap();

        {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO kanban_cards (id, title, status, priority, created_at, updated_at) VALUES ('c1', 'Card1', 'review', 'medium', datetime('now'), datetime('now'))",
                [],
            ).unwrap();
        }

        let app = api_router(db.clone(), engine);
        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/kanban-cards/c1")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"status":"done"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let conn = db.lock().unwrap();
        let transition: String = conn
            .query_row("SELECT value FROM kv_meta WHERE key = 'transition'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(transition, "review->done");

        let terminal: String = conn
            .query_row("SELECT value FROM kv_meta WHERE key = 'terminal'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(terminal, "c1:done");
    }

    #[tokio::test]
    async fn dispatch_list_with_filter() {
        let db = test_db();
        let engine = test_engine(&db);

        {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO kanban_cards (id, title, status, priority, created_at, updated_at) VALUES ('c1', 'Card1', 'ready', 'medium', datetime('now'), datetime('now'))",
                [],
            ).unwrap();
            conn.execute(
                "INSERT INTO task_dispatches (id, kanban_card_id, to_agent_id, status, title, created_at, updated_at) VALUES ('d1', 'c1', 'ag1', 'pending', 'T1', datetime('now'), datetime('now'))",
                [],
            ).unwrap();
            conn.execute(
                "INSERT INTO task_dispatches (id, kanban_card_id, to_agent_id, status, title, created_at, updated_at) VALUES ('d2', 'c1', 'ag1', 'completed', 'T2', datetime('now'), datetime('now'))",
                [],
            ).unwrap();
        }

        let app = api_router(db, engine);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dispatches?status=pending")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let dispatches = json["dispatches"].as_array().unwrap();
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0]["id"], "d1");
    }

    // ── GitHub Repos API tests ────────────────────────────────────

    #[tokio::test]
    async fn github_repos_empty_list() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(Request::builder().uri("/github/repos").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["repos"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn github_repos_register_and_list() {
        let db = test_db();
        let engine = test_engine(&db);

        // Register
        let app = api_router(db.clone(), engine.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/github/repos")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"id":"owner/repo1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["repo"]["id"], "owner/repo1");

        // List
        let app2 = api_router(db, engine);
        let response2 = app2
            .oneshot(Request::builder().uri("/github/repos").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await.unwrap();
        let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
        assert_eq!(json2["repos"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn github_repos_register_bad_format() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/github/repos")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"id":"noslash"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn github_repos_sync_not_registered() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/github/repos/unknown/repo/sync")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ── Pipeline Stages API tests ─────────────────────────────────

    #[tokio::test]
    async fn pipeline_stages_empty_list() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(Request::builder().uri("/pipeline-stages").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["stages"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pipeline_stages_create_and_list() {
        let db = test_db();
        let engine = test_engine(&db);

        // Create
        let app = api_router(db.clone(), engine.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/pipeline-stages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"repo_id":"owner/repo","stage_name":"qa-test","stage_order":1,"trigger_after":"review_pass","entry_skill":"test","timeout_minutes":60}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["stage"]["stage_name"], "qa-test");
        assert_eq!(json["stage"]["trigger_after"], "review_pass");
        assert_eq!(json["stage"]["timeout_minutes"], 60);
        let stage_id = json["stage"]["id"].as_i64().unwrap();

        // List with filter
        let app2 = api_router(db.clone(), engine.clone());
        let response2 = app2
            .oneshot(
                Request::builder()
                    .uri("/pipeline-stages?repo_id=owner/repo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await.unwrap();
        let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
        assert_eq!(json2["stages"].as_array().unwrap().len(), 1);

        // Delete
        let app3 = api_router(db, engine);
        let response3 = app3
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(&format!("/pipeline-stages/{stage_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response3.status(), StatusCode::OK);
        let body3 = axum::body::to_bytes(response3.into_body(), usize::MAX).await.unwrap();
        let json3: serde_json::Value = serde_json::from_slice(&body3).unwrap();
        assert_eq!(json3["deleted"], true);
    }

    #[tokio::test]
    async fn pipeline_stages_delete_not_found() {
        let db = test_db();
        let engine = test_engine(&db);
        let app = api_router(db, engine);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/pipeline-stages/9999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn pipeline_stages_list_filtered_by_repo() {
        let db = test_db();
        let engine = test_engine(&db);

        {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO pipeline_stages (repo_id, stage_name, stage_order, trigger_after, timeout_minutes) VALUES ('repo-a', 'test', 1, 'review_pass', 30)",
                [],
            ).unwrap();
            conn.execute(
                "INSERT INTO pipeline_stages (repo_id, stage_name, stage_order, trigger_after, timeout_minutes) VALUES ('repo-b', 'deploy', 1, 'review_pass', 60)",
                [],
            ).unwrap();
        }

        let app = api_router(db, engine);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/pipeline-stages?repo_id=repo-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let stages = json["stages"].as_array().unwrap();
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0]["stage_name"], "test");
    }
}
