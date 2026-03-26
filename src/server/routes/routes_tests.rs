use super::*;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

fn test_db() -> Db {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    crate::db::schema::migrate(&conn).unwrap();
    crate::db::wrap_conn(conn)
}

fn test_engine(db: &Db) -> PolicyEngine {
    let config = crate::config::Config::default();
    PolicyEngine::new(&config, db.clone()).unwrap()
}

#[tokio::test]
async fn health_returns_ok_with_db_status() {
    let db = test_db();
    let engine = test_engine(&db);
    let app = api_router(db, engine, None);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
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
    assert_eq!(json["ok"], true);
    assert_eq!(json["db"], true);
    assert_eq!(json["version"], env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn agents_empty_list() {
    let db = test_db();
    let engine = test_engine(&db);
    let app = api_router(db, engine, None);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/agents")
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

    let app = api_router(db, engine, None);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/agents")
                .body(Body::empty())
                .unwrap(),
        )
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
async fn agents_include_current_thread_channel_id_from_working_session() {
    let db = test_db();
    let engine = test_engine(&db);

    {
        let conn = db.lock().unwrap();
        conn.execute(
                "INSERT INTO agents (id, name, provider, status, xp) VALUES ('a1', 'Agent1', 'codex', 'idle', 0)",
                [],
            )
            .unwrap();
        conn.execute(
                "INSERT INTO sessions (session_key, agent_id, provider, status, thread_channel_id, last_heartbeat)
                 VALUES (?1, 'a1', 'codex', 'working', '1485506232256168011', datetime('now'))",
                ["mac-mini:AgentDesk-codex-adk-cdx-t1485506232256168011"],
            )
            .unwrap();
    }

    let app = api_router(db, engine, None);

    let list_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/agents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let list_body = axum::body::to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let list_json: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    assert_eq!(
        list_json["agents"][0]["current_thread_channel_id"],
        serde_json::Value::String("1485506232256168011".to_string())
    );

    let get_response = app
        .oneshot(
            Request::builder()
                .uri("/agents/a1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let get_json: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(
        get_json["agent"]["current_thread_channel_id"],
        serde_json::Value::String("1485506232256168011".to_string())
    );
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

    let app = api_router(db, engine, None);
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
    let app = api_router(db, engine, None);

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
    let app = api_router(db, engine, None);

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
    let app = api_router(db, engine, None);

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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
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
    let app = api_router(db, engine, None);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/kanban-cards")
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

    let app = api_router(db, engine, None);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/kanban-cards?status=ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
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

    let app = api_router(db, engine, None);
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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["card"]["id"], "c1");
    assert_eq!(json["card"]["title"], "Card1");
}

#[tokio::test]
async fn kanban_get_card_not_found() {
    let db = test_db();
    let engine = test_engine(&db);
    let app = api_router(db, engine, None);

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

    let app = api_router(db, engine, None);
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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["card"]["status"], "ready");
}

#[tokio::test]
async fn kanban_update_card_not_found() {
    let db = test_db();
    let engine = test_engine(&db);
    let app = api_router(db, engine, None);

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

    let app = api_router(db, engine, None);
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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["card"]["status"], "ready");
    assert_eq!(json["card"]["assigned_agent_id"], "ch-td");
}

#[tokio::test]
async fn kanban_assign_card_not_found() {
    let db = test_db();
    let engine = test_engine(&db);
    let app = api_router(db, engine, None);

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
    let app = api_router(db, engine, None);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/dispatches")
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

    let app = api_router(db.clone(), engine.clone(), None);
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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let dispatch_id = json["dispatch"]["id"].as_str().unwrap().to_string();
    assert_eq!(json["dispatch"]["status"], "pending");
    assert_eq!(json["dispatch"]["kanban_card_id"], "c1");

    // Card should be "requested"
    let conn = db.lock().unwrap();
    let card_status: String = conn
        .query_row("SELECT status FROM kanban_cards WHERE id = 'c1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(card_status, "requested");
    drop(conn);

    // GET single dispatch
    let app2 = api_router(db, engine, None);
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
    let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX)
        .await
        .unwrap();
    let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
    assert_eq!(json2["dispatch"]["id"], dispatch_id);
}

#[tokio::test]
async fn dispatch_create_card_not_found() {
    let db = test_db();
    let engine = test_engine(&db);
    let app = api_router(db, engine, None);

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
    let app = api_router(db.clone(), engine.clone(), None);
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

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let dispatch_id = json["dispatch"]["id"].as_str().unwrap().to_string();

    // Complete dispatch
    let app2 = api_router(db, engine, None);
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
    let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX)
        .await
        .unwrap();
    let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
    assert_eq!(json2["dispatch"]["status"], "completed");
}

#[tokio::test]
async fn dispatch_get_not_found() {
    let db = test_db();
    let engine = test_engine(&db);
    let app = api_router(db, engine, None);

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
        // Need an active dispatch for the transition guard (#48)
        conn.execute(
                "INSERT INTO task_dispatches (id, kanban_card_id, dispatch_type, status, title, created_at, updated_at) VALUES ('d1', 'c1', 'review', 'pending', 'Review', datetime('now'), datetime('now'))",
                [],
            ).unwrap();
    }

    let app = api_router(db.clone(), engine, None);
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
        .query_row(
            "SELECT value FROM kv_meta WHERE key = 'transition'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(transition, "review->done");

    let terminal: String = conn
        .query_row(
            "SELECT value FROM kv_meta WHERE key = 'terminal'",
            [],
            |r| r.get(0),
        )
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

    let app = api_router(db, engine, None);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/dispatches?status=pending")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
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
    let app = api_router(db, engine, None);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/github/repos")
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
    assert!(json["repos"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn github_repos_register_and_list() {
    let db = test_db();
    let engine = test_engine(&db);

    // Register
    let app = api_router(db.clone(), engine.clone(), None);
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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["repo"]["id"], "owner/repo1");

    // List
    let app2 = api_router(db, engine, None);
    let response2 = app2
        .oneshot(
            Request::builder()
                .uri("/github/repos")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX)
        .await
        .unwrap();
    let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
    assert_eq!(json2["repos"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn github_repos_register_bad_format() {
    let db = test_db();
    let engine = test_engine(&db);
    let app = api_router(db, engine, None);

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
    let app = api_router(db, engine, None);

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
    let app = api_router(db, engine, None);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/pipeline-stages")
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
    assert!(json["stages"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn pipeline_stages_create_and_list() {
    let db = test_db();
    let engine = test_engine(&db);

    // Create
    let app = api_router(db.clone(), engine.clone(), None);
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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["stage"]["stage_name"], "qa-test");
    assert_eq!(json["stage"]["trigger_after"], "review_pass");
    assert_eq!(json["stage"]["timeout_minutes"], 60);
    let stage_id = json["stage"]["id"].as_i64().unwrap();

    // List with filter
    let app2 = api_router(db.clone(), engine.clone(), None);
    let response2 = app2
        .oneshot(
            Request::builder()
                .uri("/pipeline-stages?repo_id=owner/repo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX)
        .await
        .unwrap();
    let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
    assert_eq!(json2["stages"].as_array().unwrap().len(), 1);

    // Delete
    let app3 = api_router(db, engine, None);
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
    let body3 = axum::body::to_bytes(response3.into_body(), usize::MAX)
        .await
        .unwrap();
    let json3: serde_json::Value = serde_json::from_slice(&body3).unwrap();
    assert_eq!(json3["deleted"], true);
}

#[tokio::test]
async fn pipeline_stages_delete_not_found() {
    let db = test_db();
    let engine = test_engine(&db);
    let app = api_router(db, engine, None);

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

    let app = api_router(db, engine, None);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/pipeline-stages?repo_id=repo-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let stages = json["stages"].as_array().unwrap();
    assert_eq!(stages.len(), 1);
    assert_eq!(stages[0]["stage_name"], "test");
}

// ── force-transition auth tests ──

fn seed_card_with_status(db: &Db, card_id: &str, status: &str) {
    let conn = db.lock().unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO kanban_cards (id, title, status, priority, created_at, updated_at) \
             VALUES (?1, 'test', ?2, 'medium', datetime('now'), datetime('now'))",
        rusqlite::params![card_id, status],
    )
    .unwrap();
}

fn set_pmd_channel(db: &Db, channel_id: &str) {
    let conn = db.lock().unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO kv_meta (key, value) VALUES ('kanban_manager_channel_id', ?1)",
        [channel_id],
    )
    .unwrap();
}

#[tokio::test]
async fn force_transition_rejects_without_channel_header() {
    let db = test_db();
    let engine = test_engine(&db);
    seed_card_with_status(&db, "card-ft1", "backlog");
    set_pmd_channel(&db, "pmd-chan-123");

    let app = api_router(db, engine, None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/kanban-cards/card-ft1/force-transition")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"status":"ready"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn force_transition_rejects_wrong_channel() {
    let db = test_db();
    let engine = test_engine(&db);
    seed_card_with_status(&db, "card-ft2", "backlog");
    set_pmd_channel(&db, "pmd-chan-123");

    let app = api_router(db, engine, None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/kanban-cards/card-ft2/force-transition")
                .header("content-type", "application/json")
                .header("x-channel-id", "wrong-channel")
                .body(Body::from(r#"{"status":"ready"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn force_transition_succeeds_with_correct_channel() {
    let db = test_db();
    let engine = test_engine(&db);
    seed_card_with_status(&db, "card-ft3", "requested");
    set_pmd_channel(&db, "pmd-chan-123");

    let app = api_router(db, engine, None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/kanban-cards/card-ft3/force-transition")
                .header("content-type", "application/json")
                .header("x-channel-id", "pmd-chan-123")
                .body(Body::from(r#"{"status":"done"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["forced"], true);
}
