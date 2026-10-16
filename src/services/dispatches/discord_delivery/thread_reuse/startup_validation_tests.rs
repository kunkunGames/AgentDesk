use super::*;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::oneshot, task::JoinHandle};

struct Reply {
    thread: &'static str,
    status: StatusCode,
    body: Value,
    started: Option<oneshot::Sender<String>>,
    release: Option<oneshot::Receiver<()>>,
}

impl Reply {
    fn valid(thread: &'static str, parent: &'static str) -> Self {
        Self {
            thread,
            status: StatusCode::OK,
            body: json!({"parent_id": parent}),
            started: None,
            release: None,
        }
    }

    fn missing(thread: &'static str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            ..Self::valid(thread, "0")
        }
    }

    fn held(mut self) -> (Self, oneshot::Receiver<String>, oneshot::Sender<()>) {
        let (started, observed) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        self.started = Some(started);
        self.release = Some(wait);
        (self, observed, release)
    }
}

struct DiscordState {
    replies: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<String>>,
}

struct DiscordFixture {
    state: Arc<DiscordState>,
    base_url: String,
    server: JoinHandle<()>,
}

impl DiscordFixture {
    async fn start(replies: Vec<Reply>) -> Self {
        async fn channel(
            Path(thread): Path<String>,
            State(state): State<Arc<DiscordState>>,
        ) -> Response {
            state.requests.lock().unwrap().push(thread.clone());
            let reply = {
                let mut replies = state.replies.lock().unwrap();
                let index = replies.iter().position(|reply| reply.thread == thread);
                index.and_then(|index| replies.remove(index))
            };
            let Some(mut reply) = reply else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"unexpected": true})),
                )
                    .into_response();
            };
            if let Some(started) = reply.started.take() {
                let _ = started.send(thread);
            }
            if let Some(release) = reply.release.take() {
                release.await.unwrap();
            }
            (reply.status, Json(reply.body)).into_response()
        }
        let state = Arc::new(DiscordState {
            replies: Mutex::new(replies.into()),
            requests: Mutex::new(Vec::new()),
        });
        let app = Router::new()
            .route("/channels/{thread}", get(channel))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            state,
            base_url,
            server,
        }
    }

    fn validate(&self, pool: &PgPool) -> JoinHandle<(usize, usize)> {
        let pool = pool.clone();
        let base_url = self.base_url.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap();
            validate_channel_thread_maps_on_startup_with_base_url_pg(
                &pool,
                &client,
                "fixture-token",
                &base_url,
            )
            .await
        })
    }

    fn requests(&self) -> Vec<String> {
        self.state.requests.lock().unwrap().clone()
    }

    async fn stop(self) {
        assert!(self.state.replies.lock().unwrap().is_empty());
        self.server.abort();
        assert!(self.server.await.unwrap_err().is_cancelled());
    }
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("fixture did not settle")
}

async fn policy(pool: &PgPool, raw: Option<&str>, expired: bool) {
    sqlx::query(
        "INSERT INTO kv_meta(key,value,expires_at) VALUES('runtime-config',$1,
           CASE WHEN $2 THEN clock_timestamp()-INTERVAL '1 second' ELSE NULL END)
         ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,expires_at=EXCLUDED.expires_at",
    )
    .bind(raw)
    .bind(expired)
    .execute(pool)
    .await
    .unwrap();
}

async fn enabled(pool: &PgPool, value: bool) {
    let raw = json!({"internalTasksEnabled": value}).to_string();
    policy(pool, Some(&raw), false).await;
}

async fn card(pool: &PgPool, id: &str, map: &str, active: &str) {
    sqlx::query(
        "INSERT INTO kanban_cards(id,title,status,channel_thread_map,active_thread_id,metadata,
             created_at,updated_at)
         VALUES($1,'Startup validation fixture','ready',$2::text::jsonb,$3,
             '{\"retained\":true}'::jsonb,NOW()-INTERVAL '1 day',NOW()-INTERVAL '1 day')",
    )
    .bind(id)
    .bind(map)
    .bind(active)
    .execute(pool)
    .await
    .unwrap();
}

async fn full_card(pool: &PgPool, id: &str) -> Value {
    sqlx::query_scalar("SELECT to_jsonb(k) FROM kanban_cards k WHERE id=$1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn row(pool: &PgPool, id: &str) -> ThreadMapValidationRow {
    startup_thread_map_rows(pool)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.card_id == id)
        .unwrap()
}

fn cleanup() -> ThreadMapValidationOutcome {
    ThreadMapValidationOutcome {
        checked: 1,
        cleared: 1,
        persist: true,
        new_map: None,
        new_active_thread_id: None,
    }
}

fn assert_only_cleanup_changed(before: &Value, after: &Value) {
    let mut before = before.as_object().unwrap().clone();
    let mut after = after.as_object().unwrap().clone();
    assert_ne!(before["updated_at"], after["updated_at"]);
    for key in ["channel_thread_map", "active_thread_id", "updated_at"] {
        before.remove(key);
        after.remove(key);
    }
    assert_eq!(before, after);
}

async fn waiting_on_lock(pool: &PgPool, query_prefix: &str) {
    bounded(async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                 WHERE datname=current_database() AND pid<>pg_backend_pid()
                   AND wait_event_type='Lock' AND query LIKE $1)",
            )
            .bind(format!("{query_prefix}%"))
            .fetch_one(pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

async fn wait_until_expired(pool: &PgPool) {
    bounded(async {
        loop {
            let expired: bool = sqlx::query_scalar(
                "SELECT expires_at<=clock_timestamp() FROM kv_meta WHERE key='runtime-config'",
            )
            .fetch_one(pool)
            .await
            .unwrap();
            if expired {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn startup_thread_map_validation_defers_false_until_true() {
    let db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "startup_map",
        "deferred startup validation",
    )
    .await;
    let pool = db.connect_and_migrate().await;
    enabled(&pool, false).await;
    card(&pool, "deferred", r#"{"101":"201"}"#, "201").await;
    let before = full_card(&pool, "deferred").await;
    let discord = DiscordFixture::start(vec![Reply::missing("201")]).await;
    let task = discord.validate(&pool);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!task.is_finished());
    assert!(discord.requests().is_empty());
    assert_eq!(full_card(&pool, "deferred").await, before);
    enabled(&pool, true).await;
    assert_eq!(bounded(task).await.unwrap(), (1, 1));
    let after = full_card(&pool, "deferred").await;
    assert!(after["channel_thread_map"].is_null());
    assert!(after["active_thread_id"].is_null());
    assert_only_cleanup_changed(&before, &after);
    assert_eq!(discord.requests(), ["201"]);
    discord.stop().await;
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn startup_thread_map_validation_revalidates_after_mid_read_fence() {
    let db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "startup_map",
        "mid-request startup fence",
    )
    .await;
    let pool = db.connect_and_migrate().await;
    enabled(&pool, true).await;
    card(&pool, "mid-read", r#"{"101":"201","102":"202"}"#, "201").await;
    let before = full_card(&pool, "mid-read").await;
    let (first, first_started, first_release) = Reply::missing("201").held();
    let (second, second_started, second_release) = Reply::missing("201").held();
    let discord = DiscordFixture::start(vec![
        first,
        second,
        Reply::valid("201", "101"),
        Reply::valid("202", "102"),
    ])
    .await;
    let task = discord.validate(&pool);
    assert_eq!(bounded(first_started).await.unwrap(), "201");
    enabled(&pool, false).await;
    first_release.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(discord.requests(), ["201"]);
    assert!(!task.is_finished());
    assert_eq!(full_card(&pool, "mid-read").await, before);
    enabled(&pool, true).await;
    assert_eq!(bounded(second_started).await.unwrap(), "201");
    // Reopening before the response returns must still discard the old 404.
    enabled(&pool, false).await;
    enabled(&pool, true).await;
    second_release.send(()).unwrap();
    assert_eq!(bounded(task).await.unwrap(), (2, 0));
    assert_eq!(discord.requests(), ["201", "201", "201", "202"]);
    assert_eq!(full_card(&pool, "mid-read").await, before);
    discord.stop().await;
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn startup_thread_map_validation_serializes_cleanup_with_public_fence() {
    let db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "startup_map",
        "startup fence write ordering",
    )
    .await;
    let pool = db.connect_and_migrate_with_max_connections(8).await;
    enabled(&pool, true).await;
    card(&pool, "ordered", r#"{"101":"201"}"#, "201").await;
    let before = full_card(&pool, "ordered").await;
    let captured = row(&pool, "ordered").await;
    let admission = startup_thread_map_admission(&pool).await.unwrap().unwrap();
    let mut card_lock = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM kanban_cards WHERE id='ordered' FOR UPDATE")
        .fetch_one(&mut *card_lock)
        .await
        .unwrap();
    let writer_pool = pool.clone();
    let writer = tokio::spawn(async move {
        persist_startup_thread_map_if_admitted(&writer_pool, &captured, &cleanup(), &admission)
            .await
    });
    waiting_on_lock(&pool, "SELECT id FROM kanban_cards").await;
    let fence_pool = pool.clone();
    let fence = tokio::spawn(async move {
        enabled(&fence_pool, false).await;
    });
    waiting_on_lock(&pool, "INSERT INTO kv_meta").await;
    assert!(!writer.is_finished() && !fence.is_finished());
    card_lock.commit().await.unwrap();
    assert!(bounded(writer).await.unwrap().unwrap());
    bounded(fence).await.unwrap();
    assert_only_cleanup_changed(&before, &full_card(&pool, "ordered").await);

    sqlx::query("UPDATE kanban_cards SET channel_thread_map='{\"101\":\"201\"}'::jsonb,active_thread_id='201' WHERE id='ordered'")
        .execute(&pool).await.unwrap();
    enabled(&pool, true).await;
    let captured = row(&pool, "ordered").await;
    let admission = startup_thread_map_admission(&pool).await.unwrap().unwrap();
    let before = full_card(&pool, "ordered").await;
    let mut fence_lock = pool.begin().await.unwrap();
    sqlx::query(
        "UPDATE kv_meta SET value='{\"internalTasksEnabled\":false}' WHERE key='runtime-config'",
    )
    .execute(&mut *fence_lock)
    .await
    .unwrap();
    let writer_pool = pool.clone();
    let writer = tokio::spawn(async move {
        persist_startup_thread_map_if_admitted(&writer_pool, &captured, &cleanup(), &admission)
            .await
    });
    waiting_on_lock(&pool, "SELECT xmin::text").await;
    fence_lock.commit().await.unwrap();
    assert!(!bounded(writer).await.unwrap().unwrap());
    assert_eq!(full_card(&pool, "ordered").await, before);

    // Both settings-lock and card-lock waits must use post-wait wall-clock TTL.
    for lock_card in [false, true] {
        enabled(&pool, true).await;
        sqlx::query("UPDATE kv_meta SET expires_at=clock_timestamp()+INTERVAL '800 milliseconds' WHERE key='runtime-config'")
            .execute(&pool).await.unwrap();
        let captured = row(&pool, "ordered").await;
        let admission = startup_thread_map_admission(&pool).await.unwrap().unwrap();
        let mut held = pool.begin().await.unwrap();
        let lock_sql = if lock_card {
            "SELECT id FROM kanban_cards WHERE id='ordered' FOR UPDATE"
        } else {
            "SELECT key FROM kv_meta WHERE key='runtime-config' FOR UPDATE"
        };
        sqlx::query(lock_sql).fetch_one(&mut *held).await.unwrap();
        let writer_pool = pool.clone();
        let writer = tokio::spawn(async move {
            persist_startup_thread_map_if_admitted(&writer_pool, &captured, &cleanup(), &admission)
                .await
        });
        waiting_on_lock(
            &pool,
            if lock_card {
                "SELECT id FROM kanban_cards"
            } else {
                "SELECT xmin::text"
            },
        )
        .await;
        wait_until_expired(&pool).await;
        held.commit().await.unwrap();
        assert!(!bounded(writer).await.unwrap().unwrap());
        assert_eq!(full_card(&pool, "ordered").await, before);
    }
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn startup_thread_map_validation_fails_closed_for_invalid_policy() {
    let db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "startup_map",
        "startup admission policy semantics",
    )
    .await;
    let pool = db.connect_and_migrate_with_max_connections(1).await;
    card(&pool, "policy", r#"{"101":"201"}"#, "201").await;
    let before = full_card(&pool, "policy").await;
    let captured = row(&pool, "policy").await;
    sqlx::query("DELETE FROM kv_meta WHERE key='runtime-config'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        !bounded(persist_startup_thread_map_if_admitted(
            &pool,
            &captured,
            &cleanup(),
            "absent"
        ))
        .await
        .unwrap()
    );
    for (raw, expired) in [
        (None, false),
        (Some("{"), false),
        (Some("{}"), false),
        (Some(r#"{"internalTasksEnabled":false}"#), false),
        (Some(r#"{"internalTasksEnabled":"true"}"#), false),
        (Some(r#"{"internalTasksEnabled":1}"#), false),
        (Some(r#"{"internalTasksEnabled":null}"#), false),
        (Some(r#"{"internalTasksEnabled":true}"#), true),
    ] {
        policy(&pool, raw, expired).await;
        assert!(
            !crate::services::settings::internal_tasks_enabled(&pool)
                .await
                .unwrap_or(false)
        );
        assert!(
            bounded(startup_thread_map_admission(&pool))
                .await
                .unwrap_or(None)
                .is_none()
        );
        assert!(
            !bounded(persist_startup_thread_map_if_admitted(
                &pool,
                &captured,
                &cleanup(),
                "unadmitted"
            ))
            .await
            .unwrap_or(false)
        );
        assert_eq!(full_card(&pool, "policy").await, before);
    }
    // A real database read error must not turn into permission or a card write.
    sqlx::query("ALTER TABLE kv_meta RENAME COLUMN value TO fixture_unavailable_value")
        .execute(&pool)
        .await
        .unwrap();
    assert!(bounded(startup_thread_map_admission(&pool)).await.is_err());
    assert!(
        bounded(persist_startup_thread_map_if_admitted(
            &pool,
            &captured,
            &cleanup(),
            "unadmitted"
        ))
        .await
        .is_err()
    );
    assert_eq!(full_card(&pool, "policy").await, before);
    sqlx::query("ALTER TABLE kv_meta RENAME COLUMN fixture_unavailable_value TO value")
        .execute(&pool)
        .await
        .unwrap();
    enabled(&pool, true).await;
    let admission = bounded(startup_thread_map_admission(&pool))
        .await
        .unwrap()
        .unwrap();
    assert!(
        bounded(persist_startup_thread_map_if_admitted(
            &pool,
            &captured,
            &cleanup(),
            &admission
        ))
        .await
        .unwrap()
    );
    assert_only_cleanup_changed(&before, &full_card(&pool, "policy").await);
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn startup_thread_map_validation_preserves_concurrent_card_binding() {
    let db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "startup_map",
        "concurrent startup card binding",
    )
    .await;
    let pool = db.connect_and_migrate().await;
    enabled(&pool, true).await;
    card(&pool, "concurrent", r#"{"101":"201"}"#, "201").await;
    for (replace_map, old_thread, new_thread, parent) in
        [(true, "201", "202", "102"), (false, "202", "202", "102")]
    {
        let (missing, started, release) = Reply::missing(old_thread).held();
        let discord = DiscordFixture::start(vec![missing, Reply::valid(new_thread, parent)]).await;
        let task = discord.validate(&pool);
        assert_eq!(bounded(started).await.unwrap(), old_thread);
        if replace_map {
            sqlx::query("UPDATE kanban_cards SET channel_thread_map='{\"102\":\"202\"}'::jsonb,active_thread_id='202',updated_at=NOW() WHERE id='concurrent'")
                .execute(&pool).await.unwrap();
        } else {
            sqlx::query("UPDATE kanban_cards SET active_thread_id='203',updated_at=NOW() WHERE id='concurrent'")
                .execute(&pool).await.unwrap();
        }
        let changed = full_card(&pool, "concurrent").await;
        release.send(()).unwrap();
        let (_, cleared) = bounded(task).await.unwrap();
        assert_eq!(cleared, 0);
        assert_eq!(discord.requests(), [old_thread, new_thread]);
        assert_eq!(full_card(&pool, "concurrent").await, changed);
        discord.stop().await;
    }
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn startup_thread_map_validation_cleans_only_invalid_bindings_when_enabled() {
    let db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "startup_map",
        "enabled startup cleanup",
    )
    .await;
    let pool = db.connect_and_migrate().await;
    enabled(&pool, true).await;
    card(&pool, "valid", r#"{"101":"201"}"#, "201").await;
    card(&pool, "mixed", r#"{"101":"202","102":"203"}"#, "202").await;
    card(&pool, "malformed", r#"["retained"]"#, "204").await;
    let valid = full_card(&pool, "valid").await;
    let mixed = full_card(&pool, "mixed").await;
    let malformed = full_card(&pool, "malformed").await;
    let discord = DiscordFixture::start(vec![
        Reply::valid("201", "101"),
        Reply::missing("202"),
        Reply::valid("203", "102"),
    ])
    .await;
    assert_eq!(bounded(discord.validate(&pool)).await.unwrap(), (3, 2));
    assert_eq!(full_card(&pool, "valid").await, valid);
    let mixed_after = full_card(&pool, "mixed").await;
    assert_eq!(mixed_after["channel_thread_map"], json!({"102": "203"}));
    assert_eq!(mixed_after["active_thread_id"], "203");
    assert_only_cleanup_changed(&mixed, &mixed_after);
    let malformed_after = full_card(&pool, "malformed").await;
    assert!(malformed_after["channel_thread_map"].is_null());
    assert_eq!(malformed_after["active_thread_id"], "204");
    assert_only_cleanup_changed(&malformed, &malformed_after);
    assert_eq!(discord.requests().len(), 3);
    discord.stop().await;
    pool.close().await;
    db.drop().await;
}
