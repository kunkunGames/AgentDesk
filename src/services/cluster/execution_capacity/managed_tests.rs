//! The shared lease contract must not depend on a particular CLI or model.
use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execution_policy_uses_canonical_agent_for_threads_and_headless_turns_pg() {
    use crate::services::platform::with_provider_execution_context;
    use crate::services::provider_cli::ProviderExecutionContext;
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let limits = ExecutionLimits {
        max_elapsed_secs: 60,
        max_tool_calls: 3,
    };
    for (id, channel, policy) in [
        ("parent-agent", "100", json!({"execution_limits": limits})),
        (
            "other-agent",
            "200",
            json!({"execution_limits": {"max_elapsed_secs": 120, "max_tool_calls": 9}}),
        ),
        ("unbounded-agent", "300", json!({})),
        (
            "invalid-agent",
            "400",
            json!({"execution_limits": {"max_elapsed_secs": 0, "max_tool_calls": 1}}),
        ),
    ] {
        sqlx::query("INSERT INTO agents(id, name, discord_channel_id, execution_requirements) VALUES($1,$1,$2,$3)")
            .bind(id).bind(channel).bind(policy).execute(&pool).await.unwrap();
    }
    for mode in ["discord_turn", "headless_turn"] {
        // The thread has no legacy channel binding; the other channel has a
        // different policy. Both must retain the already resolved parent.
        for channel in ["unmapped-thread", "200"] {
            let check_pool = pool.clone();
            let actual = tokio::task::spawn_blocking(move || {
                with_provider_execution_context(
                    ProviderExecutionContext {
                        provider: "opencode".into(),
                        agent_id: Some("parent-agent".into()),
                        channel_id: Some(channel.into()),
                        execution_mode: Some(mode.into()),
                        ..Default::default()
                    },
                    || admission::limits(&check_pool, "opencode", channel),
                )
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(actual, Some(limits.clone()));
        }
    }
    for (agent, expected) in [
        (None, Some(limits.clone())),
        (Some("yaml-only-role"), Some(limits.clone())),
        (Some("unbounded-agent"), None),
    ] {
        let check_pool = pool.clone();
        let actual = tokio::task::spawn_blocking(move || {
            with_provider_execution_context(
                ProviderExecutionContext {
                    provider: "opencode".into(),
                    agent_id: agent.map(str::to_string),
                    ..Default::default()
                },
                || admission::limits(&check_pool, "opencode", "100"),
            )
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(actual, expected);
    }
    let check_pool = pool.clone();
    tokio::task::spawn_blocking(move || {
        with_provider_execution_context(
            ProviderExecutionContext {
                provider: "opencode".into(),
                agent_id: Some("invalid-agent".into()),
                ..Default::default()
            },
            || assert!(admission::limits(&check_pool, "opencode", "100").is_err()),
        )
    })
    .await
    .unwrap();
    pool.close().await;
    fixture.drop().await;
}

// Test-only seam: native adapter tests exercise the production guard, renewal,
// admission and finalization without mutating global cluster configuration.
pub(crate) fn guarded(
    pool: PgPool,
    provider: &str,
    channel: &str,
    resource: &str,
    limits: ExecutionLimits,
) -> (Arc<CancelToken>, ExecutionGuard) {
    let cancel = Arc::new(CancelToken::new());
    let semaphore = Arc::new(Semaphore::new(1));
    let guard = acquire_guard(
        pool,
        "native-test".into(),
        provider.into(),
        channel.into(),
        Arc::clone(&cancel),
        semaphore.try_acquire_owned().unwrap(),
        Some(LeaseAdmission {
            resource_key: resource.into(),
            limits: Some(limits),
        })
        .into(),
    )
    .unwrap();
    (cancel, guard)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_deadline_cancels_silent_adapters_without_polling_the_token_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let execute_pool = pool.clone();
    let (cancel, guard) = tokio::task::spawn_blocking(move || {
        let (cancel, guard) = guarded(
            execute_pool,
            "codex",
            "silent",
            "workspace",
            ExecutionLimits {
                max_elapsed_secs: 1,
                max_tool_calls: 1,
            },
        );
        cancel
            .managed_execution
            .get()
            .unwrap()
            .bind_runtime(&super::recovery::RuntimeIdentity::fixture())
            .unwrap();
        cancel
            .managed_execution
            .get()
            .unwrap()
            .admit_prompt("session")
            .unwrap();
        (cancel, guard)
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        // Deliberately do not call cancel_requested: quiet adapters may only
        // inspect this atomic in their existing watchdog.
        while !cancel.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("common owner must publish the deadline cancellation");
    assert_eq!(
        cancel.cancel_source().as_deref(),
        Some("managed_attempt_watchdog_timeout")
    );
    tokio::task::spawn_blocking(move || drop(guard))
        .await
        .unwrap();
    let state: String = sqlx::query_scalar(
        "SELECT execution_state FROM node_execution_leases WHERE channel_id='silent'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, "unknown");
    pool.close().await;
    fixture.drop().await;
}

async fn reserve(
    pool: &PgPool,
    provider: &str,
    channel: &str,
    resource: &str,
) -> Arc<ManagedAttempt> {
    let nonce = uuid::Uuid::new_v4();
    let deadline = store::acquire_scoped(
        pool,
        "node",
        provider,
        channel,
        nonce,
        Some(&LeaseAdmission {
            resource_key: resource.into(),
            limits: Some(ExecutionLimits {
                max_elapsed_secs: 60,
                max_tool_calls: 2,
            }),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    Arc::new(ManagedAttempt::new(
        pool.clone(),
        LeaseIdentity {
            node: "node".into(),
            provider: provider.into(),
            channel: channel.into(),
            nonce,
        },
        deadline,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_attempt_budget_session_binding_and_release_are_provider_neutral_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    for provider in ["opencode", "codex", "grok", "qwen"] {
        let attempt = reserve(&pool, provider, "same-channel", "same-workspace").await;
        let legacy_start = sqlx::query(
            "UPDATE node_execution_leases SET execution_state='running' WHERE provider=$1",
        )
        .bind(provider)
        .execute(&pool)
        .await
        .unwrap_err();
        assert_eq!(
            legacy_start
                .as_database_error()
                .and_then(|error| error.constraint()),
            Some("managed_execution_requires_runtime")
        );
        let admitted = Arc::clone(&attempt);
        tokio::task::spawn_blocking(move || {
            assert!(admitted.admit_prompt("unbound-session").is_err());
            admitted
                .bind_runtime(&super::recovery::RuntimeIdentity::fixture())
                .unwrap();
            admitted.admit_prompt("session").unwrap();
            assert!(admitted.admit_prompt("replacement-session").is_err());
            assert!(
                admitted
                    .admit_permission("other-session", "grant-1")
                    .is_err()
            );
            admitted.admit_permission("session", "grant-1").unwrap();
            admitted.admit_permission("session", "grant-1").unwrap();
            admitted.admit_permission("session", "grant-2").unwrap();
            assert!(admitted.admit_permission("session", "grant-3").is_err());
        })
        .await
        .unwrap();
        let used: i32 = sqlx::query_scalar(
            "SELECT cardinality(permission_ids) FROM node_execution_leases WHERE provider=$1",
        )
        .bind(provider)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            used, 2,
            "redelivery cannot spend again, nor reset the shared budget"
        );
        let finish = Arc::clone(&attempt);
        tokio::task::spawn_blocking(move || finish.record_quiesced("session"))
            .await
            .unwrap()
            .unwrap();
        attempt.finish().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM node_execution_leases")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_writer_survives_expiry_cleanup_and_reconnect_while_independent_work_progresses_pg()
{
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    sqlx::query("INSERT INTO worker_nodes(instance_id,last_heartbeat_at,capabilities) VALUES('node',NOW(),$1)")
        .bind(json!({"execution_capacity":{"version":1,"slots":2}})).execute(&pool).await.unwrap();
    let attempt = reserve(&pool, "codex", "writer-a", "workspace-a").await;
    let start = Arc::clone(&attempt);
    tokio::task::spawn_blocking(move || {
        start.bind_runtime(&super::recovery::RuntimeIdentity::fixture())?;
        start.admit_prompt("session-a")
    })
    .await
    .unwrap()
    .unwrap();
    attempt.finish().await.unwrap();
    sqlx::query("UPDATE node_execution_leases SET expires_at=NOW()-INTERVAL '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    // Simulate a legacy TTL sweeper. Its DELETE must not remove unknown work.
    assert_eq!(
        sqlx::query("DELETE FROM node_execution_leases WHERE expires_at<NOW()")
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected(),
        0
    );
    assert!(
        store::check_retained(&pool, "node", "another-channel", Some("workspace-a"))
            .await
            .is_err()
    );
    assert!(
        store::check_retained(&pool, "node", "writer-a", None)
            .await
            .is_err()
    );
    let conflict = store::acquire_scoped(
        &pool,
        "node",
        "qwen",
        "writer-b",
        uuid::Uuid::new_v4(),
        Some(&LeaseAdmission {
            resource_key: "workspace-a".into(),
            limits: None,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(
        conflict.as_database_error().and_then(|e| e.constraint()),
        Some("managed_execution_resource_available")
    );
    // Another provider in an independent workspace may use the remaining slot.
    let independent = store::acquire_scoped(
        &pool,
        "node",
        "grok",
        "independent-c",
        uuid::Uuid::new_v4(),
        Some(&LeaseAdmission {
            resource_key: "workspace-c".into(),
            limits: None,
        }),
    )
    .await;
    assert!(independent.is_ok());
    assert!(is_exhausted(
        &store::acquire(&pool, "node", "opencode", "overflow", uuid::Uuid::new_v4())
            .await
            .unwrap_err()
    ));
    drop(attempt);
    pool.close().await;
    let reconnected = fixture.connect_and_migrate().await;
    let state: String = sqlx::query_scalar(
        "SELECT execution_state FROM node_execution_leases WHERE channel_id='writer-a'",
    )
    .fetch_one(&reconnected)
    .await
    .unwrap();
    assert_eq!(state, "unknown");
    assert_eq!(
        super::recovery::observe_retained(&reconnected, "node")
            .await
            .unwrap(),
        1
    );
    let row: (String, Value, Value) = sqlx::query_as("SELECT execution_state,runtime_identity,stop_observation FROM node_execution_leases WHERE channel_id='writer-a'")
        .fetch_one(&reconnected).await.unwrap();
    assert_eq!(row.0, "unknown");
    assert_eq!(
        row.1,
        serde_json::to_value(super::recovery::RuntimeIdentity::fixture()).unwrap()
    );
    assert_eq!(row.2["local_writer_status"], "unobservable");
    assert_eq!(row.2["inference_status"], "unknown");
    assert!(
        store::check_retained(&reconnected, "node", "replacement", Some("workspace-a"))
            .await
            .is_err()
    );
    reconnected.close().await;
    fixture.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_budget_survives_ttl_and_recovery_observation_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    sqlx::query("INSERT INTO worker_nodes(instance_id,last_heartbeat_at,capabilities) VALUES('node',NOW(),$1)")
        .bind(json!({"execution_capacity":{"version":1,"slots":1}}))
        .execute(&pool)
        .await
        .unwrap();
    let attempt = reserve(&pool, "opencode", "writer-a", "workspace-a").await;
    let start = Arc::clone(&attempt);
    tokio::task::spawn_blocking(move || {
        start.bind_runtime(&super::recovery::RuntimeIdentity::fixture())?;
        start.admit_prompt("session-a")?;
        start.admit_permission("session-a", "tool-1")
    })
    .await
    .unwrap()
    .unwrap();
    attempt.finish().await.unwrap();
    sqlx::query("UPDATE node_execution_leases SET expires_at=NOW()-INTERVAL '1 second', deadline_at=NOW()-INTERVAL '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        super::recovery::observe_retained(&pool, "node")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query("DELETE FROM node_execution_leases WHERE expires_at<NOW()")
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected(),
        0
    );
    let nonce = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT nonce FROM node_execution_leases WHERE channel_id='writer-a'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    store::release(&pool, "node", "opencode", "writer-a", nonce)
        .await
        .unwrap();
    let held: (String, i32) = sqlx::query_as(
        "SELECT execution_state, cardinality(permission_ids)::int FROM node_execution_leases WHERE channel_id='writer-a'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(held.0, "unknown");
    assert_eq!(
        held.1, 1,
        "ttl expiry and release must not refund spent tools"
    );
    assert!(
        store::acquire_scoped(
            &pool,
            "node",
            "opencode",
            "writer-b",
            uuid::Uuid::new_v4(),
            Some(&LeaseAdmission {
                resource_key: "workspace-a".into(),
                limits: Some(ExecutionLimits {
                    max_elapsed_secs: 60,
                    max_tool_calls: 2,
                }),
            }),
        )
        .await
        .is_err()
    );
    // A text receipt is not evidence of remote inference termination or
    // local writer containment. No recovery API may turn it into capacity.
    assert_eq!(
        super::recovery::observe_retained(&pool, "node")
            .await
            .unwrap(),
        1
    );
    let retained: (String, i32) = sqlx::query_as(
        "SELECT execution_state, cardinality(permission_ids)::int FROM node_execution_leases WHERE channel_id='writer-a'"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(retained, ("unknown".into(), 1));
    drop(attempt);
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renewal_cannot_reset_managed_deadline_or_authorize_a_late_tool_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let attempt = reserve(&pool, "qwen", "deadline", "workspace").await;
    let start = Arc::clone(&attempt);
    tokio::task::spawn_blocking(move || {
        start.bind_runtime(&super::recovery::RuntimeIdentity::fixture())?;
        start.admit_prompt("session")
    })
    .await
    .unwrap()
    .unwrap();
    sqlx::query("UPDATE node_execution_leases SET deadline_at=NOW()-INTERVAL '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    // Exercise the production renewal statement against the row's real nonce.
    let nonce = sqlx::query_scalar::<_, uuid::Uuid>("SELECT nonce FROM node_execution_leases")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        store::renew(&pool, "node", "qwen", "deadline", nonce)
            .await
            .unwrap()
    );
    let late = Arc::clone(&attempt);
    tokio::task::spawn_blocking(move || {
        assert!(late.admit_permission("session", "late-grant").is_err());
        assert!(late.record_quiesced("session").is_err());
    })
    .await
    .unwrap();
    attempt.finish().await.unwrap();
    store::release(&pool, "node", "qwen", "deadline", nonce)
        .await
        .unwrap();
    let state: String = sqlx::query_scalar("SELECT execution_state FROM node_execution_leases")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        state, "unknown",
        "abort/release does not establish termination"
    );
    pool.close().await;
    fixture.drop().await;
}
