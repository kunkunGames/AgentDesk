use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;
use crate::db::campaigns::{self, graph::store};

const DISPATCH: &str = "capacity-admission-attempt";
const TASK: &str = "capacity:plan:initial";
const CHANNEL: u64 = 521_807;
const POSTED_MESSAGE: u64 = 521_808;

async fn fixture(pool: &PgPool) -> (String, String, Goal) {
    sqlx::query(
        "INSERT INTO agents(id,name,provider) VALUES('qwen','Qwen','qwen'),('qa','QA','opencode')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO kv_meta(key,value) VALUES('runtime-config','{\"internalTasksEnabled\":true}')",
    )
    .execute(pool)
    .await
    .unwrap();
    let goal: Goal = serde_json::from_value(json!({
        "objective":"Test original campaign admission", "success":["validated result"],
        "constraints":["no external mutations"], "repo":"test/repo", "planner":"qwen",
        "verifier":"qa", "agents":["qwen","qa"],
        "budget":{"max_dispatches":12,"max_elapsed_secs":600,"node_timeout_secs":120,
            "max_evolutions":2,"max_observed_tool_calls":20}
    }))
    .unwrap();
    let campaign = campaigns::create(
        pool,
        "capacity-policy".into(),
        serde_json::from_value(
            json!({"title":"Capacity policy","status":"active","round":1,"nodes":[]}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    store::enroll(pool, &campaign.id, campaign.revision, &goal)
        .await
        .unwrap();
    let prompt = "Create a bounded plan with independent validation";
    let card = store::publish(
        pool,
        &campaign.id,
        &store::Publication {
            key: TASK,
            node: "plan",
            role: "plan",
            version: None,
            agent: "qwen",
            title: "Plan",
            prompt,
            digest: &contract_digest(&prompt).unwrap(),
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,thread_id) VALUES($1,$2,'qwen','implementation','dispatched',$3)")
        .bind(DISPATCH).bind(&card).bind(CHANNEL.to_string()).execute(pool).await.unwrap();
    crate::db::dispatches::persist_dispatch_message_target_pg(
        pool,
        DISPATCH,
        &CHANNEL.to_string(),
        &POSTED_MESSAGE.to_string(),
    )
    .await
    .unwrap();
    assert!(
        matches!(observe(pool).await.unwrap(), Inspection::Campaign(_)),
        "valid dispatch must be admitted before testing mutations"
    );
    (campaign.id, card, goal)
}

async fn observe_sources(pool: &PgPool, source_ids: &[u64]) -> Result<Inspection, String> {
    inspect(
        pool,
        DISPATCH,
        &ProviderKind::Qwen,
        CHANNEL,
        Some("qwen"),
        source_ids,
    )
    .await
}

async fn observe(pool: &PgPool) -> Result<Inspection, String> {
    observe_sources(pool, &[POSTED_MESSAGE]).await
}

fn expect_campaign(observed: Inspection) -> Observed {
    match observed {
        Inspection::Campaign(observed) => observed,
        Inspection::Ordinary => panic!("expected a campaign input, got ordinary intake"),
        Inspection::TargetPending => panic!("expected the posted dispatch target to be durable"),
    }
}

#[tokio::test]
async fn campaign_capacity_policy_keeps_original_deadline_and_soft_pause_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (campaign, _, goal) = fixture(&pool).await;
    let original = expect_campaign(observe(&pool).await.unwrap());
    let created: DateTime<Utc> =
        sqlx::query_scalar("SELECT created_at FROM task_dispatches WHERE id=$1")
            .bind(DISPATCH)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        original.deadline,
        created + chrono::Duration::seconds(i64::from(goal.budget.node_timeout_secs))
    );
    sqlx::query(
        "UPDATE campaigns SET document=jsonb_set(document,'{status}','\"paused\"') WHERE id=$1",
    )
    .bind(&campaign)
    .execute(&pool)
    .await
    .unwrap();
    let paused = expect_campaign(observe(&pool).await.unwrap());
    assert_eq!(original.identity, paused.identity);
    assert_eq!(original.deadline, paused.deadline);
    sqlx::query(
        "UPDATE campaign_execution_specs SET hold_reason='unknown outcome' WHERE campaign_id=$1",
    )
    .bind(campaign)
    .execute(&pool)
    .await
    .unwrap();
    assert!(observe(&pool).await.is_err());
}

#[tokio::test]
async fn campaign_capacity_policy_rejects_stale_prompt_provider_terminal_and_deadline_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (_, card, _) = fixture(&pool).await;
    let profile: Value =
        sqlx::query_scalar("SELECT execution_profile FROM campaign_graph_tasks WHERE task_key=$1")
            .bind(TASK)
            .fetch_one(&pool)
            .await
            .unwrap();
    for (field, value) in [
        ("prompt_digest", json!("changed")),
        ("goal_digest", json!("changed")),
        ("provider_requested", json!("codex")),
    ] {
        let mut changed = profile.clone();
        changed[field] = value;
        sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=$2 WHERE task_key=$1")
            .bind(TASK)
            .bind(changed)
            .execute(&pool)
            .await
            .unwrap();
        assert!(observe(&pool).await.is_err(), "{field}");
    }
    sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=$2 WHERE task_key=$1")
        .bind(TASK)
        .bind(profile)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        inspect(
            &pool,
            DISPATCH,
            &ProviderKind::Qwen,
            CHANNEL + 1,
            Some("qwen"),
            &[POSTED_MESSAGE],
        )
        .await
        .is_err()
    );
    assert!(
        inspect(
            &pool,
            DISPATCH,
            &ProviderKind::Qwen,
            CHANNEL,
            Some("qa"),
            &[POSTED_MESSAGE],
        )
        .await
        .is_err()
    );
    sqlx::query("UPDATE task_dispatches SET created_at=NOW()-INTERVAL '121 seconds' WHERE id=$1")
        .bind(DISPATCH)
        .execute(&pool)
        .await
        .unwrap();
    assert!(observe(&pool).await.is_err());
    sqlx::query("UPDATE task_dispatches SET created_at=NOW(),status='completed',completed_at=NOW() WHERE id=$1")
        .bind(DISPATCH).execute(&pool).await.unwrap();
    assert!(observe(&pool).await.is_err());
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_dispatches WHERE kanban_card_id=$1")
            .bind(card)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count, 1,
        "read-only admission cannot enqueue a second attempt"
    );
}

#[tokio::test]
async fn campaign_capacity_policy_does_not_downgrade_missing_campaign_authority_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (_, _, _) = fixture(&pool).await;
    sqlx::query("INSERT INTO task_dispatches(id,status,dispatch_type) VALUES('ordinary','dispatched','implementation')")
        .execute(&pool).await.unwrap();
    assert!(matches!(
        inspect(
            &pool,
            "ordinary",
            &ProviderKind::Qwen,
            CHANNEL,
            None,
            &[POSTED_MESSAGE],
        )
        .await
        .unwrap(),
        Inspection::Ordinary
    ));
    let ordinary_pool = pool.clone();
    tokio::task::spawn_blocking(move || {
        // capture performs a read-only inflight snapshot before classifying
        // the dispatch. Keep that lookup in an empty owned runtime root.
        let root = tempfile::tempdir().unwrap();
        let _root_guard = crate::config::set_agentdesk_root_for_test(root.path());
        let provider = ProviderKind::OpenCode;
        assert!(
            inflight::load_inflight_state_read_only_result(&provider, CHANNEL)
                .unwrap()
                .is_none()
        );
        let host = crate::services::turn_host::TurnHost::Tmux;
        let turn = StreamingTurn {
            pool: Some(&ordinary_pool),
            campaign_dispatch_id: Some("ordinary"),
            provider: &provider,
            prompt: "DISPATCH:ordinary [Task] - ordinary input without a tmux runtime",
            session_id: None,
            working_dir: root.path().to_str().unwrap(),
            system_prompt: None,
            allowed_tools: &[],
            cancel: Arc::new(crate::services::provider::CancelToken::new()),
            remote_profile: None,
            tmux_session_name: None,
            teardown: None,
            host: &host,
            channel_id: CHANNEL,
            model: None,
            native_fast_mode: None,
            codex_goals: None,
            compact_percent: None,
            compact_lower_bound_tokens: 0,
            compact_token_limit: None,
            cache_ttl_minutes: None,
            dispatch_type: Some("implementation"),
            force_fresh: false,
            submission: None,
        };
        assert!(
            CampaignAdmission::capture(&turn).unwrap().is_none(),
            "ordinary dispatches need neither a campaign owner nor a tmux runtime"
        );
        assert_eq!(
            std::fs::read_dir(root.path()).unwrap().count(),
            0,
            "ordinary admission must leave the isolated runtime root untouched"
        );
    })
    .await
    .unwrap();
    assert!(
        inspect(
            &pool,
            "missing",
            &ProviderKind::Qwen,
            CHANNEL,
            None,
            &[POSTED_MESSAGE],
        )
        .await
        .is_err()
    );
    sqlx::query("UPDATE task_dispatches SET created_at=NULL WHERE id=$1")
        .bind(DISPATCH)
        .execute(&pool)
        .await
        .unwrap();
    assert!(observe(&pool).await.is_err());
    sqlx::query("UPDATE task_dispatches SET created_at=NOW() WHERE id=$1")
        .bind(DISPATCH)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM campaign_graph_tasks WHERE task_key=$1")
        .bind(TASK)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        observe(&pool).await.is_err(),
        "a canonical card must not become ordinary intake"
    );
}

#[tokio::test]
async fn campaign_capacity_review_uses_frozen_policy_and_requires_control_authority_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (campaign, _, goal) = fixture(&pool).await;
    sqlx::query("UPDATE campaign_graph_tasks SET role='template_review' WHERE task_key=$1")
        .bind(TASK)
        .execute(&pool)
        .await
        .unwrap();
    assert!(observe(&pool).await.is_err());
    sqlx::query("INSERT INTO campaign_dag_families(id,root_campaign_id,scope_digest,scope) VALUES('capacity-family',$1,'fixture','{}')")
        .bind(&campaign).execute(&pool).await.unwrap();
    let claimed: DateTime<Utc> =
        sqlx::query_scalar("SELECT clock_timestamp()-INTERVAL '30 seconds'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let policy = ReviewPolicy::freeze(&goal, claimed).unwrap();
    let snapshot = json!({"goal":goal,"execution_policy":policy});
    sqlx::query("INSERT INTO campaign_dag_reviews(id,family_id,owner_campaign_id,task_key,superior_agent_id,sample_count,threshold,sample_digest,snapshot,created_at) VALUES('capacity-review','capacity-family',$1,$2,'qwen',1,1,'fixture',$3,$4)")
        .bind(&campaign).bind(TASK).bind(&snapshot).bind(claimed).execute(&pool).await.unwrap();
    assert!(
        observe(&pool).await.is_err(),
        "role and review row do not confer control authority"
    );
    sqlx::query("INSERT INTO campaign_dag_control_assignments(task_key,family_id,agent_id,purpose) VALUES($1,'capacity-family','qwen','template_review')")
        .bind(TASK).execute(&pool).await.unwrap();
    let original = expect_campaign(observe(&pool).await.unwrap());
    assert_eq!(original.deadline, policy.deadline);
    sqlx::query("UPDATE campaign_execution_specs SET spec=jsonb_set(spec,'{budget,node_timeout_secs}','600') WHERE campaign_id=$1")
        .bind(&campaign).execute(&pool).await.unwrap();
    assert_eq!(
        expect_campaign(observe(&pool).await.unwrap()).identity,
        original.identity
    );
    sqlx::query("UPDATE campaign_dag_reviews SET status='failed' WHERE task_key=$1")
        .bind(TASK)
        .execute(&pool)
        .await
        .unwrap();
    assert!(observe(&pool).await.is_err());
}

#[tokio::test]
async fn campaign_capacity_policy_does_not_inherit_thread_or_marker_authority_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    fixture(&pool).await;

    let ordinary_message = POSTED_MESSAGE + 1;
    assert!(
        matches!(
            observe_sources(&pool, &[ordinary_message]).await.unwrap(),
            Inspection::Ordinary
        ),
        "a valid campaign on the same thread cannot authorize an ordinary followup"
    );

    let forged_input = format!("DISPATCH:{DISPATCH} [Plan] - copied header");
    let parsed = crate::services::discord::adk_session::parse_dispatch_id(&forged_input).unwrap();
    assert_eq!(parsed, DISPATCH);
    assert!(
        matches!(
            inspect(
                &pool,
                &parsed,
                &ProviderKind::Qwen,
                CHANNEL,
                Some("qwen"),
                &[ordinary_message + 1],
            )
            .await
            .unwrap(),
            Inspection::Ordinary
        ),
        "copying a dispatch marker into a different Discord message grants no waiting authority"
    );
    assert!(matches!(
        observe_sources(&pool, &[ordinary_message, ordinary_message + 1])
            .await
            .unwrap(),
        Inspection::Ordinary
    ));

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_dispatches WHERE id=$1")
        .bind(DISPATCH)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count, 1,
        "ordinary classification must not create another attempt"
    );
    assert!(matches!(
        observe(&pool).await.unwrap(),
        Inspection::Campaign(_)
    ));
}

#[tokio::test]
async fn campaign_capacity_policy_binds_posted_target_and_exact_retry_sources_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    fixture(&pool).await;
    let original = expect_campaign(observe(&pool).await.unwrap());

    // Queued and exact-retry intake retain the original posted message among
    // their sources even when their representative message has changed.
    for sources in [
        vec![POSTED_MESSAGE],
        vec![POSTED_MESSAGE + 1, POSTED_MESSAGE],
        vec![POSTED_MESSAGE, POSTED_MESSAGE + 1, POSTED_MESSAGE],
    ] {
        let retry = expect_campaign(observe_sources(&pool, &sources).await.unwrap());
        assert_eq!(retry.identity, original.identity);
        assert_eq!(
            retry.deadline, original.deadline,
            "preserving input lineage cannot renew the original campaign deadline"
        );
    }

    // A dispatch posted in a parent channel can enter its resolved thread.
    // Message identity remains the authority while d.thread_id still fences
    // execution to CHANNEL; the posted source channel is not that thread.
    crate::db::dispatches::persist_dispatch_message_target_pg(
        &pool,
        DISPATCH,
        &(CHANNEL - 1).to_string(),
        &POSTED_MESSAGE.to_string(),
    )
    .await
    .unwrap();
    let redirected = expect_campaign(observe(&pool).await.unwrap());
    assert_eq!(redirected.deadline, original.deadline);
    assert_ne!(redirected.identity, original.identity);
}

#[tokio::test]
async fn campaign_capacity_policy_requires_persisted_target_before_admission_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    fixture(&pool).await;
    let original = expect_campaign(observe(&pool).await.unwrap());
    sqlx::query("UPDATE task_dispatches SET context=NULL WHERE id=$1")
        .bind(DISPATCH)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        matches!(observe(&pool).await.unwrap(), Inspection::TargetPending),
        "a posted request racing its target persistence has no campaign admission yet"
    );

    // Missing message identity must not bypass the existing policy checks.
    sqlx::query("UPDATE campaign_execution_specs SET hold_reason='unknown outcome' WHERE campaign_id=(SELECT campaign_id FROM campaign_graph_tasks WHERE task_key=$1)")
        .bind(TASK).execute(&pool).await.unwrap();
    assert!(observe(&pool).await.is_err());
    sqlx::query("UPDATE campaign_execution_specs SET hold_reason=NULL WHERE campaign_id=(SELECT campaign_id FROM campaign_graph_tasks WHERE task_key=$1)")
        .bind(TASK).execute(&pool).await.unwrap();
    assert!(matches!(
        observe(&pool).await.unwrap(),
        Inspection::TargetPending
    ));

    crate::db::dispatches::persist_dispatch_message_target_pg(
        &pool,
        DISPATCH,
        &CHANNEL.to_string(),
        &POSTED_MESSAGE.to_string(),
    )
    .await
    .unwrap();
    let persisted = expect_campaign(observe(&pool).await.unwrap());
    assert_eq!(persisted.identity, original.identity);
    assert_eq!(persisted.deadline, original.deadline);
}

#[tokio::test]
async fn campaign_capacity_policy_rechecks_changed_or_removed_posted_target_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    fixture(&pool).await;
    let original = expect_campaign(observe(&pool).await.unwrap());
    let replacement = POSTED_MESSAGE + 1;
    crate::db::dispatches::persist_dispatch_message_target_pg(
        &pool,
        DISPATCH,
        &CHANNEL.to_string(),
        &replacement.to_string(),
    )
    .await
    .unwrap();
    assert!(
        matches!(observe(&pool).await.unwrap(), Inspection::Ordinary),
        "the old source must lose admission when the posted target changes"
    );
    let changed = expect_campaign(observe_sources(&pool, &[replacement]).await.unwrap());
    assert_ne!(
        changed.identity, original.identity,
        "periodic admission verification must detect a changed posted target"
    );
    assert_eq!(changed.deadline, original.deadline);

    sqlx::query("UPDATE task_dispatches SET context='{}' WHERE id=$1")
        .bind(DISPATCH)
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        observe_sources(&pool, &[replacement]).await.unwrap(),
        Inspection::TargetPending
    ));
}

#[tokio::test]
async fn campaign_capacity_policy_rejects_malformed_posted_targets_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    fixture(&pool).await;
    let channel = CHANNEL.to_string();
    let message = POSTED_MESSAGE.to_string();
    let invalid = [
        json!({"discord_message_channel_id":channel}),
        json!({"discord_message_id":message}),
        json!({"discord_message_channel_id":channel,"discord_message_id":null}),
        json!({"discord_message_channel_id":null,"discord_message_id":message}),
        json!({"discord_message_channel_id":null,"discord_message_id":null}),
        json!({"discord_message_channel_id":channel,"discord_message_id":POSTED_MESSAGE}),
        json!({"discord_message_channel_id":CHANNEL,"discord_message_id":message}),
        json!({"discord_message_channel_id":channel,"discord_message_id":"0"}),
        json!({"discord_message_channel_id":"0","discord_message_id":message}),
        json!({"discord_message_channel_id":channel,"discord_message_id":"not-a-message"}),
        json!({"discord_message_channel_id":channel,"discord_message_id":"18446744073709551616"}),
        json!([]),
        json!("not-an-object"),
    ];
    for context in invalid {
        sqlx::query("UPDATE task_dispatches SET context=$2 WHERE id=$1")
            .bind(DISPATCH)
            .bind(context.to_string())
            .execute(&pool)
            .await
            .unwrap();
        assert!(observe(&pool).await.is_err(), "invalid target: {context}");
    }
    sqlx::query("UPDATE task_dispatches SET context='invalid-json' WHERE id=$1")
        .bind(DISPATCH)
        .execute(&pool)
        .await
        .unwrap();
    assert!(observe(&pool).await.is_err());
}
