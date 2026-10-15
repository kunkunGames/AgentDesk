//! Public Kanban edits must not replace an issued native graph contract.
use super::*;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use tower::ServiceExt;

const TEST_TOKEN: &str = "campaign-graph-card-contract-fixture";

fn api(pool: &PgPool, root: &std::path::Path) -> (Router, PolicyEngine) {
    let mut config = crate::config::Config::default();
    config.server.auth_token = Some(TEST_TOKEN.into());
    config.policies.dir = root.join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    let broadcast = crate::eventbus::new_broadcast();
    let buffer = crate::eventbus::spawn_batch_flusher(broadcast.clone());
    let routes = crate::server::routes::api_router_with_pg(
        engine.clone(),
        config,
        broadcast,
        buffer,
        None,
        Some(pool.clone()),
    );
    (Router::new().nest("/api", routes), engine)
}

async fn request(app: &Router, method: Method, path: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn ledger(pool: &PgPool) -> Value {
    sqlx::query_scalar(
        "SELECT jsonb_build_object(
          'campaigns',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.id) FROM campaigns c),
          'specs',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.campaign_id) FROM campaign_execution_specs s),
          'versions',(SELECT jsonb_agg(to_jsonb(v) ORDER BY v.campaign_id,v.version) FROM campaign_graph_versions v),
          'tasks',(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.task_key) FROM campaign_graph_tasks t),
          'cards',(SELECT jsonb_agg(to_jsonb(k) ORDER BY k.id) FROM kanban_cards k),
          'runs',(SELECT jsonb_agg(to_jsonb(r) ORDER BY r.id) FROM auto_queue_runs r),
          'entries',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.id) FROM auto_queue_entries e),
          'dispatches',(SELECT jsonb_agg(to_jsonb(d) ORDER BY d.id) FROM task_dispatches d),
          'failures',(SELECT jsonb_agg(to_jsonb(f) ORDER BY f.id) FROM campaign_graph_failures f))",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn work_card(pool: &PgPool, campaign: &campaigns::Campaign) -> String {
    let graph = graph();
    let version = store::stage(pool, &campaign.id, &graph, None)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE campaign_id=$1")
        .bind(&campaign.id)
        .execute(pool)
        .await
        .unwrap();
    store::activate(pool, &campaign.id, version, campaign.revision)
        .await
        .unwrap();
    let node = &graph.nodes[0];
    store::publish(
        pool,
        &campaign.id,
        &store::Publication {
            key: &node.task_key,
            node: &node.id,
            role: "work",
            version: Some(version),
            agent: &node.agent,
            title: &node.title,
            prompt: &node.details,
            digest: &store::task_digest(node).unwrap(),
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn public_patch_preserves_graph_executor_prompt_and_execution_policy_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let campaign = setup(&pool).await;
    let card = work_card(&pool, &campaign).await;
    let root = tempfile::tempdir().unwrap();
    let (app, _) = api(&pool, root.path());
    let original = ledger(&pool).await;
    let metadata: Value = sqlx::query_scalar("SELECT metadata FROM kanban_cards WHERE id=$1")
        .bind(&card)
        .fetch_one(&pool)
        .await
        .unwrap();
    let mut edits = vec![
        json!({"assignee_agent_id":"monitoring"}),
        json!({"repo_id":"other/repo"}),
        json!({"title":"Replacement instruction"}),
        json!({"description":"Replace the issued graph prompt"}),
        json!({"github_issue_url":"https://github.com/test/repo/issues/999"}),
        json!({"metadata":{}}),
    ];
    for (key, value) in [
        ("campaign_id", json!("another-campaign")),
        ("node_id", json!("another-node")),
        ("graph_version", json!(999)),
        ("graph_task_key", json!("another-task")),
        ("scope_assessment_status", json!("pending")),
        ("scope_depth", json!("full")),
        ("sandbox_preflight", json!(true)),
        ("production_mutation_allowed", json!(false)),
        ("managed_campaign", json!(true)),
        ("sandbox_preflight", Value::Null),
    ] {
        let mut changed = metadata.clone();
        changed[key] = value;
        edits.push(json!({"metadata":changed}));
    }
    for edit in edits {
        let (status, body) = request(
            &app,
            Method::PATCH,
            &format!("/api/kanban-cards/{card}"),
            edit.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{edit}: {body}");
        assert!(body.to_string().contains("execution contract is immutable"));
        assert_eq!(
            ledger(&pool).await,
            original,
            "rejected edit changed ledger"
        );
    }
    // Ordinary lifecycle metadata remains writable without relaxing identity.
    let mut runtime_metadata = metadata;
    runtime_metadata["preflight_status"] = json!("cleared");
    runtime_metadata["thread_id"] = json!("fixture-thread");
    let (status, body) = request(
        &app,
        Method::PATCH,
        &format!("/api/kanban-cards/{card}"),
        json!({"priority":"high","metadata":runtime_metadata,
            "assignee_agent_id":"qwen","title":"Produce","description":"Write answer.json"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let after = ledger(&pool).await;
    for key in [
        "campaigns",
        "specs",
        "versions",
        "tasks",
        "runs",
        "entries",
        "dispatches",
        "failures",
    ] {
        assert_eq!(after[key], original[key], "runtime edit changed {key}");
    }
    let before_number_edit = ledger(&pool).await;
    assert!(
        sqlx::query("UPDATE kanban_cards SET github_issue_number=999 WHERE id=$1")
            .bind(&card)
            .execute(&pool)
            .await
            .unwrap_err()
            .to_string()
            .contains("campaign_graph_card_contract_immutable")
    );
    assert_eq!(ledger(&pool).await, before_number_edit);
    let (status, _) = request(
        &app,
        Method::DELETE,
        &format!("/api/kanban-cards/{card}"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        ledger(&pool).await,
        before_number_edit,
        "existing card FK must retain graph history"
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn public_assign_cannot_rebind_issued_graph_card_after_setting_change_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let campaign = setup(&pool).await;
    let card = work_card(&pool, &campaign).await;
    let root = tempfile::tempdir().unwrap();
    let (app, _) = api(&pool, root.path());
    sqlx::query("UPDATE kv_meta SET value=$1 WHERE key='runtime-config'")
        .bind(
            json!({"internalTasksEnabled":true,"campaignSuperiorAgentId":"monitoring"}).to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();
    let original = ledger(&pool).await;
    let (status, body) = request(
        &app,
        Method::POST,
        &format!("/api/kanban-cards/{card}/assign"),
        json!({"agent_id":"monitoring"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(ledger(&pool).await, original);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT assigned_agent_id FROM kanban_cards WHERE id=$1")
            .bind(&card)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "qwen"
    );
    // The migration does not freeze cards outside the graph execution ledger.
    sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('ordinary-card','Ordinary','backlog','qwen')")
        .execute(&pool).await.unwrap();
    let (status, body) = request(
        &app,
        Method::PATCH,
        "/api/kanban-cards/ordinary-card",
        json!({"assignee_agent_id":"monitoring","description":"Ordinary updated prompt"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn graph_card_contract_guard_allows_native_receipt_and_normal_completion_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let campaign = setup(&pool).await;
    let root = tempfile::tempdir().unwrap();
    let (_, engine) = api(&pool, root.path());
    sqlx::query("UPDATE agents SET discord_channel_id='1470000000000000001' WHERE id='qwen'")
        .execute(&pool)
        .await
        .unwrap();
    control(
        &pool,
        &campaign.id,
        &goal(),
        "plan",
        None,
        "initial",
        GRAPH_SCHEMA,
    )
    .await
    .unwrap();
    let card: String =
        sqlx::query_scalar("SELECT card_id FROM campaign_graph_tasks WHERE campaign_id=$1")
            .bind(&campaign.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let issued: Value = sqlx::query_scalar("SELECT jsonb_build_object('assigned_agent_id',assigned_agent_id,'repo_id',repo_id,'title',title,'description',description,'metadata',metadata) FROM kanban_cards WHERE id=$1")
        .bind(&card).fetch_one(&pool).await.unwrap();
    // Seed the admitted queue state in this disposable PG fixture.
    sqlx::query("UPDATE auto_queue_runs SET status='active' WHERE id IN (SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=$1)")
        .bind(&card).execute(&pool).await.unwrap();
    // Execute the canonical dispatch writer and its ready -> in_progress FSM.
    // The supplied workspace exists, and no outbox delivery worker is launched.
    let (dispatch, _, reused) = crate::dispatch::create_dispatch_core_with_options(
        &pool,
        &card,
        "qwen",
        "implementation",
        issued["title"].as_str().unwrap(),
        &json!({"worktree_path":root.path(),"worktree_branch":"fixture-native"}),
        crate::dispatch::DispatchCreateOptions {
            skip_outbox: true,
            sidecar_dispatch: false,
        },
    )
    .await
    .unwrap();
    assert!(!reused);
    let entry: String =
        sqlx::query_scalar("SELECT id FROM auto_queue_entries WHERE kanban_card_id=$1")
            .bind(&card)
            .fetch_one(&pool)
            .await
            .unwrap();
    // Use the same canonical writer as activate's existing-dispatch attach
    // branch. This tests writer compatibility, not queue capacity admission.
    crate::db::auto_queue::update_entry_status_on_pg(
        &pool,
        &entry,
        crate::db::auto_queue::ENTRY_STATUS_DISPATCHED,
        "activate_attach_existing_dispatch_pg",
        &crate::db::auto_queue::EntryStatusUpdateOptions {
            dispatch_id: Some(dispatch.clone()),
            slot_index: None,
        },
    )
    .await
    .unwrap();
    let attached: (String, Option<String>) =
        sqlx::query_as("SELECT status,dispatch_id FROM auto_queue_entries WHERE kanban_card_id=$1")
            .bind(&card)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(attached, ("dispatched".into(), Some(dispatch.clone())));
    let metadata: Value = sqlx::query_scalar("SELECT metadata FROM kanban_cards WHERE id=$1")
        .bind(&card)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(metadata, issued["metadata"]);
    // Consume the frozen planning input exactly as native intake does. The
    // explicit provider fixture must retain this workspace through observation.
    let workspace = prepare_control_fixture(&pool, &dispatch).await;
    let profile: Value =
        sqlx::query_scalar("SELECT execution_profile FROM campaign_graph_tasks WHERE card_id=$1")
            .bind(&card)
            .fetch_one(&pool)
            .await
            .unwrap();
    let input_file = crate::db::campaigns::graph::planning_input::INPUT_FILE;
    let input_bytes = std::fs::read(workspace.path().join(input_file)).unwrap();
    assert_eq!(
        crate::db::campaigns::graph::digest(&input_bytes),
        profile["control_input_manifest"]["files"][input_file]
            .as_str()
            .unwrap()
    );
    let input: Value = serde_json::from_slice(&input_bytes).unwrap();
    assert_eq!(input["campaign_id"], campaign.id);
    assert_eq!(input["goal_digest"], contract_digest(&goal()).unwrap());
    let mut result = json!({"campaign_graph":graph(),"campaign_receipt":{"outcome":"passed","summary":"Fixture independently bounded plan","observations":[{"command":"fixture contract check","exit_code":0,"evidence":"Disposable fixture graph contract parsed and checked; no live provider execution"}]}});
    crate::db::campaigns::graph::receipt::validate_envelope(&result).unwrap();
    crate::db::campaigns::graph::schema::validate_result(&result).unwrap();
    result["summary"] = json!(crate::db::campaigns::graph::result::CAPTURE_SUMMARY);
    result["work_outcome"] = json!("artifact");
    result["completion_source"] = json!("campaign_native_receipt");
    crate::db::campaigns::graph::result::validate_dispatch(&result).unwrap();
    assert_eq!(
        crate::dispatch::set_dispatch_status_on_pg_async(
            &pool,
            &dispatch,
            "completed",
            Some(&result),
            "campaign_native_receipt",
            Some(&["pending"]),
            true,
        )
        .await
        .unwrap(),
        1
    );
    let events: Vec<Value> = sqlx::query_scalar(
        "SELECT jsonb_build_object('card_id',kanban_card_id,'dispatch_type',dispatch_type,
          'from_status',from_status,'to_status',to_status,'source',transition_source,'payload',payload_json)
         FROM dispatch_events WHERE dispatch_id=$1 AND to_status='completed'",
    )
    .bind(&dispatch)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["card_id"], card);
    assert_eq!(events[0]["dispatch_type"], "implementation");
    assert_eq!(events[0]["from_status"], "pending");
    assert_eq!(events[0]["to_status"], "completed");
    assert_eq!(events[0]["source"], "campaign_native_receipt");
    assert_eq!(events[0]["payload"], result);
    sqlx::query("UPDATE kanban_cards SET review_status='none',review_notes='Fixture receipt awaits native graph acceptance',updated_at=NOW() WHERE id=$1")
        .bind(&card).execute(&pool).await.unwrap();
    reconcile::observe(&pool, &campaign.id, &goal(), None)
        .await
        .unwrap();
    let observed = store::status(&pool, &campaign.id).await.unwrap();
    assert_eq!(observed["failures"], json!([]), "{observed}");
    assert_eq!(observed["tasks"].as_array().unwrap().len(), 1);
    assert_eq!(observed["tasks"][0]["accepted"], true, "{observed}");
    assert_eq!(observed["tasks"][0]["observation"]["dispatch_id"], dispatch);
    assert_eq!(observed["tasks"][0]["observation"]["outcome"], "accepted");
    assert_eq!(observed["versions"].as_array().unwrap().len(), 1);
    assert_eq!(observed["versions"][0]["status"], "candidate");
    assert_eq!(observed["versions"][0]["planner_agent_id"], "qwen");
    assert!(observed["active_version"].is_null());
    reconcile::settle_accepted(&pool, &engine, &campaign.id)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM kanban_cards WHERE id=$1")
            .bind(&card)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "done"
    );
    let saved: Value = sqlx::query_scalar("SELECT jsonb_build_object('assigned_agent_id',assigned_agent_id,'repo_id',repo_id,'title',title,'description',description,'metadata',metadata) FROM kanban_cards WHERE id=$1")
        .bind(&card).fetch_one(&pool).await.unwrap();
    for key in ["assigned_agent_id", "repo_id", "title", "description"] {
        assert_eq!(saved[key], issued[key]);
    }
    for key in ["campaign_id", "node_id", "graph_version", "graph_task_key"] {
        assert_eq!(saved["metadata"][key], issued["metadata"][key]);
    }
    assert!(
        sqlx::query("UPDATE kanban_cards SET assigned_agent_id='monitoring' WHERE id=$1")
            .bind(&card)
            .execute(&pool)
            .await
            .unwrap_err()
            .to_string()
            .contains("campaign_graph_card_contract_immutable")
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn v26_campaign_migrations_and_named_guards_are_installed_pg() {
    use sha2::{Digest, Sha384};

    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = crate::db::postgres::connect_test_pool_with_max_connections(
        &fixture.database_url,
        "campaign immutable guards",
        4,
    )
    .await
    .unwrap();
    crate::db::postgres::install_campaign_fixture_through(&pool, 145).await;
    crate::db::postgres::migrate(&pool).await.unwrap();
    let successful_versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations WHERE success ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    let latest_version = successful_versions.last().copied().unwrap();
    let failed_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE NOT success")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(latest_version, 146);
    assert_eq!(failed_count, 0);
    assert!(
        crate::db::postgres::applied_migration_checksum_mismatch_details(&pool)
            .await
            .unwrap()
            .is_empty()
    );
    let sources: [(i64, &[u8]); 5] = [
        (
            139,
            include_bytes!(
                "../../../migrations/postgres_campaign_v5/0139_campaign_dag_templates.sql"
            ),
        ),
        (
            142,
            include_bytes!(
                "../../../migrations/postgres_campaign_v5/0142_campaign_control_ownership.sql"
            ),
        ),
        (
            143,
            include_bytes!(
                "../../../migrations/postgres_campaign_v5/0143_campaign_review_execution_policy.sql"
            ),
        ),
        (
            144,
            include_bytes!(
                "../../../migrations/postgres_campaign_v5/0144_campaign_graph_card_contract.sql"
            ),
        ),
        (
            145,
            include_bytes!(
                "../../../migrations/postgres_campaign_v5/0145_campaign_workflow_manager.sql"
            ),
        ),
    ];
    let mut migration_checksums = Vec::new();
    let mut source_migration_sha256 = Vec::new();
    for (version, bytes) in sources {
        let source_hash = crate::db::campaigns::graph::digest(bytes);
        if version == 139 {
            assert_eq!(
                source_hash,
                "29cc9653f622dab7dbc599c5217d1cf4fdf086dce6e16cba18c948ec61b7144d"
            );
        } else if version == 142 {
            assert_eq!(
                source_hash,
                "0dc9005b7bfa7002ce4fec2b135159891c3ae2276a79ae0e484f4eab294af94a"
            );
        } else if version == 145 {
            assert_eq!(
                source_hash,
                "09980d2c3b59e6d2095af02d2bd979fb8b10f33c0088a9a6eb17b4dd7b69b596"
            );
        }
        let applied: String = sqlx::query_scalar(
            "SELECT encode(checksum,'hex') FROM _sqlx_migrations WHERE version=$1 AND success",
        )
        .bind(version)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(applied, hex::encode(Sha384::digest(bytes)));
        migration_checksums.push(json!({"version":version,"checksum_sha384":applied}));
        source_migration_sha256.push(json!({"version":version,"sha256":source_hash}));
    }
    let mut named_guards = Vec::new();
    for (name, table) in [
        ("guard_graph_dispatch", "task_dispatches"),
        ("guard_graph_control_ownership", "task_dispatches"),
        ("guard_graph_review_policy", "task_dispatches"),
        ("guard_review_claim_identity", "campaign_dag_reviews"),
        ("guard_graph_card_contract", "kanban_cards"),
        ("guard_workflow_control", "task_dispatches"),
    ] {
        let row = sqlx::query("SELECT n.nspname AS table_schema,r.relname AS table_name,
            t.tgenabled::text AS enabled,pg_get_triggerdef(t.oid,true)||chr(10)||pg_get_functiondef(t.tgfoid) AS definition
            FROM pg_trigger t JOIN pg_class r ON r.oid=t.tgrelid
            JOIN pg_namespace n ON n.oid=r.relnamespace
            WHERE t.tgname=$1 AND NOT t.tgisinternal AND n.nspname='public'")
            .bind(name).fetch_one(&pool).await.unwrap();
        let table_schema: String = row.get("table_schema");
        let table_name: String = row.get("table_name");
        let enabled: String = row.get("enabled");
        let definition: String = row.get("definition");
        assert_eq!(table_name, table);
        assert_eq!(enabled, "O");
        named_guards.push(json!({"name":name,"table_schema":table_schema,"table_name":table_name,
            "enabled":enabled,"definition_sha256":crate::db::campaigns::graph::digest(definition.as_bytes()),
            "definition":definition}));
    }
    let evidence = json!({"latest_version":latest_version,
        "successful_versions":successful_versions,"failed_count":failed_count,
        "migration_checksums":migration_checksums,"source_migration_sha256":source_migration_sha256,
        "named_guards":named_guards});
    // Emit success evidence only after the owned fixture database is removed.
    pool.close().await;
    fixture.drop().await;
    println!("ADK_V26_MIGRATION_EVIDENCE={evidence}");
}
