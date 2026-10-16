use super::*;
use crate::db::campaigns::graph::recovery::replan::{self, Replan};

async fn pause(pool: &PgPool, id: &str) -> campaigns::Campaign {
    let current = campaigns::get(pool, id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    campaigns::replace(pool, id, current.revision, input)
        .await
        .unwrap()
}

pub(super) async fn settle(pool: &PgPool) {
    for run in sqlx::query_scalar::<_, String>("SELECT id FROM auto_queue_runs")
        .fetch_all(pool)
        .await
        .unwrap()
    {
        crate::services::auto_queue::cancel_run::end_run_with_pg(None, pool, &run)
            .await
            .unwrap();
    }
}

async fn held(pool: &PgPool, validator: bool) -> (campaigns::Campaign, Graph, String, Replan) {
    held_with_category(pool, validator, "unknown").await
}

pub(super) async fn held_with_category(
    pool: &PgPool,
    validator: bool,
    category: &str,
) -> (campaigns::Campaign, Graph, String, Replan) {
    let c = setup(pool).await;
    let mut original = graph();
    if validator {
        original.nodes[0].kind = Kind::Inspect;
    }
    let version = store::stage(pool, &c.id, &original, None).await.unwrap();
    let card = if validator {
        control(
            pool,
            &c.id,
            &goal(),
            "validate",
            Some(version),
            "1",
            "Read-only graph QA",
        )
        .await
        .unwrap();
        store::card_id(&format!("{}:validate:1", c.id))
    } else {
        sqlx::query("UPDATE campaign_graph_versions SET status='validated'")
            .execute(pool)
            .await
            .unwrap();
        store::activate(pool, &c.id, version, c.revision)
            .await
            .unwrap();
        let n = &original.nodes[0];
        store::publish(
            pool,
            &c.id,
            &store::Publication {
                key: &n.task_key,
                node: &n.id,
                role: "work",
                version: Some(version),
                agent: &n.agent,
                title: &n.title,
                prompt: &n.details,
                digest: &store::task_digest(n).unwrap(),
            },
        )
        .await
        .unwrap()
    };
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    let raw = if validator {
        "{}".to_owned()
    } else {
        json!({"campaign_receipt":{"outcome":"blocked","failure_category":category,
            "summary":"Original prerequisites were incorrectly interpreted",
            "observations":[{"command":"inspect source","exit_code":0,"evidence":"read-only inspection"}]}}).to_string()
    };
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at)
        VALUES('original-attempt',$1,$2,'implementation',$3,$4,NOW()-INTERVAL '8 seconds',NOW())")
        .bind(&card).bind(if validator { "monitoring" } else { "qwen" })
        .bind(if validator { "failed" } else { "completed" }).bind(&raw)
        .execute(pool).await.unwrap();
    progress::tool_use(
        Some(pool),
        Some("original-attempt"),
        Some("old-tool"),
        "input-hash",
    )
    .await;
    reconcile::observe(pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let c = pause(pool, &c.id).await;
    let before = store::status(pool, &c.id).await.unwrap();
    let request = Replan {
        cancelled_validation: None,
        rejected_validation: None,
        expected_revision: c.revision, request_id: "verified-business-repair-v1".into(),
        failure_id: before["failures"][0]["id"].as_str().unwrap().into(),
        evidence: "Verified original business scope and source requirements. Preserve original failed attempts; independently inspect before proposing a correction.".into(),
        budget: None,
        supersede_candidate: None,
        unattempted_effect: None,
        settled_effect: None,
    };
    (c, original, raw, request)
}

#[tokio::test]
async fn operator_replan_keeps_business_scope_history_usage_and_requires_existing_planner_qa_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, original, raw, mut request) = held(&pool, false).await;
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("cleanup")
    );
    settle(&pool).await;
    let before = store::status(&pool, &c.id).await.unwrap();
    let mut expanded = goal().budget;
    expanded.max_dispatches = 16;
    expanded.max_evolutions = 4;
    request.budget = Some(expanded.clone());
    let reply = replan::request(&pool, &c.id, &request).await.unwrap();
    assert_eq!(reply.planner_agent_id, "qwen");
    assert!(reply.independent_validation_required && reply.goal_scope_preserved);
    let reopened = fixture.connect_and_migrate().await;
    assert_eq!(
        replan::request(&reopened, &c.id, &request)
            .await
            .unwrap()
            .revision,
        reply.revision
    );
    let after = store::status(&reopened, &c.id).await.unwrap();
    for field in [
        "tasks",
        "versions",
        "active_version",
        "occurrences",
        "observed_tool_calls",
        "elapsed_dispatch_seconds",
    ] {
        assert_eq!(before[field], after[field], "{field}");
    }
    assert!(
        after["failures"]
            .as_array()
            .unwrap()
            .contains(&before["failures"][0])
    );
    let mut expected_goal = before["goal"].clone();
    expected_goal["budget"] = json!(expanded);
    assert_eq!(after["goal"], expected_goal);
    assert_eq!(after["goal_revisions"][0]["previous_goal"], before["goal"]);
    assert_eq!(after["goal_revisions"][0]["revised_goal"], expected_goal);
    assert_eq!(
        campaigns::get(&pool, &c.id).await.unwrap().status,
        CampaignStatus::Paused
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_dispatches WHERE id='original-attempt'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        raw
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_tasks WHERE role='plan'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    request.evidence.push_str(" changed");
    assert!(replan::request(&pool, &c.id, &request).await.is_err());

    // Exercise the actual tick after operator resume: a changed superior must
    // not take over this existing campaign's repair or published work.
    sqlx::query("UPDATE kv_meta SET value='{\"internalTasksEnabled\":true,\"campaignSuperiorAgentId\":\"monitoring\"}' WHERE key='runtime-config'")
        .execute(&pool).await.unwrap();
    let paused = campaigns::get(&pool, &c.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&paused).unwrap()).unwrap();
    input.status = CampaignStatus::Active;
    campaigns::replace(&pool, &c.id, paused.revision, input)
        .await
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    // The test has no global queue bridge; publication still uses the real tick.
    let _ = tick(&pool, &engine, &c.id, None).await;
    let key = format!("{}:plan:{}", c.id, reply.repair_failure_id);
    let planner = store::card_id(&key);
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT assigned_agent_id FROM kanban_cards WHERE id=$1"
        )
        .bind(&planner)
        .fetch_one(&pool)
        .await
        .unwrap()
        .as_deref(),
        Some("qwen")
    );
    settle(&pool).await;
    let mut corrected = original;
    corrected.nodes[0].task_key.push_str(":repaired");
    corrected.nodes[0]
        .details
        .push_str(" Verify source prerequisites first.");
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&planner)
        .execute(&pool)
        .await
        .unwrap();
    let proposal = json!({"campaign_receipt":{"outcome":"passed","summary":"Source-verified correction",
        "observations":[{"command":"inspect source","exit_code":0,"evidence":"Prerequisite corrected"}]},"campaign_graph":corrected});
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,completed_at)
        VALUES('repair-plan',$1,'qwen','implementation','completed',$2,NOW())")
        .bind(&planner).bind(proposal.to_string()).execute(&pool).await.unwrap();
    let _control_workspace = prepare_control_fixture(&pool, "repair-plan").await;
    reconcile::observe(
        &pool,
        &c.id,
        &store::goal(&pool, &c.id).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    let proposed = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(proposed["versions"][0]["status"], "candidate");
    assert_eq!(proposed["active_version"], 1);
    assert!(
        store::activate(
            &pool,
            &c.id,
            2,
            campaigns::get(&pool, &c.id).await.unwrap().revision
        )
        .await
        .is_err()
    );
    control(
        &pool,
        &c.id,
        &store::goal(&pool, &c.id).await.unwrap(),
        "validate",
        Some(2),
        "2",
        "Independent QA",
    )
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT assigned_agent_id FROM kanban_cards WHERE id=$1"
        )
        .bind(store::card_id(&format!("{}:validate:2", c.id)))
        .fetch_one(&pool)
        .await
        .unwrap()
        .as_deref(),
        Some("monitoring")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_tasks WHERE role='work'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "no business replay before QA"
    );
    reopened.close().await;
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn operator_replan_refuses_unknown_effects_stale_revision_live_owners_and_malformed_receipts_pg()
 {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, raw, mut request) = held(&pool, false).await;
    settle(&pool).await;
    let before = store::status(&pool, &c.id).await.unwrap();
    request.expected_revision -= 1;
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("revision")
    );
    request.expected_revision = c.revision;
    sqlx::query("UPDATE task_dispatches SET result='{}' WHERE id='original-attempt'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(replan::request(&pool, &c.id, &request).await.is_err());
    sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='original-attempt'")
        .bind(raw)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{external_effect_requires_reconciliation}','true')")
        .execute(&pool).await.unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("external effects")
    );
    sqlx::query("UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{external_effect_requires_reconciliation}','false')")
        .execute(&pool).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET graph=jsonb_set(graph,'{nodes,0,effect}','\"external\"')")
        .execute(&pool).await.unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("external business")
    );
    sqlx::query("UPDATE campaign_graph_versions SET graph=jsonb_set(graph,'{nodes,0,effect}','\"artifact_only\"')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_failures(id,campaign_id,task_key,fingerprint,category,evidence)
        SELECT 'other-unknown',campaign_id,task_key,'other','unknown',evidence FROM campaign_graph_failures")
        .execute(&pool).await.unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("additional unknown")
    );
    sqlx::query("DELETE FROM campaign_graph_failures WHERE id='other-unknown'")
        .execute(&pool)
        .await
        .unwrap();
    let card: String = sqlx::query_scalar("SELECT card_id FROM campaign_graph_tasks")
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id)
        VALUES('live-owner','qwen','qwen','turn_active','original-attempt')",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("active execution")
    );
    for status in ["awaiting_bg", "awaiting_user"] {
        sqlx::query("UPDATE sessions SET status=$1 WHERE session_key='live-owner'")
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            replan::request(&pool, &c.id, &request)
                .await
                .unwrap_err()
                .to_string()
                .contains("active execution")
        );
    }
    sqlx::query(
        "UPDATE sessions SET status='idle',active_dispatch_id=NULL WHERE session_key='live-owner'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut unbounded = goal().budget;
    unbounded.max_observed_tool_calls = None;
    request.budget = Some(unbounded);
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("finite increases")
    );
    request.budget = None;
    let mut excessive = goal().budget;
    excessive.max_evolutions = crate::db::campaigns::graph::MAX_AUDITED_EVOLUTIONS + 1;
    request.budget = Some(excessive);
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("invalid finite budget")
    );
    request.budget = None;
    // Publishing a dependent contract makes its input immutable, even when it
    // has no dispatch. A repair must not secretly replace that input.
    let n = &graph().nodes[1];
    store::publish(
        &pool,
        &c.id,
        &store::Publication {
            key: &n.task_key,
            node: &n.id,
            role: "work",
            version: Some(1),
            agent: &n.agent,
            title: &n.title,
            prompt: &n.details,
            digest: &store::task_digest(n).unwrap(),
        },
    )
    .await
    .unwrap_err(); // paused publication is fenced
    sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('published-descendant','Verify','ready','monitoring')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,version,node_id,role,card_id,contract_digest)
        VALUES($1,$2,1,'verify','work','published-descendant',$3)")
        .bind(&n.task_key).bind(&c.id).bind(store::task_digest(n).unwrap()).execute(&pool).await.unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("dependent task")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_goal_revisions")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_reconciliations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["hold_reason"],
        before["hold_reason"]
    );
    assert!(!card.is_empty());
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn operator_replan_failed_prebusiness_validation_retires_only_latest_candidate_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, raw, request) = held(&pool, true).await;
    let mut over_initial = goal();
    over_initial.budget.max_evolutions = 4;
    assert!(
        store::enroll(&pool, &c.id, c.revision, &over_initial)
            .await
            .unwrap_err()
            .to_string()
            .contains("initial admission")
    );
    settle(&pool).await;
    let before = store::status(&pool, &c.id).await.unwrap();
    let reply = replan::request(&pool, &c.id, &request).await.unwrap();
    let after = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(after["versions"][0]["status"], "rejected");
    assert_eq!(
        after["versions"][0]["graph"],
        before["versions"][0]["graph"]
    );
    assert_eq!(after["tasks"], before["tasks"]);
    assert_eq!(after["goal"], before["goal"]);
    assert_eq!(
        after["elapsed_dispatch_seconds"],
        before["elapsed_dispatch_seconds"]
    );
    assert_eq!(after["observed_tool_calls"], 1);
    assert!(after["active_version"].is_null());
    assert!(after["hold_reason"].is_null());
    assert_eq!(reply.planner_agent_id, "qwen");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_dispatches WHERE id='original-attempt'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        raw
    );
    assert_eq!(
        campaigns::get(&pool, &c.id).await.unwrap().status,
        CampaignStatus::Paused
    );
    pool.close().await;
    fixture.drop().await;
}
