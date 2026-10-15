use super::*;
use crate::db::campaigns::graph::recovery::{self, Reconcile};

async fn rejected_identity(pool: &PgPool) -> (campaigns::Campaign, Graph, Reconcile) {
    let c = setup(pool).await;
    let original = graph();
    let version = store::stage(pool, &c.id, &original, None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated'")
        .execute(pool)
        .await
        .unwrap();
    store::activate(pool, &c.id, version, c.revision)
        .await
        .unwrap();
    let node = &original.nodes[0];
    let card = store::publish(
        pool,
        &c.id,
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
    .unwrap();
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    let blocked = json!({"campaign_receipt":{"outcome":"blocked","failure_category":"code",
        "summary":"artifact output contract needs bounded reconstruction"}});
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at) VALUES('artifact-attempt',$1,'qwen','implementation','completed',$2,NOW()-INTERVAL '10 seconds',NOW())")
        .bind(&card).bind(blocked.to_string()).execute(pool).await.unwrap();
    reconcile::observe(pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let failed = store::status(pool, &c.id).await.unwrap();
    assert_eq!(failed["failures"][0]["category"], "code");
    let failure_id = failed["failures"][0]["id"].as_str().unwrap();
    let plan_key = format!("{}:plan:{failure_id}", c.id);
    control(
        pool,
        &c.id,
        &goal(),
        "plan",
        Some(version),
        failure_id,
        "Repair artifact delivery",
    )
    .await
    .unwrap();
    let plan = store::card_id(&plan_key);
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&plan)
        .execute(pool)
        .await
        .unwrap();
    let mut proposed = original.clone();
    proposed.nodes[0]
        .details
        .push_str(" Use bounded output and verified round-trip reconstruction.");
    let result = json!({"campaign_graph":proposed,"campaign_receipt":{"outcome":"passed",
        "summary":"planning proposal only","observations":[{"command":"inspect source",
        "exit_code":0,"evidence":"checked proposed artifact contract"}]}});
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at) VALUES('identity-proposal',$1,'qwen','implementation','completed',$2,NOW()-INTERVAL '12 seconds',NOW())")
        .bind(&plan).bind(result.to_string()).execute(pool).await.unwrap();
    let _control_workspace = prepare_control_fixture(pool, "identity-proposal").await;
    sqlx::query("INSERT INTO campaign_graph_progress(dispatch_id,event_id,fingerprint) VALUES('identity-proposal','tool-1','observed-input')")
        .execute(pool).await.unwrap();
    reconcile::observe(pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let before = store::status(pool, &c.id).await.unwrap();
    let failure = before["failures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["task_key"] == plan_key)
        .unwrap();
    assert_eq!(
        failure["evidence"]["failure"]["error"],
        "a published task key cannot acquire a new contract"
    );
    let runs: Vec<String> = sqlx::query_scalar("SELECT id FROM auto_queue_runs")
        .fetch_all(pool)
        .await
        .unwrap();
    for run in runs {
        crate::services::auto_queue::cancel_run::end_run_with_pg(None, pool, &run)
            .await
            .unwrap();
    }
    store::hold(pool, &c.id, "Plan evolution budget exhausted")
        .await
        .unwrap();
    let c = campaigns::get(pool, &c.id).await.unwrap();
    let request = Reconcile {expected_revision:c.revision,
        failure_id:failure["id"].as_str().unwrap().into(),
        evidence:"The already captured bounded proposal changes only an unaccepted failed artifact producer. Give that contract its own identity and require independent QA; preserve original receipt and cumulative usage.".into()};
    (c, proposed, request)
}

#[tokio::test]
async fn captured_proposal_identity_recovery_preserves_attempts_usage_and_requires_qa_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, proposal, request) = rejected_identity(&pool).await;
    let before = store::status(&pool, &c.id).await.unwrap();
    let raw: String =
        sqlx::query_scalar("SELECT result FROM task_dispatches WHERE id='identity-proposal'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        recovery::reconcile(&pool, &c.id, &request).await.unwrap(),
        2
    );
    assert_eq!(
        recovery::reconcile(&pool, &c.id, &request).await.unwrap(),
        2
    );
    let reopened = fixture.connect_and_migrate().await;
    let after = store::status(&reopened, &c.id).await.unwrap();
    for field in [
        "tasks",
        "failures",
        "observed_tool_calls",
        "elapsed_dispatch_seconds",
        "active_version",
    ] {
        assert_eq!(before[field], after[field], "{field}");
    }
    assert_eq!(after["versions"][0]["status"], "candidate");
    let changed: Graph = serde_json::from_value(after["versions"][0]["graph"].clone()).unwrap();
    assert!(changed.nodes[0].task_key.starts_with("artifact-revision:"));
    assert_ne!(changed.nodes[0].task_key, proposal.nodes[0].task_key);
    assert_eq!(changed.nodes[0].acceptance, proposal.nodes[0].acceptance);
    assert_eq!(changed.nodes[0].outputs, proposal.nodes[0].outputs);
    assert_eq!(changed.nodes[1], proposal.nodes[1]);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_dispatches WHERE id='identity-proposal'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        raw
    );
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
    let mut old = graph();
    old.nodes[0]
        .details
        .push_str(" replacement against immutable key");
    assert!(
        store::stage(&pool, &c.id, &old, Some(&request.failure_id))
            .await
            .is_err()
    );
    reopened.close().await;
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn captured_proposal_identity_recovery_rejects_accepted_effectful_and_unknown_contracts_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, request) = rejected_identity(&pool).await;
    sqlx::query("UPDATE campaign_graph_tasks SET accepted=TRUE WHERE role='work'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(recovery::reconcile(&pool, &c.id, &request).await.is_err());
    sqlx::query("UPDATE campaign_graph_tasks SET accepted=FALSE WHERE role='work'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET graph=jsonb_set(graph,'{nodes,0,effect}','\"external\"') WHERE status='active'")
        .execute(&pool).await.unwrap();
    assert!(recovery::reconcile(&pool, &c.id, &request).await.is_err());
    sqlx::query("UPDATE campaign_graph_versions SET graph=jsonb_set(graph,'{nodes,0,effect}','\"artifact_only\"') WHERE status='active'")
        .execute(&pool).await.unwrap();
    sqlx::query(
        "UPDATE campaign_graph_failures SET category='unknown' WHERE task_key='repo:sha:produce'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        recovery::reconcile(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("unknown failures")
    );
    assert!(store::status(&pool, &c.id).await.unwrap()["hold_reason"].is_string());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_reconciliations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    pool.close().await;
    fixture.drop().await;
}
