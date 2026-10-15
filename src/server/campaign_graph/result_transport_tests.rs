use super::*;
use crate::db::campaigns::graph::{
    recovery::{self, Reconcile},
    result,
};

fn captured(graph: &Graph) -> Value {
    json!({"campaign_graph":graph,"campaign_receipt":{"outcome":"passed","summary":"planning only",
        "observations":[{"command":"inspect candidate","exit_code":0,"evidence":"fixture source inspected"}]},
        "summary":result::CAPTURE_SUMMARY,"work_outcome":"artifact","completion_source":"campaign_native_receipt"})
}
async fn terminal_plan(pool: &PgPool, c: &campaigns::Campaign, value: &Value) -> tempfile::TempDir {
    control(
        pool,
        &c.id,
        &goal(),
        "plan",
        Some(0),
        "repair",
        "Repair candidate",
    )
    .await
    .unwrap();
    let card = store::card_id(&format!("{}:plan:repair", c.id));
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at) VALUES('captured-plan',$1,'qwen','implementation','completed',$2,NOW()-INTERVAL '12 seconds',NOW())")
        .bind(&card).bind(value.to_string()).execute(pool).await.unwrap();
    let _control_workspace = prepare_control_fixture(pool, "captured-plan").await;
    sqlx::query("INSERT INTO campaign_graph_progress(dispatch_id,event_id,fingerprint) VALUES('captured-plan','tool-1','pinned-input')")
        .execute(pool).await.unwrap();
    _control_workspace
}

#[tokio::test]
async fn persisted_native_metadata_stages_plan_and_independent_qa_without_business_acceptance_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let raw = captured(&graph());
    let _control_workspace = terminal_plan(&pool, &c, &raw).await;
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let state = store::status(&pool, &c.id).await.unwrap();
    assert!(state["failures"].as_array().unwrap().is_empty());
    assert_eq!(state["tasks"][0]["accepted"], true);
    assert_eq!(state["versions"][0]["status"], "candidate");
    assert!(state["active_version"].is_null());
    control(
        &pool,
        &c.id,
        &goal(),
        "validate",
        Some(1),
        "1",
        "Independent QA",
    )
    .await
    .unwrap();
    let card = store::card_id(&format!("{}:validate:1", c.id));
    let mut qa = captured(&graph());
    qa.as_object_mut().unwrap().remove("campaign_graph");
    qa["graph_validation"] = json!({"digest":state["versions"][0]["digest"],"approved":true,"reason":"independently checked"});
    qa["completion_source"] = json!("watcher_streaming_final");
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result) VALUES('captured-qa',$1,'monitoring','implementation','completed',$2)")
        .bind(&card).bind(qa.to_string()).execute(&pool).await.unwrap();
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let after = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(after["versions"][0]["status"], "validated");
    assert!(after["active_version"].is_null());
    assert!(
        after["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["role"] != "work")
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_dispatches WHERE id='captured-plan'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        raw.to_string()
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn schema_transport_recovery_preserves_history_budgets_and_requires_new_validator_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let mut proposal = graph();
    proposal.nodes[0].kind = Kind::Inspect;
    let version = store::stage(&pool, &c.id, &proposal, None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='rejected' WHERE campaign_id=$1")
        .bind(&c.id)
        .execute(&pool)
        .await
        .unwrap();
    let raw = captured(&proposal);
    let _control_workspace = terminal_plan(&pool, &c, &raw).await;
    let key = format!("{}:plan:repair", c.id);
    let error = crate::db::campaigns::graph::schema::validate_result(&raw)
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("unknown field `completion_source`"));
    let evidence = json!({"dispatch_id":"captured-plan","external_effect_requires_reconciliation":false,"failure":{"error":error}});
    sqlx::query("INSERT INTO campaign_graph_failures(id,campaign_id,task_key,fingerprint,category,evidence) VALUES('schema-failure',$1,$2,'schema-fingerprint','plan',$3)")
        .bind(&c.id).bind(&key).bind(&evidence).execute(&pool).await.unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET observation=$2 WHERE task_key=$1")
        .bind(&key)
        .bind(&evidence)
        .execute(&pool)
        .await
        .unwrap();
    let runs: Vec<String> = sqlx::query_scalar("SELECT id FROM auto_queue_runs")
        .fetch_all(&pool)
        .await
        .unwrap();
    for run in runs {
        crate::services::auto_queue::cancel_run::end_run_with_pg(None, &pool, &run)
            .await
            .unwrap();
    }
    store::hold(&pool, &c.id, "Plan evolution budget exhausted")
        .await
        .unwrap();
    let request = Reconcile {expected_revision:c.revision,failure_id:"schema-failure".into(),
        evidence:"Verified native runtime/schema boundary. Preserve captured failures; require fresh independent QA, without a new planner turn or budget reset.".into()};
    let before = store::status(&pool, &c.id).await.unwrap();
    // Captured metadata alone cannot establish ownership, validity or remaining budget.
    for (field, wrong) in [
        ("completion_source", json!("operator_override")),
        ("status", json!("completed")),
    ] {
        let mut unsafe_result = raw.clone();
        unsafe_result[field] = wrong;
        sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='captured-plan'")
            .bind(unsafe_result.to_string())
            .execute(&pool)
            .await
            .unwrap();
        assert!(recovery::reconcile(&pool, &c.id, &request).await.is_err());
    }
    sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='captured-plan'")
        .bind(raw.to_string())
        .execute(&pool)
        .await
        .unwrap();
    let mut unsafe_graph = proposal.clone();
    unsafe_graph.nodes[0].effect = Effect::External;
    sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='captured-plan'")
        .bind(captured(&unsafe_graph).to_string())
        .execute(&pool)
        .await
        .unwrap();
    assert!(recovery::reconcile(&pool, &c.id, &request).await.is_err());
    sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='captured-plan'")
        .bind(raw.to_string())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_execution_specs SET spec=jsonb_set(spec,'{budget,max_evolutions}','0') WHERE campaign_id=$1").bind(&c.id).execute(&pool).await.unwrap();
    assert!(recovery::reconcile(&pool, &c.id, &request).await.is_err());
    sqlx::query("UPDATE campaign_execution_specs SET spec=$2 WHERE campaign_id=$1")
        .bind(&c.id)
        .bind(json!(goal()))
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        recovery::reconcile(&pool, &c.id, &request).await.unwrap(),
        version + 1
    );
    assert_eq!(
        recovery::reconcile(&pool, &c.id, &request).await.unwrap(),
        version + 1
    );
    let after = store::status(&pool, &c.id).await.unwrap();
    for field in [
        "tasks",
        "failures",
        "goal",
        "observed_tool_calls",
        "elapsed_dispatch_seconds",
        "active_version",
    ] {
        assert_eq!(before[field], after[field], "{field}");
    }
    assert_eq!(after["versions"][0]["status"], "candidate");
    assert_eq!(after["versions"][1], before["versions"][0]);
    assert!(
        store::activate(
            &pool,
            &c.id,
            version + 1,
            campaigns::get(&pool, &c.id).await.unwrap().revision
        )
        .await
        .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_dispatches WHERE id='captured-plan'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        raw.to_string()
    );
    pool.close().await;
    fixture.drop().await;
}
