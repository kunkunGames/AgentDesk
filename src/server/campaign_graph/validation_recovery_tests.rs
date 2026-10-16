use super::*;
use crate::db::campaigns::graph::recovery::{self, Reconcile};

async fn lost_validation(pool: &PgPool) -> (campaigns::Campaign, Graph, Reconcile) {
    let c = setup(pool).await;
    let mut original = graph();
    original.nodes[0].kind = Kind::Inspect;
    let version = store::stage(pool, &c.id, &original, None).await.unwrap();
    control(
        pool,
        &c.id,
        &goal(),
        "validate",
        Some(version),
        "1",
        "Independently validate",
    )
    .await
    .unwrap();
    let card = store::card_id(&format!("{}:validate:1", c.id));
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at) VALUES('lost-validator',$1,'monitoring','implementation','completed',$2,NOW()-INTERVAL '8 seconds',NOW())")
        .bind(&card).bind(json!({"completion_source":"watcher_streaming_final","agent_response_present":true}).to_string())
        .execute(pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_progress(dispatch_id,event_id,fingerprint) VALUES('lost-validator','tool-1','input-hash')")
        .execute(pool).await.unwrap();
    reconcile::observe(pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let runs: Vec<String> = sqlx::query_scalar("SELECT id FROM auto_queue_runs")
        .fetch_all(pool)
        .await
        .unwrap();
    for run in runs {
        crate::services::auto_queue::cancel_run::end_run_with_pg(None, pool, &run)
            .await
            .unwrap();
    }
    let before = store::status(pool, &c.id).await.unwrap();
    let request = Reconcile {expected_revision:c.revision,failure_id:before["failures"][0]["id"].as_str().unwrap().into(),
        evidence:"Structured native receipt transport now precedes ordinary streaming completion. Preserve the terminal failed validator; independently validate a fresh candidate before any business task. Retained provider files are diagnostic evidence, not approval.".into()};
    (c, original, request)
}

#[tokio::test]
async fn lost_validation_recovery_preserves_raw_attempt_and_usage_then_requires_fresh_qa_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, original, request) = lost_validation(&pool).await;
    let before = store::status(&pool, &c.id).await.unwrap();
    let raw: String =
        sqlx::query_scalar("SELECT result FROM task_dispatches WHERE id='lost-validator'")
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
    assert_eq!(after["versions"][1]["status"], "rejected");
    let changed: Graph = serde_json::from_value(after["versions"][0]["graph"].clone()).unwrap();
    assert_eq!(changed.nodes[0].acceptance, original.nodes[0].acceptance);
    assert_eq!(changed.nodes[0].task_key, original.nodes[0].task_key);
    assert_eq!(changed.nodes[1], original.nodes[1]);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_dispatches WHERE id='lost-validator'"
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
    reopened.close().await;
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn lost_validation_recovery_refuses_other_results_or_effectful_roots_without_clearing_hold_pg()
 {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, request) = lost_validation(&pool).await;
    sqlx::query("UPDATE task_dispatches SET result='{}' WHERE id='lost-validator'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(recovery::reconcile(&pool, &c.id, &request).await.is_err());
    sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='lost-validator'")
        .bind(
            json!({"completion_source":"watcher_streaming_final","agent_response_present":true})
                .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET graph=jsonb_set(graph,'{nodes,0,effect}','\"external\"') WHERE status='candidate'").execute(&pool).await.unwrap();
    assert!(recovery::reconcile(&pool, &c.id, &request).await.is_err());
    let after = store::status(&pool, &c.id).await.unwrap();
    assert!(after["hold_reason"].is_string());
    assert_eq!(after["versions"][0]["status"], "candidate");
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
