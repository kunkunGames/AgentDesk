use super::*;
use crate::db::campaigns::graph::recovery::{self, Reconcile};

async fn blocked_plan(pool: &PgPool) -> (campaigns::Campaign, Graph, Reconcile, String) {
    let c = setup(pool).await;
    control(
        pool,
        &c.id,
        &goal(),
        "plan",
        None,
        "initial",
        "Inspect and plan",
    )
    .await
    .unwrap();
    let card: String =
        sqlx::query_scalar("SELECT card_id FROM campaign_graph_tasks WHERE campaign_id=$1")
            .bind(&c.id)
            .fetch_one(pool)
            .await
            .unwrap();
    let mut proposal = graph();
    proposal.nodes[0].kind = Kind::Inspect;
    let receipt = json!({"campaign_receipt":{"outcome":"blocked",
        "failure_category":"infrastructure","summary":"repository discovery executable unavailable"},
        "campaign_graph":proposal});
    // Persisted dispatches may contain one JSON string framing layer.
    let raw = serde_json::to_string(&receipt.to_string()).unwrap();
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at) VALUES('blocked-plan',$1,'qwen','implementation','completed',$2,NOW()-INTERVAL '12 seconds',NOW())")
        .bind(&card).bind(&raw).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_progress(dispatch_id,event_id,fingerprint) VALUES('blocked-plan','tool-1','input-hash')")
        .execute(pool).await.unwrap();
    reconcile::observe(pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let state = store::status(pool, &c.id).await.unwrap();
    let request = Reconcile {expected_revision:c.revision,
        failure_id:state["failures"][0]["id"].as_str().unwrap().into(),
        evidence:"Internal repository discovery executable verified; repeat discovery and validate independently.".into()};
    (c, proposal, request, raw)
}

async fn close_runs(pool: &PgPool) {
    let runs: Vec<String> = sqlx::query_scalar("SELECT id FROM auto_queue_runs")
        .fetch_all(pool)
        .await
        .unwrap();
    for run in runs {
        crate::services::auto_queue::cancel_run::end_run_with_pg(None, pool, &run)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn planning_recovery_preserves_blocked_receipt_and_requires_independent_validation_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, original, mut request, raw) = blocked_plan(&pool).await;
    assert!(
        recovery::reconcile(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("cleanup")
    );
    close_runs(&pool).await;
    request.expected_revision -= 1;
    assert!(
        recovery::reconcile(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("revision")
    );
    request.expected_revision = c.revision;
    let before = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(
        recovery::reconcile(&pool, &c.id, &request).await.unwrap(),
        1
    );
    assert_eq!(
        recovery::reconcile(&pool, &c.id, &request).await.unwrap(),
        1
    );
    let reopened = fixture.connect_and_migrate().await;
    let after = store::status(&reopened, &c.id).await.unwrap();
    assert_eq!(before["tasks"], after["tasks"]);
    assert_eq!(before["failures"], after["failures"]);
    assert_eq!(before["observed_tool_calls"], after["observed_tool_calls"]);
    assert_eq!(
        before["elapsed_dispatch_seconds"],
        after["elapsed_dispatch_seconds"]
    );
    assert_eq!(after["active_version"], Value::Null);
    assert_eq!(after["versions"][0]["status"], "candidate");
    assert_eq!(after["versions"][0]["failure_id"], request.failure_id);
    let changed: Graph = serde_json::from_value(after["versions"][0]["graph"].clone()).unwrap();
    assert_eq!(changed.nodes[1], original.nodes[1]);
    assert!(changed.nodes[0].details.contains(&request.evidence));
    let retained: String =
        sqlx::query_scalar("SELECT result FROM task_dispatches WHERE id='blocked-plan'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(retained, raw);
    let current = campaigns::get(&pool, &c.id).await.unwrap();
    assert!(
        store::activate(&pool, &c.id, 1, current.revision)
            .await
            .is_err()
    );
    request.evidence.push_str(" changed");
    assert!(recovery::reconcile(&pool, &c.id, &request).await.is_err());
    reopened.close().await;
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn planning_recovery_rejects_unknown_effects_unbounded_outputs_and_missing_proposals_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, original, request, _) = blocked_plan(&pool).await;
    close_runs(&pool).await;
    let mut external = original.clone();
    external.nodes[0].effect = Effect::External;
    let mut unbounded = original.clone();
    unbounded.nodes[0].outputs = (0..17)
        .map(|i| Output {
            path: format!("part-{i}.txt"),
            format: Format::Text,
        })
        .collect();
    for proposal in [json!(external), json!(unbounded), Value::Null] {
        let result = json!({"campaign_receipt":{"outcome":"blocked","failure_category":"infrastructure"},
            "campaign_graph":proposal});
        sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='blocked-plan'")
            .bind(result.to_string())
            .execute(&pool)
            .await
            .unwrap();
        assert!(recovery::reconcile(&pool, &c.id, &request).await.is_err());
        let state = store::status(&pool, &c.id).await.unwrap();
        assert!(state["hold_reason"].is_string());
        assert!(state["versions"].as_array().unwrap().is_empty());
    }
    sqlx::query("UPDATE campaign_graph_failures SET category='unknown' WHERE id=$1")
        .bind(&request.failure_id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        recovery::reconcile(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("unknown planning")
    );
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
