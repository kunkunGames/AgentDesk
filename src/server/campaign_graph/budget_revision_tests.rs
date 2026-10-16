use super::*;
use crate::db::campaigns::graph::revision::budget::{self, BudgetRevision};

#[tokio::test]
async fn candidate_reserves_independent_validation_and_unpublished_business_work_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let mut limited = goal();
    limited.budget.max_dispatches = 3;
    sqlx::query("UPDATE campaign_execution_specs SET spec=$2 WHERE campaign_id=$1")
        .bind(&c.id)
        .bind(json!(limited))
        .execute(&pool)
        .await
        .unwrap();
    control(
        &pool,
        &c.id,
        &limited,
        "plan",
        None,
        "initial",
        GRAPH_SCHEMA,
    )
    .await
    .unwrap();
    let before = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    assert_eq!(before["tasks"].as_array().unwrap().len(), 1);
    let error = store::stage(&pool, &c.id, &graph(), None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("requires 4 lifetime tasks") && error.contains("dispatch limit is 3"),
        "{error}"
    );
    let rejected = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    assert_eq!(rejected["tasks"], before["tasks"]);
    assert_eq!(rejected["versions"], before["versions"]);
    assert!(rejected["hold_reason"].is_null());

    limited.budget.max_dispatches = 4;
    sqlx::query("UPDATE campaign_execution_specs SET spec=$2 WHERE campaign_id=$1")
        .bind(&c.id)
        .bind(json!(limited))
        .execute(&pool)
        .await
        .unwrap();
    let version = store::stage(&pool, &c.id, &graph(), None).await.unwrap();
    assert_eq!(
        store::stage(&pool, &c.id, &graph(), None).await.unwrap(),
        version
    );
    let admitted = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    assert_eq!(admitted["versions"].as_array().unwrap().len(), 1);
    assert_eq!(admitted["tasks"], before["tasks"]);
    for run in sqlx::query_scalar::<_, String>("SELECT id FROM auto_queue_runs")
        .fetch_all(&pool)
        .await
        .unwrap()
    {
        crate::services::auto_queue::cancel_run::end_run_with_pg(None, &pool, &run)
            .await
            .unwrap();
    }
    pool.close().await;
    fixture.drop().await;
}

async fn exhausted(pool: &PgPool) -> (campaigns::Campaign, BudgetRevision) {
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
    let result = json!({"campaign_receipt":{"outcome":"blocked","failure_category":"code",
        "summary":"artifact boundary needs a bounded repair"}});
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at) VALUES('budget-attempt',$1,'qwen','implementation','completed',$2,NOW()-INTERVAL '10 seconds',NOW())")
        .bind(&card).bind(result.to_string()).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_progress(dispatch_id,event_id,fingerprint) VALUES('budget-attempt','tool-1','input')")
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
    store::hold(pool, &c.id, "Plan evolution budget exhausted")
        .await
        .unwrap();
    let c = campaigns::get(pool, &c.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let c = campaigns::replace(pool, &c.id, c.revision, input)
        .await
        .unwrap();
    let mut expanded = goal().budget;
    expanded.max_dispatches = 20;
    expanded.max_evolutions = 3;
    let request = BudgetRevision {
        expected_revision: c.revision,
        request_id: "finite-repair-v1".into(),
        evidence:
            "Explicit one additional repair after the advertised artifact limits were corrected"
                .into(),
        budget: expanded,
    };
    (c, request)
}

#[tokio::test]
async fn finite_budget_revision_retains_published_contracts_usage_and_raw_failure_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, mut request) = exhausted(&pool).await;
    let before = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    let revision = budget::extend(&pool, &c.id, &request).await.unwrap();
    assert_eq!(
        budget::extend(&pool, &c.id, &request).await.unwrap(),
        revision
    );
    let reopened = fixture.connect_and_migrate().await;
    let after = canonical_inspection(store::status(&reopened, &c.id).await.unwrap());
    for field in [
        "versions",
        "tasks",
        "failures",
        "active_version",
        "observed_tool_calls",
        "elapsed_dispatch_seconds",
    ] {
        assert_eq!(before[field], after[field], "{field}");
    }
    let mut old_goal = before["goal"].clone();
    old_goal["budget"] = json!(request.budget);
    assert_eq!(old_goal, after["goal"]);
    assert_eq!(after["goal_revisions"][0]["previous_goal"], before["goal"]);
    assert_eq!(after["goal_revisions"][0]["revised_goal"], after["goal"]);
    assert!(after["hold_reason"].is_null());
    assert_eq!(
        campaigns::get(&pool, &c.id).await.unwrap().status,
        CampaignStatus::Paused
    );
    request.evidence.push_str(" changed");
    assert!(budget::extend(&pool, &c.id, &request).await.is_err());
    assert!(sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) SELECT 'replay',card_id,'qwen','implementation','pending' FROM campaign_graph_tasks WHERE campaign_id=$1")
        .bind(&c.id).execute(&pool).await.is_err());
    reopened.close().await;
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn finite_budget_revision_rejects_unbounded_reductions_timeout_changes_and_unknown_effects_pg()
 {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, request) = exhausted(&pool).await;
    let before = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    let mut bad = request.budget.clone();
    bad.max_dispatches = 3;
    let mut unbounded = request.budget.clone();
    unbounded.max_observed_tool_calls = None;
    let mut timeout = request.budget.clone();
    timeout.node_timeout_secs += 1;
    let mut unlimited_evolution = request.budget.clone();
    unlimited_evolution.max_evolutions = crate::db::campaigns::graph::MAX_AUDITED_EVOLUTIONS + 1;
    for budget in [bad, unbounded, timeout, unlimited_evolution] {
        let change = BudgetRevision {
            budget,
            ..BudgetRevision {
                expected_revision: request.expected_revision,
                request_id: request.request_id.clone(),
                evidence: request.evidence.clone(),
                budget: request.budget.clone(),
            }
        };
        assert!(budget::extend(&pool, &c.id, &change).await.is_err());
    }
    assert_eq!(
        before,
        canonical_inspection(store::status(&pool, &c.id).await.unwrap())
    );
    sqlx::query("UPDATE campaign_graph_failures SET category='unknown' WHERE campaign_id=$1")
        .bind(&c.id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        budget::extend(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("reconciliation")
    );
    assert!(
        canonical_inspection(store::status(&pool, &c.id).await.unwrap())["hold_reason"].is_string()
    );
    assert!(
        canonical_inspection(store::status(&pool, &c.id).await.unwrap())["goal_revisions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    pool.close().await;
    fixture.drop().await;
}
