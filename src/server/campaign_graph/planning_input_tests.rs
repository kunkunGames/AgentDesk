//! Explicit PG fixtures verify admission and frozen advice, not production samples.
use super::*;
use crate::db::campaigns::graph::{control_budget, learning, planning_input};

#[tokio::test]
async fn control_budget_waits_before_publication_and_retains_old_usage_and_identity_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    learning::family(&pool, &c.id, &goal()).await.unwrap();
    let v = store::stage(&pool, &c.id, &graph(), None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated'")
        .execute(&pool)
        .await
        .unwrap();
    store::activate(&pool, &c.id, v, c.revision).await.unwrap();
    let task = &graph().nodes[0];
    let old = store::Publication {
        key: &task.task_key,
        node: &task.id,
        role: "work",
        version: Some(v),
        agent: &task.agent,
        title: &task.title,
        prompt: "fixture producer",
        digest: "fixture",
    };
    let card = store::publish(&pool, &c.id, &old).await.unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,status,dispatch_type,created_at,updated_at,completed_at)
        VALUES('elapsed-fixture',$1,'qwen','completed','implementation',NOW()-INTERVAL '543.7 seconds',NOW(),NOW())")
        .bind(&card).execute(&pool).await.unwrap();
    let key = format!("{}:plan:repair", c.id);
    let p = store::Publication {
        key: &key,
        node: "plan",
        role: "plan",
        version: Some(v),
        agent: "qwen",
        title: "Repair",
        prompt: "fixture repair",
        digest: "repair",
    };
    let error = store::publish(&pool, &c.id, &p).await.unwrap_err();
    assert!(
        error
            .downcast_ref::<control_budget::Unavailable>()
            .is_some(),
        "{error}"
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM campaign_graph_tasks WHERE task_key=$1")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count, 0,
        "a planner/card/batch must not start after reserve refusal"
    );
    let hold: String =
        sqlx::query_scalar("SELECT hold_reason FROM campaign_execution_specs WHERE campaign_id=$1")
            .bind(&c.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(hold.starts_with("Control budget requires audited recovery"));
    assert_eq!(
        store::publish(&pool, &c.id, &old).await.unwrap(),
        card,
        "old idempotent identity remains available even while held"
    );
    let elapsed = crate::db::campaigns::graph::progress::elapsed_seconds(&pool, &c.id)
        .await
        .unwrap();
    assert!(
        (elapsed - 543.7).abs() < 0.01,
        "usage must remain {elapsed}"
    );
    let dispatches: i64 = sqlx::query_scalar("SELECT count(*) FROM task_dispatches")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(dispatches, 1);

    // A finite audited increase can release only this admission hold after
    // real ownership has settled; it cannot forgive an execution outcome.
    sqlx::query("UPDATE campaign_graph_tasks SET observation='{}'::jsonb")
        .execute(&pool)
        .await
        .unwrap();
    for run in sqlx::query_scalar::<_, String>("SELECT id FROM auto_queue_runs")
        .fetch_all(&pool)
        .await
        .unwrap()
    {
        crate::services::auto_queue::cancel_run::end_run_with_pg(None, &pool, &run)
            .await
            .unwrap();
    }
    let current = campaigns::get(&pool, &c.id).await.unwrap();
    let mut input: campaigns::CampaignInput = serde_json::from_value(json!(current)).unwrap();
    input.status = CampaignStatus::Paused;
    let paused = campaigns::replace(&pool, &c.id, current.revision, input)
        .await
        .unwrap();
    let mut revised = goal().budget;
    revised.max_elapsed_secs = 620; // 76.3s still cannot fund the 90s reserve.
    let mut request = crate::db::campaigns::graph::revision::budget::BudgetRevision {
        expected_revision: paused.revision,
        request_id: "admission-recovery".into(),
        evidence: "Explicit fixture audit retains lifetime usage and settled ownership".into(),
        budget: revised,
    };
    assert!(
        crate::db::campaigns::graph::revision::budget::extend(&pool, &c.id, &request)
            .await
            .is_err()
    );
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["hold_reason"],
        hold
    );
    request.budget.max_elapsed_secs = 660;
    crate::db::campaigns::graph::revision::budget::extend(&pool, &c.id, &request)
        .await
        .unwrap();
    let recovered = store::status(&pool, &c.id).await.unwrap();
    assert!(recovered["hold_reason"].is_null());
    assert!((recovered["elapsed_dispatch_seconds"].as_f64().unwrap() - 543.7).abs() < 0.01);
}

async fn completed_sample(pool: &PgPool, c: &campaigns::Campaign) {
    let mut g = graph();
    for node in &mut g.nodes {
        node.task_key = format!("{}:{}", c.id, node.id);
    }
    learning::family(pool, &c.id, &goal()).await.unwrap();
    let version = store::stage(pool, &c.id, &g, None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated'")
        .execute(pool)
        .await
        .unwrap();
    store::activate(pool, &c.id, version, c.revision)
        .await
        .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let root = artifact_root().unwrap();
    for node in &g.nodes {
        for output in &node.outputs {
            std::fs::write(workspace.path().join(&output.path), "42").unwrap();
        }
        let manifest = artifacts::capture(workspace.path(), node, &root).unwrap();
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
                prompt: "explicit fixture work",
                digest: &store::task_digest(node).unwrap(),
            },
        )
        .await
        .unwrap();
        let dispatch = format!("fixture:{}", node.id);
        sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,status,dispatch_type,created_at,completed_at,result) VALUES($1,$2,$3,'completed','implementation',NOW(),NOW(),$4)")
            .bind(&dispatch).bind(&card).bind(&node.agent).bind(json!({"campaign_receipt":{"outcome":"passed","summary":"explicit fixture assertion","failure_category":"code","observations":[{"command":"fixture assert 42","exit_code":0,"evidence":"42"}]}}).to_string())
            .execute(pool).await.unwrap();
        sqlx::query("UPDATE kanban_cards SET status='done' WHERE id=$1")
            .bind(&card)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("UPDATE campaign_graph_tasks SET accepted=TRUE,observation=$2,artifact_manifest=$3 WHERE task_key=$1")
            .bind(&node.task_key).bind(json!({"dispatch_id":dispatch,"outcome":"accepted"})).bind(manifest).execute(pool).await.unwrap();
    }
    let mut input: campaigns::CampaignInput = serde_json::from_value(
        serde_json::to_value(campaigns::get(pool, &c.id).await.unwrap()).unwrap(),
    )
    .unwrap();
    input.status = CampaignStatus::Completed;
    for node in &mut input.nodes {
        node.status = campaigns::NodeStatus::Completed;
    }
    let revision = campaigns::get(pool, &c.id).await.unwrap().revision;
    campaigns::replace(pool, &c.id, revision, input)
        .await
        .unwrap();
    assert!(
        learning::samples::collect(pool, &c.id, &goal())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn follower_advice_is_frozen_materialized_and_verified_without_template_promotion_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    completed_sample(&pool, &c).await;
    let child = campaigns::create(
        &pool,
        "advice-child".into(),
        serde_json::from_value(
            json!({"title":"Advice child","status":"active","round":1,"nodes":[]}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    store::enroll(&pool, &child.id, child.revision, &goal())
        .await
        .unwrap();
    sqlx::query("INSERT INTO campaign_graph_occurrences(campaign_id,scheduled_at,child_id) VALUES($1,NOW(),$2)").bind(&c.id).bind(&child.id).execute(&pool).await.unwrap();
    let family = learning::family(&pool, &child.id, &goal()).await.unwrap();
    sqlx::query("UPDATE campaign_dag_families SET bootstrap_campaign_id=$2 WHERE id=$1")
        .bind(&family)
        .bind(&c.id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        learning::initial_assignment(&pool, &child.id, &goal())
            .await
            .unwrap()
            .is_none()
    );
    let input = planning_input::freeze(&pool, &child.id, &goal())
        .await
        .unwrap();
    let bytes = std::fs::read(
        artifact_root()
            .unwrap()
            .join(input.manifest["bundle"].as_str().unwrap())
            .join(planning_input::INPUT_FILE),
    )
    .unwrap();
    let advice: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(advice["purpose"], "planning_advice_only");
    assert_eq!(
        advice["verified_reference_structures"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        advice["verified_reference_structures"][0]["structure"]["nodes"][0]["task_key"].is_null()
    );
    let key = format!("{}:plan:initial", child.id);
    let p = store::Publication {
        key: &key,
        node: "plan",
        role: "plan",
        version: None,
        agent: "qwen",
        title: "Follower",
        prompt: "Read frozen advice",
        digest: "advice",
    };
    let card = store::publish_planning(&pool, &child.id, &p, None, &input)
        .await
        .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let context = json!({"worktree_path":workspace.path()});
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,status,dispatch_type,context) VALUES('advice-dispatch',$1,'qwen','dispatched','implementation',$2)").bind(&card).bind(context.to_string()).execute(&pool).await.unwrap();
    crate::services::campaign_control_input::prepare_input(&pool, "advice-dispatch")
        .await
        .unwrap();
    crate::services::campaign_control_input::verify_planning_input(&pool, &key, &context)
        .await
        .unwrap();
    std::fs::write(
        workspace.path().join(planning_input::INPUT_FILE),
        "{\"tampered\":true}",
    )
    .unwrap();
    assert!(
        crate::services::campaign_control_input::verify_planning_input(&pool, &key, &context)
            .await
            .is_err()
    );
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM campaign_dag_samples),(SELECT count(*) FROM campaign_dag_templates),(SELECT count(*) FROM campaign_dag_reviews)").fetch_one(&pool).await.unwrap();
    assert_eq!(counts, (1, 0, 0));
}
