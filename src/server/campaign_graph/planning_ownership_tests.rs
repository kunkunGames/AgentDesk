//! Reproduce paused-result, retired-validation and concurrent-plan admission races.
use super::replan_tests::{held_with_category, settle};
use super::*;
use crate::db::campaigns::graph::recovery::replan::{self, Replan, UnpublishedCandidate};

#[cfg(test)]
#[path = "unstarted_planner_tests.rs"]
mod unstarted_planner_tests;

async fn resume(pool: &PgPool, c: &campaigns::Campaign) -> campaigns::Campaign {
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(c).unwrap()).unwrap();
    input.status = CampaignStatus::Active;
    campaigns::replace(pool, &c.id, c.revision, input)
        .await
        .unwrap()
}

fn engine(pool: &PgPool) -> (tempfile::TempDir, PolicyEngine) {
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    (root, engine)
}

#[tokio::test]
async fn paused_retired_candidate_observes_late_qa_without_approval_failure_or_publication_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, mut proposed, _, mut request) = held_with_category(&pool, false, "plan").await;
    settle(&pool).await;
    let c = resume(&pool, &c).await;
    for node in &mut proposed.nodes {
        node.task_key.push_str(":retired");
    }
    let version = store::stage(&pool, &c.id, &proposed, Some(&request.failure_id))
        .await
        .unwrap();
    let digest = contract_digest(&proposed).unwrap();
    control(
        &pool,
        &c.id,
        &goal(),
        "validate",
        Some(version),
        "2",
        "Independent candidate QA",
    )
    .await
    .unwrap();
    let key = format!("{}:validate:2", c.id);
    let card = store::card_id(&key);
    let raw = json!({"campaign_receipt":{"outcome":"passed","summary":"Artifact-only candidate assessment",
        "observations":[{"command":"fixture source check","exit_code":0,"evidence":"Candidate assessment completed"}]},
        "graph_validation":{"digest":digest,"approved":true,"reason":"Plan approval is not business acceptance"}}).to_string();
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at)
        VALUES('late-retired-qa',$1,'monitoring','implementation','completed',$2,NOW(),NOW())")
        .bind(&card).bind(&raw).execute(&pool).await.unwrap();
    settle(&pool).await;
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let paused = campaigns::replace(&pool, &c.id, c.revision, input)
        .await
        .unwrap();
    request.expected_revision = paused.revision;
    request.supersede_candidate = Some(UnpublishedCandidate {
        version,
        digest: digest.clone(),
    });
    replan::request(&pool, &c.id, &request).await.unwrap();
    let before = store::status(&pool, &c.id).await.unwrap();
    assert!(
        reconciliation_candidates(&pool)
            .await
            .unwrap()
            .contains(&c.id)
    );
    assert!(
        !planning::observe_retired_validation(
            &pool,
            &c.id,
            &key,
            "late-retired-qa",
            version,
            &json!({"digest":"0".repeat(64)})
        )
        .await
        .unwrap()
    );
    let (_root, engine) = engine(&pool);
    tick(&pool, &engine, &c.id, None).await.unwrap();
    let after = store::status(&pool, &c.id).await.unwrap();
    let qa = after["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["task_key"] == key)
        .unwrap();
    assert_eq!(qa["accepted"], false);
    assert_eq!(qa["observation"]["outcome"], "superseded");
    for field in [
        "versions",
        "failures",
        "goal",
        "active_version",
        "occurrences",
        "observed_tool_calls",
        "elapsed_dispatch_seconds",
    ] {
        assert_eq!(before[field], after[field], "{field}");
    }
    assert_eq!(
        before["tasks"].as_array().unwrap().len(),
        after["tasks"].as_array().unwrap().len()
    );
    assert_eq!(
        campaigns::get(&pool, &c.id).await.unwrap().status,
        CampaignStatus::Paused
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_dispatches WHERE id='late-retired-qa'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        raw
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn issued_planner_at_evolution_boundary_is_not_cancelled_or_duplicated_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, _, request) = held_with_category(&pool, false, "plan").await;
    settle(&pool).await;
    let c = resume(&pool, &c).await;
    for suffix in ["old-1", "old-2", "current"] {
        control(
            &pool,
            &c.id,
            &goal(),
            "plan",
            Some(1),
            suffix,
            "Existing scoped planner",
        )
        .await
        .unwrap();
    }
    sqlx::query(
        "UPDATE campaign_graph_tasks SET observation='{\"outcome\":\"failed\"}'
        WHERE role='plan' AND task_key NOT LIKE '%:current'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let card = store::card_id(&format!("{}:plan:current", c.id));
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at)
        VALUES('issued-planner',$1,'qwen','implementation','dispatched','{}',NOW())")
        .bind(&card).execute(&pool).await.unwrap();
    // Keep the fixture queue inactive; the actual durable dispatch still owns its turn.
    sqlx::query("UPDATE auto_queue_runs SET status='completed'")
        .execute(&pool)
        .await
        .unwrap();
    let before = store::status(&pool, &c.id).await.unwrap();
    assert!(planning::pending(&before));
    assert_eq!(
        before["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t["role"] == "plan")
            .count(),
        3
    );
    let (_root, engine) = engine(&pool);
    tick(&pool, &engine, &c.id, None).await.unwrap();
    tick(&pool, &engine, &c.id, None).await.unwrap();
    let after = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(after["hold_reason"], Value::Null);
    assert_eq!(after["tasks"], before["tasks"]);
    assert_eq!(after["failures"], before["failures"]);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM task_dispatches WHERE id='issued-planner'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "dispatched"
    );
    assert!(
        after["failures"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["id"] == request.failure_id)
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn cancelled_latest_planner_repair_preserves_history_and_carries_source_correction_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, _, request) = held_with_category(&pool, false, "plan").await;
    settle(&pool).await;
    let reply = replan::request(&pool, &c.id, &request).await.unwrap();
    let c = campaigns::get(&pool, &c.id).await.unwrap();
    let c = resume(&pool, &c).await;
    control(
        &pool,
        &c.id,
        &goal(),
        "plan",
        Some(1),
        &reply.repair_failure_id,
        "Source correction planner",
    )
    .await
    .unwrap();
    let card = store::card_id(&format!("{}:plan:{}", c.id, reply.repair_failure_id));
    let raw = "{\"error\":\"Cancelled by erroneous next-turn admission\"}";
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at)
        VALUES('cancelled-planner',$1,'qwen','implementation','cancelled',$2,NOW()-INTERVAL '3 seconds',NOW())")
        .bind(&card).bind(raw).execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO campaign_graph_progress(dispatch_id,event_id,fingerprint)
        SELECT 'cancelled-planner','tool-'||n::text,'input-'||n::text FROM generate_series(1,20) n",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let c = campaigns::replace(&pool, &c.id, c.revision, input)
        .await
        .unwrap();
    store::hold(&pool, &c.id, "Plan evolution budget exhausted")
        .await
        .unwrap();
    assert!(
        reconciliation_candidates(&pool)
            .await
            .unwrap()
            .contains(&c.id)
    );
    let (_root, engine) = engine(&pool);
    tick(&pool, &engine, &c.id, None).await.unwrap();
    settle(&pool).await;
    tick(&pool, &engine, &c.id, None).await.unwrap();
    let before = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(
        before["hold_reason"],
        "Execution outcome requires reconciliation; automatic replay is disabled"
    );
    let failure = before["failures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["category"] == "unknown")
        .unwrap();
    let planning_input = reconcile::planning_evidence(&pool, failure).await.unwrap();
    assert_eq!(
        planning_input["operator_source_correction"]["operator_evidence"],
        request.evidence
    );
    let mut budget = goal().budget;
    budget.max_evolutions = 4;
    budget.max_observed_tool_calls = Some(40);
    let repair = Replan {cancelled_validation:None,rejected_validation:None,expected_revision:c.revision, request_id:"verified-cancelled-control-repair".into(),
        failure_id:failure["id"].as_str().unwrap().into(), evidence:"Verified cancelled control issued only read-only discovery; preserve original cause and full source correction, require new planner and independent QA.".into(),
        budget:Some(budget), supersede_candidate:None, unattempted_effect:None, settled_effect:None};
    let response = replan::request(&pool, &c.id, &repair).await.unwrap();
    assert!(response.independent_validation_required && response.goal_scope_preserved);
    let after = store::status(&pool, &c.id).await.unwrap();
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
    assert!(after["failures"].as_array().unwrap().contains(failure));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_dispatches WHERE id='cancelled-planner'"
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
