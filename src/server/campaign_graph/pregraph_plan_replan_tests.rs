//! Exercise a real initial recovery followed by a second native capacity refusal.
use super::*;
const SECOND: &str = "fixture-pregraph-second-capacity-refusal";

struct Chain {
    initial: InitialFixture,
    request: Replan,
    card: String,
}

async fn publish(pool: &PgPool, id: &str) {
    set_status(pool, id, CampaignStatus::Active).await;
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    let _ = tick(pool, &engine, id, None).await;
}

async fn chain(pool: &PgPool) -> Chain {
    let mut budget = goal().budget;
    budget.max_evolutions = 8;
    let initial = fixture_with_budget(pool, Some(budget)).await;
    let id = &initial.campaign.id;
    let first = replan::request(pool, id, &initial.request).await.unwrap();
    let (request, card) = fail_repair(
        pool,
        id,
        &first.repair_failure_id,
        SECOND,
        "fixture-pregraph-second-repair",
    )
    .await;
    Chain {
        initial,
        request,
        card,
    }
}

async fn fail_repair(
    pool: &PgPool,
    id: &str,
    repair: &str,
    attempt: &str,
    request_id: &str,
) -> (Replan, String) {
    publish(pool, id).await;
    let key = format!("{id}:plan:{repair}");
    let (card, version): (String, Option<i64>) =
        sqlx::query_as("SELECT card_id,version FROM campaign_graph_tasks WHERE task_key=$1")
            .bind(&key)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(version, Some(0));
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,created_at)
        VALUES($1,$2,'qwen','implementation','pending',NOW())",
    )
    .bind(attempt)
    .bind(&card)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(
        crate::dispatch::set_dispatch_status_on_pg_async(
            pool,
            attempt,
            "failed",
            Some(&capacity_error()),
            "turn_bridge_dispatch_failure",
            Some(&["pending"]),
            false
        )
        .await
        .unwrap(),
        1
    );
    let goal = store::goal(pool, id).await.unwrap();
    reconcile::observe(pool, id, &goal, None).await.unwrap();
    super::super::replan_tests::settle(pool).await;
    acknowledge_reaction(pool, attempt).await;
    let paused = set_status(pool, id, CampaignStatus::Paused).await;
    let state = store::status(pool, id).await.unwrap();
    assert_eq!(state["goal"]["budget"]["max_evolutions"], 8);
    assert!(state["active_version"].is_null());
    let failure = state["failures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["evidence"]["dispatch_id"] == attempt)
        .unwrap();
    let request = Replan { expected_revision: paused.revision,
        request_id: request_id.into(),
        failure_id: failure["id"].as_str().unwrap().into(),
        evidence: "Inspect the exact native capacity refusal and complete audited lineage; preserve occurrence budget8, history, usage and fresh independent QA.".into(),
        budget: None, supersede_candidate: None, unattempted_effect: None, settled_effect: None,
        rejected_validation: None, cancelled_validation: None };
    (request, card)
}

async fn history(pool: &PgPool, id: &str) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object(
        'tasks',(SELECT jsonb_agg(to_jsonb(t) ORDER BY task_key) FROM campaign_graph_tasks t WHERE campaign_id=$1),
        'cards',(SELECT jsonb_agg(to_jsonb(k) ORDER BY k.id) FROM kanban_cards k
            JOIN campaign_graph_tasks t ON t.card_id=k.id WHERE t.campaign_id=$1),
        'dispatches',(SELECT jsonb_agg(to_jsonb(d) ORDER BY d.id) FROM task_dispatches d
            JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id WHERE t.campaign_id=$1),
        'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.id) FROM dispatch_events e
            JOIN campaign_graph_tasks t ON t.card_id=e.kanban_card_id WHERE t.campaign_id=$1),
        'failures',(SELECT jsonb_agg(to_jsonb(f) ORDER BY id) FROM campaign_graph_failures f WHERE campaign_id=$1),
        'reconciliations',(SELECT jsonb_agg(to_jsonb(r) ORDER BY failure_id) FROM campaign_graph_reconciliations r WHERE campaign_id=$1),
        'revisions',(SELECT jsonb_agg(to_jsonb(r) ORDER BY request_id) FROM campaign_goal_revisions r WHERE campaign_id=$1))")
        .bind(id).fetch_one(pool).await.unwrap()
}

async fn refuse(pool: &PgPool, chain: &Chain, request: &Replan) -> String {
    let id = &chain.initial.campaign.id;
    let before = history(pool, id).await;
    let state = canonical_inspection(store::status(pool, id).await.unwrap());
    let error = replan::request(pool, id, request)
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(before, history(pool, id).await);
    assert_eq!(
        state,
        canonical_inspection(store::status(pool, id).await.unwrap())
    );
    error
}

#[tokio::test]
async fn pregraph_second_capacity_repair_preserves_budget8_history_and_fresh_qa_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = chain(&pool).await;
    let id = &f.initial.campaign.id;
    let before = history(&pool, id).await;
    let initial = retained(&pool, &f.initial).await;
    let state = store::status(&pool, id).await.unwrap();
    let (first, same) = tokio::join!(
        replan::request(&pool, id, &f.request),
        replan::request(&pool, id, &f.request)
    );
    let reply = first.unwrap();
    assert_eq!(same.unwrap().revision, reply.revision);
    assert!(reply.goal_scope_preserved && reply.independent_validation_required);
    let after = history(&pool, id).await;
    for key in ["tasks", "cards", "dispatches", "events"] {
        assert_eq!(after[key], before[key]);
    }
    for key in ["failures", "reconciliations", "revisions"] {
        for prior in before[key].as_array().unwrap() {
            assert!(after[key].as_array().unwrap().contains(prior));
        }
    }
    assert_eq!(retained(&pool, &f.initial).await, initial);
    let next = store::status(&pool, id).await.unwrap();
    for key in [
        "goal",
        "active_version",
        "versions",
        "budget_enforcement",
        "elapsed_dispatch_seconds",
        "observed_tool_calls",
    ] {
        assert_eq!(next[key], state[key], "preserved {key}");
    }
    assert_eq!(
        replan::request(&pool, id, &f.request)
            .await
            .unwrap()
            .revision,
        reply.revision
    );
    let mut changed = copy_request(&f.request);
    changed.evidence.push_str(" altered");
    refuse(&pool, &f, &changed).await;
    publish(&pool, id).await;
    let new_key = format!("{id}:plan:{}", reply.repair_failure_id);
    let new_card: String =
        sqlx::query_scalar("SELECT card_id FROM campaign_graph_tasks WHERE task_key=$1")
            .bind(new_key)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_ne!(new_card, f.card);
    let version = store::stage(&pool, id, &graph(), Some(&reply.repair_failure_id))
        .await
        .unwrap();
    let current = campaigns::get(&pool, id).await.unwrap();
    assert!(
        store::activate(&pool, id, version, current.revision)
            .await
            .is_err()
    );
    let current_goal = store::goal(&pool, id).await.unwrap();
    control(
        &pool,
        id,
        &current_goal,
        "validate",
        Some(version),
        &version.to_string(),
        "Independently verify the second recovered candidate",
    )
    .await
    .unwrap();
    let qa: String = sqlx::query_scalar(
        "SELECT k.assigned_agent_id FROM campaign_graph_tasks t
        JOIN kanban_cards k ON k.id=t.card_id WHERE t.task_key=$1",
    )
    .bind(format!("{id}:validate:{version}"))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_ne!(qa, current_goal.planner);
    assert!(
        schedule::admit(
            &pool,
            &f.initial.parent,
            &f.initial.parent_goal,
            f.initial.later_slot
        )
        .await
        .unwrap()
        .is_none()
    );
    super::super::replan_tests::settle(&pool).await;
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn pregraph_repair_rejects_lineage_and_profile_drift_without_mutation_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = chain(&pool).await;
    let id = &f.initial.campaign.id;
    for (table, column, key, value) in [
        (
            "campaign_goal_revisions",
            "request_digest",
            "campaign_id",
            json!("invalid-digest"),
        ),
        (
            "campaign_goal_revisions",
            "evidence",
            "campaign_id",
            json!("changed audit evidence"),
        ),
        (
            "campaign_graph_reconciliations",
            "evidence",
            "campaign_id",
            json!("changed reconciliation evidence"),
        ),
    ] {
        let select = format!("SELECT {column} FROM {table} WHERE {key}=$1");
        let original: String = sqlx::query_scalar(&select)
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let update = format!("UPDATE {table} SET {column}=$2 WHERE {key}=$1");
        sqlx::query(&update)
            .bind(id)
            .bind(value.as_str().unwrap())
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            refuse(&pool, &f, &f.request)
                .await
                .contains("canonical operator reconciliation chain")
        );
        sqlx::query(&update)
            .bind(id)
            .bind(original)
            .execute(&pool)
            .await
            .unwrap();
    }
    let original: Value =
        sqlx::query_scalar("SELECT execution_profile FROM campaign_graph_tasks WHERE card_id=$1")
            .bind(&f.card)
            .fetch_one(&pool)
            .await
            .unwrap();
    let mut changed = original.clone();
    changed["graph_version"] = Value::Null;
    sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=$2 WHERE card_id=$1")
        .bind(&f.card)
        .bind(changed)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        refuse(&pool, &f, &f.request)
            .await
            .contains("profile changed")
    );
    sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=$2 WHERE card_id=$1")
        .bind(&f.card)
        .bind(original)
        .execute(&pool)
        .await
        .unwrap();
    let mut expanded = copy_request(&f.request);
    let mut budget = store::goal(&pool, id).await.unwrap().budget;
    budget.max_evolutions = 9;
    expanded.budget = Some(budget);
    assert!(
        refuse(&pool, &f, &expanded)
            .await
            .contains("preserve the occurrence budget")
    );
    replan::request(&pool, id, &f.request).await.unwrap();
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn pregraph_repair_checks_ancestor_execution_and_complete_audit_set_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = chain(&pool).await;
    let id = &f.initial.campaign.id;
    // The latest refusal stays valid: proving only the latest attempt is insufficient.
    let raw: String = sqlx::query_scalar("SELECT result FROM task_dispatches WHERE id=$1")
        .bind(ATTEMPT)
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE task_dispatches SET result=$2 WHERE id=$1")
        .bind(ATTEMPT)
        .bind(json!({"error":"503"}).to_string())
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        refuse(&pool, &f, &f.request)
            .await
            .contains("exact native pre-provider")
    );
    sqlx::query("UPDATE task_dispatches SET result=$2 WHERE id=$1")
        .bind(ATTEMPT)
        .bind(raw)
        .execute(&pool)
        .await
        .unwrap();
    progress::tool_use(
        Some(&pool),
        Some(ATTEMPT),
        Some("ancestor-tool"),
        "ancestor-tool-hash",
    )
    .await;
    assert!(
        refuse(&pool, &f, &f.request)
            .await
            .contains("observed provider tools")
    );
    sqlx::query("DELETE FROM campaign_graph_progress WHERE dispatch_id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id)
        VALUES('fixture-pregraph-ancestor-owner','codex','opencode','awaiting_bg',$1)",
    )
    .bind(ATTEMPT)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        refuse(&pool, &f, &f.request)
            .await
            .contains("active execution")
    );
    sqlx::query("DELETE FROM sessions WHERE session_key='fixture-pregraph-ancestor-owner'")
        .execute(&pool)
        .await
        .unwrap();
    for table in ["campaign_graph_reconciliations", "campaign_goal_revisions"] {
        let original: Value = sqlx::query_scalar(&format!(
            "SELECT to_jsonb(r) FROM {table} r WHERE campaign_id=$1"
        ))
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query(&format!("DELETE FROM {table} WHERE campaign_id=$1"))
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        refuse(&pool, &f, &f.request).await;
        sqlx::query(&format!(
            "INSERT INTO {table} SELECT * FROM jsonb_populate_record(NULL::{table},$1)"
        ))
        .bind(original)
        .execute(&pool)
        .await
        .unwrap();
    }
    let repair: (String, Value) = sqlx::query_as(
        "SELECT id,evidence FROM campaign_graph_failures
        WHERE campaign_id=$1 AND category='plan'",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    for field in ["original_failure", "reconciled_failure_id"] {
        let mut altered = repair.1.clone();
        altered[field] = json!("wrong-lineage");
        sqlx::query("UPDATE campaign_graph_failures SET evidence=$2 WHERE id=$1")
            .bind(&repair.0)
            .bind(altered)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            refuse(&pool, &f, &f.request)
                .await
                .contains("canonical operator reconciliation chain")
        );
    }
    sqlx::query("UPDATE campaign_graph_failures SET evidence=$2 WHERE id=$1")
        .bind(&repair.0)
        .bind(&repair.1)
        .execute(&pool)
        .await
        .unwrap();
    let original: Value =
        sqlx::query_scalar("SELECT evidence FROM campaign_graph_failures WHERE id=$1")
            .bind(&f.request.failure_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let mut altered = original.clone();
    altered["runtime"]["graph_version"] = Value::Null;
    for evidence in [&altered, &original] {
        sqlx::query("UPDATE campaign_graph_failures SET evidence=$2 WHERE id=$1")
            .bind(&f.request.failure_id)
            .bind(evidence)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE campaign_graph_tasks SET observation=$2 WHERE card_id=$1")
            .bind(&f.card)
            .bind(evidence)
            .execute(&pool)
            .await
            .unwrap();
        if evidence == &altered {
            assert!(
                refuse(&pool, &f, &f.request)
                    .await
                    .contains("failure observation")
            );
        }
    }
    sqlx::query(
        "INSERT INTO campaign_graph_versions(campaign_id,version,graph,digest,status)
        VALUES($1,1,$2,$3,'candidate')",
    )
    .bind(id)
    .bind(json!(graph()))
    .bind(contract_digest(&graph()).unwrap())
    .execute(&pool)
    .await
    .unwrap();
    assert!(refuse(&pool, &f, &f.request).await.contains("no graph"));
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn pregraph_repair_rejects_newer_unobserved_control_and_extra_unknown_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = chain(&pool).await;
    let id = &f.initial.campaign.id;
    sqlx::query(
        "INSERT INTO campaign_graph_failures(id,campaign_id,task_key,fingerprint,category,evidence)
        SELECT 'fixture-extra-unknown',campaign_id,task_key,'extra','unknown',evidence
        FROM campaign_graph_failures WHERE id=$1",
    )
    .bind(&f.request.failure_id)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        refuse(&pool, &f, &f.request)
            .await
            .contains("additional unknown")
    );
    sqlx::query("DELETE FROM campaign_graph_failures WHERE id='fixture-extra-unknown'")
        .execute(&pool)
        .await
        .unwrap();
    // Synthetic corrupt-history case: publish an unrelated planner while the
    // private fixture is unheld, then restore the exact failure hold. Production
    // publication correctly refuses to create a control while already held.
    let hold: String =
        sqlx::query_scalar("SELECT hold_reason FROM campaign_execution_specs WHERE campaign_id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query("UPDATE campaign_execution_specs SET hold_reason=NULL WHERE campaign_id=$1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    set_status(&pool, id, CampaignStatus::Active).await;
    let goal = store::goal(&pool, id).await.unwrap();
    control(
        &pool,
        id,
        &goal,
        "plan",
        Some(0),
        "orphan-newer",
        GRAPH_SCHEMA,
    )
    .await
    .unwrap();
    sqlx::query("UPDATE campaign_execution_specs SET hold_reason=$2 WHERE campaign_id=$1")
        .bind(id)
        .bind(hold)
        .execute(&pool)
        .await
        .unwrap();
    super::super::replan_tests::settle(&pool).await;
    let current = set_status(&pool, id, CampaignStatus::Paused).await;
    let mut request = copy_request(&f.request);
    request.expected_revision = current.revision;
    assert!(
        refuse(&pool, &f, &request)
            .await
            .contains("complete unbranched repair history")
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn pregraph_third_capacity_repair_proves_every_prior_reconciliation_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = chain(&pool).await;
    let id = &f.initial.campaign.id;
    let second = replan::request(&pool, id, &f.request).await.unwrap();
    let (request, _) = fail_repair(
        &pool,
        id,
        &second.repair_failure_id,
        "fixture-pregraph-third-capacity-refusal",
        "fixture-pregraph-third-repair",
    )
    .await;
    let before = history(&pool, id).await;
    assert_eq!(before["tasks"].as_array().unwrap().len(), 3);
    assert_eq!(before["failures"].as_array().unwrap().len(), 5);
    assert_eq!(before["reconciliations"].as_array().unwrap().len(), 2);
    let reply = replan::request(&pool, id, &request).await.unwrap();
    assert!(reply.independent_validation_required && reply.goal_scope_preserved);
    let after = history(&pool, id).await;
    for key in ["tasks", "cards", "dispatches", "events"] {
        assert_eq!(after[key], before[key]);
    }
    for key in ["failures", "reconciliations", "revisions"] {
        assert_eq!(
            after[key].as_array().unwrap().len(),
            before[key].as_array().unwrap().len() + 1
        );
        for old in before[key].as_array().unwrap() {
            assert!(after[key].as_array().unwrap().contains(old));
        }
    }
    let state = store::status(&pool, id).await.unwrap();
    assert_eq!(state["goal"]["budget"]["max_evolutions"], 8);
    assert!(state["active_version"].is_null());
    assert_eq!(
        replan::request(&pool, id, &request).await.unwrap().revision,
        reply.revision
    );
    pool.close().await;
    db.drop().await;
}
