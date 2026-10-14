use super::*;
use crate::db::campaigns::graph::recovery::{self, Reconcile};

#[tokio::test]
async fn invalid_repair_at_evolution_limit_holds_active_graph_without_replay_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let original = graph();
    let version = store::stage(&pool, &c.id, &original, None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated'")
        .execute(&pool)
        .await
        .unwrap();
    store::activate(&pool, &c.id, version, c.revision)
        .await
        .unwrap();
    for node in &original.nodes {
        store::publish(
            &pool,
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
    }
    for suffix in ["initial", "repair-one", "repair-two"] {
        control(
            &pool,
            &c.id,
            &goal(),
            "plan",
            Some(version),
            suffix,
            GRAPH_SCHEMA,
        )
        .await
        .unwrap();
    }
    let failed_key = format!("{}:plan:repair-two", c.id);
    let card = store::card_id(&failed_key);
    sqlx::query("UPDATE campaign_graph_tasks SET observation='{}' WHERE task_key<>$1")
        .bind(&failed_key)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE auto_queue_runs SET status='completed'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE auto_queue_entries SET status='cancelled'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
    let mut invalid = original.clone();
    invalid.nodes[0].outputs = (0..17)
        .map(|n| Output {
            path: format!("part{n}.txt"),
            format: Format::Text,
        })
        .collect();
    let result = json!({"campaign_graph":invalid,"campaign_receipt":{"outcome":"passed","summary":"proposed a repair","observations":[{"command":"fixture inspect","exit_code":0,"evidence":"repair proposal"}]}});
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,completed_at) VALUES('invalid-repair',$1,'qwen','implementation','completed',$2,NOW())")
        .bind(&card).bind(result.to_string()).execute(&pool).await.unwrap();
    let _control_workspace = prepare_control_fixture(&pool, "invalid-repair").await;
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let before = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(before["failures"][0]["category"], "plan");
    assert!(
        before["failures"][0]["evidence"]["failure"]["error"]
            .as_str()
            .unwrap()
            .contains("17 outputs")
    );
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    tick(&pool, &engine, &c.id, None).await.unwrap();
    tick(&pool, &engine, &c.id, None).await.unwrap();
    let after = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(after["hold_reason"], "Plan evolution budget exhausted");
    assert_eq!(after["active_version"], before["active_version"]);
    assert_eq!(after["tasks"], before["tasks"]);
    assert_eq!(after["failures"], before["failures"]);
    assert_eq!(after["versions"], before["versions"]);
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn blocked_validator_releases_candidate_and_retains_repair_evidence_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let v = store::stage(&pool, &c.id, &graph(), None).await.unwrap();
    control(
        &pool,
        &c.id,
        &goal(),
        "validate",
        Some(v),
        "1",
        "Check candidate",
    )
    .await
    .unwrap();
    let card: String =
        sqlx::query_scalar("SELECT card_id FROM campaign_graph_tasks WHERE campaign_id=$1")
            .bind(&c.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let result = json!({"campaign_receipt":{"outcome":"blocked","failure_category":"plan","summary":"Plan acceptance gaps","observations":[{"command":"inspect contract","exit_code":0,"evidence":"read-only check"}]},"graph_validation":{"approved":false,"reason":"Require independent backup content verification","digest":"candidate"}});
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,completed_at) VALUES('rejected-validator',$1,'monitoring','implementation','completed',$2,NOW())")
        .bind(&card).bind(result.to_string()).execute(&pool).await.unwrap();
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let before = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(before["failures"][0]["category"], "plan");
    assert_eq!(before["versions"][0]["status"], "candidate");
    // This path also repairs an already-observed rejection after a restart.
    reconcile::reject_failed_candidates(&pool, &c.id)
        .await
        .unwrap();
    reconcile::reject_failed_candidates(&pool, &c.id)
        .await
        .unwrap();
    let after = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(after["versions"][0]["status"], "rejected");
    assert_eq!(before["failures"], after["failures"]);
    assert_eq!(after["tasks"][0]["accepted"], false);
    let evidence = reconcile::planning_evidence(&pool, &after["failures"][0])
        .await
        .unwrap();
    assert_eq!(
        evidence["validation_reason"],
        "Require independent backup content verification"
    );
    let mut repaired = graph();
    repaired.nodes[0]
        .acceptance
        .push("additional independently checked criterion".into());
    let next = store::stage(&pool, &c.id, &repaired, after["failures"][0]["id"].as_str())
        .await
        .unwrap();
    assert_eq!(next, v + 1);
    reconcile::reject_failed_candidates(&pool, &c.id)
        .await
        .unwrap();
    let state = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(state["versions"][0]["status"], "candidate");
    assert!(state["active_version"].is_null());
    assert!(
        store::activate(&pool, &c.id, next, c.revision)
            .await
            .is_err()
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_accepted_receipt_settles_canonical_card_without_review_drift_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    control(&pool, &c.id, &goal(), "plan", None, "initial", GRAPH_SCHEMA)
        .await
        .unwrap();
    let card: String =
        sqlx::query_scalar("SELECT card_id FROM campaign_graph_tasks WHERE campaign_id=$1")
            .bind(&c.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let result = json!({"campaign_graph":graph(),"campaign_receipt":{"outcome":"passed","summary":"bounded planning fixture","observations":[{"command":"fixture inspect","exit_code":0,"evidence":"graph contract checked"}]}});
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,completed_at) VALUES('native-plan',$1,'qwen','implementation','completed',$2,NOW())")
        .bind(&card).bind(result.to_string()).execute(&pool).await.unwrap();
    let _control_workspace = prepare_control_fixture(&pool, "native-plan").await;
    // Model the queue admission that precedes this completed dispatch. A merely
    // published pending entry has never run and must not be counted as done.
    sqlx::query("UPDATE auto_queue_entries SET status='dispatched' WHERE kanban_card_id=$1")
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE kanban_cards SET status='in_progress',latest_dispatch_id='native-plan' WHERE id=$1",
    )
    .bind(&card)
    .execute(&pool)
    .await
    .unwrap();
    let finish = |card: String| {
        let pool = pool.clone();
        let engine = engine.clone();
        async move {
            crate::kanban::transition_status_with_opts_pg_only(
                &pool,
                &engine,
                &card,
                "done",
                "campaign_graph_completion",
                crate::engine::transition::ForceIntent::None,
            )
            .await
        }
    };
    // A provider success claim is insufficient until the graph owner validates it.
    assert!(finish(card.clone()).await.is_err());
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["tasks"][0]["accepted"],
        true
    );
    // New/unrelated work must not be cancelled just to complete an old task.
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('side-work',$1,'qwen','consultation','dispatched')")
        .bind(&card).execute(&pool).await.unwrap();
    assert!(
        reconcile::settle_accepted(&pool, &engine, &c.id)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM task_dispatches WHERE id='side-work'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "dispatched"
    );
    sqlx::query("UPDATE task_dispatches SET status='completed' WHERE id='side-work'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE kanban_cards SET latest_dispatch_id='side-work' WHERE id=$1")
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
    assert!(finish(card.clone()).await.is_err());
    sqlx::query("UPDATE kanban_cards SET latest_dispatch_id='native-plan' WHERE id=$1")
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
    reconcile::settle_accepted(&pool, &engine, &c.id)
        .await
        .unwrap();
    reconcile::settle_accepted(&pool, &engine, &c.id)
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
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM auto_queue_entries WHERE kanban_card_id=$1"
        )
        .bind(&card)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "done"
    );
    let audits:Vec<String>=sqlx::query_scalar("SELECT result FROM kanban_audit_logs WHERE card_id=$1 AND source='campaign_graph_completion' AND result LIKE 'OK%'").bind(&card).fetch_all(&pool).await.unwrap();
    assert_eq!(audits.len(), 1);
    assert!(!audits[0].contains("system_recovery") && !audits[0].contains("operator_override"));
    // Watcher replay is idempotent and cannot attach telemetry to an unrelated
    // dispatch. Store only digests, including results in the same bounded batch.
    let events = vec![
        json!({"event_id":"call-1","fingerprint":"input-hash"}),
        json!({"event_id":"call-1","output_digest":"output-hash"}),
    ];
    crate::db::campaigns::graph::progress::watcher_batch(&pool, "native-plan", &events).await;
    crate::db::campaigns::graph::progress::watcher_batch(&pool, "native-plan", &events).await;
    crate::db::campaigns::graph::progress::watcher_batch(&pool, "unrelated", &events).await;
    let rows: Vec<(String, String, String)> =
        sqlx::query_as("SELECT dispatch_id,fingerprint,output_digest FROM campaign_graph_progress")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        rows,
        vec![(
            "native-plan".into(),
            "input-hash".into(),
            "output-hash".into()
        )]
    );
    // Metadata cannot grant this transition to an ordinary PR/internal card.
    sqlx::query("INSERT INTO kanban_cards(id,title,status,metadata) VALUES('ordinary','Ordinary','in_progress',$1)")
        .bind(json!({"campaign_id":c.id,"graph_task_key":"pretend","accepted":true})).execute(&pool).await.unwrap();
    assert!(finish("ordinary".into()).await.is_err());
    drop(engine);
    pool.close().await;
    fixture.drop().await;
}

async fn held(pool: &PgPool, external: bool) -> (campaigns::Campaign, Graph, String, String) {
    let c = setup(pool).await;
    let mut original = graph();
    if external {
        original.nodes[0].effect = Effect::External;
    }
    let version = store::stage(pool, &c.id, &original, None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated'")
        .execute(pool)
        .await
        .unwrap();
    store::activate(pool, &c.id, version, c.revision)
        .await
        .unwrap();
    let n = &original.nodes[0];
    let card = store::publish(
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
    .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,completed_at) VALUES('blocked-attempt',$1,'qwen','implementation','completed',$2,NOW())")
        .bind(&card).bind(json!({"campaign_receipt":{"outcome":"blocked","failure_category":"unknown","summary":"dependency unavailable"}}).to_string()).execute(pool).await.unwrap();
    reconcile::observe(pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let s = store::status(pool, &c.id).await.unwrap();
    assert!(s["hold_reason"].is_string());
    let failure = s["failures"][0]["id"].as_str().unwrap().to_owned();
    (
        campaigns::get(pool, &c.id).await.unwrap(),
        original,
        failure,
        card,
    )
}

#[tokio::test]
async fn graph_reconciliation_preserves_failure_reuses_dag_and_requires_validation_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, original, failure, card) = held(&pool, false).await;
    let mut request=Reconcile {expected_revision:c.revision,failure_id:failure.clone(),evidence:"Dependency repaired; repeat read-only inspection and independently verify current outputs.".into()};
    assert!(
        recovery::reconcile(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("cleanup")
    );
    sqlx::query("UPDATE auto_queue_runs SET status='completed'")
        .execute(&pool)
        .await
        .unwrap();
    request.expected_revision -= 1;
    assert!(
        recovery::reconcile(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("revision")
    );
    request.expected_revision = c.revision;
    sqlx::query(
        "UPDATE campaign_execution_specs SET spec=jsonb_set(spec,'{budget,max_evolutions}','0')",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        recovery::reconcile(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("evolution budget")
    );
    assert!(store::status(&pool, &c.id).await.unwrap()["hold_reason"].is_string());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_reconciliations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query(
        "UPDATE campaign_execution_specs SET spec=jsonb_set(spec,'{budget,max_evolutions}','2')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let v = recovery::reconcile(&pool, &c.id, &request).await.unwrap();
    assert_eq!(v, 2);
    // Identical operator retry is idempotent; changing its evidence is rejected.
    assert_eq!(
        recovery::reconcile(&pool, &c.id, &request).await.unwrap(),
        2
    );
    request.evidence.push_str(" different");
    assert!(recovery::reconcile(&pool, &c.id, &request).await.is_err());
    let state = store::status(&pool, &c.id).await.unwrap();
    assert!(state["hold_reason"].is_null());
    assert_eq!(state["active_version"], 1);
    assert_eq!(state["failures"][0]["category"], "unknown");
    assert_eq!(state["tasks"][0]["accepted"], false);
    let version = state["versions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["version"] == 2)
        .unwrap();
    assert_eq!(version["status"], "candidate");
    let changed: Graph = serde_json::from_value(version["graph"].clone()).unwrap();
    assert_eq!(changed.nodes[1], original.nodes[1]);
    assert_ne!(changed.nodes[0].task_key, original.nodes[0].task_key);
    assert_eq!(changed.nodes[0].acceptance, original.nodes[0].acceptance);
    let current = campaigns::get(&pool, &c.id).await.unwrap();
    assert!(
        store::activate(&pool, &c.id, 2, current.revision)
            .await
            .is_err()
    );
    // A second dispatch of the original failed task is still forbidden.
    assert!(sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('replay',$1,'qwen','implementation','pending')").bind(&card).execute(&pool).await.is_err());
    sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE version=2")
        .execute(&pool)
        .await
        .unwrap();
    store::activate(&pool, &c.id, 2, current.revision)
        .await
        .unwrap();
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["active_version"],
        2
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn paused_artifact_reconciliation_preserves_failure_and_waits_for_budget_audit_and_resume_pg()
{
    use crate::db::campaigns::graph::revision::budget::{self, BudgetRevision};
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, failure, _) = held(&pool, false).await;
    sqlx::query("UPDATE auto_queue_runs SET status='completed'")
        .execute(&pool)
        .await
        .unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let paused = campaigns::replace(&pool, &c.id, c.revision, input)
        .await
        .unwrap();
    let before = store::status(&pool, &c.id).await.unwrap();
    let request = Reconcile {
        expected_revision: paused.revision,
        failure_id: failure,
        evidence: "Captured uncertainty remains historical evidence; repeat only artifact inspection with fresh independent verification".into(),
    };
    let candidate = recovery::reconcile(&pool, &c.id, &request).await.unwrap();
    assert_eq!(candidate, 2);
    let after = store::status(&pool, &c.id).await.unwrap();
    for key in [
        "tasks",
        "failures",
        "active_version",
        "elapsed_dispatch_seconds",
        "observed_tool_calls",
    ] {
        assert_eq!(before[key], after[key], "{key}");
    }
    assert_eq!(
        campaigns::get(&pool, &c.id).await.unwrap().status,
        CampaignStatus::Paused
    );
    assert!(after["hold_reason"].is_null());
    let mut expanded = goal().budget;
    expanded.max_elapsed_secs *= 2;
    let changed = BudgetRevision {
        expected_revision: campaigns::get(&pool, &c.id).await.unwrap().revision,
        request_id: "quiescent-artifact-repair".into(),
        evidence: "Add finite QA capacity while preserving spent usage and the failed receipt"
            .into(),
        budget: expanded,
    };
    budget::extend(&pool, &c.id, &changed).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    tick(&pool, &engine, &c.id, None).await.unwrap();
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["tasks"],
        before["tasks"]
    );
    assert!(
        store::activate(
            &pool,
            &c.id,
            candidate,
            campaigns::get(&pool, &c.id).await.unwrap().revision
        )
        .await
        .is_err()
    );
    drop(engine);
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn graph_reconciliation_rejects_external_effect_replay_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, failure, _) = held(&pool, true).await;
    sqlx::query("UPDATE auto_queue_runs SET status='completed'")
        .execute(&pool)
        .await
        .unwrap();
    let request = Reconcile {
        expected_revision: c.revision,
        failure_id: failure,
        evidence: "Operator believes retry is safe".into(),
    };
    assert!(
        recovery::reconcile(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("external effects")
    );
    assert!(store::status(&pool, &c.id).await.unwrap()["hold_reason"].is_string());
    pool.close().await;
    fixture.drop().await;
}
