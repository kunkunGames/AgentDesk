use super::*;
use crate::db::campaigns::graph::recovery::replan::{self, Replan};

async fn lost_artifact(pool: &PgPool) -> (campaigns::Campaign, Graph, Replan) {
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
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at)
        VALUES('lost-artifact',$1,'qwen','implementation','failed',NULL,NOW()-INTERVAL '8 seconds',NOW())")
        .bind(&card).execute(pool).await.unwrap();
    progress::tool_use(
        Some(pool),
        Some("lost-artifact"),
        Some("retained-tool"),
        "retained-input-hash",
    )
    .await;
    reconcile::observe(pool, &c.id, &goal(), None)
        .await
        .unwrap();
    super::replan_tests::settle(pool).await;
    let current = campaigns::get(pool, &c.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let c = campaigns::replace(pool, &c.id, current.revision, input)
        .await
        .unwrap();
    let state = store::status(pool, &c.id).await.unwrap();
    assert_eq!(state["failures"][0]["category"], "unknown");
    assert_eq!(
        state["failures"][0]["evidence"]["external_effect_requires_reconciliation"],
        false
    );
    assert_eq!(
        state["hold_reason"],
        "Execution outcome requires reconciliation; automatic replay is disabled"
    );
    let request = Replan {
        cancelled_validation: None,
        rejected_validation: None,
        expected_revision: c.revision, request_id: "audited-lost-artifact-repair".into(),
        failure_id: state["failures"][0]["id"].as_str().unwrap().into(),
        evidence: "Preserve the missing result and failed artifact-only execution. Independently inspect immutable outputs; request existing-planner source correction and fresh QA, not acceptance or replay.".into(),
        budget: None, supersede_candidate: None, unattempted_effect: None,
        settled_effect: None,
    };
    (c, original, request)
}

#[tokio::test]
async fn lost_artifact_replan_preserves_terminal_unknown_history_and_legacy_hold_requires_new_qa_pg()
 {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, original, request) = lost_artifact(&pool).await;
    // Existing releases could overwrite the execution hold with this repeat hold.
    sqlx::query("UPDATE campaign_execution_specs SET hold_reason='The same failure recurred after evolution; reconcile cause before any new attempt'")
        .execute(&pool).await.unwrap();
    let before = store::status(&pool, &c.id).await.unwrap();
    let reply = replan::request(&pool, &c.id, &request).await.unwrap();
    assert_eq!(reply.planner_agent_id, "qwen");
    assert!(reply.goal_scope_preserved && reply.independent_validation_required);
    assert_eq!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap()
            .revision,
        reply.revision
    );
    let after = store::status(&pool, &c.id).await.unwrap();
    for field in [
        "goal",
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
    assert!(after["hold_reason"].is_null());
    let row = sqlx::query("SELECT status,result FROM task_dispatches WHERE id='lost-artifact'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("status"), "failed");
    assert!(row.get::<Option<String>, _>("result").is_none());
    let paused = campaigns::get(&pool, &c.id).await.unwrap();
    assert_eq!(paused.status, CampaignStatus::Paused);
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&paused).unwrap()).unwrap();
    input.status = CampaignStatus::Active;
    let active = campaigns::replace(&pool, &c.id, paused.revision, input)
        .await
        .unwrap();
    control(
        &pool,
        &c.id,
        &goal(),
        "plan",
        Some(1),
        &reply.repair_failure_id,
        "Inspect original provenance and repair",
    )
    .await
    .unwrap();
    let planner = store::card_id(&format!("{}:plan:{}", c.id, reply.repair_failure_id));
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
    let mut corrected = original;
    corrected.nodes[0]
        .task_key
        .push_str(":corrected-provenance");
    corrected.nodes[0]
        .details
        .push_str(" Preserve original bytes separately from derived data.");
    store::stage(&pool, &c.id, &corrected, Some(&reply.repair_failure_id))
        .await
        .unwrap();
    assert!(
        store::activate(&pool, &c.id, 2, active.revision)
            .await
            .unwrap_err()
            .to_string()
            .contains("only validated versions")
    );
    control(
        &pool,
        &c.id,
        &goal(),
        "validate",
        Some(2),
        "2",
        "Independent provenance verification",
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
        "no business replay before independent QA"
    );
    super::replan_tests::settle(&pool).await;
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn lost_artifact_replan_refuses_live_owners_success_claims_external_effects_and_published_inputs_pg()
 {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, request) = lost_artifact(&pool).await;
    let before = store::status(&pool, &c.id).await.unwrap();
    sqlx::query(
        "INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id)
        VALUES('artifact-live-owner','qwen','qwen','turn_active','lost-artifact')",
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
    sqlx::query("UPDATE sessions SET status='idle',active_dispatch_id=NULL WHERE session_key='artifact-live-owner'").execute(&pool).await.unwrap();
    for status in ["completed"] {
        sqlx::query("UPDATE task_dispatches SET status=$1 WHERE id='lost-artifact'")
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        assert!(replan::request(&pool, &c.id, &request).await.is_err());
    }
    assert!(
        sqlx::query("UPDATE task_dispatches SET status='running' WHERE id='lost-artifact'")
            .execute(&pool)
            .await
            .unwrap_err()
            .to_string()
            .contains("Terminal graph attempts cannot be replayed")
    );
    sqlx::query("UPDATE task_dispatches SET status='failed' WHERE id='lost-artifact'")
        .execute(&pool)
        .await
        .unwrap();
    for outcome in ["passed", "no_work"] {
        let claim = json!({"campaign_receipt":{"outcome":outcome}}).to_string();
        for captured in [claim.clone(), json!(claim).to_string()] {
            sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='lost-artifact'")
                .bind(captured)
                .execute(&pool)
                .await
                .unwrap();
            assert!(
                replan::request(&pool, &c.id, &request)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("successful native result")
            );
        }
    }
    sqlx::query("UPDATE task_dispatches SET result=NULL WHERE id='lost-artifact'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET graph=jsonb_set(graph,'{nodes,0,effect}','\"external\"')").execute(&pool).await.unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("external business")
    );
    sqlx::query("UPDATE campaign_graph_versions SET graph=jsonb_set(graph,'{nodes,0,effect}','\"artifact_only\"')").execute(&pool).await.unwrap();
    sqlx::query("UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{external_effect_requires_reconciliation}','true')").execute(&pool).await.unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("external effects")
    );
    sqlx::query("UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{external_effect_requires_reconciliation}','false')").execute(&pool).await.unwrap();
    let dependent = &graph().nodes[1];
    sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('artifact-published-dependent','Verify','ready','monitoring')").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,version,node_id,role,card_id,contract_digest)
        VALUES($1,$2,1,'verify','work','artifact-published-dependent',$3)")
        .bind(&dependent.task_key).bind(&c.id).bind(store::task_digest(dependent).unwrap()).execute(&pool).await.unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("dependent task")
    );
    for table in ["campaign_graph_reconciliations", "campaign_goal_revisions"] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["hold_reason"],
        before["hold_reason"]
    );
    pool.close().await;
    fixture.drop().await;
}
