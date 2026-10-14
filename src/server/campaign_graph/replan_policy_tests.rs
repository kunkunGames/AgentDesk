//! Source corrections are planning inputs; they cannot approve or replay work.
use super::replan_tests::{held_with_category, settle};
use super::*;
use crate::db::campaigns::graph::recovery::replan;
use crate::db::campaigns::graph::recovery::replan::UnpublishedCandidate;

#[tokio::test]
async fn operator_source_correction_preserves_failed_artifact_and_requires_planner_qa_pg() {
    for hold in [None, Some("Plan evolution budget exhausted")] {
        let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
        let pool = fixture.connect_and_migrate().await;
        let (c, _, raw, mut request) = held_with_category(&pool, false, "plan").await;
        settle(&pool).await;
        if let Some(reason) = hold {
            store::hold(&pool, &c.id, reason).await.unwrap();
            let mut expanded = goal().budget;
            expanded.max_evolutions = 4;
            request.budget = Some(expanded);
        }
        request.evidence = "Original source permits an observed left-open outcome; the generated contract invented a private-provider approval prerequisite. Independently inspect source before changing only the failed artifact subgraph.".into();
        let before = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
        let reply = replan::request(&pool, &c.id, &request).await.unwrap();
        assert!(reply.independent_validation_required && reply.goal_scope_preserved);
        assert_eq!(reply.planner_agent_id, "qwen");
        assert_eq!(
            replan::request(&pool, &c.id, &request)
                .await
                .unwrap()
                .revision,
            reply.revision
        );
        let after = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
        for field in [
            "versions",
            "tasks",
            "active_version",
            "occurrences",
            "observed_tool_calls",
            "elapsed_dispatch_seconds",
        ] {
            assert_eq!(after[field], before[field], "{field}");
        }
        let mut expected_goal = before["goal"].clone();
        if let Some(expanded) = &request.budget {
            expected_goal["budget"] = serde_json::to_value(expanded).unwrap();
        }
        assert_eq!(after["goal"], expected_goal);
        assert_eq!(after["failures"].as_array().unwrap().len(), 2);
        assert!(
            after["failures"]
                .as_array()
                .unwrap()
                .contains(&before["failures"][0])
        );
        let repair = after["failures"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["id"] == reply.repair_failure_id)
            .unwrap();
        assert_eq!(repair["category"], "plan");
        assert_eq!(repair["evidence"]["operator_evidence"], request.evidence);
        assert_eq!(
            repair["evidence"]["original_failure"],
            before["failures"][0]["evidence"]
        );
        assert!(after["hold_reason"].is_null());
        assert_eq!(
            campaigns::get(&pool, &c.id).await.unwrap().status,
            CampaignStatus::Paused
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT result FROM task_dispatches WHERE id='original-attempt'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            raw
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_goal_revisions")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        pool.close().await;
        fixture.drop().await;
    }
}

#[tokio::test]
async fn operator_source_correction_refuses_unknown_outcomes_live_owners_and_published_inputs_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, original, _, mut request) = held_with_category(&pool, false, "plan").await;
    settle(&pool).await;
    let before = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    request.expected_revision -= 1;
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("revision")
    );
    request.expected_revision = c.revision;
    store::hold(
        &pool,
        &c.id,
        "Execution outcome requires reconciliation; automatic replay is disabled",
    )
    .await
    .unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("cannot clear")
    );
    sqlx::query("UPDATE campaign_execution_specs SET hold_reason=NULL")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO campaign_graph_failures(id,campaign_id,task_key,fingerprint,category,evidence)
        SELECT 'unresolved-execution',campaign_id,task_key,'other','unknown',evidence FROM campaign_graph_failures")
        .execute(&pool).await.unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("additional unknown")
    );
    sqlx::query("DELETE FROM campaign_graph_failures WHERE id='unresolved-execution'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{external_effect_requires_reconciliation}','true')")
        .execute(&pool).await.unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("external effects")
    );
    sqlx::query("UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{external_effect_requires_reconciliation}','false')")
        .execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id)
        VALUES('live-source-owner','qwen','qwen','turn_active','original-attempt')",
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
    sqlx::query("UPDATE sessions SET status='idle',active_dispatch_id=NULL")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET graph=jsonb_set(graph,'{nodes,0,effect}','\"external\"')")
        .execute(&pool).await.unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("external business")
    );
    sqlx::query("UPDATE campaign_graph_versions SET graph=$1")
        .bind(serde_json::to_value(&original).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_execution_specs SET active_version=2")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("active graph")
    );
    sqlx::query("UPDATE campaign_execution_specs SET active_version=1")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET accepted=TRUE")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("unaccepted")
    );
    sqlx::query("UPDATE campaign_graph_tasks SET accepted=FALSE")
        .execute(&pool)
        .await
        .unwrap();
    let n = &original.nodes[1];
    sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('locked-input','Verify','ready','monitoring')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,version,node_id,role,card_id,contract_digest)
        VALUES($1,$2,1,'verify','work','locked-input',$3)")
        .bind(&n.task_key).bind(&c.id).bind(store::task_digest(n).unwrap()).execute(&pool).await.unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("dependent task")
    );
    for table in ["campaign_goal_revisions", "campaign_graph_reconciliations"] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }
    assert_eq!(
        canonical_inspection(store::status(&pool, &c.id).await.unwrap())["failures"],
        before["failures"]
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn operator_source_correction_retires_only_pinned_unpublished_successor_without_erasing_qa_pg()
 {
    for validated in [false, true] {
        let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
        let pool = fixture.connect_and_migrate().await;
        let (mut c, mut proposed, _, mut request) = held_with_category(&pool, false, "plan").await;
        settle(&pool).await;
        let mut input: campaigns::CampaignInput =
            serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
        input.status = CampaignStatus::Active;
        c = campaigns::replace(&pool, &c.id, c.revision, input)
            .await
            .unwrap();
        for n in &mut proposed.nodes {
            n.task_key.push_str(":pending-source-repair");
        }
        let version = store::stage(&pool, &c.id, &proposed, Some(&request.failure_id))
            .await
            .unwrap();
        assert_eq!(version, 2);
        let digest = crate::db::campaigns::graph::contract_digest(&proposed).unwrap();
        let qa_raw = json!({"campaign_receipt":{"outcome":"passed","summary":"Fixture independent candidate approval",
            "observations":[{"command":"fixture compare source contract","exit_code":0,"evidence":"Unexecuted candidate verified in this fixture"}]},
            "graph_validation":{"digest":digest,"approved":true,"reason":"Fixture approval does not create business execution"}}).to_string();
        if validated {
            control(
                &pool,
                &c.id,
                &goal(),
                "validate",
                Some(version),
                "2",
                "Fixture independent candidate QA",
            )
            .await
            .unwrap();
            let card = store::card_id(&format!("{}:validate:2", c.id));
            sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
                .bind(&card)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at)
                VALUES('successor-qa',$1,'monitoring','implementation','completed',$2,NOW(),NOW())")
                .bind(&card).bind(&qa_raw).execute(&pool).await.unwrap();
            reconcile::observe(&pool, &c.id, &goal(), None)
                .await
                .unwrap();
            settle(&pool).await;
        }
        let mut input: campaigns::CampaignInput =
            serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
        input.status = CampaignStatus::Paused;
        c = campaigns::replace(&pool, &c.id, c.revision, input)
            .await
            .unwrap();
        request.expected_revision = c.revision;
        let before = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
        assert_eq!(
            before["versions"][0]["status"],
            if validated { "validated" } else { "candidate" }
        );
        assert!(
            replan::request(&pool, &c.id, &request)
                .await
                .unwrap_err()
                .to_string()
                .contains("candidate")
        );
        request.supersede_candidate = Some(UnpublishedCandidate {
            version,
            digest: "0".repeat(64),
        });
        assert!(
            replan::request(&pool, &c.id, &request)
                .await
                .unwrap_err()
                .to_string()
                .contains("identity")
        );
        request.supersede_candidate.as_mut().unwrap().digest = digest.clone();
        sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('successor-business','Work','ready','qwen')")
            .execute(&pool).await.unwrap();
        let n = &proposed.nodes[0];
        sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,version,node_id,role,card_id,contract_digest)
            VALUES($1,$2,2,'produce','work','successor-business',$3)")
            .bind(&n.task_key).bind(&c.id).bind(store::task_digest(n).unwrap()).execute(&pool).await.unwrap();
        assert!(
            replan::request(&pool, &c.id, &request)
                .await
                .unwrap_err()
                .to_string()
                .contains("published business")
        );
        sqlx::query("DELETE FROM campaign_graph_tasks WHERE card_id='successor-business'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM kanban_cards WHERE id='successor-business'")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            canonical_inspection(store::status(&pool, &c.id).await.unwrap()),
            before,
            "rejected requests must roll back candidate invalidation"
        );
        let reply = replan::request(&pool, &c.id, &request).await.unwrap();
        let after = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
        assert_eq!(after["versions"][0]["status"], "rejected");
        assert_eq!(
            after["versions"][0]["graph"],
            before["versions"][0]["graph"]
        );
        for field in [
            "tasks",
            "goal",
            "active_version",
            "occurrences",
            "observed_tool_calls",
            "elapsed_dispatch_seconds",
        ] {
            assert_eq!(after[field], before[field], "{field}");
        }
        assert!(reply.independent_validation_required && reply.goal_scope_preserved);
        if validated {
            assert_eq!(
                sqlx::query_scalar::<_, String>(
                    "SELECT result FROM task_dispatches WHERE id='successor-qa'"
                )
                .fetch_one(&pool)
                .await
                .unwrap(),
                qa_raw
            );
            assert!(
                after["tasks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|t| t["role"] == "validate" && t["accepted"] == true)
            );
        }
        assert_eq!(
            replan::request(&pool, &c.id, &request)
                .await
                .unwrap()
                .revision,
            reply.revision
        );
        assert_eq!(
            campaigns::get(&pool, &c.id).await.unwrap().status,
            CampaignStatus::Paused
        );
        pool.close().await;
        fixture.drop().await;
    }
}
