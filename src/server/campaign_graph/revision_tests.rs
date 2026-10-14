use super::*;
use crate::db::campaigns::graph::revision::{self, Revision};

#[tokio::test]
async fn operator_goal_revision_retains_usage_and_requires_new_validation_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    control(&pool, &c.id, &goal(), "plan", None, "initial", "Plan")
        .await
        .unwrap();
    let card: String =
        sqlx::query_scalar("SELECT card_id FROM campaign_graph_tasks WHERE campaign_id=$1")
            .bind(&c.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let receipt =
        json!({"campaign_receipt":{"outcome":"passed"},"campaign_graph":graph()}).to_string();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at) VALUES('revision-plan',$1,'qwen','implementation','completed',$2,NOW()-INTERVAL '10 seconds',NOW())")
        .bind(&card).bind(&receipt).execute(&pool).await.unwrap();
    store::stage(&pool, &c.id, &graph(), None).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_progress(dispatch_id,event_id,fingerprint) VALUES('revision-plan','tool-1','hash')").execute(&pool).await.unwrap();
    let mut revised = goal();
    revised
        .constraints
        .push("Include existing configured workspace destinations".into());
    revised.budget.max_dispatches += 2;
    let mut request = Revision {
        expected_revision: c.revision,
        request_id: "scope-correction".into(),
        evidence: "Configured target was omitted; preserve execution workspace separately".into(),
        goal: revised,
    };
    assert!(revision::revise(&pool, &c.id, &request).await.is_err());
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let paused = campaigns::replace(&pool, &c.id, c.revision, input)
        .await
        .unwrap();
    request.expected_revision = paused.revision;
    assert!(
        revision::revise(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("queue cleanup")
    );
    let runs: Vec<String> = sqlx::query_scalar("SELECT DISTINCT e.run_id FROM auto_queue_entries e JOIN campaign_graph_tasks t ON t.card_id=e.kanban_card_id WHERE t.campaign_id=$1")
        .bind(&c.id).fetch_all(&pool).await.unwrap();
    for run in runs {
        crate::services::auto_queue::cancel_run::end_run_with_pg(None, &pool, &run)
            .await
            .unwrap();
    }
    let before = store::status(&pool, &c.id).await.unwrap();
    let revision = revision::revise(&pool, &c.id, &request).await.unwrap();
    assert_eq!(revision, paused.revision + 1);
    assert_eq!(
        revision::revise(&pool, &c.id, &request).await.unwrap(),
        revision
    );
    let after = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(after["observed_tool_calls"], before["observed_tool_calls"]);
    assert_eq!(
        after["elapsed_dispatch_seconds"],
        before["elapsed_dispatch_seconds"]
    );
    assert_eq!(after["tasks"].as_array().unwrap().len(), 1);
    assert_eq!(after["tasks"][0]["accepted"], false);
    assert_eq!(
        after["tasks"][0]["observation"]["outcome"],
        "superseded_without_acceptance"
    );
    assert_eq!(after["versions"][0]["status"], "rejected");
    assert_eq!(after["goal_revisions"].as_array().unwrap().len(), 1);
    assert_eq!(after["goal_revisions"][0]["previous_goal"], before["goal"]);
    assert_eq!(after["goal_revisions"][0]["revised_goal"], after["goal"]);
    let preserved: String =
        sqlx::query_scalar("SELECT result FROM task_dispatches WHERE id='revision-plan'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(preserved, receipt);
    assert_eq!(
        campaigns::get(&pool, &c.id).await.unwrap().status,
        CampaignStatus::Paused
    );
    request.evidence.push_str(" changed");
    assert!(revision::revise(&pool, &c.id, &request).await.is_err());
    let mut active: campaigns::CampaignInput = serde_json::from_value(
        serde_json::to_value(campaigns::get(&pool, &c.id).await.unwrap()).unwrap(),
    )
    .unwrap();
    active.status = CampaignStatus::Active;
    campaigns::replace(&pool, &c.id, revision, active)
        .await
        .unwrap();
    let mut repaired = graph();
    repaired.nodes[0]
        .details
        .push_str(" Include configured destinations");
    let next = store::stage(&pool, &c.id, &repaired, after["failures"][0]["id"].as_str())
        .await
        .unwrap();
    assert_eq!(next, 2);
    assert!(
        store::activate(&pool, &c.id, next, revision + 1)
            .await
            .is_err(),
        "old approval cannot activate corrected graph"
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn operator_goal_revision_rejects_published_business_contracts_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let version = store::stage(&pool, &c.id, &graph(), None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE campaign_id=$1")
        .bind(&c.id)
        .execute(&pool)
        .await
        .unwrap();
    store::activate(&pool, &c.id, version, c.revision)
        .await
        .unwrap();
    let c = campaigns::get(&pool, &c.id).await.unwrap();
    store::publish(
        &pool,
        &c.id,
        &store::Publication {
            key: "business",
            node: "business",
            role: "work",
            version: Some(1),
            agent: "qwen",
            title: "Business",
            prompt: "Preserve",
            digest: "digest",
        },
    )
    .await
    .unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let paused = campaigns::replace(&pool, &c.id, c.revision, input)
        .await
        .unwrap();
    let mut new_goal = goal();
    new_goal.objective.push_str(" corrected");
    let request = Revision {
        expected_revision: paused.revision,
        request_id: "deny".into(),
        evidence: "Cannot bypass a published contract".into(),
        goal: new_goal,
    };
    assert!(
        revision::revise(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("business tasks")
    );
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["goal_revisions"],
        json!([])
    );
    pool.close().await;
    fixture.drop().await;
}
