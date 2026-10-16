//! A lost successor validator must not erase its already published parent graph.
use super::replan_tests::{held_with_category, settle};
use super::*;
use crate::db::campaigns::graph::recovery::replan::{self, Replan};

#[tokio::test]
async fn operator_replan_lost_successor_validator_preserves_active_business_and_requires_new_qa_pg()
{
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (mut c, mut proposed, _, original_request) = held_with_category(&pool, false, "plan").await;
    settle(&pool).await;
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
    input.status = CampaignStatus::Active;
    c = campaigns::replace(&pool, &c.id, c.revision, input)
        .await
        .unwrap();
    proposed.nodes[0].kind = Kind::Inspect;
    for n in &mut proposed.nodes {
        n.task_key.push_str(":successor");
    }
    let version = store::stage(&pool, &c.id, &proposed, Some(&original_request.failure_id))
        .await
        .unwrap();
    control(
        &pool,
        &c.id,
        &goal(),
        "validate",
        Some(version),
        "2",
        "Read-only successor validator",
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
        VALUES('lost-successor',$1,'monitoring','implementation','failed','{}',NOW()-INTERVAL '3 seconds',NOW())")
        .bind(&card).execute(&pool).await.unwrap();
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    settle(&pool).await;
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    c = campaigns::replace(&pool, &c.id, c.revision, input)
        .await
        .unwrap();
    let before = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    assert_eq!(before["active_version"], 1);
    assert_eq!(before["versions"][0]["status"], "candidate");
    let failure = before["failures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["category"] == "unknown")
        .unwrap();
    let request = Replan {cancelled_validation:None,
        rejected_validation: None,
        expected_revision: c.revision, request_id: "lost-successor-source-repair".into(),
        failure_id: failure["id"].as_str().unwrap().into(),
        evidence: "Captured validator performed only permitted read-only source inspection. Cause remains unknown; preserve its raw failed result and the active parent, then require agent-authored planning and fresh independent QA.".into(),
        budget: None, supersede_candidate: None, unattempted_effect: None,
        settled_effect: None,
    };
    // Published work in the failed candidate is never silently retired.
    sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('executed-successor','Work','ready','qwen')")
        .execute(&pool).await.unwrap();
    let n = &proposed.nodes[0];
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,version,node_id,role,card_id,contract_digest)
        VALUES($1,$2,2,'produce','work','executed-successor',$3)")
        .bind(&n.task_key).bind(&c.id).bind(store::task_digest(n).unwrap()).execute(&pool).await.unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("published business")
    );
    sqlx::query("DELETE FROM campaign_graph_tasks WHERE card_id='executed-successor'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM kanban_cards WHERE id='executed-successor'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET parent_version=NULL WHERE version=2")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("parent")
    );
    sqlx::query("UPDATE campaign_graph_versions SET parent_version=1 WHERE version=2")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        canonical_inspection(store::status(&pool, &c.id).await.unwrap()),
        before
    );
    let response = replan::request(&pool, &c.id, &request).await.unwrap();
    assert!(response.independent_validation_required && response.goal_scope_preserved);
    let after = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    assert_eq!(after["active_version"], 1);
    assert_eq!(after["versions"][0]["status"], "rejected");
    assert_eq!(after["versions"][1], before["versions"][1]);
    assert_eq!(
        after["versions"][0]["graph"],
        before["versions"][0]["graph"]
    );
    for field in [
        "tasks",
        "goal",
        "occurrences",
        "observed_tool_calls",
        "elapsed_dispatch_seconds",
    ] {
        assert_eq!(after[field], before[field], "{field}");
    }
    assert!(after["failures"].as_array().unwrap().contains(failure));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_dispatches WHERE id='lost-successor'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "{}"
    );
    assert!(after["hold_reason"].is_null());
    assert_eq!(
        campaigns::get(&pool, &c.id).await.unwrap().status,
        CampaignStatus::Paused
    );
    pool.close().await;
    fixture.drop().await;
}
