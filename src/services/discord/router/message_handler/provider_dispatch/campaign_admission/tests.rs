use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;
use crate::db::campaigns::{self, graph::store};

const DISPATCH: &str = "capacity-admission-attempt";
const TASK: &str = "capacity:plan:initial";
const CHANNEL: u64 = 521_807;

async fn fixture(pool: &PgPool) -> (String, String, Goal) {
    sqlx::query(
        "INSERT INTO agents(id,name,provider) VALUES('qwen','Qwen','qwen'),('qa','QA','opencode')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO kv_meta(key,value) VALUES('runtime-config','{\"internalTasksEnabled\":true}')",
    )
    .execute(pool)
    .await
    .unwrap();
    let goal: Goal = serde_json::from_value(json!({
        "objective":"Test original campaign admission", "success":["validated result"],
        "constraints":["no external mutations"], "repo":"test/repo", "planner":"qwen",
        "verifier":"qa", "agents":["qwen","qa"],
        "budget":{"max_dispatches":12,"max_elapsed_secs":600,"node_timeout_secs":120,
            "max_evolutions":2,"max_observed_tool_calls":20}
    }))
    .unwrap();
    let campaign = campaigns::create(
        pool,
        "capacity-policy".into(),
        serde_json::from_value(
            json!({"title":"Capacity policy","status":"active","round":1,"nodes":[]}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    store::enroll(pool, &campaign.id, campaign.revision, &goal)
        .await
        .unwrap();
    let prompt = "Create a bounded plan with independent validation";
    let card = store::publish(
        pool,
        &campaign.id,
        &store::Publication {
            key: TASK,
            node: "plan",
            role: "plan",
            version: None,
            agent: "qwen",
            title: "Plan",
            prompt,
            digest: &contract_digest(&prompt).unwrap(),
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,thread_id) VALUES($1,$2,'qwen','implementation','dispatched',$3)")
        .bind(DISPATCH).bind(&card).bind(CHANNEL.to_string()).execute(pool).await.unwrap();
    assert!(
        observe(pool).await.unwrap().is_some(),
        "valid dispatch must be admitted before testing mutations"
    );
    (campaign.id, card, goal)
}

async fn observe(pool: &PgPool) -> Result<Option<Observed>, String> {
    inspect(pool, DISPATCH, &ProviderKind::Qwen, CHANNEL, Some("qwen")).await
}

#[tokio::test]
async fn campaign_capacity_policy_keeps_original_deadline_and_soft_pause_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (campaign, _, goal) = fixture(&pool).await;
    let original = observe(&pool).await.unwrap().unwrap();
    let created: DateTime<Utc> =
        sqlx::query_scalar("SELECT created_at FROM task_dispatches WHERE id=$1")
            .bind(DISPATCH)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        original.deadline,
        created + chrono::Duration::seconds(i64::from(goal.budget.node_timeout_secs))
    );
    sqlx::query(
        "UPDATE campaigns SET document=jsonb_set(document,'{status}','\"paused\"') WHERE id=$1",
    )
    .bind(&campaign)
    .execute(&pool)
    .await
    .unwrap();
    let paused = observe(&pool).await.unwrap().unwrap();
    assert_eq!(original.identity, paused.identity);
    assert_eq!(original.deadline, paused.deadline);
    sqlx::query(
        "UPDATE campaign_execution_specs SET hold_reason='unknown outcome' WHERE campaign_id=$1",
    )
    .bind(campaign)
    .execute(&pool)
    .await
    .unwrap();
    assert!(observe(&pool).await.is_err());
}

#[tokio::test]
async fn campaign_capacity_policy_rejects_stale_prompt_provider_terminal_and_deadline_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (_, card, _) = fixture(&pool).await;
    let profile: Value =
        sqlx::query_scalar("SELECT execution_profile FROM campaign_graph_tasks WHERE task_key=$1")
            .bind(TASK)
            .fetch_one(&pool)
            .await
            .unwrap();
    for (field, value) in [
        ("prompt_digest", json!("changed")),
        ("goal_digest", json!("changed")),
        ("provider_requested", json!("codex")),
    ] {
        let mut changed = profile.clone();
        changed[field] = value;
        sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=$2 WHERE task_key=$1")
            .bind(TASK)
            .bind(changed)
            .execute(&pool)
            .await
            .unwrap();
        assert!(observe(&pool).await.is_err(), "{field}");
    }
    sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=$2 WHERE task_key=$1")
        .bind(TASK)
        .bind(profile)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        inspect(
            &pool,
            DISPATCH,
            &ProviderKind::Qwen,
            CHANNEL + 1,
            Some("qwen")
        )
        .await
        .is_err()
    );
    assert!(
        inspect(&pool, DISPATCH, &ProviderKind::Qwen, CHANNEL, Some("qa"))
            .await
            .is_err()
    );
    sqlx::query("UPDATE task_dispatches SET created_at=NOW()-INTERVAL '121 seconds' WHERE id=$1")
        .bind(DISPATCH)
        .execute(&pool)
        .await
        .unwrap();
    assert!(observe(&pool).await.is_err());
    sqlx::query("UPDATE task_dispatches SET created_at=NOW(),status='completed',completed_at=NOW() WHERE id=$1")
        .bind(DISPATCH).execute(&pool).await.unwrap();
    assert!(observe(&pool).await.is_err());
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_dispatches WHERE kanban_card_id=$1")
            .bind(card)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count, 1,
        "read-only admission cannot enqueue a second attempt"
    );
}

#[tokio::test]
async fn campaign_capacity_policy_does_not_downgrade_missing_campaign_authority_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (_, _, _) = fixture(&pool).await;
    sqlx::query("INSERT INTO task_dispatches(id,status,dispatch_type) VALUES('ordinary','dispatched','implementation')")
        .execute(&pool).await.unwrap();
    assert!(
        inspect(&pool, "ordinary", &ProviderKind::Qwen, CHANNEL, None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        inspect(&pool, "missing", &ProviderKind::Qwen, CHANNEL, None)
            .await
            .is_err()
    );
    sqlx::query("UPDATE task_dispatches SET created_at=NULL WHERE id=$1")
        .bind(DISPATCH)
        .execute(&pool)
        .await
        .unwrap();
    assert!(observe(&pool).await.is_err());
    sqlx::query("UPDATE task_dispatches SET created_at=NOW() WHERE id=$1")
        .bind(DISPATCH)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM campaign_graph_tasks WHERE task_key=$1")
        .bind(TASK)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        observe(&pool).await.is_err(),
        "a canonical card must not become ordinary intake"
    );
}

#[tokio::test]
async fn campaign_capacity_review_uses_frozen_policy_and_requires_control_authority_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (campaign, _, goal) = fixture(&pool).await;
    sqlx::query("UPDATE campaign_graph_tasks SET role='template_review' WHERE task_key=$1")
        .bind(TASK)
        .execute(&pool)
        .await
        .unwrap();
    assert!(observe(&pool).await.is_err());
    sqlx::query("INSERT INTO campaign_dag_families(id,root_campaign_id,scope_digest,scope) VALUES('capacity-family',$1,'fixture','{}')")
        .bind(&campaign).execute(&pool).await.unwrap();
    let claimed: DateTime<Utc> =
        sqlx::query_scalar("SELECT clock_timestamp()-INTERVAL '30 seconds'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let policy = ReviewPolicy::freeze(&goal, claimed).unwrap();
    let snapshot = json!({"goal":goal,"execution_policy":policy});
    sqlx::query("INSERT INTO campaign_dag_reviews(id,family_id,owner_campaign_id,task_key,superior_agent_id,sample_count,threshold,sample_digest,snapshot,created_at) VALUES('capacity-review','capacity-family',$1,$2,'qwen',1,1,'fixture',$3,$4)")
        .bind(&campaign).bind(TASK).bind(&snapshot).bind(claimed).execute(&pool).await.unwrap();
    assert!(
        observe(&pool).await.is_err(),
        "role and review row do not confer control authority"
    );
    sqlx::query("INSERT INTO campaign_dag_control_assignments(task_key,family_id,agent_id,purpose) VALUES($1,'capacity-family','qwen','template_review')")
        .bind(TASK).execute(&pool).await.unwrap();
    let original = observe(&pool).await.unwrap().unwrap();
    assert_eq!(original.deadline, policy.deadline);
    sqlx::query("UPDATE campaign_execution_specs SET spec=jsonb_set(spec,'{budget,node_timeout_secs}','600') WHERE campaign_id=$1")
        .bind(&campaign).execute(&pool).await.unwrap();
    assert_eq!(
        observe(&pool).await.unwrap().unwrap().identity,
        original.identity
    );
    sqlx::query("UPDATE campaign_dag_reviews SET status='failed' WHERE task_key=$1")
        .bind(TASK)
        .execute(&pool)
        .await
        .unwrap();
    assert!(observe(&pool).await.is_err());
}
