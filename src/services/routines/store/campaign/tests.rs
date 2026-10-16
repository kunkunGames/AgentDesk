use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;

async fn fixture(
    pool: &PgPool,
    id: &str,
    provider: &str,
    state: CampaignStatus,
) -> (RoutineStore, AdoptCampaignOperation) {
    sqlx::query("INSERT INTO agents(id,name,provider) VALUES($1,$1,$2) ON CONFLICT(id) DO NOTHING")
        .bind(provider)
        .bind(provider)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO routines(id,agent_id,script_ref,name,status,execution_strategy,schedule,next_due_at,timeout_secs,max_retries) VALUES($1,$2,'fixture.js',$1,'enabled','fresh','*/5 * * * *',NOW()-INTERVAL '1 minute',180,0)")
        .bind(id).bind(provider).execute(pool).await.unwrap();
    let updated = sqlx::query_scalar("SELECT updated_at FROM routines WHERE id=$1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
    let input:CampaignInput=serde_json::from_value(json!({"title":id,"status":state,"round":1,"nodes":[{
        "id":"work","title":"Native maintenance","stage":"operate","round":1,"status":"pending",
        "assignee":provider,"provider":provider,"details":"Run the registered operation","acceptance":["Record the durable native outcome"]}]})).unwrap();
    campaigns::create(pool, id.into(), input).await.unwrap();
    (
        RoutineStore::new_with_timezone_and_checkpoint_limit(
            Arc::new(pool.clone()),
            "Asia/Seoul",
            65536,
        ),
        AdoptCampaignOperation {
            routine_id: id.into(),
            node_id: "work".into(),
            expected_revision: 1,
            expected_routine_updated_at: updated,
            next_due_at: Utc::now() - chrono::Duration::seconds(1),
        },
    )
}

#[tokio::test]
async fn adopted_routine_cannot_run_independently_and_campaign_claim_is_atomic_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (store, request) = fixture(&pool, "adopt", "qwen", CampaignStatus::Active).await;
    store
        .adopt_campaign_operation("adopt", request)
        .await
        .unwrap();
    assert!(store.claim_due_runs(5).await.unwrap().is_empty());
    assert!(store.claim_run_now("adopt").await.unwrap().is_none());
    assert!(store.resume_routine("adopt", None).await.is_err());
    let original = campaigns::get(&pool, "adopt").await.unwrap();
    let mut edited = input(&original);
    edited.nodes[0].details = "silently replace the registered job".into();
    assert!(
        campaigns::replace(&pool, "adopt", original.revision, edited)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE agents SET provider='claude' WHERE id='qwen'")
            .execute(&pool)
            .await
            .is_err()
    );
    let (a, b) = tokio::join!(
        store.claim_campaign_operation(),
        store.claim_campaign_operation()
    );
    let claims: Vec<_> = [a.unwrap(), b.unwrap()].into_iter().flatten().collect();
    assert_eq!(claims.len(), 1);
    let c = campaigns::get(&pool, "adopt").await.unwrap();
    assert_eq!(c.nodes[0].input.status, NodeStatus::Running);
    assert_eq!(
        c.nodes[0].input.session_id.as_deref(),
        Some(claims[0].run_id.as_str())
    );
    let owner: String = sqlx::query_scalar("SELECT campaign_id FROM routine_runs WHERE id=$1")
        .bind(&claims[0].run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(owner, "adopt");
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn campaign_pause_and_failure_hold_without_blocking_independent_work_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (store, request) = fixture(&pool, "held", "qwen", CampaignStatus::Paused).await;
    store
        .adopt_campaign_operation("held", request)
        .await
        .unwrap();
    assert!(store.claim_campaign_operation().await.unwrap().is_none());
    let c = campaigns::get(&pool, "held").await.unwrap();
    let mut next = input(&c);
    next.status = CampaignStatus::Active;
    campaigns::replace(&pool, &c.id, c.revision, next)
        .await
        .unwrap();
    let run = store.claim_campaign_operation().await.unwrap().unwrap();
    store
        .fail_run(&run.run_id, "unverified external effect", None, None)
        .await
        .unwrap();
    let restarted = RoutineStore::new_with_timezone_and_checkpoint_limit(
        Arc::new(pool.clone()),
        "Asia/Seoul",
        65536,
    );
    assert!(
        restarted
            .claim_campaign_operation()
            .await
            .unwrap()
            .is_none(),
        "restart must not replay before projection"
    );
    assert_eq!(restarted.reconcile_campaign_operations().await.unwrap(), 1);
    let c = campaigns::get(&pool, "held").await.unwrap();
    assert_eq!(c.status, CampaignStatus::Paused);
    assert_eq!(c.nodes[0].input.status, NodeStatus::Blocked);
    assert_eq!(restarted.reconcile_campaign_operations().await.unwrap(), 0);
    let (_, request) = fixture(&pool, "independent", "opencode", CampaignStatus::Active).await;
    restarted
        .adopt_campaign_operation("independent", request)
        .await
        .unwrap();
    assert_eq!(
        restarted
            .claim_campaign_operation()
            .await
            .unwrap()
            .unwrap()
            .routine_id,
        "independent"
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn recurring_completion_preserves_history_and_next_due_across_restart_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (store, request) = fixture(&pool, "recurring", "opencode", CampaignStatus::Active).await;
    store
        .adopt_campaign_operation("recurring", request)
        .await
        .unwrap();
    let run = store.claim_campaign_operation().await.unwrap().unwrap();
    store
        .finish_run(
            &run.run_id,
            Some(json!({"verified_script_receipt":true})),
            None,
            Some("no work in current batch"),
            None,
        )
        .await
        .unwrap();
    store.reconcile_campaign_operations().await.unwrap();
    let c = campaigns::get(&pool, "recurring").await.unwrap();
    assert_eq!(c.status, CampaignStatus::Active);
    assert_eq!(c.nodes[0].input.status, NodeStatus::Completed);
    assert!(
        store.claim_campaign_operation().await.unwrap().is_none(),
        "cadence must not become a busy loop"
    );
    sqlx::query("UPDATE routines SET next_due_at=NOW()-INTERVAL '1 second' WHERE id='recurring'")
        .execute(&pool)
        .await
        .unwrap();
    let restarted = RoutineStore::new_with_timezone_and_checkpoint_limit(
        Arc::new(pool.clone()),
        "Asia/Seoul",
        65536,
    );
    let second = restarted.claim_campaign_operation().await.unwrap().unwrap();
    assert_ne!(run.run_id, second.run_id);
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM routine_runs WHERE campaign_id='recurring'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 2);
    assert_eq!(
        campaigns::get(&pool, "recurring").await.unwrap().nodes[0]
            .input
            .evidence_records
            .len(),
        1
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn adoption_rejects_stale_source_and_preserves_original_state_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (store, mut request) = fixture(&pool, "stale", "qwen", CampaignStatus::Active).await;
    request.expected_routine_updated_at -= chrono::Duration::seconds(1);
    assert!(
        store
            .adopt_campaign_operation("stale", request)
            .await
            .is_err()
    );
    assert!(store.claim_campaign_operation().await.unwrap().is_none());
    let status: String = sqlx::query_scalar("SELECT status FROM routines WHERE id='stale'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "enabled");
    assert_eq!(campaigns::get(&pool, "stale").await.unwrap().revision, 1);
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn campaign_agent_text_without_current_typed_completion_is_not_success_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (store, request) = fixture(&pool, "completion", "qwen", CampaignStatus::Active).await;
    store
        .adopt_campaign_operation("completion", request)
        .await
        .unwrap();
    let run = store.claim_campaign_operation().await.unwrap().unwrap();
    store
        .complete_agent_run(
            &run.run_id,
            Some(json!({"text":"success"})),
            None,
            Some("done"),
            NextDueAtUpdate::Preserve,
        )
        .await
        .unwrap();
    store.reconcile_campaign_operations().await.unwrap();
    let c = campaigns::get(&pool, "completion").await.unwrap();
    assert_eq!(c.status, CampaignStatus::Paused);
    assert_eq!(c.nodes[0].input.status, NodeStatus::Blocked);
    let mut next = input(&c);
    next.status = CampaignStatus::Active;
    next.nodes[0].status = NodeStatus::Pending;
    campaigns::replace(&pool, &c.id, c.revision, next)
        .await
        .unwrap();
    sqlx::query("UPDATE routines SET next_due_at=NOW()-INTERVAL '1 second' WHERE id='completion'")
        .execute(&pool)
        .await
        .unwrap();
    let second = store.claim_campaign_operation().await.unwrap().unwrap();
    store
        .complete_agent_run(
            &second.run_id,
            Some(json!({
                "status":"completed", "fresh_context_guaranteed":true,
                "completion_evidence":"session_transcripts", "turn_terminal_status":"completed"
            })),
            None,
            Some("typed completion"),
            NextDueAtUpdate::Preserve,
        )
        .await
        .unwrap();
    store.reconcile_campaign_operations().await.unwrap();
    let c = campaigns::get(&pool, "completion").await.unwrap();
    assert_eq!(c.status, CampaignStatus::Active);
    assert_eq!(c.nodes[0].input.status, NodeStatus::Completed);
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn failed_campaign_requires_explicit_rearm_and_preserves_paused_source_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (store, request) = fixture(&pool, "rearm", "qwen", CampaignStatus::Active).await;
    store
        .adopt_campaign_operation("rearm", request)
        .await
        .unwrap();
    let first = store.claim_campaign_operation().await.unwrap().unwrap();
    store
        .fail_run_and_pause_routine(&first.run_id, "interrupted native work", None)
        .await
        .unwrap();
    store.reconcile_campaign_operations().await.unwrap();
    let c = campaigns::get(&pool, "rearm").await.unwrap();
    let mut next = input(&c);
    next.status = CampaignStatus::Active;
    let active = campaigns::replace(&pool, &c.id, c.revision, next)
        .await
        .unwrap();
    assert!(store.claim_campaign_operation().await.unwrap().is_none());
    let mut next = input(&active);
    next.nodes[0].status = NodeStatus::Pending;
    campaigns::replace(&pool, &active.id, active.revision, next)
        .await
        .unwrap();
    let second = store.claim_campaign_operation().await.unwrap().unwrap();
    assert_ne!(first.run_id, second.run_id);
    let source: String = sqlx::query_scalar("SELECT status FROM routines WHERE id='rearm'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(source, "paused");
    pool.close().await;
    db.drop().await;
}
