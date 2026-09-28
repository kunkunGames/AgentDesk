//! Database boundary tests use explicitly synthetic collector receipts. Real
//! subprocess evidence is exercised separately in native_tests.
use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;
use std::collections::BTreeMap;

pub(super) fn contract(review: contract::Review) -> contract::Contract {
    let template = contract::Template {
        workspace: "/managed/workspace".into(),
        artifact_root: "/managed/artifacts".into(),
        allowed_changes: vec!["answer.txt".into()],
        acceptance: vec![contract::Acceptance {
            id: "correct-answer".into(),
            checks: vec!["answer".into()],
        }],
        checks: BTreeMap::from([(
            "answer".into(),
            contract::Check {
                executable: "/usr/bin/python3".into(),
                wrapper: "/managed/check.py".into(),
                executable_sha256: "a".repeat(64),
                wrapper_sha256: "b".repeat(64),
                timeout_secs: 5,
                kind: contract::CheckKind::Assertion,
            },
        )]),
        review,
        max_candidates: 3,
        max_check_attempts: 2,
    };
    let baseline = BTreeMap::<String, String>::new();
    let digest = contract::digest(&serde_json::to_vec(&(&template, &baseline)).unwrap());
    contract::Contract {
        template,
        baseline,
        digest,
    }
}

pub(super) async fn seed(pool: &PgPool, contract: &contract::Contract) {
    sqlx::query(
        "INSERT INTO kanban_cards(id,title,status) VALUES('card','check fixture','backlog')",
    )
    .execute(pool)
    .await
    .unwrap();
    store::enroll(pool, "card", contract).await.unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status,result) VALUES('work','card','implementation','completed','candidate output')")
        .execute(pool).await.unwrap();
    sqlx::query("UPDATE kanban_cards SET status='review',latest_dispatch_id='work',review_entered_at=NOW() WHERE id='card'")
        .execute(pool).await.unwrap();
}

async fn synthetic_pass(pool: &PgPool) -> store::Work {
    let work = store::load(pool, "card").await.unwrap().unwrap();
    let mut state = store::state(
        &work,
        &snapshot::Candidate {
            digest: "candidate".into(),
            path: "/managed/artifacts/candidate".into(),
        },
    );
    state["outcome"] = json!("passed");
    state["runs"]["answer"] = json!("managed-check:synthetic");
    sqlx::query("INSERT INTO test_phase_runs(id,idempotency_key,phase_key,head_sha,card_id,status,completed_at,evidence) VALUES('receipt','managed-check:synthetic','managed-check:answer','candidate','card','passed',NOW(),$1)")
        .bind(json!({"collector":"protected-command-v1","contract_digest":work.contract.digest,"dispatch_id":"work","check_id":"answer","outcome":"passed","process_reaped":true}))
        .execute(pool).await.unwrap();
    assert!(store::save(pool, &work, &state).await.unwrap());
    store::load(pool, "card").await.unwrap().unwrap()
}

#[tokio::test]
async fn public_writes_cannot_forge_managed_receipts_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let request = serde_json::from_value(
        json!({"phase_key":" managed-check:answer ","head_sha":"candidate","status":"passed"}),
    )
    .unwrap();
    assert!(
        super::super::upsert_test_phase_run(&pool, &request)
            .await
            .unwrap_err()
            .contains("protected collector")
    );
    let request=serde_json::from_value(json!({"phase_key":"ordinary","head_sha":"candidate","idempotency_key":" managed-check:synthetic ","status":"passed"})).unwrap();
    assert!(
        super::super::upsert_test_phase_run(&pool, &request)
            .await
            .is_err()
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn mandatory_receipts_and_immutable_contract_survive_raw_force_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    seed(&pool, &contract(contract::Review::None)).await;
    assert_eq!(disposition(&pool, "card").await.unwrap(), "await_checks");
    for query in [
        "UPDATE kanban_cards SET status='done' WHERE id='card'",
        "UPDATE kanban_cards SET verification_contract=NULL WHERE id='card'",
        "UPDATE kanban_cards SET status='custom_success' WHERE id='card'",
    ] {
        assert!(sqlx::query(query).execute(&pool).await.is_err());
    }
    synthetic_pass(&pool).await;
    assert_eq!(disposition(&pool, "card").await.unwrap(), "ready_to_commit");
    sqlx::query("UPDATE test_phase_runs SET evidence=evidence-'process_reaped'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        sqlx::query("UPDATE kanban_cards SET status='done' WHERE id='card'")
            .execute(&pool)
            .await
            .is_err()
    );
    // Cancellation and safe stop are not held hostage by acceptance checks.
    sqlx::query("UPDATE kanban_cards SET status='canceled' WHERE id='card'")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn replaced_result_and_new_attempt_reject_previous_eligibility_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    seed(&pool, &contract(contract::Review::None)).await;
    let old = synthetic_pass(&pool).await;
    sqlx::query("UPDATE task_dispatches SET result='replacement' WHERE id='work'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(!store::current(&pool, &old).await.unwrap());
    assert!(
        !store::save(&pool, &old, old.state.as_ref().unwrap())
            .await
            .unwrap()
    );
    assert!(
        sqlx::query("UPDATE kanban_cards SET status='done' WHERE id='card'")
            .execute(&pool)
            .await
            .is_err()
    );
    sqlx::query("UPDATE task_dispatches SET result='candidate output' WHERE id='work'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status,created_at) VALUES('work-new','card','rework','completed',NOW()+interval '1 second')").execute(&pool).await.unwrap();
    assert!(
        sqlx::query("UPDATE kanban_cards SET status='done' WHERE id='card'")
            .execute(&pool)
            .await
            .is_err()
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn approval_is_bound_to_contract_dispatch_and_candidate_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let contract = contract(contract::Review::Human);
    seed(&pool, &contract).await;
    synthetic_pass(&pool).await;
    assert_eq!(disposition(&pool, "card").await.unwrap(), "await_human");
    assert!(
        approve(
            &pool,
            "card",
            "candidate",
            "stale-work",
            &contract.digest,
            "reviewed"
        )
        .await
        .is_err()
    );
    assert!(
        approve(
            &pool,
            "card",
            "candidate",
            "work",
            "stale-contract",
            "reviewed"
        )
        .await
        .is_err()
    );
    assert!(
        approve(
            &pool,
            "card",
            "stale-candidate",
            "work",
            &contract.digest,
            "reviewed"
        )
        .await
        .is_err()
    );
    approve(
        &pool,
        "card",
        "candidate",
        "work",
        &contract.digest,
        "operator verified fixture",
    )
    .await
    .unwrap();
    assert_eq!(disposition(&pool, "card").await.unwrap(), "ready_to_commit");
    sqlx::query("UPDATE kanban_cards SET status='ready' WHERE id='card'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE kanban_cards SET status='review' WHERE id='card'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(disposition(&pool, "card").await.unwrap(), "await_checks");
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn review_wait_and_current_review_share_one_disposition_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    seed(&pool, &contract(contract::Review::Local)).await;
    let insert = "INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status,context,result) VALUES('review','card','review','completed','{}','{\"verdict\":\"pass\"}')";
    assert!(sqlx::query(insert).execute(&pool).await.is_err());
    synthetic_pass(&pool).await;
    assert_eq!(
        disposition(&pool, "card").await.unwrap(),
        "run_local_review"
    );
    assert!(
        sqlx::query("UPDATE kanban_cards SET status='done' WHERE id='card'")
            .execute(&pool)
            .await
            .is_err()
    );
    sqlx::query(insert).execute(&pool).await.unwrap();
    sqlx::query("UPDATE kanban_cards SET latest_dispatch_id='review' WHERE id='card'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(disposition(&pool, "card").await.unwrap(), "ready_to_commit");
    sqlx::query("UPDATE task_dispatches SET context='{}' WHERE id='review'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        disposition(&pool, "card").await.unwrap(),
        "run_local_review"
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_result_change_wins_before_completion_commits_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    seed(&pool, &contract(contract::Review::None)).await;
    synthetic_pass(&pool).await;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("UPDATE task_dispatches SET result='changed under lock' WHERE id='work'")
        .execute(&mut *tx)
        .await
        .unwrap();
    let other = pool.clone();
    let completion = tokio::spawn(async move {
        sqlx::query("UPDATE kanban_cards SET status='done' WHERE id='card'")
            .execute(&other)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!completion.is_finished());
    tx.commit().await.unwrap();
    assert!(completion.await.unwrap().is_err());
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn stale_collector_snapshot_cannot_overwrite_approval_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let contract = contract(contract::Review::Human);
    seed(&pool, &contract).await;
    let old = synthetic_pass(&pool).await;
    approve(
        &pool,
        "card",
        "candidate",
        "work",
        &contract.digest,
        "reviewed",
    )
    .await
    .unwrap();
    assert!(
        !store::save(&pool, &old, old.state.as_ref().unwrap())
            .await
            .unwrap()
    );
    assert!(!store::mark_wait(&pool, &old, "stale wait").await.unwrap());
    assert!(
        !store::unverifiable(&pool, &old, "stale error")
            .await
            .unwrap()
    );
    assert_eq!(disposition(&pool, "card").await.unwrap(), "ready_to_commit");
    pool.close().await;
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enrollment_rechecks_dispatch_insert_that_won_the_card_lock_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    sqlx::query(
        "INSERT INTO kanban_cards(id,title,status) VALUES('card','enrollment race','backlog')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status) VALUES('work','card','implementation','pending')")
        .execute(&mut *tx).await.unwrap();
    let other = pool.clone();
    let enrollment = tokio::spawn(async move {
        store::enroll(&other, "card", &contract(contract::Review::None)).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!enrollment.is_finished());
    tx.commit().await.unwrap();
    assert!(enrollment.await.unwrap().is_err());
    let unenrolled: bool = sqlx::query_scalar(
        "SELECT verification_contract IS NULL FROM kanban_cards WHERE id='card'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(unenrolled);
    pool.close().await;
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn js_bridge_uses_current_contract_for_review_gate_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    seed(&pool, &contract(contract::Review::None)).await;
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().to_path_buf();
    let engine = crate::engine::PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    let blocked: bool = engine.eval_js("(() => { try { agentdesk.kanban.setStatus('card', 'done'); return false; } catch (_) { return true; } })()").unwrap();
    assert!(blocked);
    synthetic_pass(&pool).await;
    let result: String = engine.eval_js("JSON.stringify({disposition: agentdesk.review.disposition('card'), transition: agentdesk.kanban.setStatus('card', 'done')})").unwrap();
    let result: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(result["disposition"], "ready_to_commit");
    assert_eq!(result["transition"]["changed"], true);
    assert_eq!(disposition(&pool, "card").await.unwrap(), "completed");
    drop(engine);
    pool.close().await;
    db.drop().await;
}
