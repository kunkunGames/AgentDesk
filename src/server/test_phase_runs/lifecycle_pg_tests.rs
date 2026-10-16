//! Real PostgreSQL regressions for collector ownership and atomic lifecycle.
use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;

fn start_request(key: &str, job: &str) -> TestPhaseRunStartRequest {
    serde_json::from_value(json!({
        "phase_key": "acceptance", "head_sha": "candidate-a",
        "idempotency_key": key, "card_id": "card-a", "repo_id": "repo-a",
        "required_capabilities": {"check": "fixture"},
        "resource_lock_key": "check-workspace", "holder_instance_id": "node-a",
        "holder_job_id": job, "ttl_secs": 60,
    }))
    .unwrap()
}

fn completion(run: &TestPhaseRun) -> TestPhaseRunCompleteRequest {
    serde_json::from_value(json!({
        "phase_key": run.phase_key, "head_sha": run.head_sha,
        "idempotency_key": run.idempotency_key,
        "resource_lock_key": run.resource_lock_key,
        "holder_instance_id": run.holder_instance_id, "holder_job_id": run.holder_job_id,
        "status": "passed", "evidence": {"exit_code": 0, "report": "fixture"},
        "release_lock": true,
    }))
    .unwrap()
}

#[tokio::test]
async fn queued_contract_survives_start_and_receipt_replay_can_release_its_own_lock_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let queued: TestPhaseRunRequest = serde_json::from_value(json!({
        "phase_key":"acceptance", "head_sha":"candidate-a", "idempotency_key":"check",
        "card_id":"card-a", "repo_id":"repo-a", "required_capabilities":{"check":"fixture"}
    }))
    .unwrap();
    upsert_test_phase_run(&pool, &queued).await.unwrap();
    let mut request = start_request("check", "job");
    request.run.required_capabilities = None;
    let run = start_test_phase_run(&pool, &request)
        .await
        .unwrap()
        .run
        .unwrap();
    assert_eq!(run.required_capabilities, json!({"check":"fixture"}));
    let mut result = completion(&run);
    result.release_lock = false;
    let first = complete_test_phase_run(&pool, &result).await.unwrap();
    assert!(first.lock_released.is_none());
    assert!(
        crate::server::resource_locks::get_resource_lock(&pool, "check-workspace")
            .await
            .unwrap()
            .is_some()
    );
    result.release_lock = true;
    let replay = complete_test_phase_run(&pool, &result).await.unwrap();
    assert_eq!(replay.lock_released, Some(true));
    assert_eq!(replay.run.completed_at, first.run.completed_at);
    assert!(
        crate::server::resource_locks::get_resource_lock(&pool, "check-workspace")
            .await
            .unwrap()
            .is_none()
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_start_commits_exactly_one_execution_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let mut jobs = tokio::task::JoinSet::new();
    for n in 0..12 {
        let pool = pool.clone();
        jobs.spawn(async move {
            start_test_phase_run(&pool, &start_request("same-check", &format!("job-{n}")))
                .await
                .unwrap()
        });
    }
    let mut started = Vec::new();
    let mut returned_ids = std::collections::BTreeSet::new();
    while let Some(result) = jobs.join_next().await {
        let outcome = result.unwrap();
        let run = outcome
            .run
            .expect("all duplicate deliveries return the same run");
        returned_ids.insert(run.id.clone());
        if outcome.started {
            started.push(run);
        }
    }
    assert_eq!(started.len(), 1);
    assert_eq!(returned_ids.len(), 1);
    let lock = crate::server::resource_locks::get_resource_lock(&pool, "check-workspace")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(lock.holder_job_id), started[0].holder_job_id);
    assert_eq!(lock.metadata["idempotency_key"], "same-check");
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn stale_collector_cannot_complete_after_lease_takeover_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let old = start_test_phase_run(&pool, &start_request("old-check", "old-job"))
        .await
        .unwrap()
        .run
        .unwrap();
    sqlx::query("UPDATE resource_locks SET expires_at=clock_timestamp()-interval '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        complete_test_phase_run(&pool, &completion(&old))
            .await
            .unwrap_err()
            .contains("expired")
    );
    let new = start_test_phase_run(&pool, &start_request("new-check", "new-job"))
        .await
        .unwrap();
    assert!(new.started);
    assert!(
        complete_test_phase_run(&pool, &completion(&old))
            .await
            .is_err()
    );
    assert!(
        latest_passing_evidence(&pool, "acceptance", "candidate-a")
            .await
            .unwrap()
            .is_none()
    );
    let lock = crate::server::resource_locks::get_resource_lock(&pool, "check-workspace")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lock.holder_job_id, "new-job");
    let result = complete_test_phase_run(&pool, &completion(&new.run.unwrap()))
        .await
        .unwrap();
    assert_eq!(result.run.status, "passed");
    assert_eq!(result.lock_released, Some(true));
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn owned_evidence_requires_current_owner_and_terminal_replay_is_immutable_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let request = start_request("check", "job");
    let run = start_test_phase_run(&pool, &request)
        .await
        .unwrap()
        .run
        .unwrap();
    let complete = completion(&run);
    assert!(upsert_test_phase_run(&pool, &complete.run).await.is_err());
    let mut missing = complete.clone();
    missing.run.holder_job_id = None;
    assert!(complete_test_phase_run(&pool, &missing).await.is_err());
    let first = complete_test_phase_run(&pool, &complete).await.unwrap();
    assert_eq!(first.run.required_capabilities, json!({"check":"fixture"}));
    assert_eq!(first.lock_released, Some(true));
    let restart = start_test_phase_run(&pool, &request).await.unwrap();
    assert!(!restart.started);
    assert_eq!(restart.run.unwrap().status, "passed");
    // Reuse the resource with a new attempt, then replay the old receipt.
    let new = start_test_phase_run(&pool, &start_request("next-check", "next-job"))
        .await
        .unwrap();
    assert!(new.started);
    let replay = complete_test_phase_run(&pool, &complete).await.unwrap();
    assert_eq!(replay.run.completed_at, first.run.completed_at);
    assert_eq!(replay.run.updated_at, first.run.updated_at);
    assert_eq!(replay.lock_released, Some(false));
    let lock = crate::server::resource_locks::get_resource_lock(&pool, "check-workspace")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lock.holder_job_id, "next-job");
    let mut replaced = complete.clone();
    replaced.run.evidence = Some(json!({"exit_code":1}));
    assert!(
        complete_test_phase_run(&pool, &replaced)
            .await
            .unwrap_err()
            .contains("cannot be replaced")
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn identity_collision_and_reused_owner_cannot_rebind_a_check_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let original = start_request("check", "job");
    let run = start_test_phase_run(&pool, &original)
        .await
        .unwrap()
        .run
        .unwrap();
    for field in ["phase", "head", "card", "repo", "capabilities"] {
        let mut conflict = original.clone();
        conflict.resource_lock_key = "other-resource".into();
        match field {
            "phase" => conflict.run.phase_key = "other-phase".into(),
            "head" => conflict.run.head_sha = "other-candidate".into(),
            "card" => conflict.run.card_id = Some("other-card".into()),
            "repo" => conflict.run.repo_id = Some("other-repo".into()),
            _ => conflict.run.required_capabilities = Some(json!({"check":"different"})),
        }
        assert!(
            start_test_phase_run(&pool, &conflict).await.is_err(),
            "{field}"
        );
    }
    assert!(
        crate::server::resource_locks::get_resource_lock(&pool, "other-resource")
            .await
            .unwrap()
            .is_none()
    );
    complete_test_phase_run(&pool, &completion(&run))
        .await
        .unwrap();
    assert!(
        start_test_phase_run(&pool, &start_request("new-check", "job"))
            .await
            .unwrap_err()
            .contains("fresh execution")
    );
    assert!(
        crate::server::resource_locks::get_resource_lock(&pool, "check-workspace")
            .await
            .unwrap()
            .is_none()
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn run_and_resource_lease_roll_back_together_on_storage_failure_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    sqlx::query(
        "ALTER TABLE test_phase_runs ADD CONSTRAINT reject_fixture CHECK (phase_key<>'rejected')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut rejected = start_request("rejected-check", "rejected-job");
    rejected.run.phase_key = "rejected".into();
    assert!(start_test_phase_run(&pool, &rejected).await.is_err());
    assert!(
        crate::server::resource_locks::get_resource_lock(&pool, "check-workspace")
            .await
            .unwrap()
            .is_none()
    );
    let run = start_test_phase_run(&pool, &start_request("check", "job"))
        .await
        .unwrap()
        .run
        .unwrap();
    sqlx::raw_sql("CREATE FUNCTION reject_release() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture rejects release'; END $$; CREATE TRIGGER reject_release BEFORE DELETE ON resource_locks FOR EACH ROW EXECUTE FUNCTION reject_release();").execute(&pool).await.unwrap();
    assert!(
        complete_test_phase_run(&pool, &completion(&run))
            .await
            .is_err()
    );
    assert!(
        latest_passing_evidence(&pool, "acceptance", "candidate-a")
            .await
            .unwrap()
            .is_none()
    );
    let status: String = sqlx::query_scalar("SELECT status FROM test_phase_runs WHERE id=$1")
        .bind(&run.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "running");
    assert!(
        crate::server::resource_locks::get_resource_lock(&pool, "check-workspace")
            .await
            .unwrap()
            .is_some()
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn unowned_legacy_evidence_remains_available_without_claiming_a_resource_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let request: TestPhaseRunRequest = serde_json::from_value(json!({
        "phase_key":"legacy", "head_sha":"abc123", "status":"queued"
    }))
    .unwrap();
    let queued = upsert_test_phase_run(&pool, &request).await.unwrap();
    let complete = TestPhaseRunCompleteRequest {
        run: TestPhaseRunRequest {
            status: Some("passed".into()),
            evidence: Some(json!({"legacy":true})),
            ..request
        },
        release_lock: true,
    };
    let result = complete_test_phase_run(&pool, &complete).await.unwrap();
    assert_eq!(result.run.id, queued.id);
    assert_eq!(result.run.evidence, json!({"legacy":true}));
    assert_eq!(result.lock_released, Some(false));
    assert!(result.run.holder_job_id.is_none());
    pool.close().await;
    fixture.drop().await;
}
