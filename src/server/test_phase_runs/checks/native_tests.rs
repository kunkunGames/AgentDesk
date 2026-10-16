//! Disposable Linux process + PostgreSQL acceptance, deliberately opt-in.
use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;
use std::{
    fs,
    os::unix::{ffi::OsStrExt, fs::PermissionsExt},
    path::Path,
    sync::{Arc, atomic::AtomicBool},
};

fn owned(path: &Path) {
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::chown(path.as_ptr(), 65534, 65534) }, 0);
}

pub(crate) fn fixture(
    script: &str,
    review: contract::Review,
) -> (tempfile::TempDir, contract::Contract) {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o755)).unwrap();
    let mut template = super::pg_tests::contract(review).template;
    template.workspace = root.path().join("workspace");
    template.artifact_root = root.path().join("artifacts");
    fs::create_dir(&template.workspace).unwrap();
    fs::set_permissions(&template.workspace, fs::Permissions::from_mode(0o700)).unwrap();
    owned(&template.workspace);
    fs::create_dir(&template.artifact_root).unwrap();
    let check = template.checks.get_mut("answer").unwrap();
    check.executable = fs::canonicalize("/usr/bin/python3").unwrap();
    check.wrapper = root.path().join("check.py");
    fs::write(&check.wrapper, script).unwrap();
    check.executable_sha256 = contract::digest(&fs::read(&check.executable).unwrap());
    check.wrapper_sha256 = contract::digest(script.as_bytes());
    let contract = snapshot::enroll(template).unwrap();
    (root, contract)
}

pub(crate) fn answer(contract: &contract::Contract, value: &str) {
    let path = contract.template.workspace.join("answer.txt");
    fs::write(&path, value).unwrap();
    owned(&path);
}

pub(crate) fn engine(root: &Path, pool: PgPool) -> crate::engine::PolicyEngine {
    let mut config = crate::config::Config::default();
    config.policies.dir = root.join("empty-policies");
    fs::create_dir(&config.policies.dir).unwrap();
    crate::engine::PolicyEngine::new_with_pg(&config, Some(pool)).unwrap()
}

const ANSWER: &str = "import json, pathlib, sys\nprint(json.dumps({'passed': pathlib.Path(sys.argv[1], 'answer.txt').read_text() == '42'}))\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Linux root launcher, Python3, Landlock and PostgreSQL"]
async fn real_check_receipt_drives_fsm_once_and_preserves_artifact_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (root, contract) = fixture(ANSWER, contract::Review::None);
    super::pg_tests::seed(&pool, &contract).await;
    answer(&contract, "42");
    let engine = engine(root.path(), pool.clone());
    driver::drive(&pool, &engine, "card").await.unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM kanban_cards WHERE id='card'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "done");
    assert_eq!(disposition(&pool, "card").await.unwrap(), "completed");
    driver::drive(&pool, &engine, "card").await.unwrap();
    let evidence: Value =
        sqlx::query_scalar("SELECT evidence FROM test_phase_runs WHERE card_id='card'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(evidence["outcome"], "passed");
    assert_eq!(evidence["process_reaped"], true);
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM kanban_audit_logs WHERE card_id='card' AND to_status='done'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
    let artifact = completed_artifact(&pool, "card").await.unwrap().unwrap();
    assert_eq!(artifact.card_id(), "card");
    assert_eq!(artifact.files().unwrap()["answer.txt"], b"42");
    // Handoff reads the frozen output even after the agent edits its workspace.
    answer(&contract, "later edit");
    assert_eq!(artifact.files().unwrap()["answer.txt"], b"42");
    // A ledger status or an unverified card can never substitute for a receipt.
    sqlx::query("INSERT INTO kanban_cards(id,title,status) VALUES('plain','plain','done')")
        .execute(&pool)
        .await
        .unwrap();
    assert!(completed_artifact(&pool, "plain").await.unwrap().is_none());
    let state: Value =
        sqlx::query_scalar("SELECT verification_state FROM kanban_cards WHERE id='card'")
            .fetch_one(&pool)
            .await
            .unwrap();
    snapshot::verify(
        Path::new(state["candidate_path"].as_str().unwrap()),
        state["candidate_digest"].as_str().unwrap(),
    )
    .unwrap();
    assert_eq!(
        artifact.digest(),
        state["candidate_digest"].as_str().unwrap()
    );
    // Even privileged, accidental artifact corruption must stop all handoffs.
    let frozen = Path::new(state["candidate_path"].as_str().unwrap()).join("answer.txt");
    fs::set_permissions(&frozen, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&frozen, "corrupt").unwrap();
    assert!(completed_artifact(&pool, "card").await.is_err());
    drop(engine);
    pool.close().await;
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Linux root launcher, Python3, Landlock and PostgreSQL"]
async fn sweep_reaches_independent_check_after_unready_review_cards_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (root, contract) = fixture(ANSWER, contract::Review::None);
    for index in 0..16 {
        let card = format!("unready-{index:02}");
        sqlx::query("INSERT INTO kanban_cards(id,title,status) VALUES($1,'unready','backlog')")
            .bind(&card)
            .execute(&pool)
            .await
            .unwrap();
        store::enroll(&pool, &card, &contract).await.unwrap();
        // Cover both a missing result and an implementation still in flight.
        if index % 2 == 0 {
            sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status) VALUES($1,$2,'implementation','dispatched')")
                .bind(format!("pending-{index}")).bind(&card).execute(&pool).await.unwrap();
        }
        sqlx::query(
            "UPDATE kanban_cards SET status='review',updated_at=NOW()-INTERVAL '1 day' WHERE id=$1",
        )
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
    }
    super::pg_tests::seed(&pool, &contract).await;
    answer(&contract, "42");
    let engine = engine(root.path(), pool.clone());
    driver::sweep(&pool, &engine).await.unwrap();
    assert_eq!(disposition(&pool, "card").await.unwrap(), "completed");
    let waiting: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM kanban_cards WHERE id LIKE 'unready-%' AND status='review'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(waiting, 16);
    let receipts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_phase_runs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        receipts, 1,
        "unready implementations must not start a checker"
    );
    drop(engine);
    pool.close().await;
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Linux root launcher, Python3, Landlock and PostgreSQL"]
async fn rejected_captures_do_not_starve_independent_checks_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (root, contract) = fixture(ANSWER, contract::Review::None);
    for index in 0..16 {
        let card = format!("rejected-{index:02}");
        sqlx::query(
            "INSERT INTO kanban_cards(id,title,status) VALUES($1,'capture rejected','backlog')",
        )
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
        store::enroll(&pool, &card, &contract).await.unwrap();
        sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status,result) VALUES($1,$2,'implementation','completed','candidate')")
            .bind(format!("work-{index}")).bind(&card).execute(&pool).await.unwrap();
        sqlx::query("UPDATE kanban_cards SET status='review' WHERE id=$1")
            .bind(&card)
            .execute(&pool)
            .await
            .unwrap();
        let work = store::load(&pool, &card).await.unwrap().unwrap();
        // The capture rejection boundary itself has a separate real filesystem
        // test; here seed its durable result to exercise repeated scheduler ticks.
        assert!(
            store::unverifiable(&pool, &work, "fixture capture rejected")
                .await
                .unwrap()
        );
    }
    super::pg_tests::seed(&pool, &contract).await;
    answer(&contract, "42");
    let engine = engine(root.path(), pool.clone());
    driver::sweep(&pool, &engine).await.unwrap();
    driver::sweep(&pool, &engine).await.unwrap();
    assert_eq!(disposition(&pool, "card").await.unwrap(), "completed");
    let rejected: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM kanban_cards WHERE id LIKE 'rejected-%' AND status='review' AND verification_state->>'outcome'='unverifiable' AND verification_state->>'candidate_digest' IS NULL")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(rejected, 16, "a held capture must not be silently retried");
    let receipts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_phase_runs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(receipts, 1);
    drop(engine);
    pool.close().await;
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Linux root launcher, Python3, Landlock and PostgreSQL"]
async fn failed_candidate_is_not_replaced_by_editing_live_workspace_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (root, contract) = fixture(ANSWER, contract::Review::None);
    super::pg_tests::seed(&pool, &contract).await;
    answer(&contract, "wrong");
    let engine = engine(root.path(), pool.clone());
    driver::drive(&pool, &engine, "card").await.unwrap();
    assert_eq!(disposition(&pool, "card").await.unwrap(), "await_human");
    answer(&contract, "42");
    driver::drive(&pool, &engine, "card").await.unwrap();
    assert_eq!(disposition(&pool, "card").await.unwrap(), "await_human");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_phase_runs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    drop(engine);
    pool.close().await;
    db.drop().await;
}

#[test]
#[ignore = "requires disposable Linux root launcher, Python3 and Landlock"]
fn kernel_denies_candidate_mutation_network_children_and_forged_background_report() {
    let script = r#"import errno, json, os, pathlib, socket, sys
root=pathlib.Path(sys.argv[1])
def denied(action):
    try: action()
    except OSError as e: assert e.errno in (errno.EACCES, errno.EPERM), e
    else: raise AssertionError('checker boundary escaped')
denied(lambda: root.joinpath('answer.txt').write_text('forged'))
denied(lambda: socket.socket())
denied(lambda: os.fork())
print(json.dumps({'passed': root.joinpath('answer.txt').read_text() == '42'}))
"#;
    let (_root, contract) = fixture(script, contract::Review::None);
    answer(&contract, "42");
    let candidate = snapshot::freeze(&contract).unwrap();
    let identity =
        crate::services::platform::managed_process::inspect_workspace(&contract.template.workspace)
            .unwrap();
    let result = process::run(
        &contract.template.checks["answer"],
        &candidate.path,
        &candidate.digest,
        identity,
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    assert_eq!(result["outcome"], "passed");
    assert_eq!(result["process_reaped"], true);
    assert_eq!(
        fs::read_to_string(candidate.path.join("answer.txt")).unwrap(),
        "42"
    );
}

#[test]
#[ignore = "requires disposable Linux root launcher, Python3 and Landlock"]
fn deadline_reaps_a_checker_even_after_it_prints_a_passing_report() {
    let (_root, mut contract) = fixture(
        "import time\nprint('{\"passed\":true}',flush=True)\ntime.sleep(20)\n",
        contract::Review::None,
    );
    contract
        .template
        .checks
        .get_mut("answer")
        .unwrap()
        .timeout_secs = 1;
    let candidate = snapshot::freeze(&contract).unwrap();
    let identity =
        crate::services::platform::managed_process::inspect_workspace(&contract.template.workspace)
            .unwrap();
    let result = process::run(
        &contract.template.checks["answer"],
        &candidate.path,
        &candidate.digest,
        identity,
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    assert_eq!(result["outcome"], "unverifiable");
    assert_eq!(result["reason"], "deadline");
    assert_eq!(result["process_reaped"], true);
}

#[test]
#[ignore = "requires disposable Linux root launcher and Python3"]
fn candidate_rejects_literal_backslash_and_aliasing_filenames() {
    let (_root, contract) = fixture(ANSWER, contract::Review::None);
    for name in ["..\\outside", "dir\\answer.txt"] {
        let path = contract.template.workspace.join(name);
        fs::write(&path, "unexpected filename").unwrap();
        owned(&path);
        assert!(
            snapshot::freeze(&contract)
                .err()
                .unwrap()
                .contains("invalid candidate-relative path")
        );
        fs::remove_file(path).unwrap();
    }
    assert_eq!(
        fs::read_dir(&contract.template.artifact_root)
            .unwrap()
            .count(),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Linux root launcher, Python3, Landlock and PostgreSQL"]
async fn capture_scope_failure_is_visible_and_cannot_be_retried_by_editing_files_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (root, contract) = fixture(ANSWER, contract::Review::None);
    super::pg_tests::seed(&pool, &contract).await;
    answer(&contract, "42");
    let forbidden = contract.template.workspace.join("unapproved.txt");
    fs::write(&forbidden, "outside approved scope").unwrap();
    owned(&forbidden);
    let engine = engine(root.path(), pool.clone());
    driver::drive(&pool, &engine, "card").await.unwrap();
    assert_eq!(disposition(&pool, "card").await.unwrap(), "await_human");
    let state: Value =
        sqlx::query_scalar("SELECT verification_state FROM kanban_cards WHERE id='card'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state["outcome"], "unverifiable");
    assert!(state["candidate_digest"].is_null());
    fs::remove_file(forbidden).unwrap();
    driver::drive(&pool, &engine, "card").await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_phase_runs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(disposition(&pool, "card").await.unwrap(), "await_human");
    drop(engine);
    pool.close().await;
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Linux root launcher, Python3, Landlock and PostgreSQL"]
async fn missing_report_retries_are_bounded_across_reconciliation_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (root, contract) = fixture("print('no structured report')\n", contract::Review::None);
    super::pg_tests::seed(&pool, &contract).await;
    answer(&contract, "42");
    let engine = engine(root.path(), pool.clone());
    for _ in 0..3 {
        driver::drive(&pool, &engine, "card").await.unwrap();
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_phase_runs WHERE status='failed' AND evidence->>'outcome'='unverifiable'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(count, i64::from(contract.template.max_check_attempts));
    assert_eq!(disposition(&pool, "card").await.unwrap(), "await_human");
    drop(engine);
    pool.close().await;
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Linux root launcher, Python3, Landlock and PostgreSQL"]
async fn cancellation_reaps_running_checker_without_recording_success_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (root, contract) = fixture(
        "import time\nprint('{\"passed\":true}',flush=True)\ntime.sleep(20)\n",
        contract::Review::None,
    );
    super::pg_tests::seed(&pool, &contract).await;
    answer(&contract, "42");
    let engine = engine(root.path(), pool.clone());
    let cancel = async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let running: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM test_phase_runs WHERE status='running')",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if running {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        // The report is already printed, but the process is still alive.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        sqlx::query("UPDATE kanban_cards SET status='canceled' WHERE id='card'")
            .execute(&pool)
            .await
            .unwrap();
    };
    let (result, ()) = tokio::join!(driver::drive(&pool, &engine, "card"), cancel);
    result.unwrap();
    let evidence: Value =
        sqlx::query_scalar("SELECT evidence FROM test_phase_runs WHERE card_id='card'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(evidence["outcome"], "cancelled");
    assert_eq!(evidence["process_reaped"], true);
    let status: String = sqlx::query_scalar("SELECT status FROM kanban_cards WHERE id='card'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "canceled");
    drop(engine);
    pool.close().await;
    db.drop().await;
}
