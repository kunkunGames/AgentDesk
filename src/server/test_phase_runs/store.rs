//! Transaction primitives shared by all phase-run writers.
use super::*;

pub(super) fn prepare(request: &TestPhaseRunRequest) -> Result<TestPhaseRunRequest, String> {
    let mut request = request.clone();
    request.phase_key = normalize_required("phase_key", &request.phase_key)?;
    request.head_sha = normalize_required("head_sha", &request.head_sha)?;
    request.status = Some(normalize_status(
        request.status.as_deref().unwrap_or("queued"),
    )?);
    request.idempotency_key = Some(match request.idempotency_key.as_deref() {
        Some(key) => normalize_required("idempotency_key", key)?,
        None => deterministic_idempotency_key(&request.phase_key, &request.head_sha),
    });
    Ok(request)
}

pub(super) async fn lock<'a>(
    pool: &'a PgPool,
    key: &str,
) -> Result<sqlx::Transaction<'a, sqlx::Postgres>, String> {
    let mut tx = pool.begin().await.map_err(|error| error.to_string())?;
    // Covers absent rows as well as existing runs. Every lifecycle writer uses
    // this order: run key, run row, resource row. No subprocess runs under it.
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtext('agentdesk.test_phase_run.v1'), hashtext($1))",
    )
    .bind(key)
    .execute(&mut *tx)
    .await
    .map_err(|error| error.to_string())?;
    Ok(tx)
}

pub(super) async fn current(
    connection: &mut sqlx::PgConnection,
    key: &str,
) -> Result<Option<TestPhaseRun>, String> {
    let row = sqlx::query("SELECT * FROM test_phase_runs WHERE idempotency_key=$1 FOR UPDATE")
        .bind(key)
        .fetch_optional(connection)
        .await
        .map_err(|error| error.to_string())?;
    row.map(run_from_row).transpose()
}

pub(super) fn check_identity(
    run: &TestPhaseRun,
    request: &TestPhaseRunRequest,
) -> Result<(), String> {
    check_candidate(run, request)?;
    check_scope(run, request)
}

pub(super) fn check_candidate(
    run: &TestPhaseRun,
    request: &TestPhaseRunRequest,
) -> Result<(), String> {
    if run.phase_key != request.phase_key || run.head_sha != request.head_sha {
        return Err(
            "test phase idempotency key is already bound to another phase/candidate".into(),
        );
    }
    Ok(())
}

fn check_scope(run: &TestPhaseRun, request: &TestPhaseRunRequest) -> Result<(), String> {
    for (field, actual, expected) in [
        ("issue_id", &request.issue_id, &run.issue_id),
        ("card_id", &request.card_id, &run.card_id),
        ("repo_id", &request.repo_id, &run.repo_id),
    ] {
        if let Some(actual) = clean_optional(actual.as_deref())
            && Some(&actual) != expected.as_ref()
        {
            return Err(format!(
                "test phase idempotency key has a different {field}"
            ));
        }
    }
    if request
        .required_capabilities
        .as_ref()
        .is_some_and(|value| value != &run.required_capabilities)
    {
        return Err("test phase idempotency key has different required capabilities".into());
    }
    Ok(())
}

pub(super) fn owned(run: &TestPhaseRun) -> bool {
    run.resource_lock_key.is_some()
        || run.holder_instance_id.is_some()
        || run.holder_job_id.is_some()
}

pub(super) async fn persist(
    connection: &mut sqlx::PgConnection,
    request: &TestPhaseRunRequest,
) -> Result<TestPhaseRun, String> {
    let phase_key = &request.phase_key;
    let head_sha = &request.head_sha;
    let status = request.status.as_deref().ok_or("missing prepared status")?;
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .ok_or("missing prepared idempotency key")?;
    let run_id = format!("tpr-{}", uuid::Uuid::new_v4());
    let terminal = is_terminal_status(status);
    let required_capabilities = request
        .required_capabilities
        .clone()
        .unwrap_or_else(|| json!({}));
    let evidence = request.evidence.clone().unwrap_or_else(|| json!({}));

    let row = sqlx::query(
        r#"
        INSERT INTO test_phase_runs (
            id, idempotency_key, phase_key, head_sha, status, issue_id, card_id,
            repo_id, required_capabilities, resource_lock_key, holder_instance_id,
            holder_job_id, evidence, error, started_at, completed_at, created_at, updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
            CASE WHEN $5 = 'running' THEN NOW() ELSE NULL END,
            CASE WHEN $15 THEN NOW() ELSE NULL END,
            NOW(), NOW()
        )
        ON CONFLICT (idempotency_key) DO UPDATE
        SET status = EXCLUDED.status,
            issue_id = COALESCE(EXCLUDED.issue_id, test_phase_runs.issue_id),
            card_id = COALESCE(EXCLUDED.card_id, test_phase_runs.card_id),
            repo_id = COALESCE(EXCLUDED.repo_id, test_phase_runs.repo_id),
            required_capabilities = EXCLUDED.required_capabilities,
            resource_lock_key = COALESCE(EXCLUDED.resource_lock_key, test_phase_runs.resource_lock_key),
            holder_instance_id = COALESCE(EXCLUDED.holder_instance_id, test_phase_runs.holder_instance_id),
            holder_job_id = COALESCE(EXCLUDED.holder_job_id, test_phase_runs.holder_job_id),
            evidence = EXCLUDED.evidence,
            error = EXCLUDED.error,
            started_at = COALESCE(test_phase_runs.started_at, EXCLUDED.started_at),
            completed_at = CASE WHEN $15 THEN NOW() ELSE NULL END,
            updated_at = NOW()
        RETURNING id, idempotency_key, phase_key, head_sha, status, issue_id,
                  card_id, repo_id, required_capabilities, resource_lock_key,
                  holder_instance_id, holder_job_id, evidence, error, started_at,
                  completed_at, created_at, updated_at
        "#,
    )
    .bind(run_id)
    .bind(idempotency_key)
    .bind(phase_key)
    .bind(head_sha)
    .bind(status)
    .bind(clean_optional(request.issue_id.as_deref()))
    .bind(clean_optional(request.card_id.as_deref()))
    .bind(clean_optional(request.repo_id.as_deref()))
    .bind(required_capabilities)
    .bind(clean_optional(request.resource_lock_key.as_deref()))
    .bind(clean_optional(request.holder_instance_id.as_deref()))
    .bind(clean_optional(request.holder_job_id.as_deref()))
    .bind(evidence)
    .bind(clean_optional(request.error.as_deref()))
    .bind(terminal)
    .fetch_one(connection)
    .await
    .map_err(|error| format!("upsert test phase run: {error}"))?;

    run_from_row(row)
}
