//! Phase-run ownership is committed with its resource lease. A late collector
//! cannot replace the current result, and redelivery never starts work twice.
use super::*;
use crate::server::resource_locks::{self, ResourceLockRequest};

pub(super) async fn upsert(
    pool: &PgPool,
    request: &TestPhaseRunRequest,
) -> Result<TestPhaseRun, String> {
    let request = store::prepare(request)?;
    let key = request
        .idempotency_key
        .as_deref()
        .ok_or("missing prepared key")?;
    let mut tx = store::lock(pool, key).await?;
    if let Some(run) = store::current(&mut tx, key).await? {
        store::check_candidate(&run, &request)?;
        if store::owned(&run) {
            return Err("owned test phase runs require the start/complete lifecycle".into());
        }
    }
    // Generic imports remain available for legacy, unowned evidence. Owning a
    // resource requires start(), so imports cannot manufacture collector rights.
    if request.resource_lock_key.is_some()
        || request.holder_instance_id.is_some()
        || request.holder_job_id.is_some()
    {
        return Err("resource ownership requires starting the test phase run".into());
    }
    let run = store::persist(&mut tx, &request).await?;
    tx.commit().await.map_err(|error| error.to_string())?;
    Ok(run)
}

pub(super) async fn start(
    pool: &PgPool,
    request: &TestPhaseRunStartRequest,
) -> Result<TestPhaseRunStartOutcome, String> {
    let mut run_request = request.run.clone();
    run_request.status = Some("running".into());
    run_request.resource_lock_key = Some(normalize_required(
        "resource_lock_key",
        &request.resource_lock_key,
    )?);
    run_request.holder_instance_id = Some(normalize_required(
        "holder_instance_id",
        &request.holder_instance_id,
    )?);
    run_request.holder_job_id = Some(normalize_required("holder_job_id", &request.holder_job_id)?);
    run_request.evidence = Some(json!({}));
    run_request.error = None;
    let mut run_request = store::prepare(&run_request)?;
    let key = run_request
        .idempotency_key
        .clone()
        .ok_or("missing prepared key")?;
    let mut tx = store::lock(pool, &key).await?;
    if let Some(run) = store::current(&mut tx, &key).await? {
        store::check_identity(&run, &run_request)?;
        if run.status != "queued" {
            // Running/terminal deliveries return the existing execution. A new
            // attempt needs a new key after the caller reconciles the old one.
            return Ok(TestPhaseRunStartOutcome {
                started: false,
                run: Some(run),
                lock: None,
                current_lock: None,
            });
        }
        if store::owned(&run) {
            return Err("queued test phase run has conflicting resource ownership".into());
        }
        if run_request.required_capabilities.is_none() {
            run_request.required_capabilities = Some(run.required_capabilities);
        }
    }
    let lock_request = ResourceLockRequest {
        lock_key: run_request
            .resource_lock_key
            .clone()
            .ok_or("missing resource lock")?,
        holder_instance_id: run_request
            .holder_instance_id
            .clone()
            .ok_or("missing instance")?,
        holder_job_id: run_request.holder_job_id.clone().ok_or("missing job")?,
        ttl_secs: request.ttl_secs,
        metadata: Some(json!({
            "phase_key": run_request.phase_key,
            "head_sha": run_request.head_sha,
            "idempotency_key": key,
        })),
    };
    let lock = resource_locks::acquire_resource_lock_on(&mut tx, &lock_request).await?;
    if !lock.acquired {
        return Ok(TestPhaseRunStartOutcome {
            started: false,
            run: None,
            lock: None,
            current_lock: lock.current,
        });
    }
    // The same resource/owner token cannot label two different executions,
    // including after expiry. Late results must always identify one attempt.
    let reused: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM test_phase_runs WHERE resource_lock_key=$1 AND holder_instance_id=$2 AND holder_job_id=$3 AND idempotency_key<>$4)")
        .bind(&lock_request.lock_key).bind(&lock_request.holder_instance_id)
        .bind(&lock_request.holder_job_id).bind(&key).fetch_one(&mut *tx).await
        .map_err(|error| error.to_string())?;
    if reused {
        return Err("test phase holder_job_id must identify a fresh execution".into());
    }
    let run = store::persist(&mut tx, &run_request).await?;
    tx.commit().await.map_err(|error| error.to_string())?;
    Ok(TestPhaseRunStartOutcome {
        started: true,
        run: Some(run),
        lock: lock.lock,
        current_lock: None,
    })
}

pub(super) async fn complete(
    pool: &PgPool,
    request: &TestPhaseRunCompleteRequest,
) -> Result<TestPhaseRunCompleteOutcome, String> {
    let run_request = store::prepare(&request.run)?;
    if !is_terminal_status(run_request.status.as_deref().unwrap_or("")) {
        return Err("complete requires status passed, failed, or canceled".into());
    }
    let key = run_request
        .idempotency_key
        .as_deref()
        .ok_or("missing prepared key")?;
    let mut tx = store::lock(pool, key).await?;
    let previous = store::current(&mut tx, key).await?;
    let owned = previous.as_ref().is_some_and(store::owned);
    if let Some(run) = &previous {
        if owned {
            store::check_identity(run, &run_request)?;
        } else {
            store::check_candidate(run, &run_request)?;
        }
    }
    if !owned {
        // Preserve the existing import contract for unowned legacy results.
        // Such records are not authenticated managed check evidence.
        if run_request.resource_lock_key.is_some()
            || run_request.holder_instance_id.is_some()
            || run_request.holder_job_id.is_some()
        {
            return Err("test phase execution must be started before completion".into());
        }
        let run = store::persist(&mut tx, &run_request).await?;
        tx.commit().await.map_err(|error| error.to_string())?;
        return Ok(TestPhaseRunCompleteOutcome {
            run,
            lock_released: request.release_lock.then_some(false),
        });
    }
    let previous = previous.ok_or("owned test phase run is missing")?;
    for (field, actual, expected) in [
        (
            "resource_lock_key",
            &run_request.resource_lock_key,
            &previous.resource_lock_key,
        ),
        (
            "holder_instance_id",
            &run_request.holder_instance_id,
            &previous.holder_instance_id,
        ),
        (
            "holder_job_id",
            &run_request.holder_job_id,
            &previous.holder_job_id,
        ),
    ] {
        if actual.is_none() || clean_optional(actual.as_deref()) != *expected {
            return Err(format!(
                "test phase completion has a stale or missing {field}"
            ));
        }
    }
    if is_terminal_status(&previous.status) {
        if Some(&previous.status) != run_request.status.as_ref()
            || previous.evidence != run_request.evidence.clone().unwrap_or_else(|| json!({}))
            || previous.error != clean_optional(run_request.error.as_deref())
        {
            return Err("terminal test phase evidence cannot be replaced".into());
        }
        // Release only this exact receipt's resource, never a successor's lock.
        return commit_result(tx, previous, request.release_lock).await;
    }
    if previous.status != "running" {
        return Err("test phase completion requires a running execution".into());
    }
    let current_owner: Option<String> = sqlx::query_scalar("SELECT holder_job_id FROM resource_locks WHERE lock_key=$1 AND holder_instance_id=$2 AND holder_job_id=$3 AND expires_at>clock_timestamp() AND metadata->>'idempotency_key'=$4 FOR UPDATE")
        .bind(&previous.resource_lock_key).bind(&previous.holder_instance_id).bind(&previous.holder_job_id)
        .bind(key).fetch_optional(&mut *tx).await.map_err(|error| error.to_string())?;
    if current_owner.is_none() {
        return Err("test phase execution lease was lost or expired".into());
    }
    // Keep immutable identity/capability fields even when the collector omits
    // optional values. It may submit only the result of its current execution.
    let mut result = run_request;
    result.required_capabilities = Some(previous.required_capabilities);
    let run = store::persist(&mut tx, &result).await?;
    commit_result(tx, run, request.release_lock).await
}

async fn commit_result(
    mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
    run: TestPhaseRun,
    release_lock: bool,
) -> Result<TestPhaseRunCompleteOutcome, String> {
    let lock_released = if release_lock {
        Some(sqlx::query("DELETE FROM resource_locks WHERE lock_key=$1 AND holder_instance_id=$2 AND holder_job_id=$3 AND metadata->>'idempotency_key'=$4")
            .bind(&run.resource_lock_key).bind(&run.holder_instance_id).bind(&run.holder_job_id)
            .bind(&run.idempotency_key).execute(&mut *tx).await.map_err(|error| error.to_string())?.rows_affected() == 1)
    } else {
        None
    };
    tx.commit().await.map_err(|error| error.to_string())?;
    Ok(TestPhaseRunCompleteOutcome { run, lock_released })
}
