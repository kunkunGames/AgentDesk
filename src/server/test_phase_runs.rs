use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

pub(crate) mod checks;
mod lifecycle;
#[cfg(test)]
mod lifecycle_pg_tests;
mod store;

#[derive(Debug, Clone, Serialize)]
pub struct TestPhaseRun {
    pub id: String,
    pub idempotency_key: String,
    pub phase_key: String,
    pub head_sha: String,
    pub status: String,
    pub issue_id: Option<String>,
    pub card_id: Option<String>,
    pub repo_id: Option<String>,
    pub required_capabilities: Value,
    pub resource_lock_key: Option<String>,
    pub holder_instance_id: Option<String>,
    pub holder_job_id: Option<String>,
    pub evidence: Value,
    pub error: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestPhaseRunRequest {
    pub phase_key: String,
    pub head_sha: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub issue_id: Option<String>,
    #[serde(default)]
    pub card_id: Option<String>,
    #[serde(default)]
    pub repo_id: Option<String>,
    #[serde(default)]
    pub required_capabilities: Option<Value>,
    #[serde(default)]
    pub resource_lock_key: Option<String>,
    #[serde(default)]
    pub holder_instance_id: Option<String>,
    #[serde(default)]
    pub holder_job_id: Option<String>,
    #[serde(default)]
    pub evidence: Option<Value>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestPhaseRunListQuery {
    #[serde(default)]
    pub phase_key: Option<String>,
    #[serde(default)]
    pub head_sha: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestPhaseEvidenceQuery {
    pub phase_key: String,
    pub head_sha: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestPhaseRunStartRequest {
    #[serde(flatten)]
    pub run: TestPhaseRunRequest,
    pub resource_lock_key: String,
    pub holder_instance_id: String,
    pub holder_job_id: String,
    #[serde(default)]
    pub ttl_secs: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TestPhaseRunStartOutcome {
    pub started: bool,
    pub run: Option<TestPhaseRun>,
    pub lock: Option<crate::server::resource_locks::ResourceLock>,
    pub current_lock: Option<crate::server::resource_locks::ResourceLock>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestPhaseRunCompleteRequest {
    #[serde(flatten)]
    pub run: TestPhaseRunRequest,
    #[serde(default)]
    pub release_lock: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TestPhaseRunCompleteOutcome {
    pub run: TestPhaseRun,
    pub lock_released: Option<bool>,
}

pub async fn upsert_test_phase_run(
    pool: &PgPool,
    request: &TestPhaseRunRequest,
) -> Result<TestPhaseRun, String> {
    checks::reject_public_write(request)?;
    lifecycle::upsert(pool, request).await
}

pub async fn start_test_phase_run(
    pool: &PgPool,
    request: &TestPhaseRunStartRequest,
) -> Result<TestPhaseRunStartOutcome, String> {
    checks::reject_public_write(&request.run)?;
    lifecycle::start(pool, request).await
}

pub async fn complete_test_phase_run(
    pool: &PgPool,
    request: &TestPhaseRunCompleteRequest,
) -> Result<TestPhaseRunCompleteOutcome, String> {
    checks::reject_public_write(&request.run)?;
    lifecycle::complete(pool, request).await
}

pub async fn list_test_phase_runs(
    pool: &PgPool,
    query: &TestPhaseRunListQuery,
) -> Result<Vec<TestPhaseRun>, String> {
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let phase_key = clean_optional(query.phase_key.as_deref());
    let head_sha = clean_optional(query.head_sha.as_deref());
    let status = query.status.as_deref().map(normalize_status).transpose()?;

    let rows = sqlx::query(
        r#"
        SELECT id, idempotency_key, phase_key, head_sha, status, issue_id,
               card_id, repo_id, required_capabilities, resource_lock_key,
               holder_instance_id, holder_job_id, evidence, error, started_at,
               completed_at, created_at, updated_at
          FROM test_phase_runs
         WHERE ($1::TEXT IS NULL OR phase_key = $1)
           AND ($2::TEXT IS NULL OR head_sha = $2)
           AND ($3::TEXT IS NULL OR status = $3)
         ORDER BY updated_at DESC
         LIMIT $4
        "#,
    )
    .bind(phase_key)
    .bind(head_sha)
    .bind(status)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("list test phase runs: {error}"))?;

    rows.into_iter().map(run_from_row).collect()
}

pub async fn latest_passing_evidence(
    pool: &PgPool,
    phase_key: &str,
    head_sha: &str,
) -> Result<Option<TestPhaseRun>, String> {
    let phase_key = normalize_required("phase_key", phase_key)?;
    let head_sha = normalize_required("head_sha", head_sha)?;
    let row = sqlx::query(
        r#"
        SELECT id, idempotency_key, phase_key, head_sha, status, issue_id,
               card_id, repo_id, required_capabilities, resource_lock_key,
               holder_instance_id, holder_job_id, evidence, error, started_at,
               completed_at, created_at, updated_at
          FROM test_phase_runs
         WHERE phase_key = $1
           AND head_sha = $2
           AND status = 'passed'
         ORDER BY completed_at DESC NULLS LAST, updated_at DESC
         LIMIT 1
        "#,
    )
    .bind(phase_key)
    .bind(head_sha)
    .fetch_optional(pool)
    .await
    .map_err(|error| format!("latest passing test phase evidence: {error}"))?;

    row.map(run_from_row).transpose()
}

fn normalize_required(field: &str, value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{field} is required"));
    }
    if trimmed.len() > 256 {
        return Err(format!("{field} is too long"));
    }
    Ok(trimmed.to_string())
}

fn normalize_status(value: &str) -> Result<String, String> {
    let status = value.trim().to_ascii_lowercase();
    match status.as_str() {
        "queued" | "running" | "passed" | "failed" | "canceled" => Ok(status),
        _ => Err(format!("unsupported test phase status: {value}")),
    }
}

fn is_terminal_status(status: &str) -> bool {
    matches!(status, "passed" | "failed" | "canceled")
}

fn deterministic_idempotency_key(phase_key: &str, head_sha: &str) -> String {
    format!("{phase_key}:{head_sha}")
}

fn clean_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn run_from_row(row: sqlx::postgres::PgRow) -> Result<TestPhaseRun, String> {
    Ok(TestPhaseRun {
        id: row.get("id"),
        idempotency_key: row.get("idempotency_key"),
        phase_key: row.get("phase_key"),
        head_sha: row.get("head_sha"),
        status: row.get("status"),
        issue_id: row.get("issue_id"),
        card_id: row.get("card_id"),
        repo_id: row.get("repo_id"),
        required_capabilities: row.get("required_capabilities"),
        resource_lock_key: row.get("resource_lock_key"),
        holder_instance_id: row.get("holder_instance_id"),
        holder_job_id: row.get("holder_job_id"),
        evidence: row.get("evidence"),
        error: row.get("error"),
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_validation_allows_only_phase_run_states() {
        assert_eq!(normalize_status("PASSED").unwrap(), "passed");
        assert!(normalize_status("skipped").is_err());
    }

    #[test]
    fn default_idempotency_key_is_phase_and_head_sha() {
        assert_eq!(
            deterministic_idempotency_key("unreal-smoke", "abc123"),
            "unreal-smoke:abc123"
        );
    }

    #[test]
    fn terminal_statuses_close_phase_runs() {
        assert!(!is_terminal_status("running"));
        assert!(is_terminal_status("passed"));
        assert!(is_terminal_status("failed"));
        assert!(is_terminal_status("canceled"));
    }
}
