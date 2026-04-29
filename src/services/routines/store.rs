use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use std::sync::Arc;
use uuid::Uuid;

const RUN_LEASE_SECS: i64 = 30 * 60;

/// Durable PG-backed store for routines and routine_runs.
///
/// All mutating operations are transaction-scoped. Callers never hold a
/// connection across JS execution — claim and finish are always separate
/// transactions (see M-1 in PRD review notes).
#[derive(Clone)]
pub struct RoutineStore {
    pool: Arc<PgPool>,
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ClaimedRoutineRun {
    pub run_id: String,
    pub routine_id: String,
    pub agent_id: Option<String>,
    pub script_ref: String,
    pub name: String,
    pub execution_strategy: String,
    pub checkpoint: Option<Value>,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct RoutineClaimCandidate {
    id: String,
    agent_id: Option<String>,
    script_ref: String,
    name: String,
    execution_strategy: String,
    checkpoint: Option<Value>,
}

impl RoutineStore {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// Claim due routines in a short transaction.
    ///
    /// This only creates `routine_runs` rows and marks parent routines
    /// in-flight. JS execution and finish/fail handling must happen after this
    /// transaction commits.
    pub async fn claim_due_runs(&self, limit: u32) -> Result<Vec<ClaimedRoutineRun>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut tx = self.pool.begin().await?;
        let candidates: Vec<RoutineClaimCandidate> = sqlx::query_as(
            r#"
            SELECT id, agent_id, script_ref, name, execution_strategy, checkpoint
            FROM routines
            WHERE status = 'enabled'
              AND next_due_at IS NOT NULL
              AND next_due_at <= NOW()
              AND in_flight_run_id IS NULL
            ORDER BY next_due_at ASC, created_at ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| anyhow!("claim due routines: select candidates: {e}"))?;

        let mut claimed = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            claimed.push(Self::insert_running_run(&mut tx, candidate).await?);
        }

        tx.commit().await?;
        Ok(claimed)
    }

    /// Claim one enabled routine immediately, independent of its schedule.
    pub async fn claim_run_now(&self, routine_id: &str) -> Result<Option<ClaimedRoutineRun>> {
        let mut tx = self.pool.begin().await?;
        let candidate: Option<RoutineClaimCandidate> = sqlx::query_as(
            r#"
            SELECT id, agent_id, script_ref, name, execution_strategy, checkpoint
            FROM routines
            WHERE id = $1
              AND status = 'enabled'
              AND in_flight_run_id IS NULL
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(routine_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| anyhow!("claim run-now routine {routine_id}: select candidate: {e}"))?;

        let claimed = match candidate {
            Some(candidate) => Some(Self::insert_running_run(&mut tx, candidate).await?),
            None => None,
        };

        tx.commit().await?;
        Ok(claimed)
    }

    pub async fn finish_run(
        &self,
        run_id: &str,
        result_json: Option<Value>,
        checkpoint: Option<Value>,
        last_result: Option<&str>,
        next_due_at: Option<DateTime<Utc>>,
    ) -> Result<bool> {
        self.close_run(
            run_id,
            CloseRun {
                run_status: "succeeded",
                action: Some("complete"),
                result_json,
                error: None,
                checkpoint,
                last_result,
                next_due_at,
                pause_routine: false,
            },
        )
        .await
    }

    pub async fn skip_run(
        &self,
        run_id: &str,
        result_json: Option<Value>,
        checkpoint: Option<Value>,
        last_result: Option<&str>,
        next_due_at: Option<DateTime<Utc>>,
    ) -> Result<bool> {
        self.close_run(
            run_id,
            CloseRun {
                run_status: "skipped",
                action: Some("skip"),
                result_json,
                error: None,
                checkpoint,
                last_result,
                next_due_at,
                pause_routine: false,
            },
        )
        .await
    }

    pub async fn pause_after_run(
        &self,
        run_id: &str,
        result_json: Option<Value>,
        checkpoint: Option<Value>,
        last_result: Option<&str>,
    ) -> Result<bool> {
        self.close_run(
            run_id,
            CloseRun {
                run_status: "paused",
                action: Some("pause"),
                result_json,
                error: None,
                checkpoint,
                last_result,
                next_due_at: None,
                pause_routine: true,
            },
        )
        .await
    }

    pub async fn fail_run(
        &self,
        run_id: &str,
        error: &str,
        result_json: Option<Value>,
        next_due_at: Option<DateTime<Utc>>,
    ) -> Result<bool> {
        self.close_run(
            run_id,
            CloseRun {
                run_status: "failed",
                action: None,
                result_json,
                error: Some(error),
                checkpoint: None,
                last_result: Some(error),
                next_due_at,
                pause_routine: false,
            },
        )
        .await
    }

    pub async fn pause_routine(&self, routine_id: &str) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE routines
            SET status = 'paused',
                next_due_at = NULL,
                updated_at = NOW()
            WHERE id = $1
              AND status = 'enabled'
            "#,
        )
        .bind(routine_id)
        .execute(&*self.pool)
        .await
        .map_err(|e| anyhow!("pause routine {routine_id}: {e}"))?;

        Ok(result.rows_affected() == 1)
    }

    /// Resume a paused routine. `next_due_at` is authoritative; pass `None`
    /// for manual-only routines.
    pub async fn resume_routine(
        &self,
        routine_id: &str,
        next_due_at: Option<DateTime<Utc>>,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE routines
            SET status = 'enabled',
                next_due_at = $2,
                updated_at = NOW()
            WHERE id = $1
              AND status = 'paused'
            "#,
        )
        .bind(routine_id)
        .bind(next_due_at)
        .execute(&*self.pool)
        .await
        .map_err(|e| anyhow!("resume routine {routine_id}: {e}"))?;

        Ok(result.rows_affected() == 1)
    }

    /// Extend the lease for a running routine run.
    ///
    /// Executors must call this periodically while JS execution is active.
    /// Boot recovery only interrupts rows whose lease has expired.
    pub async fn heartbeat_run(&self, run_id: &str) -> Result<bool> {
        let lease_expires_at = Self::new_lease_expires_at();
        let result = sqlx::query(
            r#"
            UPDATE routine_runs
            SET lease_expires_at = $2,
                updated_at = NOW()
            WHERE id = $1
              AND status = 'running'
            "#,
        )
        .bind(run_id)
        .bind(lease_expires_at)
        .execute(&*self.pool)
        .await
        .map_err(|e| anyhow!("heartbeat routine run {run_id}: {e}"))?;

        Ok(result.rows_affected() == 1)
    }

    /// Boot recovery: mark expired-lease `running` runs as `interrupted`, clear
    /// `in_flight_run_id` on their parent routines. Called once at worker
    /// startup before the tick loop begins. Running rows without an expired
    /// lease are left alone so a second server instance cannot interrupt work
    /// that another instance is actively executing.
    ///
    /// Returns the number of expired-lease runs that were recovered.
    pub async fn recover_stale_running_runs(&self) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        let now = Utc::now();

        // Close expired leases. The UPDATE re-checks status and lease expiry
        // under the row lock so a concurrently finished run is not clobbered.
        let recovered: Vec<(String, String)> = sqlx::query_as(
            r#"
            WITH expired AS (
                SELECT id
                FROM routine_runs
                WHERE status = 'running'
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < $1
                FOR UPDATE SKIP LOCKED
            ),
            closed AS (
                UPDATE routine_runs AS rr
                SET status = 'interrupted',
                    finished_at = NOW(),
                    updated_at = NOW(),
                    lease_expires_at = NULL,
                    error = 'interrupted by expired routine lease'
                FROM expired
                WHERE rr.id = expired.id
                  AND rr.status = 'running'
                  AND rr.lease_expires_at IS NOT NULL
                  AND rr.lease_expires_at < $1
                RETURNING rr.id, rr.routine_id
            )
            SELECT id, routine_id FROM closed
            "#,
        )
        .bind(now)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| anyhow!("recover: close expired routine leases: {e}"))?;

        if recovered.is_empty() {
            tx.commit().await?;
            return Ok(0);
        }

        let count = recovered.len() as u64;
        let recovered_routine_ids: Vec<&str> =
            recovered.iter().map(|(_, rid)| rid.as_str()).collect();
        let recovered_run_ids: Vec<&str> = recovered.iter().map(|(id, _)| id.as_str()).collect();

        // Release only locks that still point at the interrupted run.
        sqlx::query(
            r#"
            UPDATE routines AS r
            SET in_flight_run_id = NULL,
                updated_at = NOW()
            FROM UNNEST($1::text[], $2::text[]) AS recovered(routine_id, run_id)
            WHERE r.id = recovered.routine_id
              AND r.in_flight_run_id = recovered.run_id
            "#,
        )
        .bind(&recovered_routine_ids)
        .bind(&recovered_run_ids)
        .execute(&mut *tx)
        .await
        .map_err(|e| anyhow!("recover: clear in_flight_run_id: {e}"))?;

        tx.commit().await?;
        Ok(count)
    }

    async fn insert_running_run(
        tx: &mut Transaction<'_, Postgres>,
        candidate: RoutineClaimCandidate,
    ) -> Result<ClaimedRoutineRun> {
        let run_id = Uuid::new_v4().to_string();
        let lease_expires_at = Self::new_lease_expires_at();

        sqlx::query(
            r#"
            INSERT INTO routine_runs (id, routine_id, status, lease_expires_at)
            VALUES ($1, $2, 'running', $3)
            "#,
        )
        .bind(&run_id)
        .bind(&candidate.id)
        .bind(lease_expires_at)
        .execute(&mut **tx)
        .await
        .map_err(|e| anyhow!("claim routine {}: insert running run: {e}", candidate.id))?;

        let updated = sqlx::query(
            r#"
            UPDATE routines
            SET in_flight_run_id = $1,
                last_run_at = NOW(),
                updated_at = NOW()
            WHERE id = $2
              AND in_flight_run_id IS NULL
            "#,
        )
        .bind(&run_id)
        .bind(&candidate.id)
        .execute(&mut **tx)
        .await
        .map_err(|e| anyhow!("claim routine {}: mark in-flight: {e}", candidate.id))?;

        if updated.rows_affected() != 1 {
            return Err(anyhow!(
                "claim routine {}: in-flight guard rejected locked candidate",
                candidate.id
            ));
        }

        Ok(ClaimedRoutineRun {
            run_id,
            routine_id: candidate.id,
            agent_id: candidate.agent_id,
            script_ref: candidate.script_ref,
            name: candidate.name,
            execution_strategy: candidate.execution_strategy,
            checkpoint: candidate.checkpoint,
            lease_expires_at,
        })
    }

    fn new_lease_expires_at() -> DateTime<Utc> {
        Utc::now() + chrono::Duration::seconds(RUN_LEASE_SECS)
    }

    async fn close_run(&self, run_id: &str, close: CloseRun<'_>) -> Result<bool> {
        let mut tx = self.pool.begin().await?;

        let routine_id: Option<String> = sqlx::query_scalar(
            r#"
            SELECT routine_id
            FROM routine_runs
            WHERE id = $1
              AND status = 'running'
            FOR UPDATE
            "#,
        )
        .bind(run_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| anyhow!("close run {run_id}: lock running run: {e}"))?;

        let Some(routine_id) = routine_id else {
            tx.commit().await?;
            return Ok(false);
        };

        let routine_updated = sqlx::query(
            r#"
            UPDATE routines
            SET in_flight_run_id = NULL,
                status = CASE WHEN $5 THEN 'paused' ELSE status END,
                next_due_at = $2,
                checkpoint = COALESCE($3, checkpoint),
                last_result = $4,
                updated_at = NOW()
            WHERE id = $1
              AND in_flight_run_id = $6
            "#,
        )
        .bind(&routine_id)
        .bind(close.next_due_at)
        .bind(&close.checkpoint)
        .bind(close.last_result)
        .bind(close.pause_routine)
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| anyhow!("close run {run_id}: update routine {routine_id}: {e}"))?;

        if routine_updated.rows_affected() != 1 {
            tx.commit().await?;
            return Ok(false);
        }

        let run_updated = sqlx::query(
            r#"
            UPDATE routine_runs
            SET status = $2,
                action = $3,
                result_json = $4,
                error = $5,
                finished_at = NOW(),
                lease_expires_at = NULL,
                updated_at = NOW()
            WHERE id = $1
              AND status = 'running'
            "#,
        )
        .bind(run_id)
        .bind(close.run_status)
        .bind(close.action)
        .bind(&close.result_json)
        .bind(close.error)
        .execute(&mut *tx)
        .await
        .map_err(|e| anyhow!("close run {run_id}: update run: {e}"))?;

        if run_updated.rows_affected() != 1 {
            return Err(anyhow!("close run {run_id}: running run guard lost row"));
        }

        tx.commit().await?;
        Ok(true)
    }
}

struct CloseRun<'a> {
    run_status: &'a str,
    action: Option<&'a str>,
    result_json: Option<Value>,
    error: Option<&'a str>,
    checkpoint: Option<Value>,
    last_result: Option<&'a str>,
    next_due_at: Option<DateTime<Utc>>,
    pause_routine: bool,
}

#[cfg(test)]
mod tests {
    // Integration tests that require a live PG connection live in
    // src/integration_tests.rs and are gated on the `integration` feature.
    // The store SQL is compiled by `cargo check`; concurrent claim/recovery
    // behavior should be covered by PG integration tests once the runtime
    // harness starts executing routines.
}
