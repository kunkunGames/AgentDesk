//! A short persisted resource reservation fences writers while inputs are
//! copied. File I/O runs outside every PostgreSQL transaction.
use super::{snapshot, store};
use crate::server::resource_locks::{self, ResourceLockRequest};
use sqlx::PgPool;

pub(super) async fn freeze(
    pool: &PgPool,
    work: &store::Work,
) -> anyhow::Result<snapshot::Candidate> {
    let key = format!(
        "managed-capture:{}",
        work.contract.template.workspace.display()
    );
    let job = uuid::Uuid::new_v4().to_string();
    let acquired = resource_locks::acquire_resource_lock(
        pool,
        &ResourceLockRequest {
            lock_key: key.clone(),
            holder_instance_id: "protected-command-v1".into(),
            holder_job_id: job.clone(),
            ttl_secs: Some(60),
            metadata: None,
        },
    )
    .await
    .map_err(anyhow::Error::msg)?;
    anyhow::ensure!(acquired.acquired, "candidate capture is already owned");
    let contract = work.contract.clone();
    let result = async {
        anyhow::ensure!(store::current(pool, work).await?, "candidate attempt changed before capture");
        let candidate = tokio::task::spawn_blocking(move || snapshot::freeze(&contract))
            .await?.map_err(anyhow::Error::msg)?;
        let live: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM resource_locks WHERE lock_key=$1 AND holder_job_id=$2 AND expires_at>clock_timestamp())")
            .bind(&key).bind(&job).fetch_one(pool).await?;
        anyhow::ensure!(live && store::current(pool, work).await?, "candidate capture lease or attempt changed");
        Ok(candidate)
    }.await;
    resource_locks::release_resource_lock(pool, &key, "protected-command-v1", &job)
        .await
        .map_err(anyhow::Error::msg)?;
    result
}
