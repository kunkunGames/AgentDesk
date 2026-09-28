use super::*;

#[cfg(test)]
pub(super) async fn acquire(
    pool: &PgPool,
    node: &str,
    provider: &str,
    channel: &str,
    nonce: uuid::Uuid,
) -> Result<(), sqlx::Error> {
    acquire_scoped(pool, node, provider, channel, nonce, None)
        .await
        .map(|_| ())
}

pub(super) async fn acquire_scoped(
    pool: &PgPool,
    node: &str,
    provider: &str,
    channel: &str,
    nonce: uuid::Uuid,
    admission: Option<&LeaseAdmission>,
) -> Result<Option<std::time::Instant>, sqlx::Error> {
    // Never subtract the worker's wall clock from a database timestamp. The
    // clocks may differ. Start before I/O and use the DB's remaining interval;
    // transaction/transport latency can only shorten the local hard deadline.
    let started = std::time::Instant::now();
    let mut tx = pool.begin().await?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtext('agentdesk.execution_capacity.v1'),hashtext($1))",
    )
    .bind(node)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "DELETE FROM node_execution_leases WHERE instance_id=$1 AND NOT managed AND expires_at<=clock_timestamp()",
    )
    .bind(node)
    .execute(&mut *tx)
    .await?;
    let limits = admission.and_then(|a| a.limits.as_ref());
    let remaining: Option<f64> = sqlx::query_scalar("INSERT INTO node_execution_leases(instance_id,provider,channel_id,nonce,expires_at,resource_key,managed,deadline_at,max_tool_calls) VALUES($1,$2,$3,$4,clock_timestamp()+($5*INTERVAL '1 second'),$6,$7,clock_timestamp()+($8*INTERVAL '1 second'),$9) RETURNING EXTRACT(EPOCH FROM (deadline_at-clock_timestamp()))::float8")
        .bind(node).bind(provider).bind(channel).bind(nonce).bind(LEASE_SECONDS)
        .bind(admission.map(|a| &a.resource_key)).bind(limits.is_some())
        .bind(limits.map(|v| v.max_elapsed_secs as i64)).bind(limits.map(|v| v.max_tool_calls as i32))
        .fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(remaining.map(|seconds| started + Duration::from_secs_f64(seconds.max(0.0))))
}

pub(super) async fn check_retained(
    pool: &PgPool,
    node: &str,
    channel: &str,
    resource: Option<&str>,
) -> Result<(), String> {
    let occupied: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_execution_leases WHERE instance_id=$1 AND managed AND (channel_id=$2 OR resource_key=$3))")
        .bind(node).bind(channel).bind(resource).fetch_one(pool).await
        .map_err(|error| format!("retained execution lookup: {error}"))?;
    if occupied {
        Err(
            "managed execution remains occupied; explicit quiescence reconciliation is required"
                .into(),
        )
    } else {
        Ok(())
    }
}

pub(super) async fn renew(
    pool: &PgPool,
    node: &str,
    provider: &str,
    channel: &str,
    nonce: uuid::Uuid,
) -> Result<bool, sqlx::Error> {
    sqlx::query("UPDATE node_execution_leases SET expires_at=clock_timestamp()+($5*INTERVAL '1 second') WHERE instance_id=$1 AND provider=$2 AND channel_id=$3 AND nonce=$4 AND expires_at>clock_timestamp()")
        .bind(node).bind(provider).bind(channel).bind(nonce).bind(LEASE_SECONDS).execute(pool).await.map(|r|r.rows_affected()==1)
}

pub(super) async fn release(
    pool: &PgPool,
    node: &str,
    provider: &str,
    channel: &str,
    nonce: uuid::Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM node_execution_leases WHERE instance_id=$1 AND provider=$2 AND channel_id=$3 AND nonce=$4")
        .bind(node).bind(provider).bind(channel).bind(nonce).execute(pool).await?;
    Ok(())
}
