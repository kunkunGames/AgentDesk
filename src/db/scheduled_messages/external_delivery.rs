//! Rolling-upgrade capability gates for scheduled provider fan-out.

use sqlx::{PgPool, Postgres, Transaction};

/// True when every online cluster node advertises the consumer contract that
/// atomically hands provider-targeted push fires to both outboxes.
pub async fn external_delivery_rollout_ready_pg(pool: &PgPool) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT NOT EXISTS (\
             SELECT 1 FROM worker_nodes \
             WHERE status = 'online' \
               AND COALESCE(\
                   capabilities #>> '{scheduled_messages,external_delivery_consumer_v1}', \
                   'false'\
               ) <> 'true'\
         )",
    )
    .fetch_one(pool)
    .await
}

/// Declare that this transaction understands provider-targeted scheduled push
/// fan-out. Migration 0108 rejects provider-plan claims from older binaries.
pub(super) async fn declare_external_delivery_consumer_v1_tx(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT set_config(\
            'agentdesk.scheduled_external_delivery_consumer_v1', \
            'enabled', \
            true\
         )",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}
