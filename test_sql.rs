use sqlx::{PgPool, Postgres, QueryBuilder};
fn main() {
    let sql = "WITH candidate_ids AS (
            SELECT dispatch_id FROM (
                SELECT SUBSTRING(key FROM LENGTH('dispatch_reserving:') + 1) AS dispatch_id
                  FROM kv_meta
                 WHERE key > 'dispatch_reserving:' || $1
                   AND key LIKE 'dispatch\\_reserving:%' ESCAPE '\\'
                 ORDER BY key
                 LIMIT $2
            ) r
            UNION
            SELECT dispatch_id FROM (
                SELECT SUBSTRING(key FROM LENGTH('dispatch_notified:') + 1) AS dispatch_id
                  FROM kv_meta
                 WHERE key > 'dispatch_notified:' || $1
                   AND key LIKE 'dispatch\\_notified:%' ESCAPE '\\'
                 ORDER BY key
                 LIMIT $2
            ) n
            ORDER BY dispatch_id
            LIMIT $2
        ),
        grouped AS (
            SELECT c.dispatch_id,
                   (SELECT COUNT(*) FROM kv_meta WHERE key = 'dispatch_reserving:' || c.dispatch_id)::BIGINT AS reserving_count,
                   (SELECT COUNT(*) FROM kv_meta WHERE key = 'dispatch_notified:' || c.dispatch_id)::BIGINT AS notified_count
              FROM candidate_ids c
        )
        SELECT grouped.dispatch_id,
               grouped.reserving_count,
               grouped.notified_count,
               latest.status AS typed_status
          FROM grouped
          LEFT JOIN LATERAL (
              SELECT events.status
                FROM dispatch_delivery_events events
               WHERE events.dispatch_id = grouped.dispatch_id
                 AND events.operation = 'send'
                 AND events.target_kind = 'channel'
               ORDER BY events.updated_at DESC, events.id DESC
               LIMIT 1
          ) latest ON TRUE
         ORDER BY grouped.dispatch_id";
    // just to check syntax
}
