#[cfg(test)]
mod tests {
    use super::super::clear_slot_sessions_pg;
    use crate::db::auto_queue::test_support::TestPostgresDb;

    #[tokio::test]
    async fn graph_runtime_stop_is_scoped_to_cancelled_owned_attempts_pg() {
        let db = TestPostgresDb::create().await;
        let pool = db.connect_and_migrate().await;
        crate::db::campaigns::create(
            &pool,
            "stop-graph".into(),
            serde_json::from_value(
                serde_json::json!({"title":"Stop", "status":"active", "round":1,"nodes":[]}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        sqlx::raw_sql("INSERT INTO agents(id,name,provider) VALUES('stop-agent','Stop','qwen');
            INSERT INTO campaign_execution_specs(campaign_id,spec) VALUES('stop-graph','{}');
            INSERT INTO kanban_cards(id,title,status) VALUES('graph-stop-card','Graph','todo'),('ordinary-stop-card','Ordinary','todo');
            INSERT INTO campaign_graph_tasks(task_key,campaign_id,node_id,role,contract_digest,card_id)
            VALUES('stop-key','stop-graph','plan','plan','digest','graph-stop-card');
            INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,thread_id)
            VALUES('graph-stop','graph-stop-card','stop-agent','implementation','cancelled','invalid-thread'),
                  ('ordinary-stop','ordinary-stop-card','stop-agent','implementation','cancelled','invalid-thread');")
            .execute(&pool).await.unwrap();
        // An invalid locator must be irrelevant for ordinary or unowned work,
        // but must keep a graph cleanup job pending instead of releasing its slot.
        let stop = super::super::stop_cancelled_graph_dispatches_pg;
        stop(None, &pool, &[]).await.unwrap();
        stop(None, &pool, &["ordinary-stop".into(), "unrelated".into()])
            .await
            .unwrap();
        assert!(
            stop(None, &pool, &["graph-stop".into()])
                .await
                .unwrap_err()
                .contains("Invalid campaign dispatch channel")
        );
        sqlx::query("UPDATE task_dispatches SET status='completed' WHERE id='graph-stop'")
            .execute(&pool)
            .await
            .unwrap();
        stop(None, &pool, &["graph-stop".into()]).await.unwrap();
    }

    #[tokio::test]
    async fn clear_slot_sessions_pg_batches_unique_targets_and_preserves_ineligible_rows() {
        let pg_db = TestPostgresDb::create().await;
        let pool = pg_db.connect_and_migrate().await;
        let target_thread_id = 7_001_u64;
        let target_thread_id_text = target_thread_id.to_string();
        let resettable_statuses = ["turn_active", "awaiting_bg", "awaiting_user", "idle"];

        for (index, status) in resettable_statuses.iter().enumerate() {
            sqlx::query(
                "INSERT INTO sessions (
                 session_key,
                 provider,
                 status,
                 active_dispatch_id,
                 session_info,
                 tokens,
                 thread_channel_id,
                 claude_session_id
             )
             VALUES ($1, 'claude', $2, $3, 'before reset', 41, $4, $5)",
            )
            .bind(format!("clear-slot-batch-{index}"))
            .bind(status)
            .bind(format!("dispatch-{index}"))
            .bind(&target_thread_id_text)
            .bind(format!("claude-session-{index}"))
            .execute(&pool)
            .await
            .expect("seed resettable slot session"); // agentdesk-audit: allow-unwrap — test-only PostgreSQL fixture
        }

        sqlx::query(
            "INSERT INTO sessions (
             session_key, provider, status, active_dispatch_id, session_info, tokens,
             thread_channel_id, claude_session_id
         )
         VALUES
             ('clear-slot-disconnected', 'claude', 'disconnected', 'keep-dispatch',
              'keep disconnected', 73, $1, 'keep-claude-session'),
             ('clear-slot-unrelated', 'claude', 'turn_active', 'unrelated-dispatch',
              'keep unrelated', 89, '7002', 'unrelated-claude-session')",
        )
        .bind(&target_thread_id_text)
        .execute(&pool)
        .await
        .expect("seed preserved slot sessions"); // agentdesk-audit: allow-unwrap — test-only PostgreSQL fixture

        assert_eq!(
            clear_slot_sessions_pg(&pool, &[])
                .await
                .expect("skip an empty slot-session batch"),
            0
        );
        assert_eq!(
            clear_slot_sessions_pg(&pool, &[target_thread_id, target_thread_id])
                .await
                .expect("clear a deduplicated slot-session batch"),
            resettable_statuses.len()
        );

        let reset_rows: Vec<(String, Option<String>, Option<String>, i64, Option<String>)> =
            sqlx::query_as(
                "SELECT status, active_dispatch_id, session_info, tokens, claude_session_id
         FROM sessions
         WHERE thread_channel_id = $1
           AND session_key LIKE 'clear-slot-batch-%'
         ORDER BY session_key",
            )
            .bind(&target_thread_id_text)
            .fetch_all(&pool)
            .await
            .expect("load reset slot sessions"); // agentdesk-audit: allow-unwrap — test-only PostgreSQL assertion
        assert_eq!(reset_rows.len(), resettable_statuses.len());
        assert!(reset_rows.iter().all(|row| {
            row == &(
                "idle".to_string(),
                None,
                Some("Slot thread reset".to_string()),
                0,
                None,
            )
        }));

        let preserved_rows: Vec<(
            String,
            String,
            Option<String>,
            Option<String>,
            i64,
            Option<String>,
        )> = sqlx::query_as(
            "SELECT session_key, status, active_dispatch_id, session_info, tokens, claude_session_id
         FROM sessions
         WHERE session_key IN ('clear-slot-disconnected', 'clear-slot-unrelated')
         ORDER BY session_key",
        )
        .fetch_all(&pool)
        .await
        .expect("load preserved slot sessions"); // agentdesk-audit: allow-unwrap — test-only PostgreSQL assertion
        assert_eq!(
            preserved_rows,
            vec![
                (
                    "clear-slot-disconnected".to_string(),
                    "disconnected".to_string(),
                    Some("keep-dispatch".to_string()),
                    Some("keep disconnected".to_string()),
                    73,
                    Some("keep-claude-session".to_string()),
                ),
                (
                    "clear-slot-unrelated".to_string(),
                    "turn_active".to_string(),
                    Some("unrelated-dispatch".to_string()),
                    Some("keep unrelated".to_string()),
                    89,
                    Some("unrelated-claude-session".to_string()),
                ),
            ]
        );

        pool.close().await;
        pg_db.drop().await;
    }
}
