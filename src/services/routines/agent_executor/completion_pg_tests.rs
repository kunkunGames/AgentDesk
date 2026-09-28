//! Exercise completion authority against real persisted transcript/quality rows.
use super::*;
use crate::db::{
    auto_queue::test_support::TestPostgresDb, session_transcripts::SessionTranscriptEvent,
};

#[tokio::test]
async fn routine_completion_rejects_transport_failures_without_analytics_races_pg() {
    let database = TestPostgresDb::create().await;
    let pool = database.connect_and_migrate().await;
    let executor = RoutineAgentExecutor::new(Arc::new(pool.clone()), None, 30);
    let run = super::tests::running_run(None);
    let turn = run.turn_id.as_deref().unwrap();
    sqlx::query(
        "INSERT INTO session_transcripts(turn_id,assistant_message) VALUES($1,'work completed')",
    )
    .bind(turn)
    .execute(&pool)
    .await
    .unwrap();

    // Historical rows still have typed quality errors. A misleading text
    // response must not override the same attempt's failed terminal outcome.
    sqlx::query("INSERT INTO agent_quality_event(source_event_id,correlation_id,event_type,payload) VALUES($1,$1,'turn_error',$2)")
        .bind(turn).bind(json!({"details":{"outcome":"transport_error","duration_ms":240000}}))
        .execute(&pool).await.unwrap();
    let failed = executor.find_turn_completion(&run).await.unwrap().unwrap();
    assert_eq!(failed.terminal_status.as_deref(), Some("transport_error"));
    assert!(provider_error_from_completion(&failed).is_some());

    sqlx::query("DELETE FROM agent_quality_event")
        .execute(&pool)
        .await
        .unwrap();
    // The bridge persists its decision with the text, even if the analytics
    // worker has not flushed, is stopped, or has dropped its queued event.
    for outcome in ["transport_error", "prompt_too_long", "cancelled"] {
        sqlx::query("UPDATE session_transcripts SET events_json=$2 WHERE turn_id=$1")
            .bind(turn)
            .bind(json!([SessionTranscriptEvent::turn_outcome(outcome)]))
            .execute(&pool)
            .await
            .unwrap();
        let failed = executor.find_turn_completion(&run).await.unwrap().unwrap();
        assert!(
            provider_error_from_completion(&failed).is_some(),
            "{outcome}"
        );
    }
    for outcome in ["delivery_pending", "delivery_unresolved", "recovery_retry"] {
        sqlx::query("UPDATE session_transcripts SET events_json=$2 WHERE turn_id=$1")
            .bind(turn)
            .bind(json!([SessionTranscriptEvent::turn_outcome(outcome)]))
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            executor.find_turn_completion(&run).await.unwrap().is_none(),
            "{outcome}"
        );
    }
    sqlx::query("UPDATE session_transcripts SET events_json=$2 WHERE turn_id=$1")
        .bind(turn)
        .bind(json!([SessionTranscriptEvent::turn_outcome("completed")]))
        .execute(&pool)
        .await
        .unwrap();
    let completed = executor.find_turn_completion(&run).await.unwrap().unwrap();
    assert!(provider_error_from_completion(&completed).is_none());
    assert!(completed.evidence.confirms_assistant_delivery());

    // A different attempt cannot poison this one, and a quoted tool error
    // remains useful work when the bridge's final outcome says completed.
    sqlx::query("INSERT INTO agent_quality_event(source_event_id,correlation_id,event_type,payload) VALUES('different-turn',$1,'turn_error',$2)")
        .bind(turn).bind(json!({"details":{"outcome":"transport_error"}}))
        .execute(&pool).await.unwrap();
    assert!(
        provider_error_from_completion(
            &executor.find_turn_completion(&run).await.unwrap().unwrap()
        )
        .is_none()
    );

    // Old rows from a previous start must not close a fresh retry.
    let mut retry = run.clone();
    retry.attempts =
        json!([{"event":"started", "at":(Utc::now() + Duration::seconds(1)).to_rfc3339()}]);
    assert!(
        executor
            .find_turn_completion(&retry)
            .await
            .unwrap()
            .is_none()
    );
    pool.close().await;
}
