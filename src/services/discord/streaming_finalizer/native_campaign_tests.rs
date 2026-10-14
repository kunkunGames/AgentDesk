use super::*;
use serde_json::json;

struct NativeReceiptFixture {
    database: crate::db::auto_queue::test_support::TestPostgresDb,
    pool: sqlx::PgPool,
    workspace: tempfile::TempDir,
    proposed: serde_json::Value,
}

async fn native_receipt_fixture(initial_status: &str) -> NativeReceiptFixture {
    assert!(matches!(initial_status, "pending" | "completed"));
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    sqlx::query("INSERT INTO agents(id,name,provider) VALUES
        ('receipt-planner','Receipt planner','qwen'),('receipt-validator','Receipt validator','opencode')")
        .execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO kv_meta(key,value) VALUES('runtime-config','{\"internalTasksEnabled\":true}')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let campaign = crate::db::campaigns::create(
        &pool,
        "transport-fixture".into(),
        serde_json::from_value(
            json!({"title":"Receipt transport","status":"active","round":1,"nodes":[]}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let goal: crate::db::campaigns::graph::Goal = serde_json::from_value(json!({
        "objective":"Transport an independently rejected candidate receipt",
        "success":["Captured receipt and its completion event preserve the provider report"],
        "constraints":["Disposable fixture only; no provider or external business execution"],
        "repo":"test/receipt-transport","planner":"receipt-planner","verifier":"receipt-validator",
        "agents":["receipt-planner","receipt-validator"],
        "budget":{"max_dispatches":6,"max_elapsed_secs":600,"node_timeout_secs":120,
            "max_evolutions":1,"max_observed_tool_calls":20}
    }))
    .unwrap();
    crate::db::campaigns::graph::store::enroll(&pool, &campaign.id, campaign.revision, &goal)
        .await
        .unwrap();
    // Establish the native executor before the task makes card identity immutable.
    sqlx::query(
        "INSERT INTO kanban_cards(id,title,status,assigned_agent_id)
        VALUES('native-validation','Validate','in_progress','receipt-validator')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,node_id,role,version,contract_digest,card_id) VALUES('validation-key',$1,'validate','validate',1,'pinned-contract','native-validation')")
        .bind(&campaign.id).execute(&pool).await.unwrap();
    let workspace = tempfile::tempdir().unwrap();
    for args in [
        vec!["init"],
        vec!["config", "user.email", "fixture@example.invalid"],
        vec!["config", "user.name", "Receipt fixture"],
    ] {
        assert!(
            std::process::Command::new("git")
                .args(args)
                .current_dir(workspace.path())
                .output()
                .unwrap()
                .status
                .success()
        );
    }
    std::fs::write(workspace.path().join("tracked.txt"), "original").unwrap();
    for args in [
        vec!["add", "tracked.txt"],
        vec!["commit", "-m", "Fixture baseline"],
    ] {
        assert!(
            std::process::Command::new("git")
                .args(args)
                .current_dir(workspace.path())
                .output()
                .unwrap()
                .status
                .success()
        );
    }
    // The reader fixture retains its completed row. The writer fixture starts
    // pending under the real graph dispatch guards and never reopens a terminal row.
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,context,created_at)
        VALUES('native-dispatch','native-validation','receipt-validator','implementation',$1,$2,NOW()-INTERVAL '2 seconds')")
        .bind(initial_status).bind(json!({"worktree_path":workspace.path()}).to_string())
        .execute(&pool).await.unwrap();
    let proposed = json!({"campaign_receipt":{"outcome":"passed","summary":"planning validation only",
        "observations":[{"command":"check candidate","exit_code":0,"evidence":"independent review"}]},
        "graph_validation":{"digest":"candidate-digest","approved":true,"reason":"reviewed candidate"}});
    let path = workspace.path().join(".agentdesk-campaign-result.json");
    std::fs::write(&path, proposed.to_string()).unwrap();
    NativeReceiptFixture {
        database: fixture,
        pool,
        workspace,
        proposed,
    }
}

#[tokio::test]
async fn native_streaming_result_preserves_receipt_and_fails_closed_on_missing_stale_or_foreign_files_pg()
 {
    let NativeReceiptFixture {
        database: fixture,
        pool,
        workspace,
        proposed,
    } = native_receipt_fixture("completed").await;
    let path = workspace.path().join(".agentdesk-campaign-result.json");
    let cwd = workspace.path().to_str().unwrap();
    let request = WatcherStreamingFinalRequest {
        pg_pool: Some(&pool),
        dispatch_id: "native-dispatch",
        adk_cwd: Some(cwd),
        full_response: "Final prose cannot replace the receipt",
        has_assistant_response: true,
    };
    let captured = watcher_completion_result(&request).await.unwrap();
    assert_eq!(captured["campaign_receipt"], proposed["campaign_receipt"]);
    assert_eq!(captured["graph_validation"], proposed["graph_validation"]);
    assert!(captured["agent_response_present"].is_null());
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT accepted FROM campaign_graph_tasks WHERE task_key='validation-key'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    sqlx::query(
        "UPDATE task_dispatches SET created_at=NOW()+INTERVAL '1 day' WHERE id='native-dispatch'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        watcher_completion_result(&request)
            .await
            .unwrap_err()
            .to_string()
            .contains("predates")
    );
    sqlx::query("UPDATE task_dispatches SET created_at=NOW()-INTERVAL '2 seconds' WHERE id='native-dispatch'")
        .execute(&pool).await.unwrap();
    let foreign = tempfile::tempdir().unwrap();
    let wrong = WatcherStreamingFinalRequest {
        adk_cwd: foreign.path().to_str(),
        ..request
    };
    assert!(
        watcher_completion_result(&wrong)
            .await
            .unwrap_err()
            .to_string()
            .contains("differs")
    );
    std::fs::write(workspace.path().join("tracked.txt"), "uncommitted").unwrap();
    assert!(
        watcher_completion_result(&request)
            .await
            .unwrap_err()
            .to_string()
            .contains("commit tracked")
    );
    std::fs::write(workspace.path().join("tracked.txt"), "original").unwrap();
    let mut forbidden = proposed.clone();
    forbidden["status"] = json!("completed");
    std::fs::write(&path, forbidden.to_string()).unwrap();
    assert!(
        watcher_completion_result(&request)
            .await
            .unwrap_err()
            .to_string()
            .contains("reserved")
    );
    std::fs::remove_file(&path).unwrap();
    assert!(watcher_completion_result(&request).await.is_err());
    // Ordinary work still uses its existing completion contract without a
    // native file. No result read manufactures terminal business acceptance.
    let ordinary = WatcherStreamingFinalRequest {
        dispatch_id: "ordinary-dispatch",
        adk_cwd: None,
        ..request
    };
    let result = watcher_completion_result(&ordinary).await.unwrap();
    assert_eq!(result["completion_source"], "watcher_streaming_final");
    assert_eq!(result["agent_response_present"], true);
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn native_streaming_rejected_receipt_persists_matching_completion_source_and_event_pg() {
    let NativeReceiptFixture {
        database: fixture,
        pool,
        workspace,
        mut proposed,
    } = native_receipt_fixture("pending").await;
    proposed["campaign_receipt"]["outcome"] = json!("blocked");
    proposed["campaign_receipt"]["failure_category"] = json!("plan");
    proposed["graph_validation"]["approved"] = json!(false);
    let path = workspace.path().join(".agentdesk-campaign-result.json");
    std::fs::write(&path, proposed.to_string()).unwrap();
    let source_bytes = std::fs::read(&path).unwrap();
    let request = WatcherStreamingFinalRequest {
        pg_pool: Some(&pool),
        dispatch_id: "native-dispatch",
        adk_cwd: workspace.path().to_str(),
        full_response: "A rejected candidate is not business acceptance",
        has_assistant_response: true,
    };
    let captured = watcher_completion_result(&request).await.unwrap();
    assert_eq!(captured["completion_source"], "watcher_streaming_final");
    assert_eq!(captured["campaign_receipt"], proposed["campaign_receipt"]);
    assert_eq!(captured["graph_validation"], proposed["graph_validation"]);
    crate::db::campaigns::graph::result::validate_dispatch(&captured).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), source_bytes);
    let initial: (String, bool, bool) = sqlx::query_as(
        "SELECT status,completed_at IS NULL,result IS NULL FROM task_dispatches WHERE id='native-dispatch'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(initial, ("pending".into(), true, true));
    // Use the explicit disposable fixture pool, never the runtime PG fallback.
    assert_eq!(
        crate::dispatch::set_dispatch_status_on_pg_async(
            &pool,
            "native-dispatch",
            "completed",
            Some(&captured),
            "watcher_streaming_final",
            Some(&["pending"]),
            true
        )
        .await
        .unwrap(),
        1
    );
    let raw: String =
        sqlx::query_scalar("SELECT result FROM task_dispatches WHERE id='native-dispatch'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let event: serde_json::Value = sqlx::query_scalar(
        "SELECT payload_json FROM dispatch_events
        WHERE dispatch_id='native-dispatch' AND from_status='pending' AND to_status='completed'
        AND transition_source='watcher_streaming_final' ORDER BY id LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&raw).unwrap(),
        captured
    );
    assert_eq!(event, captured);
    assert_eq!(std::fs::read(&path).unwrap(), source_bytes);
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT accepted FROM campaign_graph_tasks WHERE task_key='validation-key'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    pool.close().await;
    fixture.drop().await;
}
