//! Real PG histories: an audited replan may settle an orphan, never a live/unknown owner.
use super::*;

async fn pause(pool: &PgPool, id: &str) -> campaigns::Campaign {
    let c = campaigns::get(pool, id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    campaigns::replace(pool, id, c.revision, input)
        .await
        .unwrap()
}

async fn completed_skipped_queue(pool: &PgPool, card: &str) {
    sqlx::query("UPDATE auto_queue_entries SET status='skipped' WHERE kanban_card_id=$1")
        .bind(card)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE auto_queue_runs SET status='completed' WHERE id IN
        (SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=$1)",
    )
    .bind(card)
    .execute(pool)
    .await
    .unwrap();
}

async fn legacy_planner(pool: &PgPool) -> (campaigns::Campaign, Replan, String) {
    let (c, _, _, mut request) = held_with_category(pool, false, "plan").await;
    settle(pool).await;
    let c = resume(pool, &c).await;
    control(
        pool,
        &c.id,
        &goal(),
        "plan",
        Some(0),
        "legacy",
        "Pinned legacy planner",
    )
    .await
    .unwrap();
    let card = store::card_id(&format!("{}:plan:legacy", c.id));
    completed_skipped_queue(pool, &card).await;
    let c = pause(pool, &c.id).await;
    request.expected_revision = c.revision;
    (c, request, card)
}

async fn history(pool: &PgPool, card: &str) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object(
        'task',to_jsonb(t)-'observation','card',to_jsonb(k),
        'queues',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.id) FROM auto_queue_entries e WHERE e.kanban_card_id=k.id),
        'runs',(SELECT jsonb_agg(to_jsonb(r) ORDER BY r.id) FROM auto_queue_runs r WHERE r.id IN
          (SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=k.id)),
        'assignments',(SELECT jsonb_agg(to_jsonb(a) ORDER BY a.task_key)
          FROM campaign_dag_control_assignments a WHERE a.task_key=t.task_key))
        FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id WHERE k.id=$1")
        .bind(card).fetch_one(pool).await.unwrap()
}

async fn observation(pool: &PgPool, card: &str) -> Option<Value> {
    sqlx::query_scalar("SELECT observation FROM campaign_graph_tasks WHERE card_id=$1")
        .bind(card)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn restore_pre_upgrade_observation(pool: &PgPool, card: &str) {
    // Emulate the persisted pre-142 state after a real native operator request:
    // its old binary wrote the durable audit but left this orphan unobserved.
    // This is disposable fixture state, not an alternative production API.
    sqlx::query("UPDATE campaign_graph_tasks SET observation=NULL WHERE card_id=$1")
        .bind(card)
        .execute(pool)
        .await
        .unwrap();
}

async fn publication_tick(pool: &PgPool, id: &str) {
    let (_root, engine) = engine(pool);
    if let Err(error) = tick(pool, &engine, id, None).await {
        // These PG fixtures have no Discord bridge. Only the subsequent queue
        // activation may fail; native planning/publication errors must fail QA.
        assert!(error.to_string().starts_with("activation:"), "{error:#}");
    }
}

async fn assert_retired_dispatch_rejected(pool: &PgPool, card: &str) {
    for (n, (kind, status)) in [
        ("implementation", "pending"),
        ("rework", "dispatched"),
        ("plan", "dispatched"),
        ("review", "pending"),
    ]
    .into_iter()
    .enumerate()
    {
        let error = sqlx::query(
            "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status)
            VALUES($1,$2,'qwen',$3,$4)",
        )
        .bind(format!("forbidden-old-control-{n}"))
        .bind(card)
        .bind(kind)
        .bind(status)
        .execute(pool)
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Superseded never-dispatched graph control"),
            "{error}"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_dispatches WHERE kanban_card_id=$1"
        )
        .bind(card)
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn audited_replan_settles_only_never_started_skipped_planner_and_keeps_pinned_history_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, request, card) = legacy_planner(&pool).await;
    let pinned = history(&pool, &card).await;
    let before = store::status(&pool, &c.id).await.unwrap();
    assert!(planning::pending(&before));
    assert_eq!(
        replan::settle_superseded_unstarted_planners(&pool, &c.id)
            .await
            .unwrap(),
        0
    );
    assert!(
        observation(&pool, &card).await.is_none(),
        "an unaudited orphan still owns its turn"
    );
    let reply = replan::request(&pool, &c.id, &request).await.unwrap();
    let retired = observation(&pool, &card).await.unwrap();
    assert_eq!(retired["outcome"], "superseded_unstarted");
    assert_eq!(retired["operator_replan"], request.request_id);
    assert_eq!(retired["repair_failure_id"], reply.repair_failure_id);
    assert_eq!(retired["graph_version"], 0);
    assert_eq!(history(&pool, &card).await, pinned);
    let after = store::status(&pool, &c.id).await.unwrap();
    for field in [
        "versions",
        "goal",
        "active_version",
        "occurrences",
        "observed_tool_calls",
        "elapsed_dispatch_seconds",
    ] {
        assert_eq!(before[field], after[field], "{field}");
    }
    assert!(
        after["failures"]
            .as_array()
            .unwrap()
            .contains(&before["failures"][0])
    );
    assert_eq!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap()
            .revision,
        reply.revision
    );
    assert_eq!(
        replan::settle_superseded_unstarted_planners(&pool, &c.id)
            .await
            .unwrap(),
        0
    );
    assert_eq!(observation(&pool, &card).await.unwrap(), retired);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM campaign_goal_revisions WHERE campaign_id=$1"
        )
        .bind(&c.id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM campaign_graph_reconciliations WHERE campaign_id=$1"
        )
        .bind(&c.id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    let paused = campaigns::get(&pool, &c.id).await.unwrap();
    resume(&pool, &paused).await;
    assert_retired_dispatch_rejected(&pool, &card).await;
    assert_eq!(history(&pool, &card).await, pinned);
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn already_audited_replan_backfills_orphan_and_native_tick_publishes_current_planner_once_pg()
{
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, request, card) = legacy_planner(&pool).await;
    let reply = replan::request(&pool, &c.id, &request).await.unwrap();
    restore_pre_upgrade_observation(&pool, &card).await;
    let pinned = history(&pool, &card).await;
    let before = store::status(&pool, &c.id).await.unwrap();
    let (_root, engine) = engine(&pool);
    tick(&pool, &engine, &c.id, None).await.unwrap();
    let paused = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(
        paused["tasks"].as_array().unwrap().len(),
        before["tasks"].as_array().unwrap().len()
    );
    assert_eq!(
        observation(&pool, &card).await.unwrap()["repair_failure_id"],
        reply.repair_failure_id
    );
    assert_eq!(history(&pool, &card).await, pinned);
    let c = campaigns::get(&pool, &c.id).await.unwrap();
    resume(&pool, &c).await;
    publication_tick(&pool, &c.id).await;
    let key = format!("{}:plan:{}", c.id, reply.repair_failure_id);
    let current = store::card_id(&key);
    let assigned: Option<String> =
        sqlx::query_scalar("SELECT assigned_agent_id FROM kanban_cards WHERE id=$1")
            .bind(&current)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(assigned.as_deref(), Some("qwen"));
    let published = store::status(&pool, &c.id).await.unwrap();
    assert!(planning::pending(&published));
    assert_eq!(
        published["tasks"].as_array().unwrap().len(),
        before["tasks"].as_array().unwrap().len() + 1
    );
    // Queue activation remains a separate fixture boundary. This newly issued
    // planner must retain ownership while its terminal result is unobserved.
    publication_tick(&pool, &c.id).await;
    let repeated = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(published["tasks"], repeated["tasks"]);
    assert_eq!(published["failures"], repeated["failures"]);
    assert!(observation(&pool, &current).await.is_none());
    assert_eq!(history(&pool, &card).await, pinned);
    assert_retired_dispatch_rejected(&pool, &card).await;
    sqlx::query(
        "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status)
        VALUES('real-current-owner',$1,'qwen','implementation','pending')",
    )
    .bind(&current)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        replan::settle_superseded_unstarted_planners(&pool, &c.id)
            .await
            .unwrap(),
        0
    );
    assert!(observation(&pool, &current).await.is_none());
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn audited_history_does_not_retire_pending_running_unknown_queue_session_or_cleanup_owners_pg()
 {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, request, _) = legacy_planner(&pool).await;
    replan::request(&pool, &c.id, &request).await.unwrap();
    let c = campaigns::get(&pool, &c.id).await.unwrap();
    resume(&pool, &c).await;
    let mut cards = Vec::new();
    for suffix in [
        "pending", "running", "unknown", "queue", "session", "cleanup", "slot",
    ] {
        control(
            &pool,
            &c.id,
            &goal(),
            "plan",
            Some(0),
            suffix,
            "Preserved legacy owner",
        )
        .await
        .unwrap();
        let card = store::card_id(&format!("{}:plan:{suffix}", c.id));
        completed_skipped_queue(&pool, &card).await;
        // Seed pre-upgrade historical controls around a real durable audit.
        sqlx::query(
            "UPDATE campaign_graph_tasks SET created_at=NOW()-INTERVAL '1 day' WHERE card_id=$1",
        )
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
        match suffix {
            "pending" | "running" | "unknown" => {
                sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result)
                    VALUES($1,$2,'qwen','implementation',$3,$4)")
                    .bind(format!("owned-{suffix}")).bind(&card)
                    .bind(match suffix {
                        "running" => "dispatched",
                        "unknown" => "failed",
                        _ => "pending",
                    })
                    .bind((suffix == "unknown").then_some("{\"error\":\"raw unknown execution receipt\"}"))
                    .execute(&pool).await.unwrap();
                if suffix == "running" {
                    // Native execution uses a dispatched queue/dispatch, an
                    // active run, and a linked turn_active provider session.
                    sqlx::query(
                        "UPDATE auto_queue_entries SET status='dispatched',
                        dispatch_id='owned-running',dispatched_at=NOW()
                        WHERE kanban_card_id=$1",
                    )
                    .bind(&card)
                    .execute(&pool)
                    .await
                    .unwrap();
                    sqlx::query(
                        "UPDATE auto_queue_runs SET status='active' WHERE id IN
                        (SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=$1)",
                    )
                    .bind(&card)
                    .execute(&pool)
                    .await
                    .unwrap();
                    sqlx::query(
                        "INSERT INTO sessions(session_key,agent_id,status,active_dispatch_id,cwd)
                        VALUES('legacy-running-session','qwen','turn_active','owned-running',$1)",
                    )
                    .bind(format!("/isolated/worktrees/card-{card}/nested"))
                    .execute(&pool)
                    .await
                    .unwrap();
                    assert!(
                        sqlx::query_scalar::<_, bool>(
                            "SELECT EXISTS(SELECT 1 FROM task_dispatches d
                            JOIN auto_queue_entries e ON e.dispatch_id=d.id
                            JOIN auto_queue_runs r ON r.id=e.run_id
                            JOIN sessions s ON s.active_dispatch_id=d.id
                            WHERE d.kanban_card_id=$1 AND d.status='dispatched'
                            AND e.status='dispatched' AND e.dispatched_at IS NOT NULL
                            AND r.status='active' AND s.status='turn_active')"
                        )
                        .bind(&card)
                        .fetch_one(&pool)
                        .await
                        .unwrap(),
                        "running fixture must preserve actual native execution ownership"
                    );
                }
            }
            "queue" => {
                sqlx::query(
                    "UPDATE auto_queue_entries SET status='pending' WHERE kanban_card_id=$1",
                )
                .bind(&card)
                .execute(&pool)
                .await
                .unwrap();
                sqlx::query(
                    "UPDATE auto_queue_runs SET status='active' WHERE id IN
                    (SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=$1)",
                )
                .bind(&card)
                .execute(&pool)
                .await
                .unwrap();
            }
            "session" => {
                sqlx::query(
                    "INSERT INTO sessions(session_key,agent_id,status,cwd)
                    VALUES('legacy-owned-session','qwen','awaiting_user',$1)",
                )
                .bind(format!("/isolated/worktrees/card-{card}/nested"))
                .execute(&pool)
                .await
                .unwrap();
            }
            "cleanup" => {
                sqlx::query(
                    "INSERT INTO auto_queue_run_cleanup_tasks(run_ids)
                    SELECT ARRAY[run_id] FROM auto_queue_entries WHERE kanban_card_id=$1",
                )
                .bind(&card)
                .execute(&pool)
                .await
                .unwrap();
            }
            "slot" => {
                sqlx::query(
                    "INSERT INTO auto_queue_slots(agent_id,slot_index,assigned_run_id)
                    SELECT 'qwen',17,run_id FROM auto_queue_entries WHERE kanban_card_id=$1",
                )
                .bind(&card)
                .execute(&pool)
                .await
                .unwrap();
            }
            _ => unreachable!(),
        }
        let original = history(&pool, &card).await;
        assert_eq!(
            replan::settle_superseded_unstarted_planners(&pool, &c.id)
                .await
                .unwrap(),
            0,
            "the {suffix} owner must block historical retirement"
        );
        assert_eq!(history(&pool, &card).await, original, "{suffix}");
        assert!(observation(&pool, &card).await.is_none(), "{suffix}");
        cards.push(card);
    }
    let mut pinned = Vec::new();
    for card in &cards {
        pinned.push(history(&pool, card).await);
    }
    let raw: Value = sqlx::query_scalar(
        "SELECT jsonb_agg(to_jsonb(d) ORDER BY d.id)
        FROM task_dispatches d WHERE id LIKE 'owned-%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let owners_query = "SELECT jsonb_build_object(
        'sessions',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.session_key) FROM sessions s),
        'slots',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.agent_id,s.slot_index) FROM auto_queue_slots s),
        'cleanup',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.id) FROM auto_queue_run_cleanup_tasks c))";
    let owners: Value = sqlx::query_scalar(owners_query)
        .fetch_one(&pool)
        .await
        .unwrap();
    let before = store::status(&pool, &c.id).await.unwrap();
    assert!(planning::pending(&before));
    assert_eq!(
        replan::settle_superseded_unstarted_planners(&pool, &c.id)
            .await
            .unwrap(),
        0
    );
    for (card, original) in cards.iter().zip(pinned) {
        assert_eq!(history(&pool, card).await, original);
        assert!(observation(&pool, card).await.is_none());
    }
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["tasks"],
        before["tasks"]
    );
    assert_eq!(
        sqlx::query_scalar::<_, Value>(
            "SELECT jsonb_agg(to_jsonb(d) ORDER BY d.id)
        FROM task_dispatches d WHERE id LIKE 'owned-%'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        raw
    );
    assert_eq!(
        sqlx::query_scalar::<_, Value>(owners_query)
            .fetch_one(&pool)
            .await
            .unwrap(),
        owners
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn busy_historical_queue_retains_control_until_its_owner_releases_row_lock_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, request, card) = legacy_planner(&pool).await;
    replan::request(&pool, &c.id, &request).await.unwrap();
    restore_pre_upgrade_observation(&pool, &card).await;
    let pinned = history(&pool, &card).await;
    let mut owner = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM auto_queue_entries WHERE kanban_card_id=$1 FOR UPDATE")
        .bind(&card)
        .fetch_one(&mut *owner)
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            replan::settle_superseded_unstarted_planners(&pool, &c.id)
        )
        .await
        .unwrap()
        .unwrap(),
        0
    );
    assert!(observation(&pool, &card).await.is_none());
    owner.commit().await.unwrap();
    assert_eq!(
        replan::settle_superseded_unstarted_planners(&pool, &c.id)
            .await
            .unwrap(),
        1
    );
    assert_eq!(history(&pool, &card).await, pinned);
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn racing_first_dispatch_reads_retired_observation_after_campaign_lock_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, request, card) = legacy_planner(&pool).await;
    replan::request(&pool, &c.id, &request).await.unwrap();
    let retired = observation(&pool, &card).await.unwrap();
    let c = campaigns::get(&pool, &c.id).await.unwrap();
    resume(&pool, &c).await;
    restore_pre_upgrade_observation(&pool, &card).await;
    let mut settlement = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM campaigns WHERE id=$1 FOR UPDATE")
        .bind(&c.id)
        .fetch_one(&mut *settlement)
        .await
        .unwrap();
    let dispatch_pool = pool.clone();
    let dispatch_card = card.clone();
    let attempt = tokio::spawn(async move {
        sqlx::query(
            "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status)
            VALUES('racing-first-dispatch',$1,'qwen','implementation','pending')",
        )
        .bind(dispatch_card)
        .execute(&dispatch_pool)
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
                 AND wait_event_type='Lock' AND query LIKE 'INSERT INTO task_dispatches%'
                 AND query LIKE '%racing-first-dispatch%')",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first dispatch must wait on the campaign settlement lock");
    assert!(!attempt.is_finished());
    // Restore the actual native settlement record while its campaign lock is
    // held. The trigger must reread it after waiting, not use a pre-lock value.
    sqlx::query("UPDATE campaign_graph_tasks SET observation=$2 WHERE card_id=$1")
        .bind(&card)
        .bind(retired)
        .execute(&mut *settlement)
        .await
        .unwrap();
    settlement.commit().await.unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), attempt)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Superseded never-dispatched graph control"),
        "{error}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_dispatches WHERE id='racing-first-dispatch'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    pool.close().await;
    fixture.drop().await;
}
