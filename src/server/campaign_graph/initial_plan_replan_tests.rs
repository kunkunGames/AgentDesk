//! Native pre-provider refusal remains history; a new enrolled planner needs fresh QA.
use super::*;
use crate::db::campaigns::graph::{
    digest,
    recovery::replan::{self, Replan},
    schedule,
};
use chrono::Timelike;

const ATTEMPT: &str = "fixture-initial-capacity-refusal";

struct InitialFixture {
    campaign: campaigns::Campaign,
    parent: String,
    parent_goal: Goal,
    later_slot: chrono::DateTime<chrono::Utc>,
    request: Replan,
    card: String,
}

fn capacity_error() -> Value {
    // Literal native producer envelope, independently of the recovery predicate.
    json!({"error":"⚠️ provider가 응답을 완료하지 못했어요.\n같은 요청을 다시 시도해 주세요. 문제가 반복되면 `!clear`로 세션을 초기화한 뒤 다시 보내 주세요.\n||**상세**\n```text\nError: node execution capacity exhausted; retry when a turn completes\n```||"})
}

async fn set_status(pool: &PgPool, id: &str, status: CampaignStatus) -> campaigns::Campaign {
    let current = campaigns::get(pool, id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
    input.status = status;
    campaigns::replace(pool, id, current.revision, input)
        .await
        .unwrap()
}

async fn acknowledge_reaction(pool: &PgPool) {
    use crate::db::dispatches::outbox::{mark_dispatch_outbox_claimed_pg, mark_outbox_done_pg};
    let mut tx = pool.begin().await.unwrap();
    let rows = sqlx::query(
        "SELECT id,status,claim_owner,claimed_at FROM dispatch_outbox
        WHERE dispatch_id=$1 AND action='status_reaction' FOR UPDATE",
    )
    .bind(ATTEMPT)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.get::<String, _>("status"), "pending");
    assert!(row.get::<Option<String>, _>("claim_owner").is_none());
    assert!(
        row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("claimed_at")
            .is_none()
    );
    let id: i64 = row.get("id");
    let owner = "fixture-initial-plan-reaction";
    let claimed_at = mark_dispatch_outbox_claimed_pg(&mut tx, id, owner)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let receipt = json!({"fixture":true,"external_delivery_performed":false,
        "reason":"Disposable initial-plan fixture acknowledges its canonical status reaction"});
    mark_outbox_done_pg(
        pool,
        id,
        "suppressed",
        &receipt.to_string(),
        owner,
        claimed_at,
    )
    .await
    .unwrap();
    let after: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM dispatch_outbox o WHERE id=$1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(after["status"], "done");
    assert_eq!(after["delivery_result"], receipt);
    assert!(after["claim_owner"].is_null() && after["claimed_at"].is_null());
}

async fn fixture(pool: &PgPool) -> InitialFixture {
    let parent = setup(pool).await;
    sqlx::query("INSERT INTO agents(id,name,provider) VALUES('codex','Superior','codex')")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE kv_meta SET value='{\"internalTasksEnabled\":true,\"campaignSuperiorAgentId\":\"codex\"}'
        WHERE key='runtime-config'").execute(pool).await.unwrap();
    let first_slot = chrono::Utc::now() + chrono::Duration::minutes(2);
    let later_slot = first_slot + chrono::Duration::hours(6);
    let paused = set_status(pool, &parent.id, CampaignStatus::Paused).await;
    schedule::activation::activate(
        pool,
        &parent.id,
        &schedule::activation::Activation {
            expected_revision: paused.revision,
            request_id: "fixture-old-occurrence".into(),
            evidence: "Disposable fixture pins the old occurrence before recovery without backfill"
                .into(),
            schedule: schedule::Schedule {
                timezone: "UTC".into(),
                daily_minutes: [
                    (first_slot.hour() * 60 + first_slot.minute()) as u16,
                    (later_slot.hour() * 60 + later_slot.minute()) as u16,
                ]
                .into_iter()
                .collect(),
            },
            occurrence_budget: None,
            activate: true,
        },
    )
    .await
    .unwrap();
    let parent_goal = store::goal(pool, &parent.id).await.unwrap();
    let child = schedule::admit(pool, &parent.id, &parent_goal, first_slot)
        .await
        .unwrap()
        .unwrap();
    let child_goal = store::goal(pool, &child).await.unwrap();
    assert!(child_goal.schedule.is_none() && child_goal.observation_window.is_some());
    control(
        pool,
        &child,
        &child_goal,
        "plan",
        None,
        "initial",
        GRAPH_SCHEMA,
    )
    .await
    .unwrap();
    let card = store::card_id(&format!("{child}:plan:initial"));
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,created_at)
        VALUES($1,$2,'codex','implementation','pending',NOW())",
    )
    .bind(ATTEMPT)
    .bind(&card)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(
        crate::dispatch::set_dispatch_status_on_pg_async(
            pool,
            ATTEMPT,
            "failed",
            Some(&capacity_error()),
            "turn_bridge_dispatch_failure",
            Some(&["pending"]),
            false
        )
        .await
        .unwrap(),
        1
    );
    reconcile::observe(pool, &child, &child_goal, None)
        .await
        .unwrap();
    super::replan_tests::settle(pool).await;
    acknowledge_reaction(pool).await;
    // Configuration drift is legal; the old profile is historical authority.
    // New work is still assigned to the unchanged enrolled planner, qwen.
    sqlx::query("UPDATE agents SET provider='opencode' WHERE id='codex'")
        .execute(pool)
        .await
        .unwrap();
    let campaign = set_status(pool, &child, CampaignStatus::Paused).await;
    let state = store::status(pool, &child).await.unwrap();
    assert!(state["active_version"].is_null() && state["versions"] == json!([]));
    let failure = state["failures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["evidence"]["dispatch_id"] == ATTEMPT)
        .unwrap();
    assert_eq!(failure["evidence"]["runtime"]["observed_tool_calls"], 0);
    assert_eq!(failure["evidence"]["runtime"]["provider"], "codex");
    let request = Replan {expected_revision:campaign.revision,
        request_id:"fixture-native-initial-capacity-repair".into(),
        failure_id:failure["id"].as_str().unwrap().into(),
        evidence:"Exact native pre-provider capacity refusal, zero progress and settled owners. Preserve the old occurrence window, raw failure, missing completion timestamp and lifetime usage; request fresh enrolled planning and independent QA.".into(),
        budget:None,supersede_candidate:None,unattempted_effect:None,settled_effect:None,
        rejected_validation:None,cancelled_validation:None};
    InitialFixture {
        campaign,
        parent: parent.id,
        parent_goal,
        later_slot,
        request,
        card,
    }
}

async fn retained(pool: &PgPool, fixture: &InitialFixture) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object(
        'dispatch',(SELECT to_jsonb(d) FROM task_dispatches d WHERE id=$1),
        'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.id) FROM dispatch_events e WHERE dispatch_id=$1),
        'task',(SELECT to_jsonb(t) FROM campaign_graph_tasks t WHERE card_id=$2),
        'card',(SELECT to_jsonb(k) FROM kanban_cards k WHERE id=$2),
        'failure',(SELECT to_jsonb(f) FROM campaign_graph_failures f WHERE id=$3),
        'assignment',(SELECT to_jsonb(a) FROM campaign_dag_control_assignments a
            JOIN campaign_graph_tasks t ON t.task_key=a.task_key WHERE t.card_id=$2),
        'outbox',(SELECT jsonb_agg(to_jsonb(o) ORDER BY o.id) FROM dispatch_outbox o WHERE dispatch_id=$1),
        'occurrences',(SELECT jsonb_agg(to_jsonb(o) ORDER BY o.scheduled_at)
            FROM campaign_graph_occurrences o WHERE campaign_id=$4),
        'parent',(SELECT to_jsonb(c) FROM campaigns c WHERE id=$4),
        'parent_spec',(SELECT to_jsonb(s) FROM campaign_execution_specs s WHERE campaign_id=$4))")
        .bind(ATTEMPT).bind(&fixture.card).bind(&fixture.request.failure_id).bind(&fixture.parent)
        .fetch_one(pool).await.unwrap()
}

async fn refusal(pool: &PgPool, fixture: &InitialFixture, request: &Replan) -> String {
    let state = canonical_inspection(store::status(pool, &fixture.campaign.id).await.unwrap());
    let history = retained(pool, fixture).await;
    let error = replan::request(pool, &fixture.campaign.id, request)
        .await
        .unwrap_err();
    assert_eq!(
        canonical_inspection(store::status(pool, &fixture.campaign.id).await.unwrap()),
        state
    );
    assert_eq!(retained(pool, fixture).await, history);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM campaign_goal_revisions WHERE campaign_id=$1"
        )
        .bind(&fixture.campaign.id)
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
    error.to_string()
}

fn copy_request(request: &Replan) -> Replan {
    serde_json::from_value(serde_json::to_value(request).unwrap()).unwrap()
}

#[tokio::test]
async fn initial_plan_capacity_replan_preserves_old_occurrence_and_issues_enrolled_plan_with_fresh_qa_pg()
 {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = fixture(&pool).await;
    let before = store::status(&pool, &f.campaign.id).await.unwrap();
    let history = retained(&pool, &f).await;
    let description = history["card"]["description"].as_str().unwrap();
    assert!(description.contains(crate::db::campaigns::graph::schema::GRAPH_REFERENCE_RULES));
    assert!(description.contains("node.id (Task.id) values, never persistent task_key"));
    control(
        &pool,
        &f.campaign.id,
        &store::goal(&pool, &f.campaign.id).await.unwrap(),
        "plan",
        None,
        "initial",
        "A later binary has different planning guidance",
    )
    .await
    .unwrap();
    assert_eq!(retained(&pool, &f).await, history);
    assert_eq!(
        store::status(&pool, &f.campaign.id).await.unwrap()["tasks"],
        before["tasks"]
    );
    assert!(history["dispatch"]["completed_at"].is_null());
    let original_raw = history["dispatch"]["result"].as_str().unwrap();
    let raw_sha = digest(original_raw.as_bytes());
    let reply = replan::request(&pool, &f.campaign.id, &f.request)
        .await
        .unwrap();
    assert_eq!(reply.revision, f.campaign.revision + 1);
    assert_eq!(reply.planner_agent_id, "qwen");
    assert!(reply.independent_validation_required && reply.goal_scope_preserved);
    let after = store::status(&pool, &f.campaign.id).await.unwrap();
    for field in [
        "goal",
        "active_version",
        "versions",
        "tasks",
        "budget_enforcement",
        "elapsed_dispatch_seconds",
        "observed_tool_calls",
    ] {
        assert_eq!(after[field], before[field], "preserved {field}");
    }
    assert!(after["hold_reason"].is_null());
    assert_eq!(
        campaigns::get(&pool, &f.campaign.id).await.unwrap().status,
        CampaignStatus::Paused
    );
    assert_eq!(retained(&pool, &f).await, history);
    assert_eq!(
        replan::request(&pool, &f.campaign.id, &f.request)
            .await
            .unwrap()
            .revision,
        reply.revision
    );
    let mut changed = copy_request(&f.request);
    changed.evidence.push_str(" changed");
    assert!(
        replan::request(&pool, &f.campaign.id, &changed)
            .await
            .is_err()
    );
    assert!(
        schedule::admit(&pool, &f.parent, &f.parent_goal, f.later_slot)
            .await
            .unwrap()
            .is_none()
    );
    set_status(&pool, &f.campaign.id, CampaignStatus::Active).await;
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    // A disposable fixture has no global queue bridge; verify the real tick's
    // committed publication even if subsequent bridge activation is unavailable.
    let _ = tick(&pool, &engine, &f.campaign.id, None).await;
    let key = format!("{}:plan:{}", f.campaign.id, reply.repair_failure_id);
    let planner: Value = sqlx::query_scalar(
        "SELECT to_jsonb(t)||jsonb_build_object(
        'assigned_agent_id',k.assigned_agent_id) FROM campaign_graph_tasks t
        JOIN kanban_cards k ON k.id=t.card_id WHERE t.task_key=$1",
    )
    .bind(&key)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(planner["role"], "plan");
    assert_eq!(planner["version"], 0);
    assert_eq!(planner["assigned_agent_id"], "qwen");
    assert_ne!(planner["card_id"], f.card);
    let current_goal = store::goal(&pool, &f.campaign.id).await.unwrap();
    assert_eq!(
        current_goal.observation_window,
        before["goal"]["observation_window"]
            .as_str()
            .map(str::to_owned)
    );
    let version = store::stage(
        &pool,
        &f.campaign.id,
        &graph(),
        Some(&reply.repair_failure_id),
    )
    .await
    .unwrap();
    let current = campaigns::get(&pool, &f.campaign.id).await.unwrap();
    assert!(
        store::activate(&pool, &f.campaign.id, version, current.revision)
            .await
            .is_err()
    );
    control(
        &pool,
        &f.campaign.id,
        &current_goal,
        "validate",
        Some(version),
        &version.to_string(),
        "Fresh independent candidate QA",
    )
    .await
    .unwrap();
    let qa: Option<String> = sqlx::query_scalar(
        "SELECT k.assigned_agent_id FROM campaign_graph_tasks t
        JOIN kanban_cards k ON k.id=t.card_id WHERE t.task_key=$1",
    )
    .bind(format!("{}:validate:{version}", f.campaign.id))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(qa.as_deref(), Some("monitoring"));
    assert_eq!(retained(&pool, &f).await, history);
    assert_eq!(
        digest(
            retained(&pool, &f).await["dispatch"]["result"]
                .as_str()
                .unwrap()
                .as_bytes()
        ),
        raw_sha
    );
    assert!(
        schedule::admit(&pool, &f.parent, &f.parent_goal, f.later_slot)
            .await
            .unwrap()
            .is_none()
    );
    super::replan_tests::settle(&pool).await;
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn initial_plan_capacity_replan_rejects_other_errors_raw_drift_and_noncanonical_events_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = fixture(&pool).await;
    let original = retained(&pool, &f).await;
    for raw in [
        "not-json".to_owned(),
        json!({"error":"node execution capacity exhausted; retry when a turn completes"})
            .to_string(),
        json!({"error":"Provider quota exhausted"}).to_string(),
        json!({"error":capacity_error()["error"],"extra":true}).to_string(),
        serde_json::to_string(&capacity_error().to_string()).unwrap(),
    ] {
        sqlx::query("UPDATE task_dispatches SET result=$2 WHERE id=$1")
            .bind(ATTEMPT)
            .bind(raw)
            .execute(&pool)
            .await
            .unwrap();
        refusal(&pool, &f, &f.request).await;
    }
    sqlx::query("UPDATE task_dispatches SET result=$2 WHERE id=$1")
        .bind(ATTEMPT)
        .bind(original["dispatch"]["result"].as_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE dispatch_events SET transition_source='operator_status_patch'
        WHERE dispatch_id=$1 AND to_status='failed'",
    )
    .bind(ATTEMPT)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("producer provenance")
    );
    sqlx::query("UPDATE dispatch_events SET transition_source='turn_bridge_dispatch_failure',payload_json=$2
        WHERE dispatch_id=$1 AND to_status='failed'").bind(ATTEMPT).bind(json!({"error":"other"}))
        .execute(&pool).await.unwrap();
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("producer provenance")
    );
    sqlx::query(
        "UPDATE dispatch_events SET payload_json=$2 WHERE dispatch_id=$1 AND to_status='failed'",
    )
    .bind(ATTEMPT)
    .bind(capacity_error())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO dispatch_events(dispatch_id,kanban_card_id,dispatch_type,from_status,to_status,transition_source,payload_json)
        VALUES($1,$2,'implementation','dispatched','failed','turn_bridge_dispatch_failure',$3)")
        .bind(ATTEMPT).bind(&f.card).bind(capacity_error()).execute(&pool).await.unwrap();
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("one canonical")
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn initial_plan_capacity_replan_rejects_profile_evidence_authority_and_chronology_drift_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = fixture(&pool).await;
    let original = retained(&pool, &f).await;
    let mut changed = original["failure"]["evidence"].clone();
    changed["external_effect_requires_reconciliation"] = json!(true);
    sqlx::query("UPDATE campaign_graph_failures SET evidence=$2 WHERE id=$1")
        .bind(&f.request.failure_id)
        .bind(&changed)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET observation=$2 WHERE card_id=$1")
        .bind(&f.card)
        .bind(&changed)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("unknown external effects")
    );
    sqlx::query("UPDATE campaign_graph_failures SET evidence=$2 WHERE id=$1")
        .bind(&f.request.failure_id)
        .bind(&original["failure"]["evidence"])
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET observation=$2 WHERE card_id=$1")
        .bind(&f.card)
        .bind(&original["task"]["observation"])
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=NULL WHERE card_id=$1")
        .bind(&f.card)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("execution profile missing")
    );
    sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=$2 WHERE card_id=$1")
        .bind(&f.card)
        .bind(&original["task"]["execution_profile"])
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET role='work' WHERE card_id=$1")
        .bind(&f.card)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("versioned failed task required")
    );
    sqlx::query("UPDATE campaign_graph_tasks SET role='plan' WHERE card_id=$1")
        .bind(&f.card)
        .execute(&pool)
        .await
        .unwrap();
    for field in [
        "goal_digest",
        "prompt_digest",
        "provider_requested",
        "control_input_digest",
    ] {
        let mut profile = original["task"]["execution_profile"].clone();
        profile[field] = json!("changed");
        sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=$2 WHERE card_id=$1")
            .bind(&f.card)
            .bind(profile)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            refusal(&pool, &f, &f.request)
                .await
                .contains("input profile changed")
        );
    }
    sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=$2 WHERE card_id=$1")
        .bind(&f.card)
        .bind(&original["task"]["execution_profile"])
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE task_dispatches SET to_agent_id='qwen' WHERE id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("input profile changed")
    );
    sqlx::query("UPDATE task_dispatches SET to_agent_id='codex' WHERE id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_dag_control_assignments SET agent_id='qwen' WHERE task_key=$1")
        .bind(original["task"]["task_key"].as_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("bootstrap assignment")
    );
    sqlx::query("UPDATE campaign_dag_control_assignments SET agent_id='codex' WHERE task_key=$1")
        .bind(original["task"]["task_key"].as_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET observation='{}' WHERE card_id=$1")
        .bind(&f.card)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("failure observation")
    );
    sqlx::query("UPDATE campaign_graph_tasks SET observation=$2 WHERE card_id=$1")
        .bind(&f.card)
        .bind(&original["task"]["observation"])
        .execute(&pool)
        .await
        .unwrap();
    for field in ["created_at", "updated_at"] {
        // Both retained timestamps are nullable in the authoritative schema.
        sqlx::query(&format!(
            "UPDATE task_dispatches SET {field}=NULL WHERE id=$1"
        ))
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
        refusal(&pool, &f, &f.request).await;
        sqlx::query(&format!(
            "UPDATE task_dispatches SET {field}=$2::text::timestamptz WHERE id=$1"
        ))
        .bind(ATTEMPT)
        .bind(original["dispatch"][field].as_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::query("UPDATE task_dispatches SET completed_at=updated_at WHERE id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("missing completion timestamp")
    );
    sqlx::query("UPDATE task_dispatches SET completed_at=NULL WHERE id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(retained(&pool, &f).await, original);
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn initial_plan_capacity_replan_rejects_active_owners_cleanup_progress_graph_and_replay_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = fixture(&pool).await;
    let mut stale = copy_request(&f.request);
    stale.expected_revision -= 1;
    assert!(
        refusal(&pool, &f, &stale)
            .await
            .contains("expected revision")
    );
    sqlx::query(
        "INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id)
        VALUES('fixture-initial-owner','codex','opencode','awaiting_bg',$1)",
    )
    .bind(ATTEMPT)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("active execution")
    );
    sqlx::query("DELETE FROM sessions WHERE session_key='fixture-initial-owner'")
        .execute(&pool)
        .await
        .unwrap();
    let cleanup: i64 = sqlx::query_scalar(
        "INSERT INTO auto_queue_run_cleanup_tasks(run_ids,dispatch_ids)
        VALUES('{}',ARRAY[$1]::text[]) RETURNING id",
    )
    .bind(ATTEMPT)
    .fetch_one(&pool)
    .await
    .unwrap();
    refusal(&pool, &f, &f.request).await;
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks WHERE id=$1")
        .bind(cleanup)
        .execute(&pool)
        .await
        .unwrap();
    let cleanup: i64 = sqlx::query_scalar(
        "INSERT INTO auto_queue_run_cleanup_tasks(run_ids,pending_emits)
        VALUES('{}','{\"unidentified\":true}') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("durable cleanup")
    );
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks WHERE id=$1")
        .bind(cleanup)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE dispatch_outbox SET status='pending' WHERE dispatch_id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    assert!(refusal(&pool, &f, &f.request).await.contains("delivery"));
    sqlx::query("UPDATE dispatch_outbox SET status='done' WHERE dispatch_id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    progress::tool_use(
        Some(&pool),
        Some(ATTEMPT),
        Some("fixture-unexpected-tool"),
        "tool-hash",
    )
    .await;
    assert!(
        refusal(&pool, &f, &f.request)
            .await
            .contains("observed provider tools")
    );
    sqlx::query("DELETE FROM campaign_graph_progress WHERE dispatch_id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO campaign_graph_versions(campaign_id,version,graph,digest,status)
        VALUES($1,1,$2,$3,'candidate')",
    )
    .bind(&f.campaign.id)
    .bind(json!(graph()))
    .bind(contract_digest(&graph()).unwrap())
    .execute(&pool)
    .await
    .unwrap();
    assert!(refusal(&pool, &f, &f.request).await.contains("no graph"));
    sqlx::query("DELETE FROM campaign_graph_versions WHERE campaign_id=$1")
        .bind(&f.campaign.id)
        .execute(&pool)
        .await
        .unwrap();
    // A terminal history row is representable; a new active replay is not.
    sqlx::query(
        "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,created_at)
        VALUES('fixture-second-initial-attempt',$1,'codex','implementation','failed',NOW())",
    )
    .bind(&f.card)
    .execute(&pool)
    .await
    .unwrap();
    refusal(&pool, &f, &f.request).await;
    pool.close().await;
    db.drop().await;
}
