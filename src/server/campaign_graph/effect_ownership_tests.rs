//! PostgreSQL native ownership observations with explicit provider-free fixtures.
use super::*;
use crate::db::campaigns::graph::effect_ownership::{self, Envelope, Query};

struct ControlHistory {
    campaign: String,
    task: String,
    card: String,
    dispatch: String,
    failure: String,
    repair: String,
    request: String,
}

async fn reconciled_control(
    pool: &PgPool,
    role: &str,
    status: &str,
    dispatch: &str,
) -> ControlHistory {
    use crate::db::campaigns::graph::recovery::replan::{self, Replan};
    let g = goal();
    // The selected current owner already installed the common fixture agents
    // and settings. Each retained control belongs to a separate native campaign.
    let campaign = campaigns::create(
        pool,
        format!("historical-{role}"),
        serde_json::from_value(
            json!({"title":"Retained control","status":"active","round":1,"nodes":[]}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    store::enroll(pool, &campaign.id, campaign.revision, &g)
        .await
        .unwrap();
    // Task keys are durable identities across campaigns. Retained controls
    // need their own graph, rather than borrowing the selected owner's keys.
    let mut retained_graph = graph();
    retained_graph.nodes[0].kind = Kind::Inspect;
    retained_graph.nodes[0].title = "Inspect retained answer".into();
    retained_graph.nodes[0].details =
        "Read the answer source and record the observed JSON integer without external mutation"
            .into();
    for node in &mut retained_graph.nodes {
        node.task_key = format!("test/repo:{}:history:{role}:{}", campaign.id, node.id);
    }
    let version = store::stage(pool, &campaign.id, &retained_graph, None)
        .await
        .unwrap();
    if role == "plan" {
        sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE campaign_id=$1")
            .bind(&campaign.id)
            .execute(pool)
            .await
            .unwrap();
        store::activate(pool, &campaign.id, version, campaign.revision)
            .await
            .unwrap();
    }
    control(
        pool,
        &campaign.id,
        &g,
        role,
        Some(version),
        "retained-control",
        "Explicit provider-free read-only control fixture",
    )
    .await
    .unwrap();
    let task = format!("{}:{role}:retained-control", campaign.id);
    let card = store::card_id(&task);
    let agent = if role == "plan" {
        &g.planner
    } else {
        &g.verifier
    };
    sqlx::query("UPDATE kanban_cards SET status='in_progress',latest_dispatch_id=$2 WHERE id=$1")
        .bind(&card)
        .bind(dispatch)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at)
        VALUES($1,$2,$3,'implementation',$4,'{}',NOW()-INTERVAL '8 seconds',NULL)")
        .bind(dispatch).bind(&card).bind(agent).bind(status).execute(pool).await.unwrap();
    reconcile::observe(pool, &campaign.id, &g, None)
        .await
        .unwrap();
    super::replan_tests::settle(pool).await;
    let current = campaigns::get(pool, &campaign.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let paused = campaigns::replace(pool, &campaign.id, current.revision, input)
        .await
        .unwrap();
    let state = store::status(pool, &campaign.id).await.unwrap();
    let failure = state["failures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["task_key"] == task)
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let request = format!("native-{role}-history-audit");
    let budget = if role == "plan" {
        let mut expanded = g.budget.clone();
        expanded.max_dispatches += 1;
        Some(expanded)
    } else {
        None
    };
    let repaired = replan::request(pool, &campaign.id, &Replan {
        cancelled_validation: None,
        expected_revision: paused.revision,
        request_id: request.clone(),
        failure_id: failure.clone(),
        evidence: "Explicit provider-free native reconciliation fixture preserves the failed control, NULL terminal timestamp and unknown OS exit; no external effect occurred.".into(),
        budget,
        supersede_candidate: None,
        unattempted_effect: None,
        settled_effect: None,
        rejected_validation: None,
    }).await.unwrap();
    ControlHistory {
        campaign: campaign.id,
        task,
        card,
        dispatch: dispatch.into(),
        failure,
        repair: repaired.repair_failure_id,
        request,
    }
}

#[tokio::test]
async fn effect_ownership_native_reconciled_health_plan_and_upstream_qa_preserve_history_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, mut q) = current(&pool).await;
    let health = reconciled_control(
        &pool,
        "plan",
        "cancelled",
        "24a0c9db-7b55-431c-8ddb-436eed99645e",
    )
    .await;
    let upstream = reconciled_control(
        &pool,
        "validate",
        "failed",
        "1186404a-7486-4488-876a-9621c441e071",
    )
    .await;
    q.owner_campaign_ids = format!("{id},{},{}", health.campaign, upstream.campaign);
    let before = owner_snapshot(&pool).await;
    let p = observed(&pool, &id, &q).await;
    assert!(
        p.effect_ownership.allowed && p.effect_ownership.current_owner_valid,
        "{p:?}"
    );
    assert!(p.effect_ownership.released_effects.is_empty());
    assert!(p.effect_ownership.retired_artifact_obligations.is_empty());
    assert_eq!(p.effect_ownership.reconciled_control_obligations.len(), 2);
    for (history, role, status) in [
        (&health, "plan", "cancelled"),
        (&upstream, "validate", "failed"),
    ] {
        let debt = p
            .effect_ownership
            .reconciled_control_obligations
            .iter()
            .find(|debt| debt.dispatch_id == history.dispatch)
            .unwrap();
        assert_eq!(debt.role, role);
        assert_eq!(debt.status, status);
        assert!(debt.missing_terminal_timestamp);
        assert!(!debt.external_effect_requires_reconciliation);
        assert!(debt.limitation.contains("OS exit remain unknown"));
        assert!(
            debt.limitation
                .contains("No success, effect authority or replay")
        );
        let retained: Value = sqlx::query_scalar("SELECT jsonb_build_object('status',d.status,'completed_at',d.completed_at,'raw',d.result,
            'card_status',k.status,'latest',k.latest_dispatch_id,'accepted',t.accepted)
            FROM task_dispatches d JOIN kanban_cards k ON k.id=d.kanban_card_id JOIN campaign_graph_tasks t ON t.card_id=k.id WHERE d.id=$1")
            .bind(&history.dispatch).fetch_one(&pool).await.unwrap();
        assert_eq!(retained["status"], status);
        assert!(retained["completed_at"].is_null());
        assert_eq!(retained["raw"], "{}");
        assert_eq!(retained["card_status"], "in_progress");
        assert_eq!(retained["latest"], history.dispatch);
        assert_eq!(retained["accepted"], false);
    }
    assert_eq!(
        before,
        owner_snapshot(&pool).await,
        "projection must not mutate native history"
    );
}

#[tokio::test]
async fn effect_ownership_control_history_requires_native_reconciliation_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, mut q) = current(&pool).await;
    let old = reconciled_control(
        &pool,
        "validate",
        "failed",
        "1186404a-7486-4488-876a-9621c441e071",
    )
    .await;
    q.owner_campaign_ids = format!("{id},{}", old.campaign);
    assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    sqlx::query("DELETE FROM campaign_graph_reconciliations WHERE failure_id=$1")
        .bind(&old.failure)
        .execute(&pool)
        .await
        .unwrap();
    let p = observed(&pool, &id, &q).await;
    blocked(&p, "dispatch");
    assert!(p.effect_ownership.reconciled_control_obligations.is_empty());
    assert!(p.effect_ownership.current_owner_valid);
}

#[tokio::test]
async fn effect_ownership_control_history_rejects_foreign_native_request_provenance_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, mut q) = current(&pool).await;
    let old = reconciled_control(
        &pool,
        "plan",
        "cancelled",
        "24a0c9db-7b55-431c-8ddb-436eed99645e",
    )
    .await;
    q.owner_campaign_ids = format!("{id},{}", old.campaign);
    let revision: Value = sqlx::query_scalar(
        "SELECT to_jsonb(r) FROM campaign_goal_revisions r WHERE campaign_id=$1 AND request_id=$2",
    )
    .bind(&old.campaign)
    .bind(&old.request)
    .fetch_one(&pool)
    .await
    .unwrap();
    for mutation in [
        "UPDATE campaign_goal_revisions SET request_digest=repeat('0',64) WHERE campaign_id=$1 AND request_id=$2",
        "UPDATE campaign_goal_revisions SET campaign_revision=campaign_revision+1 WHERE campaign_id=$1 AND request_id=$2",
        "UPDATE campaign_goal_revisions SET evidence='foreign audit' WHERE campaign_id=$1 AND request_id=$2",
        "UPDATE campaign_goal_revisions SET created_at=created_at+INTERVAL '1 second' WHERE campaign_id=$1 AND request_id=$2",
        "UPDATE campaign_goal_revisions SET revised_goal=jsonb_set(revised_goal,'{objective}','\"foreign business scope\"') WHERE campaign_id=$1 AND request_id=$2",
    ] {
        sqlx::query(mutation)
            .bind(&old.campaign)
            .bind(&old.request)
            .execute(&pool)
            .await
            .unwrap();
        blocked(&observed(&pool, &id, &q).await, "dispatch");
        sqlx::query("UPDATE campaign_goal_revisions r SET request_digest=original.request_digest,campaign_revision=original.campaign_revision,
            evidence=original.evidence,created_at=original.created_at,revised_goal=original.revised_goal
            FROM jsonb_populate_record(NULL::campaign_goal_revisions,$1) original WHERE r.campaign_id=original.campaign_id AND r.request_id=original.request_id")
            .bind(&revision).execute(&pool).await.unwrap();
        assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    }
    let repair: Value =
        sqlx::query_scalar("SELECT to_jsonb(f) FROM campaign_graph_failures f WHERE id=$1")
            .bind(&old.repair)
            .fetch_one(&pool)
            .await
            .unwrap();
    for mutation in [
        "UPDATE campaign_graph_failures SET fingerprint=repeat('0',64) WHERE id=$1",
        "UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{operator_replan}','\"arbitrary operator label\"') WHERE id=$1",
        "UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{original_failure}','{}') WHERE id=$1",
    ] {
        sqlx::query(mutation)
            .bind(&old.repair)
            .execute(&pool)
            .await
            .unwrap();
        blocked(&observed(&pool, &id, &q).await, "dispatch");
        sqlx::query("UPDATE campaign_graph_failures f SET fingerprint=original.fingerprint,evidence=original.evidence
            FROM jsonb_populate_record(NULL::campaign_graph_failures,$1) original WHERE f.id=original.id")
            .bind(&repair).execute(&pool).await.unwrap();
        assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    }
}

#[tokio::test]
async fn effect_ownership_control_history_requires_original_native_task_binding_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, mut q) = current(&pool).await;
    let old = reconciled_control(
        &pool,
        "validate",
        "failed",
        "1186404a-7486-4488-876a-9621c441e071",
    )
    .await;
    q.owner_campaign_ids = format!("{id},{}", old.campaign);
    let task: Value =
        sqlx::query_scalar("SELECT to_jsonb(t) FROM campaign_graph_tasks t WHERE task_key=$1")
            .bind(&old.task)
            .fetch_one(&pool)
            .await
            .unwrap();
    for mutation in [
        "UPDATE campaign_graph_tasks SET contract_digest=repeat('0',64) WHERE task_key=$1",
        "UPDATE campaign_graph_tasks SET version=NULL WHERE task_key=$1",
        "UPDATE campaign_graph_tasks SET role='work' WHERE task_key=$1",
        "UPDATE campaign_graph_tasks SET observation=jsonb_set(observation,'{summary}','\"changed original observation\"') WHERE task_key=$1",
    ] {
        sqlx::query(mutation)
            .bind(&old.task)
            .execute(&pool)
            .await
            .unwrap();
        blocked(&observed(&pool, &id, &q).await, "dispatch");
        sqlx::query("UPDATE campaign_graph_tasks t SET contract_digest=original.contract_digest,version=original.version,role=original.role,observation=original.observation
            FROM jsonb_populate_record(NULL::campaign_graph_tasks,$1) original WHERE t.task_key=original.task_key")
            .bind(&task).execute(&pool).await.unwrap();
        assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    }
    let dispatch: Value =
        sqlx::query_scalar("SELECT to_jsonb(d) FROM task_dispatches d WHERE id=$1")
            .bind(&old.dispatch)
            .fetch_one(&pool)
            .await
            .unwrap();
    for mutation in [
        "UPDATE task_dispatches SET created_at=NULL WHERE id=$1",
        "UPDATE task_dispatches SET to_agent_id='qwen' WHERE id=$1",
    ] {
        sqlx::query(mutation)
            .bind(&old.dispatch)
            .execute(&pool)
            .await
            .unwrap();
        blocked(&observed(&pool, &id, &q).await, "dispatch");
        sqlx::query("UPDATE task_dispatches d SET created_at=original.created_at,to_agent_id=original.to_agent_id,status=original.status
            FROM jsonb_populate_record(NULL::task_dispatches,$1) original WHERE d.id=original.id")
            .bind(&dispatch).execute(&pool).await.unwrap();
        assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    }
    let refused = sqlx::query("UPDATE task_dispatches SET status='pending' WHERE id=$1")
        .bind(&old.dispatch)
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(
        refused.as_database_error().unwrap().message(),
        "Terminal graph attempts cannot be replayed"
    );
    assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    let version = task["version"].as_i64().unwrap();
    let graph: Value = sqlx::query_scalar(
        "SELECT graph FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2",
    )
    .bind(&old.campaign)
    .bind(version)
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET graph=jsonb_set(graph,'{nodes,0,task_key}',to_jsonb($3::text)) WHERE campaign_id=$1 AND version=$2")
        .bind(&old.campaign).bind(version).bind(&old.task).execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "dispatch");
    sqlx::query("UPDATE campaign_graph_versions SET graph=$3 WHERE campaign_id=$1 AND version=$2")
        .bind(&old.campaign)
        .bind(version)
        .bind(graph)
        .execute(&pool)
        .await
        .unwrap();
    assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    let failure: Value =
        sqlx::query_scalar("SELECT to_jsonb(f) FROM campaign_graph_failures f WHERE id=$1")
            .bind(&old.failure)
            .fetch_one(&pool)
            .await
            .unwrap();
    for mutation in [
        "UPDATE campaign_graph_failures SET category='code' WHERE id=$1",
        "UPDATE campaign_graph_failures SET category='plan' WHERE id=$1",
        "UPDATE campaign_graph_failures SET created_at=NOW()+INTERVAL '1 minute' WHERE id=$1",
    ] {
        sqlx::query(mutation)
            .bind(&old.failure)
            .execute(&pool)
            .await
            .unwrap();
        blocked(&observed(&pool, &id, &q).await, "dispatch");
        sqlx::query("UPDATE campaign_graph_failures f SET category=original.category,created_at=original.created_at
            FROM jsonb_populate_record(NULL::campaign_graph_failures,$1) original WHERE f.id=original.id")
            .bind(&failure).execute(&pool).await.unwrap();
        assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    }
    let wrong = Query {
        dispatch_id: "not-the-selected-owner".into(),
        ..q
    };
    blocked(&observed(&pool, &id, &wrong).await, "current_owner");
}

#[tokio::test]
async fn effect_ownership_control_history_requires_literal_nonexternal_evidence_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, mut q) = current(&pool).await;
    let old = reconciled_control(
        &pool,
        "validate",
        "failed",
        "1186404a-7486-4488-876a-9621c441e071",
    )
    .await;
    q.owner_campaign_ids = format!("{id},{}", old.campaign);
    let original: Value =
        sqlx::query_scalar("SELECT evidence FROM campaign_graph_failures WHERE id=$1")
            .bind(&old.failure)
            .fetch_one(&pool)
            .await
            .unwrap();
    for flag in [json!(true), json!("false"), Value::Null] {
        // Keep the observation and repair copies equal so this exercises the
        // literal nonexternal boundary, not a different-record mismatch.
        let mut changed = original.clone();
        changed["external_effect_requires_reconciliation"] = flag;
        sqlx::query("UPDATE campaign_graph_failures SET evidence=$2 WHERE id=$1")
            .bind(&old.failure)
            .bind(&changed)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE campaign_graph_tasks SET observation=$2 WHERE task_key=$1")
            .bind(&old.task)
            .bind(&changed)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{original_failure}',$2) WHERE id=$1")
            .bind(&old.repair).bind(&changed).execute(&pool).await.unwrap();
        let p = observed(&pool, &id, &q).await;
        blocked(&p, "dispatch");
        assert!(p.effect_ownership.reconciled_control_obligations.is_empty());
        assert!(p.effect_ownership.released_effects.is_empty());
    }
    sqlx::query("UPDATE campaign_graph_failures SET evidence=$2 WHERE id=$1")
        .bind(&old.failure)
        .bind(&original)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET observation=$2 WHERE task_key=$1")
        .bind(&old.task)
        .bind(&original)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{original_failure}',$2) WHERE id=$1")
        .bind(&old.repair).bind(&original).execute(&pool).await.unwrap();
    assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
}

#[tokio::test]
async fn effect_ownership_reconciled_control_cannot_release_live_retry_or_cleanup_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, mut q) = current(&pool).await;
    let old = reconciled_control(
        &pool,
        "plan",
        "cancelled",
        "24a0c9db-7b55-431c-8ddb-436eed99645e",
    )
    .await;
    q.owner_campaign_ids = format!("{id},{}", old.campaign);
    sqlx::query("INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id) VALUES('old-control-live','qwen','qwen','awaiting_user',$1)")
        .bind(&old.dispatch).execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "session");
    sqlx::query("DELETE FROM sessions WHERE session_key='old-control-live'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO auto_queue_run_cleanup_tasks(run_ids,dispatch_ids) VALUES('{}',ARRAY[$1])",
    )
    .bind(&old.dispatch)
    .execute(&pool)
    .await
    .unwrap();
    blocked(&observed(&pool, &id, &q).await, "cleanup");
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks")
        .execute(&pool)
        .await
        .unwrap();
    assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    let historical_campaign = campaigns::get(&pool, &old.campaign).await.unwrap();
    let mut resumed: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&historical_campaign).unwrap()).unwrap();
    resumed.status = CampaignStatus::Active;
    campaigns::replace(&pool, &old.campaign, historical_campaign.revision, resumed)
        .await
        .unwrap();
    let refused = sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('new-control-attempt',$1,'qwen','implementation','dispatched')")
        .bind(&old.card).execute(&pool).await.unwrap_err();
    assert_eq!(
        refused.as_database_error().unwrap().message(),
        "Graph tasks require a reconciled evolution contract before another attempt"
    );
    // A later unsettled terminal-history fixture is permitted by native
    // admission. No retry was launched, and the older audit cannot retire it.
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('later-terminal-control',$1,'qwen','implementation','failed')")
        .bind(&old.card).execute(&pool).await.unwrap();
    let p = observed(&pool, &id, &q).await;
    blocked(&p, "dispatch");
    assert!(
        p.effect_ownership.reconciled_control_obligations.is_empty(),
        "the old audit cannot retire a later unmatched terminal history record"
    );
    assert!(p.effect_ownership.released_effects.is_empty());
}

async fn current(pool: &PgPool) -> (String, Query) {
    let c = setup(pool).await;
    let mut g = graph();
    g.nodes[0].effect = Effect::External;
    let v = store::stage(pool, &c.id, &g, None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE campaign_id=$1")
        .bind(&c.id)
        .execute(pool)
        .await
        .unwrap();
    store::activate(pool, &c.id, v, c.revision).await.unwrap();
    let n = &g.nodes[0];
    let card = store::publish(
        pool,
        &c.id,
        &store::Publication {
            key: &n.task_key,
            node: &n.id,
            role: "work",
            version: Some(v),
            agent: &n.agent,
            title: &n.title,
            prompt: &n.details,
            digest: &store::task_digest(n).unwrap(),
        },
    )
    .await
    .unwrap();
    // Publication owns a generated native queue. Settle that provider-free
    // fixture before installing the explicitly selected execution owner.
    super::replan_tests::settle(pool).await;
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('current-owner',$1,'qwen','implementation','dispatched')").bind(&card).execute(pool).await.unwrap();
    sqlx::query("UPDATE kanban_cards SET latest_dispatch_id='current-owner',status='in_progress' WHERE id=$1").bind(&card).execute(pool).await.unwrap();
    let q = Query {
        owner_campaign_ids: c.id.clone(),
        active_version: v,
        task_key: n.task_key.clone(),
        contract_digest: store::task_digest(n).unwrap(),
        card_id: card,
        dispatch_id: "current-owner".into(),
        agent_id: "qwen".into(),
    };
    (c.id, q)
}
async fn own_queue(pool: &PgPool, q: &Query) {
    sqlx::query("INSERT INTO auto_queue_runs(id,agent_id,status,unified_thread_channel_id) VALUES('own-run','qwen','active','own-thread')").execute(pool).await.unwrap();
    sqlx::query("INSERT INTO auto_queue_entries(id,run_id,kanban_card_id,agent_id,dispatch_id,status,slot_index,thread_group) VALUES('own-entry','own-run',$1,'qwen','current-owner','dispatched',0,0)").bind(&q.card_id).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO auto_queue_slots(agent_id,slot_index,assigned_run_id,assigned_thread_group) VALUES('qwen',0,'own-run',0)").execute(pool).await.unwrap();
}
async fn sibling(pool: &PgPool, campaign: &str) {
    sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('old-control-card','Old control','done','monitoring')").execute(pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,node_id,role,card_id,contract_digest,observation) VALUES('old:control:task',$1,'old-control','plan','old-control-card',$2,'{}')").bind(campaign).bind(crate::db::campaigns::graph::digest(b"old control contract")).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,completed_at) VALUES('old-control-dispatch','old-control-card','monitoring','implementation','completed',NOW())").execute(pool).await.unwrap();
}
async fn observed(pool: &PgPool, id: &str, q: &Query) -> Envelope {
    effect_ownership::inspect(pool, id, q).await.unwrap()
}
async fn owner_snapshot(pool: &PgPool) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('tasks',(SELECT jsonb_agg(to_jsonb(t)) FROM campaign_graph_tasks t),'dispatches',(SELECT jsonb_agg(to_jsonb(d)) FROM task_dispatches d),'slots',(SELECT jsonb_agg(to_jsonb(s)) FROM auto_queue_slots s),'runs',(SELECT jsonb_agg(to_jsonb(r)) FROM auto_queue_runs r),'sessions',(SELECT jsonb_agg(to_jsonb(s)) FROM sessions s))")
        .fetch_one(pool)
        .await
        .unwrap()
}
fn blocked(p: &Envelope, kind: &str) {
    assert!(!p.effect_ownership.allowed, "{p:?}");
    assert!(
        p.effect_ownership
            .blocking_owners
            .iter()
            .any(|b| b.kind == kind),
        "{p:?}"
    );
}

#[tokio::test]
async fn effect_ownership_exact_current_run_entry_slot_session_is_read_only_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, q) = current(&pool).await;
    own_queue(&pool, &q).await;
    sqlx::query(
        "INSERT INTO auto_queue_runs(id,agent_id,status) VALUES('old-own-run','qwen','completed')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO auto_queue_entries(id,run_id,kanban_card_id,agent_id,dispatch_id,status) VALUES('old-own-entry','old-own-run',$1,'qwen','old-ended-dispatch','done')").bind(&q.card_id).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id,cwd,thread_channel_id) VALUES('own-session','qwen','qwen','turn_active','current-owner',$1,'own-thread')").bind(format!("/fixture/card-{}",q.card_id)).execute(&pool).await.unwrap();
    let before = owner_snapshot(&pool).await;
    let p = observed(&pool, &id, &q).await;
    assert!(
        p.effect_ownership.allowed && p.effect_ownership.current_owner_valid,
        "{p:?}"
    );
    assert_eq!(p.effect_ownership.coverage, effect_ownership::COVERAGE);
    assert!(p.effect_ownership.released_effects.is_empty());
    let after = owner_snapshot(&pool).await;
    assert_eq!(before, after);
}

#[tokio::test]
async fn effect_ownership_bigint_queue_identity_is_read_only_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, q) = current(&pool).await;
    own_queue(&pool, &q).await;
    let slot = i64::from(i32::MAX) + 1;
    let thread = slot + 1;
    sqlx::query("UPDATE auto_queue_entries SET slot_index=$1,thread_group=$2 WHERE id='own-entry'")
        .bind(slot)
        .bind(thread)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE auto_queue_slots SET slot_index=$1,assigned_thread_group=$2 WHERE assigned_run_id='own-run'")
        .bind(slot)
        .bind(thread)
        .execute(&pool)
        .await
        .unwrap();
    let before = owner_snapshot(&pool).await;
    let p = observed(&pool, &id, &q).await;
    assert!(
        p.effect_ownership.allowed && p.effect_ownership.current_owner_valid,
        "{p:?}"
    );
    assert_eq!(before, owner_snapshot(&pool).await);
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn effect_ownership_current_identity_and_unknown_chronology_are_fenced_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, q) = current(&pool).await;
    assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    assert!(
        sqlx::query("UPDATE task_dispatches SET status='running' WHERE id='current-owner'")
            .execute(&pool)
            .await
            .is_err()
    );
    sqlx::query("UPDATE agents SET provider=NULL WHERE id='qwen'")
        .execute(&pool)
        .await
        .unwrap();
    blocked(&observed(&pool, &id, &q).await, "current_owner");
    sqlx::query("UPDATE agents SET provider='qwen' WHERE id='qwen'")
        .execute(&pool)
        .await
        .unwrap();
    let mut changed = q.clone();
    changed.contract_digest = crate::db::campaigns::graph::digest(b"different contract");
    blocked(&observed(&pool, &id, &changed).await, "current_owner");
    changed = q.clone();
    changed.agent_id = "monitoring".into();
    blocked(&observed(&pool, &id, &changed).await, "current_owner");
    sqlx::query("UPDATE task_dispatches SET created_at=NULL WHERE id='current-owner'")
        .execute(&pool)
        .await
        .unwrap();
    blocked(&observed(&pool, &id, &q).await, "current_owner");
    sqlx::query("UPDATE task_dispatches SET created_at=NOW() WHERE id='current-owner'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE kanban_cards SET latest_dispatch_id=NULL WHERE id=$1")
        .bind(&q.card_id)
        .execute(&pool)
        .await
        .unwrap();
    blocked(&observed(&pool, &id, &q).await, "current_owner");
}

#[tokio::test]
async fn effect_ownership_sibling_unknown_null_and_unobserved_control_block_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, q) = current(&pool).await;
    sibling(&pool, &id).await;
    assert!(
        observed(&pool, &id, &q).await.effect_ownership.allowed,
        "terminal observed control history is not external work"
    );
    for status in [None, Some("unknown"), Some("working")] {
        assert!(
            sqlx::query("UPDATE task_dispatches SET status=$1 WHERE id='old-control-dispatch'")
                .bind(status)
                .execute(&pool)
                .await
                .is_err()
        );
    }
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('old-live-dispatch','old-control-card','monitoring','review','pending')").execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "dispatch");
    sqlx::query("DELETE FROM task_dispatches WHERE id='old-live-dispatch'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE task_dispatches SET status='completed' WHERE id='old-control-dispatch'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE campaign_graph_tasks SET observation=NULL WHERE task_key='old:control:task'",
    )
    .execute(&pool)
    .await
    .unwrap();
    blocked(&observed(&pool, &id, &q).await, "observation");
}

#[tokio::test]
async fn effect_ownership_raw_sessions_cwd_thread_foreign_scope_and_duplicates_block_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, q) = current(&pool).await;
    sqlx::query("INSERT INTO sessions(session_key,agent_id,provider,status,cwd) VALUES('ambiguous-session','qwen','qwen',NULL,$1)").bind(format!("/fixture/card-{}",q.card_id)).execute(&pool).await.unwrap();
    for status in [None, Some("awaiting_user"), Some("turn_active")] {
        sqlx::query("UPDATE sessions SET status=$1 WHERE session_key='ambiguous-session'")
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        blocked(&observed(&pool, &id, &q).await, "session");
    }
    sqlx::query(
        "INSERT INTO kanban_cards(id,title,status) VALUES('foreign-card','Unrelated card','done')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE sessions SET active_dispatch_id='current-owner',cwd='/fixture/card-foreign-card' WHERE session_key='ambiguous-session'").execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "session");
    sqlx::query("UPDATE sessions SET cwd=NULL,thread_channel_id='foreign-thread' WHERE session_key='ambiguous-session'").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO auto_queue_runs(id,agent_id,status,unified_thread_id) VALUES('foreign-run','monitoring','completed','foreign-thread')").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO auto_queue_entries(id,run_id,kanban_card_id,agent_id,status) VALUES('foreign-entry','foreign-run','foreign-card','monitoring','done')").execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "session");
    sqlx::query("UPDATE sessions SET thread_channel_id=NULL WHERE session_key='ambiguous-session'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    sqlx::query("INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id) VALUES('duplicate-session','qwen','qwen','turn_active','current-owner')").execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "session");
}

#[tokio::test]
async fn effect_ownership_mixed_runs_unknown_runs_extra_slots_and_native_delivery_block_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, q) = current(&pool).await;
    own_queue(&pool, &q).await;
    assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    sqlx::query(
        "INSERT INTO kanban_cards(id,title,status) VALUES('foreign-card','Foreign','ready')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO auto_queue_entries(id,run_id,kanban_card_id,agent_id,status) VALUES('mixed-entry','own-run','foreign-card','monitoring','done')").execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "run");
    sqlx::query("DELETE FROM auto_queue_entries WHERE id='mixed-entry'")
        .execute(&pool)
        .await
        .unwrap();
    for status in [None, Some("restoring"), Some("unknown")] {
        sqlx::query("UPDATE auto_queue_runs SET status=$1 WHERE id='own-run'")
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        blocked(&observed(&pool, &id, &q).await, "run");
    }
    sqlx::query("UPDATE auto_queue_runs SET status='active' WHERE id='own-run'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO auto_queue_slots(agent_id,slot_index,assigned_run_id,assigned_thread_group) VALUES('qwen',1,'own-run',1)").execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "slot");
    sqlx::query("DELETE FROM auto_queue_slots WHERE slot_index=1")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO dispatch_queue(kanban_card_id) VALUES($1)")
        .bind(&q.card_id)
        .execute(&pool)
        .await
        .unwrap();
    blocked(&observed(&pool, &id, &q).await, "legacy_queue");
    sqlx::query("DELETE FROM dispatch_queue")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO dispatch_outbox(dispatch_id,action,status) VALUES('current-owner','followup','pending')").execute(&pool).await.unwrap();
    for status in ["pending", "processing", "failed", "unknown"] {
        sqlx::query("UPDATE dispatch_outbox SET status=$1")
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        blocked(&observed(&pool, &id, &q).await, "delivery");
    }
    sqlx::query("UPDATE dispatch_outbox SET status='done'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
}

#[tokio::test]
async fn effect_ownership_cleanup_direct_dispatch_card_rollback_and_unknown_stop_block_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, q) = current(&pool).await;
    sqlx::query("INSERT INTO auto_queue_run_cleanup_tasks(run_ids,dispatch_ids) VALUES('{}',ARRAY['current-owner'])").execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "cleanup");
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO auto_queue_run_cleanup_tasks(run_ids,card_rollback_tasks) VALUES('{}',$1)",
    )
    .bind(json!([{"card_id":q.card_id,"dispatch_id":"current-owner"}]))
    .execute(&pool)
    .await
    .unwrap();
    blocked(&observed(&pool, &id, &q).await, "cleanup");
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO auto_queue_run_cleanup_tasks(run_ids,card_rollback_tasks) VALUES('{}','[{\"card_id\":\"unrelated-card\",\"dispatch_id\":\"unrelated-dispatch\"}]')").execute(&pool).await.unwrap();
    assert!(
        observed(&pool, &id, &q).await.effect_ownership.allowed,
        "unrelated valid cleanup is outside this explicit scope"
    );
    sqlx::query("INSERT INTO dispatch_events(dispatch_id,to_status,transition_source,payload_json) VALUES('current-owner','dispatched','campaign_review_owner_stop','{\"outcome\":\"owned_stopped\"}'),('current-owner','dispatched','campaign_review_owner_stop_settled','{\"outcome\":\"owned_stopped\"}')").execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "owner_stop");
}

#[derive(Clone, Copy)]
enum HistoryKind {
    Settled,
    Unattempted,
    ArtifactOnlyUnknown,
}
async fn historical(pool: &PgPool, kind: HistoryKind) -> (String, String, String) {
    let unattempted = matches!(kind, HistoryKind::Unattempted);
    let artifact = matches!(kind, HistoryKind::ArtifactOnlyUnknown);
    use crate::db::campaigns::graph::recovery::replan::{self, Replan, SettledExternalEffect};
    let c = campaigns::create(
        pool,
        "historical-owner".into(),
        serde_json::from_value(
            json!({"title":"Retained partial fixture","status":"active","round":1,"nodes":[]}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    store::enroll(pool, &c.id, c.revision, &goal())
        .await
        .unwrap();
    let mut g = graph();
    for n in &mut g.nodes {
        n.task_key = format!("historic:{}", n.task_key);
    }
    g.nodes[0].effect = if artifact {
        Effect::ArtifactOnly
    } else {
        Effect::External
    };
    let v = store::stage(pool, &c.id, &g, None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE campaign_id=$1")
        .bind(&c.id)
        .execute(pool)
        .await
        .unwrap();
    store::activate(pool, &c.id, v, c.revision).await.unwrap();
    let n = &g.nodes[0];
    let card = store::publish(
        pool,
        &c.id,
        &store::Publication {
            key: &n.task_key,
            node: &n.id,
            role: "work",
            version: Some(v),
            agent: &n.agent,
            title: &n.title,
            prompt: &n.details,
            digest: &store::task_digest(n).unwrap(),
        },
    )
    .await
    .unwrap();
    let raw = if artifact {
        "Historical artifact-only receipt unavailable; original OS exit remains unknown".into()
    } else {
        json!({"summary":crate::db::campaigns::graph::result::CAPTURE_SUMMARY,"work_outcome":"artifact","completion_source":"campaign_native_receipt","campaign_receipt":{"outcome":"blocked","failure_category":if unattempted {"plan"} else {"code"},"summary":"Explicit retained blocked fixture; no historical missing exit is invented","observations":[{"command":"retained fixture","exit_code":1,"evidence":"Explicit provider-free native receipt fixture"}]}}).to_string()
    };
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,completed_at) VALUES('retained-partial',$1,'qwen','implementation',$3,$2,CASE WHEN $3='completed' THEN NOW() END)").bind(&card).bind(&raw).bind(if artifact {"failed"} else {"completed"}).execute(pool).await.unwrap();
    reconcile::observe(pool, &c.id, &goal(), None)
        .await
        .unwrap();
    // Preserve the failed receipt while ending its generated native queue
    // through the same cleanup owner used by the other recovery fixtures.
    super::replan_tests::settle(pool).await;
    let now = campaigns::get(pool, &c.id).await.unwrap();
    let mut input: campaigns::CampaignInput = serde_json::from_value(json!(now)).unwrap();
    input.status = CampaignStatus::Paused;
    let paused = campaigns::replace(pool, &c.id, now.revision, input)
        .await
        .unwrap();
    let state = store::status(pool, &c.id).await.unwrap();
    let pin = crate::db::campaigns::graph::digest(b"immutable complete configured fixture state");
    let configured = json!([{"id":"fixture-target","expected_sha256":pin}]);
    let bundle = json!({"schema_version":1,"campaign_id":c.id,"task_key":n.task_key,"contract_digest":store::task_digest(n).unwrap(),
        "goal_scope_sha256":crate::db::campaigns::graph::learning::scope_digest(&goal()).unwrap(),"source_sha256":pin,"configuration_sha256":pin,
        "target_set_sha256":contract_digest(&configured).unwrap(),"configured_targets":configured,"observed_at":chrono::Utc::now().to_rfc3339(),
        "targets":[{"id":"fixture-target","state":"converged","expected_sha256":pin,"current_sha256":pin,"prior_evidence_sha256":pin}]});
    let audit:SettledExternalEffect=serde_json::from_value(json!({"dispatch_id":"retained-partial","result_sha256":crate::db::campaigns::graph::digest(raw.as_bytes()),
        "audit_sha256":contract_digest(&bundle).unwrap(),"bundle":bundle,"operator_attestation":{"complete_target_coverage":true,"current_state_verified":true,"no_unknown_effects":true}})).unwrap();
    let request=Replan {cancelled_validation:None,rejected_validation:None,expected_revision:paused.revision,request_id:"read-only-native-projection-audit".into(),failure_id:state["failures"][0]["id"].as_str().unwrap().into(),
        evidence:"Explicit native PostgreSQL fixture administrator attests complete current target state, preserving missing original exit; no provider or live external effect occurred.".into(),
        budget:None,supersede_candidate:None,
        unattempted_effect:unattempted.then(||replan::UnattemptedEffect{dispatch_id:"retained-partial".into(),result_sha256:crate::db::campaigns::graph::digest(raw.as_bytes()),audit_sha256:pin}),
        settled_effect:matches!(kind,HistoryKind::Settled).then_some(audit)};
    replan::request_as_server_admin(pool, &c.id, &request)
        .await
        .unwrap();
    (c.id, n.task_key.clone(), raw)
}

#[tokio::test]
async fn effect_ownership_authenticated_release_rechecks_exact_retained_raw_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, mut q) = current(&pool).await;
    let (old, key, raw) = historical(&pool, HistoryKind::Settled).await;
    q.owner_campaign_ids = format!("{id},{old}");
    let before = observed(&pool, &id, &q).await;
    assert!(before.effect_ownership.allowed, "{before:?}");
    let releases = &before.effect_ownership.released_effects;
    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0].task_key, key);
    assert_eq!(
        releases[0].current_result_sha256,
        crate::db::campaigns::graph::digest(raw.as_bytes())
    );
    sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='retained-partial'")
        .bind(format!("{raw} "))
        .execute(&pool)
        .await
        .unwrap();
    let changed = observed(&pool, &id, &q).await;
    assert!(
        !changed.effect_ownership.allowed && changed.effect_ownership.released_effects.is_empty()
    );
    assert_eq!(changed.effect_ownership.unresolved_effects.len(), 1);
}

#[tokio::test]
async fn effect_ownership_reconciled_artifact_without_exit_allows_new_owner_but_keeps_debt_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, mut q) = current(&pool).await;
    let (old, _, raw) = historical(&pool, HistoryKind::ArtifactOnlyUnknown).await;
    q.owner_campaign_ids = format!("{id},{old}");
    let p = observed(&pool, &id, &q).await;
    assert!(p.effect_ownership.allowed, "{p:?}");
    assert!(p.effect_ownership.released_effects.is_empty());
    assert_eq!(p.effect_ownership.retired_artifact_obligations.len(), 1);
    assert!(p.effect_ownership.retired_artifact_obligations[0].missing_terminal_timestamp);
    assert!(
        p.effect_ownership.retired_artifact_obligations[0]
            .limitation
            .contains("OS exit remain unknown")
    );
    let (retained, ended): (Option<String>, bool) = sqlx::query_as(
        "SELECT result,completed_at IS NOT NULL FROM task_dispatches WHERE id='retained-partial'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(retained.as_deref(), Some(raw.as_str()));
    assert!(!ended);
    sqlx::query("INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id) VALUES('old-still-live','qwen','qwen','awaiting_user','retained-partial')").execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "session");
    sqlx::query("UPDATE sessions SET status=NULL WHERE session_key='old-still-live'")
        .execute(&pool)
        .await
        .unwrap();
    blocked(&observed(&pool, &id, &q).await, "session");
    sqlx::query("DELETE FROM sessions WHERE session_key='old-still-live'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO auto_queue_runs(id,agent_id,status) VALUES('retired-run','qwen','cancelled')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO auto_queue_entries(id,run_id,kanban_card_id,agent_id,status) SELECT 'retired-entry','retired-run',kanban_card_id,'qwen','done' FROM task_dispatches WHERE id='retained-partial'").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO auto_queue_slots(agent_id,slot_index,assigned_run_id) VALUES('qwen',7,'retired-run')").execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "slot");
    sqlx::query("DELETE FROM auto_queue_slots WHERE slot_index=7")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO auto_queue_run_cleanup_tasks(run_ids,dispatch_ids) VALUES('{}',ARRAY['retained-partial'])").execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "cleanup");
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        sqlx::query("UPDATE task_dispatches SET status='unknown' WHERE id='retained-partial'")
            .execute(&pool)
            .await
            .is_err()
    );
    sqlx::query("UPDATE campaign_graph_tasks SET observation=jsonb_set(observation,'{summary}','\"changed observation\"') WHERE campaign_id=$1").bind(&old).execute(&pool).await.unwrap();
    blocked(&observed(&pool, &id, &q).await, "dispatch");
    sqlx::query("UPDATE campaign_graph_tasks t SET observation=f.evidence FROM campaign_graph_failures f WHERE t.campaign_id=$1 AND f.campaign_id=t.campaign_id AND f.task_key=t.task_key AND f.evidence->>'dispatch_id'='retained-partial'").bind(&old).execute(&pool).await.unwrap();
    assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    sqlx::query("DELETE FROM campaign_graph_reconciliations WHERE campaign_id=$1")
        .bind(&old)
        .execute(&pool)
        .await
        .unwrap();
    blocked(&observed(&pool, &id, &q).await, "dispatch");
}

#[tokio::test]
async fn effect_ownership_release_goal_authority_latest_attempt_and_terminal_fences_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, mut q) = current(&pool).await;
    let (old, _, _) = historical(&pool, HistoryKind::Settled).await;
    q.owner_campaign_ids = format!("{old},{id}");
    sqlx::query("UPDATE campaign_execution_specs SET spec=jsonb_set(spec,'{objective}','\"changed objective\"') WHERE campaign_id=$1").bind(&old).execute(&pool).await.unwrap();
    let p = observed(&pool, &id, &q).await;
    assert!(p.effect_ownership.released_effects.is_empty() && !p.effect_ownership.allowed);
    sqlx::query("UPDATE campaign_execution_specs SET spec=$2 WHERE campaign_id=$1")
        .bind(&old)
        .bind(serde_json::to_value(goal()).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{settled_effect_authority}','\"untrusted agent label\"') WHERE campaign_id=$1 AND evidence ? 'operator_replan'").bind(&old).execute(&pool).await.unwrap();
    assert!(
        observed(&pool, &id, &q)
            .await
            .effect_ownership
            .released_effects
            .is_empty()
    );
    sqlx::query("UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{settled_effect_authority}','\"configured_server_admin_bearer\"') WHERE campaign_id=$1 AND evidence ? 'operator_replan'").bind(&old).execute(&pool).await.unwrap();
    assert!(observed(&pool, &id, &q).await.effect_ownership.allowed);
    sqlx::query("UPDATE task_dispatches SET completed_at=NULL WHERE id='retained-partial'")
        .execute(&pool)
        .await
        .unwrap();
    blocked(&observed(&pool, &id, &q).await, "dispatch");
    sqlx::query("UPDATE task_dispatches SET completed_at=NOW() WHERE id='retained-partial'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at) SELECT 'later-historical-attempt',kanban_card_id,to_agent_id,dispatch_type,'completed',result,NOW()+INTERVAL '1 second',NOW() FROM task_dispatches WHERE id='retained-partial'").execute(&pool).await.unwrap();
    let p = observed(&pool, &id, &q).await;
    assert!(!p.effect_ownership.allowed && p.effect_ownership.released_effects.is_empty());
}

#[tokio::test]
async fn effect_ownership_unattempted_null_audit_fields_and_null_repairs_do_not_mask_proof_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, mut q) = current(&pool).await;
    let (old, key, _) = historical(&pool, HistoryKind::Unattempted).await;
    q.owner_campaign_ids = format!("{id},{old}");
    let p = observed(&pool, &id, &q).await;
    assert!(
        p.effect_ownership.allowed && p.effect_ownership.released_effects.len() == 1,
        "{p:?}"
    );
    sqlx::query("INSERT INTO campaign_graph_failures(id,campaign_id,task_key,fingerprint,category,evidence)
        SELECT 'ordinary-null-repair-'||i,$1,$2,'ordinary-null-fingerprint-'||i,'plan',
        jsonb_build_object('operator_replan','ordinary-'||i,'reconciled_failure_id',original.id,'settled_effect',NULL,'unattempted_effect',NULL)
        FROM generate_series(1,40) i CROSS JOIN LATERAL (SELECT id FROM campaign_graph_failures WHERE campaign_id=$1 AND NOT evidence ? 'operator_replan' LIMIT 1) original")
        .bind(&old).bind(&key).execute(&pool).await.unwrap();
    let p = observed(&pool, &id, &q).await;
    assert!(
        p.effect_ownership.allowed && !p.effect_ownership.truncated,
        "{p:?}"
    );
    assert_eq!(p.effect_ownership.released_effects.len(), 1);
    sqlx::query("UPDATE dispatch_outbox SET status='done'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE task_dispatches SET result=result||' ' WHERE id='retained-partial'")
        .execute(&pool)
        .await
        .unwrap();
    let p = observed(&pool, &id, &q).await;
    assert!(!p.effect_ownership.allowed && p.effect_ownership.released_effects.is_empty());
}

#[tokio::test]
async fn effect_ownership_finite_coverage_never_clears_a_truncated_scope_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, q) = current(&pool).await;
    sqlx::query("INSERT INTO kanban_cards(id,title,status) SELECT 'extra-card-'||i,'Unused fixture task','done' FROM generate_series(1,256) i").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,node_id,role,card_id,contract_digest,observation) SELECT 'z:extra:task:'||i,$1,'extra-'||i,'plan','extra-card-'||i,$2,'{}' FROM generate_series(1,256) i").bind(&id).bind(crate::db::campaigns::graph::digest(b"fixture extra task")).execute(&pool).await.unwrap();
    let p = observed(&pool, &id, &q).await;
    assert!(p.effect_ownership.truncated && !p.effect_ownership.allowed);
    blocked(&p, "coverage");
}

#[tokio::test]
async fn effect_ownership_get_route_uses_existing_auth_and_typed_no_store_shape_pg() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (id, q) = current(&pool).await;
    let mut config = crate::config::Config::default();
    config.server.host = "0.0.0.0".into();
    config.server.auth_token = Some("fixture-admin".into());
    let tx = crate::eventbus::new_broadcast();
    let buffer = crate::eventbus::spawn_batch_flusher(tx.clone());
    let app = crate::server::routes::api_router_with_pg(
        crate::engine::PolicyEngine::new(&config).unwrap(),
        config,
        tx,
        buffer,
        None,
        Some(pool),
    );
    let url = format!(
        "/campaigns/{id}/execution/effect-ownership?owner_campaign_ids={id}&active_version={}&task_key={}&contract_digest={}&card_id={}&dispatch_id={}&agent_id={}",
        q.active_version, q.task_key, q.contract_digest, q.card_id, q.dispatch_id, q.agent_id
    );
    let response = app
        .clone()
        .oneshot(Request::builder().uri(&url).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert!(
        !response.status().is_success(),
        "protected projection cannot be read without existing auth"
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&url)
                .header("authorization", "Bearer fixture-admin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        body["effect_ownership"]["schema"],
        "campaign-effect-ownership/v1"
    );
    assert_eq!(body["effect_ownership"]["current_task_key"], q.task_key);
    assert_eq!(
        body["effect_ownership"]["current_contract_digest"],
        q.contract_digest
    );
    assert_eq!(body["effect_ownership"]["allowed"], true);
    assert!(
        body["effect_ownership"]["limitations"]
            .as_str()
            .unwrap()
            .contains("Not caller execution authentication")
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&url)
                .header("authorization", "Bearer fixture-admin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[test]
fn effect_ownership_query_preserves_graph_task_key_contract_and_bounds_scope() {
    let q = Query {
        owner_campaign_ids: "campaign".into(),
        active_version: 1,
        task_key: "저장소/작업 키".into(),
        contract_digest: crate::db::campaigns::graph::digest(b"contract"),
        card_id: "card".into(),
        dispatch_id: "dispatch".into(),
        agent_id: "agent".into(),
    };
    assert_eq!(q.validate("campaign").unwrap(), ["campaign"]);
    for scope in ["other", "campaign,campaign", "campaign,a,b,c,d", ""] {
        let mut invalid = q.clone();
        invalid.owner_campaign_ids = scope.into();
        assert!(invalid.validate("campaign").is_err());
    }
}
