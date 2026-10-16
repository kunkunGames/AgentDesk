//! Private PostgreSQL fixtures: exhausted occurrence history is never reset.
use super::*;
use crate::db::campaigns::graph::schedule::{self, activation::Activation};
use chrono::{DateTime, Timelike, Utc};

struct Occurrence {
    parent: String,
    child: String,
    goal: Goal,
    first: DateTime<Utc>,
    next: DateTime<Utc>,
}

async fn status(pool: &PgPool, id: &str, status: CampaignStatus) -> i64 {
    let c = campaigns::get(pool, id).await.unwrap();
    let mut input: campaigns::CampaignInput = serde_json::from_value(json!(c)).unwrap();
    input.status = status;
    campaigns::replace(pool, id, c.revision, input)
        .await
        .unwrap()
        .revision
}

async fn occurrence(pool: &PgPool) -> Occurrence {
    let parent = setup(pool).await;
    let first = Utc::now() + chrono::Duration::minutes(2);
    let next = first + chrono::Duration::minutes(2);
    let mut policy = goal().budget;
    policy.max_evolutions = 8;
    schedule::activation::activate(
        pool,
        &parent.id,
        &Activation {
            expected_revision: status(pool, &parent.id, CampaignStatus::Paused).await,
            request_id: "eight-replans".into(),
            evidence: "Private PG fixture: fresh budget per slot".into(),
            schedule: schedule::Schedule {
                timezone: "UTC".into(),
                daily_minutes: [first, next]
                    .iter()
                    .map(|t| (t.hour() * 60 + t.minute()) as u16)
                    .collect(),
            },
            occurrence_budget: Some(policy),
            activate: true,
        },
    )
    .await
    .unwrap();
    let g = store::goal(pool, &parent.id).await.unwrap();
    let child = schedule::admit(pool, &parent.id, &g, first)
        .await
        .unwrap()
        .unwrap();
    Occurrence {
        parent: parent.id,
        child,
        goal: g,
        first,
        next,
    }
}

async fn plans(pool: &PgPool, child: &str, count: u32) {
    for index in 0..count {
        let key = format!("{child}:fixture-plan:{index}");
        let card = store::card_id(&key);
        sqlx::query(
            "INSERT INTO kanban_cards(id,title,status) VALUES($1,'Captured fixture plan','done')",
        )
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,node_id,role,contract_digest,card_id,observation)
            VALUES($1,$2,$1,'plan','fixture-contract',$3,'{\"outcome\":\"blocked\",\"fixture\":true}')")
            .bind(key).bind(child).bind(card).execute(pool).await.unwrap();
    }
    store::hold(pool, child, "Plan evolution budget exhausted")
        .await
        .unwrap();
}

async fn retained(pool: &PgPool, child: &str) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object(
        'tasks',(SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.task_key),'[]') FROM campaign_graph_tasks t WHERE t.campaign_id=$1),
        'versions',(SELECT COALESCE(jsonb_agg(to_jsonb(v) ORDER BY v.version),'[]') FROM campaign_graph_versions v WHERE v.campaign_id=$1),
        'failures',(SELECT COALESCE(jsonb_agg(to_jsonb(f) ORDER BY f.id),'[]') FROM campaign_graph_failures f WHERE f.campaign_id=$1),
        'spec',(SELECT spec FROM campaign_execution_specs WHERE campaign_id=$1))")
        .bind(child).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn exhausted_occurrence_rolls_once_with_fresh_eight_replans_and_all_history_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = occurrence(&pool).await;
    plans(&pool, &f.child, 9).await;
    // Fill the ordinary retained revision window. Rollover must not prune it.
    for _ in 0..10 {
        status(&pool, &f.child, CampaignStatus::Active).await;
    }
    let revisions: Vec<Value> = sqlx::query_scalar(
        "SELECT document FROM campaign_revisions WHERE campaign_id=$1 ORDER BY revision",
    )
    .bind(&f.child)
    .fetch_all(&pool)
    .await
    .unwrap();
    let before = retained(&pool, &f.child).await;
    let (left, right) = tokio::join!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next),
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
    );
    let next = left.unwrap().unwrap();
    assert_eq!(right.unwrap().unwrap(), next);
    assert_ne!(next, f.child);
    assert_eq!(
        campaigns::get(&pool, &f.child).await.unwrap().status,
        CampaignStatus::Cancelled
    );
    assert_eq!(retained(&pool, &f.child).await, before);
    let after: Vec<Value> = sqlx::query_scalar(
        "SELECT document FROM campaign_revisions WHERE campaign_id=$1 ORDER BY revision",
    )
    .bind(&f.child)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(after.len(), revisions.len() + 1);
    assert_eq!(&after[..revisions.len()], revisions.as_slice());
    let reopened = db.connect_and_migrate().await;
    let new_state = store::status(&reopened, &next).await.unwrap();
    assert_eq!(new_state["goal"]["budget"]["max_evolutions"], 8);
    assert_eq!(new_state["control_budget"]["usage"]["published_tasks"], 0);
    assert!(new_state["versions"].as_array().unwrap().is_empty());
    assert!(new_state["tasks"].as_array().unwrap().is_empty());
    assert_eq!(
        schedule::admit(&reopened, &f.parent, &f.goal, f.next)
            .await
            .unwrap(),
        Some(next)
    );
    reopened.close().await;
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn same_slot_manual_pause_other_hold_and_one_remaining_replan_are_preserved_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = occurrence(&pool).await;
    plans(&pool, &f.child, 8).await;
    assert!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
            .await
            .unwrap()
            .is_none()
    );
    // Template-created versions also count; the ninth version exhausts 8 replans.
    sqlx::query("INSERT INTO campaign_graph_versions(campaign_id,version,graph,digest,status) VALUES($1,9,'{\"nodes\":[]}','fixture-rejected-template','rejected')")
        .bind(&f.child).execute(&pool).await.unwrap();
    assert_eq!(
        schedule::admit(&pool, &f.parent, &f.goal, f.first)
            .await
            .unwrap(),
        Some(f.child.clone())
    );
    for state in [CampaignStatus::Paused, CampaignStatus::Planned] {
        status(&pool, &f.child, state).await;
        assert!(
            schedule::admit(&pool, &f.parent, &f.goal, f.next)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(campaigns::get(&pool, &f.child).await.unwrap().status, state);
    }
    status(&pool, &f.child, CampaignStatus::Active).await;
    store::hold(
        &pool,
        &f.child,
        "Execution outcome requires reconciliation; automatic replay is disabled",
    )
    .await
    .unwrap();
    assert!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
            .await
            .unwrap()
            .is_none()
    );
    store::hold(&pool, &f.child, "Plan evolution budget exhausted")
        .await
        .unwrap();
    assert!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
            .await
            .unwrap()
            .is_some()
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn unobserved_tasks_and_unknown_or_infrastructure_outcomes_block_rollover_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = occurrence(&pool).await;
    plans(&pool, &f.child, 9).await;
    let key = format!("{}:fixture-plan:0", f.child);
    sqlx::query("UPDATE campaign_graph_tasks SET observation=NULL WHERE task_key=$1")
        .bind(&key)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("UPDATE campaign_graph_tasks SET observation='{\"outcome\":\"blocked\",\"fixture\":true}' WHERE task_key=$1").bind(&key).execute(&pool).await.unwrap();
    for category in ["unknown", "infrastructure"] {
        sqlx::query("INSERT INTO campaign_graph_failures(id,campaign_id,task_key,fingerprint,category,evidence) VALUES($1,$2,$3,$1,$1,'{\"fixture\":true}')")
            .bind(category).bind(&f.child).bind(&key).execute(&pool).await.unwrap();
        let before = retained(&pool, &f.child).await;
        assert!(
            schedule::admit(&pool, &f.parent, &f.goal, f.next)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(retained(&pool, &f.child).await, before);
    }
    assert_eq!(
        campaigns::get(&pool, &f.child).await.unwrap().status,
        CampaignStatus::Active
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn live_session_dispatch_queue_and_busy_task_lock_block_rollover_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = occurrence(&pool).await;
    plans(&pool, &f.child, 9).await;
    let key = format!("{}:fixture-plan:0", f.child);
    let card = store::card_id(&key);
    sqlx::query("INSERT INTO sessions(session_key,agent_id,provider,status,cwd) VALUES('fixture-live','qwen','qwen','awaiting_user',$1)")
        .bind(format!("/fixture/card-{card}")).execute(&pool).await.unwrap();
    assert!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("UPDATE sessions SET status='idle' WHERE session_key='fixture-live'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO dispatch_queue(kanban_card_id) VALUES($1)")
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("DELETE FROM dispatch_queue WHERE kanban_card_id=$1")
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
    let mut owner = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('campaign-task'),hashtext($1))")
        .bind(&key)
        .execute(&mut *owner)
        .await
        .unwrap();
    assert!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
            .await
            .unwrap()
            .is_none()
    );
    owner.rollback().await.unwrap();
    assert!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
            .await
            .unwrap()
            .is_some()
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn external_effect_requires_independent_verification_before_new_occurrence_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = occurrence(&pool).await;
    plans(&pool, &f.child, 9).await;
    let mut proposal = graph();
    proposal.nodes[0].effect = Effect::External;
    proposal.nodes[1].effect = Effect::External;
    sqlx::query("INSERT INTO campaign_graph_versions(campaign_id,version,graph,digest,status) VALUES($1,1,$2,'fixture-effects','rejected')")
        .bind(&f.child).bind(json!(proposal)).execute(&pool).await.unwrap();
    for n in &proposal.nodes {
        let card = store::card_id(&n.task_key);
        sqlx::query(
            "INSERT INTO kanban_cards(id,title,status) VALUES($1,'Captured fixture work','done')",
        )
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,node_id,role,version,contract_digest,card_id,observation,accepted)
            VALUES($1,$2,$3,'work',1,'fixture-work',$4,'{\"outcome\":\"accepted\",\"fixture\":true}',$5)")
            .bind(&n.task_key).bind(&f.child).bind(&n.id).bind(card).bind(n.kind == Kind::Execute).execute(&pool).await.unwrap();
    }
    assert!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("UPDATE campaign_graph_tasks SET accepted=true WHERE task_key=$1")
        .bind(&proposal.nodes[1].task_key)
        .execute(&pool)
        .await
        .unwrap();
    let before = retained(&pool, &f.child).await;
    assert!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(retained(&pool, &f.child).await, before);
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn new_child_failure_rolls_back_retirement_and_parent_pause_fences_admission_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = occurrence(&pool).await;
    plans(&pool, &f.child, 9).await;
    sqlx::query("UPDATE campaign_execution_specs SET occurrence_budget='{\"max_evolutions\":9}' WHERE campaign_id=$1")
        .bind(&f.parent).execute(&pool).await.unwrap();
    assert!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
            .await
            .is_err()
    );
    assert_eq!(
        campaigns::get(&pool, &f.child).await.unwrap().status,
        CampaignStatus::Active
    );
    status(&pool, &f.parent, CampaignStatus::Paused).await;
    assert!(
        schedule::admit(&pool, &f.parent, &f.goal, f.next)
            .await
            .is_err()
    );
    assert_eq!(
        campaigns::get(&pool, &f.child).await.unwrap().status,
        CampaignStatus::Active
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn recurring_enrollment_accepts_eight_while_single_campaign_keeps_initial_three_pg() {
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    setup(&pool).await;
    let c = campaigns::create(
        &pool,
        "budget-eight".into(),
        serde_json::from_value(json!({"title":"Eight","status":"active","round":1,"nodes":[]}))
            .unwrap(),
    )
    .await
    .unwrap();
    let mut g = goal();
    g.budget.max_evolutions = 8;
    assert!(store::enroll(&pool, &c.id, c.revision, &g).await.is_err());
    g.schedule = Some(schedule::Schedule {
        timezone: "UTC".into(),
        daily_minutes: [0].into(),
    });
    store::enroll(&pool, &c.id, c.revision, &g).await.unwrap();
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["occurrence_budget"]["max_evolutions"],
        8
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn scheduled_child_budget_revision_cannot_bypass_eight_replans_pg() {
    use crate::db::campaigns::graph::revision::{
        self,
        budget::{self, BudgetRevision},
    };
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = occurrence(&pool).await;
    let revision = status(&pool, &f.child, CampaignStatus::Paused).await;
    let before = retained(&pool, &f.child).await;
    let mut expanded = store::goal(&pool, &f.child).await.unwrap();
    expanded.budget.max_evolutions = 9;
    let request = BudgetRevision {
        expected_revision: revision,
        request_id: "over-eight".into(),
        evidence: "Fixture proves a recurring child cannot borrow the next slot budget".into(),
        budget: expanded.budget.clone(),
    };
    assert!(
        budget::extend(&pool, &f.child, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("eight replans")
    );
    let revision_request = revision::Revision {
        expected_revision: revision,
        request_id: "full-over-eight".into(),
        evidence: "Full goal revisions obey the same occurrence boundary".into(),
        goal: expanded,
    };
    assert!(
        revision::revise(&pool, &f.child, &revision_request)
            .await
            .unwrap_err()
            .to_string()
            .contains("eight replans")
    );
    assert_eq!(retained(&pool, &f.child).await, before);
    // A standalone campaign still supports the separately audited finite limit.
    let standalone = campaigns::create(
        &pool,
        "standalone-budget".into(),
        serde_json::from_value(
            json!({"title":"Single goal","status":"active","round":1,"nodes":[]}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    store::enroll(&pool, &standalone.id, standalone.revision, &goal())
        .await
        .unwrap();
    let revision = status(&pool, &standalone.id, CampaignStatus::Paused).await;
    let single = BudgetRevision {
        expected_revision: revision,
        ..request
    };
    budget::extend(&pool, &standalone.id, &single)
        .await
        .unwrap();
    assert_eq!(
        store::goal(&pool, &standalone.id)
            .await
            .unwrap()
            .budget
            .max_evolutions,
        9
    );
    pool.close().await;
    db.drop().await;
}
