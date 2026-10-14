//! Persisted recurrence policy, audited changes and stale-tick admission fences.
use super::*;
use crate::db::campaigns::graph::{
    revision::budget::{self, BudgetRevision},
    schedule::{self, activation::Activation},
};
use chrono::Timelike;

async fn pause(pool: &PgPool, id: &str) -> i64 {
    let c = campaigns::get(pool, id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    campaigns::replace(pool, id, c.revision, input)
        .await
        .unwrap()
        .revision
}

fn request(revision: i64, id: &str, times: &[chrono::DateTime<chrono::Utc>]) -> Activation {
    Activation {
        expected_revision: revision,
        request_id: id.into(),
        evidence:
            "Audited fixture recurrence: bounded work remains independent of parent review spending"
                .into(),
        schedule: schedule::Schedule {
            timezone: "UTC".into(),
            daily_minutes: times
                .iter()
                .map(|t| (t.hour() * 60 + t.minute()) as u16)
                .collect(),
        },
        occurrence_budget: None,
        activate: true,
    }
}

#[tokio::test]
async fn parent_budget_extension_preserves_occurrence_policy_and_fences_stale_ticks_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let future = chrono::Utc::now() + chrono::Duration::minutes(2);
    let mut first = request(pause(&pool, &c.id).await, "initial", &[future]);
    first.activate = false;
    assert!(
        schedule::activation::activate(&pool, &c.id, &first)
            .await
            .unwrap_err()
            .to_string()
            .contains("existing recurring parent")
    );
    assert!(store::goal(&pool, &c.id).await.unwrap().schedule.is_none());
    assert_eq!(
        campaigns::get(&pool, &c.id).await.unwrap().revision,
        first.expected_revision
    );
    first.activate = true;
    schedule::activation::activate(&pool, &c.id, &first)
        .await
        .unwrap();
    let before = store::goal(&pool, &c.id).await.unwrap();
    let mut expanded = before.budget.clone();
    expanded.max_elapsed_secs *= 4;
    let change = BudgetRevision {
        expected_revision: pause(&pool, &c.id).await,
        request_id: "parent-reviews-only".into(),
        evidence: "More finite parent review capacity; keep every occurrence at its original limit"
            .into(),
        budget: expanded,
    };
    let revision = budget::extend(&pool, &c.id, &change).await.unwrap();
    let resume = request(revision, "resume", &[future]);
    schedule::activation::activate(&pool, &c.id, &resume)
        .await
        .unwrap();
    let current = store::goal(&pool, &c.id).await.unwrap();
    assert!(
        schedule::admit(&pool, &c.id, &before, future)
            .await
            .unwrap()
            .is_none()
    );
    let child = schedule::admit(&pool, &c.id, &current, future)
        .await
        .unwrap()
        .unwrap();
    let reopened = fixture.connect_and_migrate().await;
    let child_goal = store::goal(&reopened, &child).await.unwrap();
    assert_eq!(
        serde_json::to_value(&child_goal.budget).unwrap(),
        serde_json::to_value(&before.budget).unwrap()
    );
    assert_eq!(
        crate::db::campaigns::graph::learning::scope_digest(&current).unwrap(),
        crate::db::campaigns::graph::learning::scope_digest(&child_goal).unwrap()
    );
    let state = store::status(&reopened, &c.id).await.unwrap();
    assert_eq!(state["goal"]["budget"]["max_elapsed_secs"], 2400);
    assert_eq!(state["occurrence_budget"]["max_elapsed_secs"], 600);
    assert_eq!(state["occurrences"][0]["status"], "active");
    assert_eq!(state["occurrences"][0]["sampled"], false);
    assert_eq!(
        state["schedule_revisions"][0]["previous_occurrence_budget"],
        state["occurrence_budget"]
    );
    assert_eq!(
        state["schedule_revisions"][0]["revised_occurrence_budget"],
        state["occurrence_budget"]
    );
    assert_eq!(
        budget::extend(&pool, &c.id, &change).await.unwrap(),
        revision
    );
    reopened.close().await;
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn explicit_occurrence_budget_is_audited_without_changing_existing_child_or_overlapping_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let first_time = chrono::Utc::now() + chrono::Duration::minutes(2);
    let next_time = first_time + chrono::Duration::minutes(2);
    let mut first = request(
        pause(&pool, &c.id).await,
        "initial",
        &[first_time, next_time],
    );
    let mut policy = goal().budget;
    policy.max_elapsed_secs = 900;
    first.occurrence_budget = Some(policy.clone());
    schedule::activation::activate(&pool, &c.id, &first)
        .await
        .unwrap();
    let child = schedule::admit(
        &pool,
        &c.id,
        &store::goal(&pool, &c.id).await.unwrap(),
        first_time,
    )
    .await
    .unwrap()
    .unwrap();
    let revision = pause(&pool, &c.id).await;
    let mut revised = request(revision, "revised", &[first_time, next_time]);
    let mut bad = policy.clone();
    bad.max_evolutions = 9;
    revised.occurrence_budget = Some(bad);
    assert!(
        schedule::activation::activate(&pool, &c.id, &revised)
            .await
            .is_err()
    );
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["occurrence_budget"],
        json!(policy)
    );
    policy.max_elapsed_secs = 1200;
    policy.max_evolutions = 8;
    revised.occurrence_budget = Some(policy.clone());
    revised.activate = false;
    let admission_before: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT schedule_admission_started_at FROM campaign_execution_specs WHERE campaign_id=$1",
    )
    .bind(&c.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let revision = schedule::activation::activate(&pool, &c.id, &revised)
        .await
        .unwrap();
    assert_eq!(
        schedule::activation::activate(&pool, &c.id, &revised)
            .await
            .unwrap(),
        revision
    );
    assert_eq!(
        campaigns::get(&pool, &c.id).await.unwrap().status,
        CampaignStatus::Paused
    );
    assert_eq!(sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>(
        "SELECT schedule_admission_started_at FROM campaign_execution_specs WHERE campaign_id=$1",
    ).bind(&c.id).fetch_one(&pool).await.unwrap(), admission_before);
    assert!(
        schedule::admit(
            &pool,
            &c.id,
            &store::goal(&pool, &c.id).await.unwrap(),
            next_time
        )
        .await
        .is_err()
    );
    revised.occurrence_budget.as_mut().unwrap().max_elapsed_secs += 1;
    assert!(
        schedule::activation::activate(&pool, &c.id, &revised)
            .await
            .is_err()
    );
    assert_eq!(
        store::goal(&pool, &child)
            .await
            .unwrap()
            .budget
            .max_elapsed_secs,
        900
    );
    let resume = request(revision, "resume-policy-eight", &[first_time, next_time]);
    schedule::activation::activate(&pool, &c.id, &resume)
        .await
        .unwrap();
    let parent_goal = store::goal(&pool, &c.id).await.unwrap();
    assert!(
        schedule::admit(&pool, &c.id, &parent_goal, next_time)
            .await
            .unwrap()
            .is_none()
    );
    let old = campaigns::get(&pool, &child).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&old).unwrap()).unwrap();
    // No work was executed in this fixture; close it through operator cancellation.
    input.status = CampaignStatus::Cancelled;
    campaigns::replace(&pool, &child, old.revision, input)
        .await
        .unwrap();
    let next = schedule::admit(&pool, &c.id, &parent_goal, next_time)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store::goal(&pool, &next)
            .await
            .unwrap()
            .budget
            .max_elapsed_secs,
        1200
    );
    assert_eq!(
        store::goal(&pool, &next)
            .await
            .unwrap()
            .budget
            .max_evolutions,
        8
    );
    assert_eq!(
        store::goal(&pool, &child)
            .await
            .unwrap()
            .budget
            .max_evolutions,
        2
    );
    let state = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(
        state["schedule_revisions"][1]["previous_occurrence_budget"]["max_elapsed_secs"],
        900
    );
    assert_eq!(
        state["schedule_revisions"][1]["revised_occurrence_budget"],
        json!(policy)
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn migration_preserves_original_legacy_occurrence_budget_and_bad_policy_fails_closed_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let future = chrono::Utc::now() + chrono::Duration::minutes(2);
    let activate = request(pause(&pool, &c.id).await, "legacy-initial", &[future]);
    schedule::activation::activate(&pool, &c.id, &activate)
        .await
        .unwrap();
    // Construct the previous schema and a parent whose lifetime budget was enlarged.
    sqlx::raw_sql("ALTER TABLE campaign_execution_specs DROP COLUMN occurrence_budget;
        ALTER TABLE campaign_schedule_revisions DROP COLUMN previous_occurrence_budget;
        ALTER TABLE campaign_schedule_revisions DROP COLUMN revised_occurrence_budget;
        DELETE FROM _sqlx_migrations WHERE version=141;
        UPDATE campaign_execution_specs SET spec=jsonb_set(spec,'{budget,max_elapsed_secs}','2400');")
        .execute(&pool).await.unwrap();
    let reopened = fixture.connect_and_migrate().await;
    let state = store::status(&reopened, &c.id).await.unwrap();
    assert_eq!(state["goal"]["budget"]["max_elapsed_secs"], 2400);
    assert_eq!(state["occurrence_budget"]["max_elapsed_secs"], 600);
    sqlx::query("UPDATE campaign_execution_specs SET occurrence_budget=$2 WHERE campaign_id=$1")
        .bind(&c.id)
        .bind(json!({"max_dispatches":2000000}))
        .execute(&reopened)
        .await
        .unwrap();
    let g = store::goal(&reopened, &c.id).await.unwrap();
    assert!(schedule::admit(&reopened, &c.id, &g, future).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_occurrences")
            .fetch_one(&reopened)
            .await
            .unwrap(),
        0
    );
    reopened.close().await;
    pool.close().await;
    fixture.drop().await;
}

#[test]
fn activation_legacy_request_digest_is_preserved() {
    let legacy = json!({"expected_revision":3,"request_id":"legacy","evidence":"retained audit",
        "schedule":{"timezone":"UTC","daily_minutes":[0]}});
    let parsed: Activation = serde_json::from_value(legacy.clone()).unwrap();
    assert!(parsed.activate);
    assert_eq!(serde_json::to_value(&parsed).unwrap(), legacy);
    assert_eq!(
        contract_digest(&parsed).unwrap(),
        contract_digest(&legacy).unwrap()
    );
    let paused = Activation {
        activate: false,
        ..parsed
    };
    assert_eq!(serde_json::to_value(&paused).unwrap()["activate"], false);
    assert_ne!(
        contract_digest(&paused).unwrap(),
        contract_digest(&legacy).unwrap()
    );
}
