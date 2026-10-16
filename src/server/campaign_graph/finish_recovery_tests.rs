//! Disposable native fixtures test explicit admin recovery, never private-command proof.
use super::*;
use crate::db::campaigns::graph::{digest, effect_ownership, recovery::finish};

const ATTEMPT: &str = "fixture-quota-finish";

struct Fixture {
    campaign: campaigns::Campaign,
    graph: Graph,
    key: String,
    request: finish::Recovery,
}

fn quota_error() -> Value {
    // This literal models the actual retained producer envelope. The fixture's
    // attestation is explicit and does not claim hash telemetry proves commands.
    json!({"error":"⚠️ provider가 응답을 완료하지 못했어요.\n같은 요청을 다시 시도해 주세요. 문제가 반복되면 `!clear`로 세션을 초기화한 뒤 다시 보내 주세요.\n||**상세**\n```text\nError: Your workspace is out of credits. Add credits to continue.\n```||"})
}

async fn fixture(pool: &PgPool) -> Fixture {
    let (campaign, graph, version) = accepted_prefix(pool, true).await;
    for (index, node) in graph.nodes.iter().enumerate().skip(2) {
        publish_work(pool, &campaign.id, version, node).await;
        accept_fixture_work(
            pool,
            &campaign.id,
            &graph,
            node,
            &format!("fixture-work-{index}"),
        )
        .await;
    }
    super::super::replan_tests::settle(pool).await;
    acknowledge_fixture_status_reactions(pool, &campaign.id, &["fixture-work-2", "fixture-work-3"])
        .await;
    // The real failing owner is Codex. Only this disposable independent final
    // executor changes provider; the already accepted business history remains.
    sqlx::query("UPDATE agents SET provider='codex' WHERE id='monitoring'")
        .execute(pool)
        .await
        .unwrap();
    let (_, key) = propose_complete(pool, &campaign.id).await;
    acknowledge_fixture_status_reactions(pool, &campaign.id, &["fixture-manager-complete"]).await;
    let card = store::card_id(&key);
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,created_at)
        VALUES($1,$2,'monitoring','implementation','pending',NOW())",
    )
    .bind(ATTEMPT)
    .bind(&card)
    .execute(pool)
    .await
    .unwrap();
    progress::tool_use(
        Some(pool),
        Some(ATTEMPT),
        Some("fixture-read-1"),
        "read-input-hash",
    )
    .await;
    progress::tool_use(
        Some(pool),
        Some(ATTEMPT),
        Some("fixture-read-2"),
        "read-receipt-hash",
    )
    .await;
    assert_eq!(
        crate::dispatch::set_dispatch_status_on_pg_async(
            pool,
            ATTEMPT,
            "failed",
            Some(&quota_error()),
            "turn_bridge_dispatch_failure",
            Some(&["pending"]),
            false
        )
        .await
        .unwrap(),
        1
    );
    reconcile::observe(pool, &campaign.id, &goal(), None)
        .await
        .unwrap();
    super::super::replan_tests::settle(pool).await;
    acknowledge_fixture_status_reactions(pool, &campaign.id, &[ATTEMPT]).await;
    let current = campaigns::get(pool, &campaign.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let campaign = campaigns::replace(pool, &campaign.id, current.revision, input)
        .await
        .unwrap();
    let state = store::status(pool, &campaign.id).await.unwrap();
    let failure = state["failures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|failure| failure["evidence"]["dispatch_id"] == ATTEMPT)
        .unwrap();
    assert_eq!(failure["evidence"]["runtime"]["observed_tool_calls"], 2);
    let raw: String = sqlx::query_scalar("SELECT result::text FROM task_dispatches WHERE id=$1")
        .bind(ATTEMPT)
        .fetch_one(pool)
        .await
        .unwrap();
    let rows = sqlx::query(
        "SELECT event_id,fingerprint,output_digest FROM campaign_graph_progress
        WHERE dispatch_id=$1 ORDER BY event_id",
    )
    .bind(ATTEMPT)
    .fetch_all(pool)
    .await
    .unwrap();
    let progress: Vec<(String, String, Option<String>)> = rows
        .iter()
        .map(|row| {
            (
                row.get("event_id"),
                row.get("fingerprint"),
                row.get("output_digest"),
            )
        })
        .collect();
    let request = finish::Recovery {
        expected_revision: campaign.revision,
        request_id: "fixture-explicit-read-only-quota-audit".into(),
        failure_id: failure["id"].as_str().unwrap().into(),
        evidence: "Explicit disposable administrator fixture attests retained read-only commands and provider repair. Hashes alone are not command proof; original failure, missing completion timestamp and lifetime usage remain.".into(),
        failed_finish: finish::FailedFinish {
            dispatch_id: ATTEMPT.into(), graph_version: version,
            graph_digest: state["versions"][0]["digest"].as_str().unwrap().into(),
            raw_result_sha256: digest(raw.as_bytes()),
            contract_digest: failure["evidence"]["runtime"]["contract_digest"].as_str().unwrap().into(),
            progress_sha256: contract_digest(&progress).unwrap(),
        },
        operator_attestation: finish::OperatorAttestation {
            read_only_execution_verified: true,no_unknown_effects: true,provider_cause_repaired: true,
        },
    };
    Fixture {
        campaign,
        graph,
        key,
        request,
    }
}

async fn refusal(pool: &PgPool, fixture: &Fixture, request: &finish::Recovery) {
    let _ = refusal_reason(pool, fixture, request).await;
}

async fn refusal_reason(pool: &PgPool, fixture: &Fixture, request: &finish::Recovery) -> String {
    let before = canonical_inspection(store::status(pool, &fixture.campaign.id).await.unwrap());
    let error = finish::request_as_server_admin(pool, &fixture.campaign.id, request)
        .await
        .unwrap_err();
    assert_eq!(
        canonical_inspection(store::status(pool, &fixture.campaign.id).await.unwrap()),
        before
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_goal_revisions")
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
    error.to_string()
}

async fn projection(pool: &PgPool, fixture: &Fixture) -> effect_ownership::Projection {
    let node = &fixture.graph.nodes[0];
    effect_ownership::inspect(
        pool,
        &fixture.campaign.id,
        &effect_ownership::Query {
            owner_campaign_ids: fixture.campaign.id.clone(),
            active_version: fixture.request.failed_finish.graph_version,
            task_key: node.task_key.clone(),
            contract_digest: store::task_digest(node).unwrap(),
            card_id: store::card_id(&node.task_key),
            dispatch_id: "fixture-work-0".into(),
            agent_id: node.agent.clone(),
        },
    )
    .await
    .unwrap()
    .effect_ownership
}

#[tokio::test]
async fn finish_recovery_preserves_history_usage_and_requires_new_independent_goal_verifier_pg() {
    let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = database.connect_and_migrate().await;
    let fixture = fixture(&pool).await;
    let before = store::status(&pool, &fixture.campaign.id).await.unwrap();
    let original: Value =
        sqlx::query_scalar("SELECT to_jsonb(d) FROM task_dispatches d WHERE id=$1")
            .bind(ATTEMPT)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(original["completed_at"].is_null());
    let recovered = finish::request_as_server_admin(&pool, &fixture.campaign.id, &fixture.request)
        .await
        .unwrap();
    assert_eq!(recovered.revision, fixture.campaign.revision + 1);
    let after = store::status(&pool, &fixture.campaign.id).await.unwrap();
    for field in [
        "goal",
        "active_version",
        "versions",
        "tasks",
        "budget_enforcement",
        "observed_tool_calls",
        "elapsed_dispatch_seconds",
    ] {
        assert_eq!(after[field], before[field], "preserved {field}");
    }
    assert!(after["hold_reason"].is_null());
    assert_eq!(
        campaigns::get(&pool, &fixture.campaign.id)
            .await
            .unwrap()
            .status,
        CampaignStatus::Paused
    );
    assert_eq!(
        finish::request_as_server_admin(&pool, &fixture.campaign.id, &fixture.request)
            .await
            .unwrap()
            .revision,
        recovered.revision
    );
    let mut different = fixture.request.clone();
    different.evidence.push_str(" changed");
    assert!(
        finish::request_as_server_admin(&pool, &fixture.campaign.id, &different)
            .await
            .is_err()
    );
    let history = projection(&pool, &fixture).await;
    assert!(
        history
            .reconciled_control_obligations
            .iter()
            .any(|item| item.dispatch_id == ATTEMPT
                && item.role == "finish"
                && item.status == "failed"
                && item.missing_terminal_timestamp)
    );
    assert!(
        !history
            .blocking_owners
            .iter()
            .any(|item| item.id == ATTEMPT)
    );
    assert!(
        !workflow::store::complete(&pool, &fixture.campaign.id, &fixture.key)
            .await
            .is_ok()
    );
    let current = campaigns::get(&pool, &fixture.campaign.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
    input.status = CampaignStatus::Active;
    campaigns::replace(&pool, &fixture.campaign.id, current.revision, input)
        .await
        .unwrap();
    let (frontier, manager_key) = manager_turn(&pool, &fixture.campaign.id).await;
    let mut receipt = fixture_receipt("Explicit new manager fixture reuses accepted business work");
    receipt["workflow_decision"] = json!({"snapshot_digest":frontier.digest().unwrap(),
        "action":"complete","reason":"Unchanged accepted fixture proof; a new independent finish is still required"});
    let _workspace = terminal_fixture(
        &pool,
        &manager_key,
        "fixture-manager-recovered",
        &receipt,
        None,
    )
    .await;
    reconcile::observe(&pool, &fixture.campaign.id, &goal(), None)
        .await
        .unwrap();
    manager::admit(&pool, &fixture.campaign.id, &goal())
        .await
        .unwrap();
    let new_finish = format!(
        "{}:finish:{}-goal",
        fixture.campaign.id,
        frontier.digest().unwrap()
    );
    assert_ne!(new_finish, fixture.key);
    assess_goal(&pool, &fixture.campaign.id, &frontier, &new_finish, true).await;
    super::super::replan_tests::settle(&pool).await;
    acknowledge_fixture_status_reactions(
        &pool,
        &fixture.campaign.id,
        &["fixture-manager-recovered", "fixture-goal-assessment"],
    )
    .await;
    assert!(
        workflow::store::complete(&pool, &fixture.campaign.id, &new_finish)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, Value>("SELECT to_jsonb(d) FROM task_dispatches d WHERE id=$1")
            .bind(ATTEMPT)
            .fetch_one(&pool)
            .await
            .unwrap(),
        original
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT accepted FROM campaign_graph_tasks WHERE task_key=$1"
        )
        .bind(&fixture.key)
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    pool.close().await;
    database.drop().await;
}

#[tokio::test]
async fn finish_recovery_rejects_missing_admin_attestation_stale_pins_and_other_errors_pg() {
    let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = database.connect_and_migrate().await;
    let fixture = fixture(&pool).await;
    assert!(
        finish::request(&pool, &fixture.campaign.id, &fixture.request)
            .await
            .is_err()
    );
    for case in [
        "read_only",
        "effects",
        "repair",
        "revision",
        "raw",
        "progress",
        "contract",
        "graph",
    ] {
        let mut request = fixture.request.clone();
        match case {
            "read_only" => request.operator_attestation.read_only_execution_verified = false,
            "effects" => request.operator_attestation.no_unknown_effects = false,
            "repair" => request.operator_attestation.provider_cause_repaired = false,
            "revision" => request.expected_revision -= 1,
            "raw" => request.failed_finish.raw_result_sha256 = "1".repeat(64),
            "progress" => request.failed_finish.progress_sha256 = "1".repeat(64),
            "contract" => request.failed_finish.contract_digest = "1".repeat(64),
            "graph" => request.failed_finish.graph_digest = "1".repeat(64),
            _ => unreachable!(),
        }
        refusal(&pool, &fixture, &request).await;
    }
    for other in [
        json!({"error":"Your workspace is out of credits. Add credits to continue."}),
        json!({"error":"User cancelled"}),
        json!({"error":quota_error()["error"],"unknown_effect":true}),
    ] {
        sqlx::query("UPDATE task_dispatches SET result=$2 WHERE id=$1")
            .bind(ATTEMPT)
            .bind(other.to_string())
            .execute(&pool)
            .await
            .unwrap();
        let raw: String =
            sqlx::query_scalar("SELECT result::text FROM task_dispatches WHERE id=$1")
                .bind(ATTEMPT)
                .fetch_one(&pool)
                .await
                .unwrap();
        let mut request = fixture.request.clone();
        request.failed_finish.raw_result_sha256 = digest(raw.as_bytes());
        refusal(&pool, &fixture, &request).await;
    }
    pool.close().await;
    database.drop().await;
}

#[tokio::test]
async fn finish_recovery_rejects_provenance_authority_cleanup_and_budget_drift_pg() {
    let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = database.connect_and_migrate().await;
    let fixture = fixture(&pool).await;
    let attempt: Value =
        sqlx::query_scalar("SELECT to_jsonb(d) FROM task_dispatches d WHERE id=$1")
            .bind(ATTEMPT)
            .fetch_one(&pool)
            .await
            .unwrap();
    // The legacy native schema permits missing dispatch chronology. Refuse
    // those rows normally instead of panicking while decoding SQL NULL booleans.
    sqlx::query("UPDATE task_dispatches SET updated_at=NULL WHERE id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    refusal(&pool, &fixture, &fixture.request).await;
    sqlx::query("UPDATE task_dispatches SET updated_at=$2::text::timestamptz WHERE id=$1")
        .bind(ATTEMPT)
        .bind(attempt["updated_at"].as_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE task_dispatches SET created_at=NULL WHERE id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    refusal(&pool, &fixture, &fixture.request).await;
    sqlx::query("UPDATE task_dispatches SET created_at=$2::text::timestamptz WHERE id=$1")
        .bind(ATTEMPT)
        .bind(attempt["created_at"].as_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    let profile: Value =
        sqlx::query_scalar("SELECT execution_profile FROM campaign_graph_tasks WHERE task_key=$1")
            .bind(&fixture.key)
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=NULL WHERE task_key=$1")
        .bind(&fixture.key)
        .execute(&pool)
        .await
        .unwrap();
    refusal(&pool, &fixture, &fixture.request).await;
    sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=$2 WHERE task_key=$1")
        .bind(&fixture.key)
        .bind(profile)
        .execute(&pool)
        .await
        .unwrap();
    let event: Value = sqlx::query_scalar(
        "SELECT to_jsonb(e) FROM dispatch_events e WHERE dispatch_id=$1 AND to_status='failed'",
    )
    .bind(ATTEMPT)
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE dispatch_events SET transition_source='operator_status_patch' WHERE id=$1")
        .bind(event["id"].as_i64().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    refusal(&pool, &fixture, &fixture.request).await;
    sqlx::query(
        "UPDATE dispatch_events SET transition_source='turn_bridge_dispatch_failure' WHERE id=$1",
    )
    .bind(event["id"].as_i64().unwrap())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id)
        VALUES('fixture-finish-owner','monitoring','codex','awaiting_bg',$1)",
    )
    .bind(ATTEMPT)
    .execute(&pool)
    .await
    .unwrap();
    refusal(&pool, &fixture, &fixture.request).await;
    sqlx::query("DELETE FROM sessions WHERE session_key='fixture-finish-owner'")
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
    refusal(&pool, &fixture, &fixture.request).await;
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks WHERE id=$1")
        .bind(cleanup)
        .execute(&pool)
        .await
        .unwrap();
    let malformed: i64 = sqlx::query_scalar(
        "INSERT INTO auto_queue_run_cleanup_tasks(run_ids,pending_emits)
        VALUES('{}','{\"unidentified\":true}') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    refusal(&pool, &fixture, &fixture.request).await;
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks WHERE id=$1")
        .bind(malformed)
        .execute(&pool)
        .await
        .unwrap();
    let node = &fixture.graph.nodes[0];
    sqlx::query("UPDATE campaign_graph_tasks SET accepted=false WHERE task_key=$1")
        .bind(&node.task_key)
        .execute(&pool)
        .await
        .unwrap();
    refusal(&pool, &fixture, &fixture.request).await;
    sqlx::query("UPDATE campaign_graph_tasks SET accepted=true WHERE task_key=$1")
        .bind(&node.task_key)
        .execute(&pool)
        .await
        .unwrap();
    let card = store::card_id(&fixture.key);
    // A published graph card's executor is immutable. Test its actual database
    // boundary instead of fabricating a state migration 0144 forbids.
    let before_card: Value =
        sqlx::query_scalar("SELECT to_jsonb(k) FROM kanban_cards k WHERE id=$1")
            .bind(&card)
            .fetch_one(&pool)
            .await
            .unwrap();
    let before_guard =
        canonical_inspection(store::status(&pool, &fixture.campaign.id).await.unwrap());
    let mut tx = pool.begin().await.unwrap();
    let error = sqlx::query("UPDATE kanban_cards SET assigned_agent_id='qwen' WHERE id=$1")
        .bind(&card)
        .execute(&mut *tx)
        .await
        .unwrap_err();
    let database_error = error
        .as_database_error()
        .expect("database contract refusal");
    assert_eq!(database_error.code().as_deref(), Some("23514"));
    assert_eq!(
        database_error.constraint(),
        Some("campaign_graph_card_contract_immutable")
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, Value>("SELECT to_jsonb(k) FROM kanban_cards k WHERE id=$1")
            .bind(&card)
            .fetch_one(&pool)
            .await
            .unwrap(),
        before_card
    );
    assert_eq!(
        canonical_inspection(store::status(&pool, &fixture.campaign.id).await.unwrap()),
        before_guard
    );
    // Retained terminal dispatch recipients can drift independently of the card.
    // Refuse that representable mismatch through the native recovery API too.
    sqlx::query("UPDATE task_dispatches SET to_agent_id='qwen' WHERE id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        refusal_reason(&pool, &fixture, &fixture.request).await,
        "native independent finish executor, goal, prompt or frozen input changed"
    );
    sqlx::query("UPDATE task_dispatches SET to_agent_id=$2 WHERE id=$1")
        .bind(ATTEMPT)
        .bind(attempt["to_agent_id"].as_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, Value>("SELECT to_jsonb(d) FROM task_dispatches d WHERE id=$1")
            .bind(ATTEMPT)
            .fetch_one(&pool)
            .await
            .unwrap(),
        attempt
    );
    sqlx::query(
        "INSERT INTO campaign_graph_progress(dispatch_id,event_id,fingerprint)
        SELECT 'fixture-work-0','budget-'||n,n::text FROM generate_series(1,20) n",
    )
    .execute(&pool)
    .await
    .unwrap();
    refusal(&pool, &fixture, &fixture.request).await;
    pool.close().await;
    database.drop().await;
}

#[tokio::test]
async fn finish_recovery_history_does_not_release_changed_raw_progress_or_audit_pg() {
    let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = database.connect_and_migrate().await;
    let fixture = fixture(&pool).await;
    let recovered = finish::request_as_server_admin(&pool, &fixture.campaign.id, &fixture.request)
        .await
        .unwrap();
    assert_eq!(
        projection(&pool, &fixture)
            .await
            .reconciled_control_obligations
            .len(),
        1
    );
    let raw: String = sqlx::query_scalar("SELECT result::text FROM task_dispatches WHERE id=$1")
        .bind(ATTEMPT)
        .fetch_one(&pool)
        .await
        .unwrap();
    // Retained raw dispatch results are TEXT. Shared ownership inspection must
    // never parse malformed historical text in SQL or release its obligation.
    sqlx::query("UPDATE task_dispatches SET result=$2 WHERE id=$1")
        .bind(ATTEMPT)
        .bind("retained transport frame: not JSON")
        .execute(&pool)
        .await
        .unwrap();
    let malformed = projection(&pool, &fixture).await;
    assert!(malformed.reconciled_control_obligations.is_empty());
    assert!(
        malformed
            .blocking_owners
            .iter()
            .any(|item| item.id == ATTEMPT)
    );
    sqlx::query("UPDATE task_dispatches SET result=$2 WHERE id=$1")
        .bind(ATTEMPT)
        .bind(&raw)
        .execute(&pool)
        .await
        .unwrap();
    let payload: Value = sqlx::query_scalar(
        "SELECT payload_json FROM dispatch_events WHERE dispatch_id=$1 AND to_status='failed'",
    )
    .bind(ATTEMPT)
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE dispatch_events SET payload_json='{}' WHERE dispatch_id=$1 AND to_status='failed'",
    )
    .bind(ATTEMPT)
    .execute(&pool)
    .await
    .unwrap();
    let changed_payload = projection(&pool, &fixture).await;
    assert!(changed_payload.reconciled_control_obligations.is_empty());
    assert!(
        changed_payload
            .blocking_owners
            .iter()
            .any(|item| item.id == ATTEMPT)
    );
    sqlx::query(
        "UPDATE dispatch_events SET payload_json=$2 WHERE dispatch_id=$1 AND to_status='failed'",
    )
    .bind(ATTEMPT)
    .bind(payload)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        projection(&pool, &fixture)
            .await
            .reconciled_control_obligations
            .len(),
        1
    );
    sqlx::query("UPDATE task_dispatches SET result='{}' WHERE id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        projection(&pool, &fixture)
            .await
            .reconciled_control_obligations
            .is_empty()
    );
    sqlx::query("UPDATE task_dispatches SET result=$2 WHERE id=$1")
        .bind(ATTEMPT)
        .bind(raw)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_progress SET output_digest='changed' WHERE dispatch_id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        projection(&pool, &fixture)
            .await
            .reconciled_control_obligations
            .is_empty()
    );
    sqlx::query("UPDATE campaign_graph_progress SET output_digest=NULL WHERE dispatch_id=$1")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        projection(&pool, &fixture)
            .await
            .reconciled_control_obligations
            .len(),
        1
    );
    sqlx::query(
        "UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,
        '{operator_attestation,no_unknown_effects}','false') WHERE id=$1",
    )
    .bind(&recovered.recovery_id)
    .execute(&pool)
    .await
    .unwrap();
    let projection = projection(&pool, &fixture).await;
    assert!(projection.reconciled_control_obligations.is_empty());
    assert!(
        projection
            .blocking_owners
            .iter()
            .any(|item| item.id == ATTEMPT)
    );
    pool.close().await;
    database.drop().await;
}
