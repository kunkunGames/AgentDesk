//! Internal cancellation may retire a candidate, never authenticate missing QA.
use super::*;
use crate::db::campaigns::graph::recovery::replan::{self, CancelledValidation, Replan};

const ATTEMPT: &str = "internally-cancelled-validator";

struct CancelledFixture {
    campaign: campaigns::Campaign,
    request: Replan,
    graph: Graph,
    version: i64,
    raw: String,
}

async fn cancelled(pool: &PgPool, parent: bool) -> CancelledFixture {
    validator_attempt(pool, parent, Kind::Inspect, Effect::ArtifactOnly, true).await
}

async fn validator_attempt(
    pool: &PgPool,
    parent: bool,
    root_kind: Kind,
    root_effect: Effect,
    internal_cancellation: bool,
) -> CancelledFixture {
    let (mut campaign, mut proposed, source_failure) = if parent {
        let (c, proposed, _, request) =
            super::replan_tests::held_with_category(pool, false, "plan").await;
        super::replan_tests::settle(pool).await;
        let mut input: campaigns::CampaignInput =
            serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
        input.status = CampaignStatus::Active;
        let c = campaigns::replace(pool, &c.id, c.revision, input)
            .await
            .unwrap();
        (c, proposed, Some(request.failure_id))
    } else {
        (setup(pool).await, graph(), None)
    };
    proposed.nodes[0].kind = root_kind;
    proposed.nodes[0].effect = root_effect;
    if parent {
        for node in &mut proposed.nodes {
            node.task_key.push_str(":cancelled-successor");
        }
    }
    let version = store::stage(pool, &campaign.id, &proposed, source_failure.as_deref())
        .await
        .unwrap();
    control(
        pool,
        &campaign.id,
        &goal(),
        "validate",
        Some(version),
        &version.to_string(),
        "Independently inspect the pinned candidate; do not execute business work",
    )
    .await
    .unwrap();
    let key = format!("{}:validate:{version}", campaign.id);
    let card = store::card_id(&key);
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    // Model the same dispatched native QA role as the real cold-start failure.
    // Context has no campaign role fields; immutable native task/profile owns it.
    sqlx::query(
        "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,created_at)
        VALUES($1,$2,'monitoring','implementation','dispatched',NOW()-INTERVAL '8 seconds')",
    )
    .bind(ATTEMPT)
    .bind(&card)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE auto_queue_entries SET status='dispatched',dispatch_id=$2,dispatched_at=NOW()
        WHERE kanban_card_id=$1",
    )
    .bind(&card)
    .bind(ATTEMPT)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE auto_queue_runs SET status='active' WHERE id IN
        (SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=$1)",
    )
    .bind(&card)
    .execute(pool)
    .await
    .unwrap();
    if internal_cancellation {
        assert_eq!(
            crate::dispatch::cancel_dispatch_and_reset_auto_queue_on_pg(
                pool,
                ATTEMPT,
                Some("turn_bridge_cancelled")
            )
            .await
            .unwrap(),
            1
        );
    } else {
        assert_eq!(
            crate::dispatch::set_dispatch_status_on_pg_async(
                pool,
                ATTEMPT,
                "failed",
                Some(&json!({})),
                "test_failed_validator",
                Some(&["dispatched"]),
                false,
            )
            .await
            .unwrap(),
            1
        );
    }
    reconcile::observe(pool, &campaign.id, &goal(), None)
        .await
        .unwrap();
    super::replan_tests::settle(pool).await;
    campaign = campaigns::get(pool, &campaign.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&campaign).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    campaign = campaigns::replace(pool, &campaign.id, campaign.revision, input)
        .await
        .unwrap();
    let state = store::status(pool, &campaign.id).await.unwrap();
    let failure = state["failures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|failure| failure["task_key"] == key)
        .unwrap();
    assert_eq!(failure["category"], "unknown");
    assert_eq!(failure["evidence"]["runtime"]["observed_tool_calls"], 0);
    let raw: String = sqlx::query_scalar("SELECT result FROM task_dispatches WHERE id=$1")
        .bind(ATTEMPT)
        .fetch_one(pool)
        .await
        .unwrap();
    let request = Replan {
        expected_revision: campaign.revision,
        request_id: "audited-internal-cancellation-repair".into(),
        failure_id: failure["id"].as_str().unwrap().into(),
        evidence: "Verified the canonical anonymous internal cancellation and unchanged native validation scope. Preserve the unknown cancelled turn, its candidate and lifetime usage. The existing planner must propose a correction and obtain new independent QA before business execution.".into(),
        budget: None,
        supersede_candidate: None,
        unattempted_effect: None,
        settled_effect: None,
        rejected_validation: None,
        cancelled_validation: internal_cancellation.then(|| CancelledValidation {
            dispatch_id: ATTEMPT.into(),
            graph_version: version,
            graph_digest: state["versions"][0]["digest"].as_str().unwrap().into(),
            raw_result_sha256: crate::db::campaigns::graph::digest(raw.as_bytes()),
            contract_digest: failure["evidence"]["runtime"]["contract_digest"]
                .as_str().unwrap().into(),
        }),
    };
    CancelledFixture {
        campaign,
        request,
        graph: proposed,
        version,
        raw,
    }
}

async fn unchanged_rejection(pool: &PgPool, fixture: &CancelledFixture) -> String {
    let before = canonical_inspection(store::status(pool, &fixture.campaign.id).await.unwrap());
    let error = replan::request(pool, &fixture.campaign.id, &fixture.request)
        .await
        .unwrap_err()
        .to_string();
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
    error
}

async fn immutable_card_rejection(pool: &PgPool, fixture: &CancelledFixture, statement: &str) {
    let card = store::card_id(&format!(
        "{}:validate:{}",
        fixture.campaign.id, fixture.version
    ));
    let before_card: Value =
        sqlx::query_scalar("SELECT to_jsonb(k) FROM kanban_cards k WHERE id=$1")
            .bind(&card)
            .fetch_one(pool)
            .await
            .unwrap();
    let before = canonical_inspection(store::status(pool, &fixture.campaign.id).await.unwrap());
    // Published card contracts cannot drift through a legal Kanban writer.
    // Assert the production boundary rather than fabricating a protected edit.
    let mut tx = pool.begin().await.unwrap();
    let error = sqlx::query(statement)
        .bind(&card)
        .execute(&mut *tx)
        .await
        .unwrap_err();
    let database_error = error.as_database_error().unwrap();
    assert_eq!(database_error.code().as_deref(), Some("23514"));
    assert_eq!(
        database_error.constraint(),
        Some("campaign_graph_card_contract_immutable")
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, Value>("SELECT to_jsonb(k) FROM kanban_cards k WHERE id=$1")
            .bind(&card)
            .fetch_one(pool)
            .await
            .unwrap(),
        before_card
    );
    assert_eq!(
        canonical_inspection(store::status(pool, &fixture.campaign.id).await.unwrap()),
        before
    );
}

#[tokio::test]
async fn cancelled_validator_replan_preserves_unknown_attempt_parent_budget_and_requires_fresh_qa_pg()
 {
    for parent in [false, true] {
        let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
        let pool = database.connect_and_migrate().await;
        let mut fixture = cancelled(&pool, parent).await;
        let before =
            canonical_inspection(store::status(&pool, &fixture.campaign.id).await.unwrap());
        let retained: Value = sqlx::query_scalar("SELECT jsonb_build_object('dispatch',to_jsonb(d),
            'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.id) FROM dispatch_events e WHERE e.dispatch_id=d.id),
            'entries',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.id) FROM auto_queue_entries e WHERE e.dispatch_id=d.id))
            FROM task_dispatches d WHERE d.id=$1").bind(ATTEMPT).fetch_one(&pool).await.unwrap();
        assert!(retained["dispatch"]["completed_at"].is_null());
        let reply = replan::request(&pool, &fixture.campaign.id, &fixture.request)
            .await
            .unwrap();
        assert!(reply.goal_scope_preserved && reply.independent_validation_required);
        assert_eq!(reply.planner_agent_id, "qwen");
        assert_eq!(
            replan::request(&pool, &fixture.campaign.id, &fixture.request)
                .await
                .unwrap()
                .revision,
            reply.revision
        );
        let after = canonical_inspection(store::status(&pool, &fixture.campaign.id).await.unwrap());
        for field in [
            "tasks",
            "goal",
            "active_version",
            "occurrences",
            "observed_tool_calls",
            "elapsed_dispatch_seconds",
        ] {
            assert_eq!(before[field], after[field], "{field}");
        }
        let mut expected_versions = before["versions"].clone();
        expected_versions[0]["status"] = json!("rejected");
        assert_eq!(after["versions"], expected_versions);
        for failure in before["failures"].as_array().unwrap() {
            assert!(after["failures"].as_array().unwrap().contains(failure));
        }
        assert!(after["hold_reason"].is_null());
        assert_eq!(sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('dispatch',to_jsonb(d),
            'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.id) FROM dispatch_events e WHERE e.dispatch_id=d.id),
            'entries',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.id) FROM auto_queue_entries e WHERE e.dispatch_id=d.id))
            FROM task_dispatches d WHERE d.id=$1").bind(ATTEMPT).fetch_one(&pool).await.unwrap(), retained);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT result FROM task_dispatches WHERE id=$1")
                .bind(ATTEMPT)
                .fetch_one(&pool)
                .await
                .unwrap(),
            fixture.raw
        );
        let paused = campaigns::get(&pool, &fixture.campaign.id).await.unwrap();
        assert_eq!(paused.status, CampaignStatus::Paused);
        let mut input: campaigns::CampaignInput =
            serde_json::from_value(serde_json::to_value(&paused).unwrap()).unwrap();
        input.status = CampaignStatus::Active;
        let active = campaigns::replace(&pool, &paused.id, paused.revision, input)
            .await
            .unwrap();
        control(
            &pool,
            &active.id,
            &goal(),
            "plan",
            Some(after["active_version"].as_i64().unwrap_or(0)),
            &reply.repair_failure_id,
            "Independently inspect cancellation cause and correct the plan",
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT assigned_agent_id FROM kanban_cards WHERE id=$1"
            )
            .bind(store::card_id(&format!(
                "{}:plan:{}",
                active.id, reply.repair_failure_id
            )))
            .fetch_one(&pool)
            .await
            .unwrap()
            .as_deref(),
            Some("qwen")
        );
        for node in &mut fixture.graph.nodes {
            node.task_key.push_str(":fresh-qa-required");
        }
        let next = store::stage(
            &pool,
            &active.id,
            &fixture.graph,
            Some(&reply.repair_failure_id),
        )
        .await
        .unwrap();
        assert_eq!(next, fixture.version + 1);
        assert!(
            store::activate(&pool, &active.id, next, active.revision)
                .await
                .unwrap_err()
                .to_string()
                .contains("only validated versions")
        );
        control(
            &pool,
            &active.id,
            &goal(),
            "validate",
            Some(next),
            &next.to_string(),
            "A different agent must validate the new candidate",
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT assigned_agent_id FROM kanban_cards WHERE id=$1"
            )
            .bind(store::card_id(&format!("{}:validate:{next}", active.id)))
            .fetch_one(&pool)
            .await
            .unwrap()
            .as_deref(),
            Some("monitoring")
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='work'"
            )
            .bind(&active.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            i64::from(parent)
        );
        super::replan_tests::settle(&pool).await;
        pool.close().await;
        database.drop().await;
    }
}

#[tokio::test]
async fn cancelled_validator_replan_requires_explicit_exact_pins_and_preserves_legacy_serialization_pg()
 {
    let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = database.connect_and_migrate().await;
    let mut fixture = cancelled(&pool, true).await;
    let original = serde_json::to_value(&fixture.request).unwrap();
    for field in [
        "dispatch_id",
        "graph_version",
        "graph_digest",
        "raw_result_sha256",
        "contract_digest",
        "missing-audit",
    ] {
        let mut altered = original.clone();
        if field == "missing-audit" {
            altered
                .as_object_mut()
                .unwrap()
                .remove("cancelled_validation");
        } else if field == "graph_version" {
            altered["cancelled_validation"][field] = json!(fixture.version + 1);
        } else {
            altered["cancelled_validation"][field] = json!("0".repeat(64));
        }
        fixture.request = serde_json::from_value(altered).unwrap();
        unchanged_rejection(&pool, &fixture).await;
        if field == "missing-audit" {
            assert!(
                serde_json::to_value(&fixture.request)
                    .unwrap()
                    .get("cancelled_validation")
                    .is_none(),
                "absent optional audit must not change legacy request digests"
            );
        }
    }
    pool.close().await;
    database.drop().await;
}

#[tokio::test]
async fn cancelled_validator_replan_rejects_user_stops_missing_or_conflicting_native_provenance_pg()
{
    for boundary in [
        "user-stop",
        "missing-event",
        "provider-event",
        "conflicting-terminal",
        "unknown-chronology",
        "successful-result",
    ] {
        let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
        let pool = database.connect_and_migrate().await;
        let mut fixture = cancelled(&pool, true).await;
        match boundary {
            "user-stop" | "successful-result" => {
                let result = if boundary == "user-stop" {
                    json!({"reason":"user_stop"})
                } else {
                    json!({"reason":"turn_bridge_cancelled", "campaign_receipt":{"outcome":"passed"}})
                };
                let raw = result.to_string();
                sqlx::query("UPDATE task_dispatches SET result=$2 WHERE id=$1")
                    .bind(ATTEMPT)
                    .bind(&raw)
                    .execute(&pool)
                    .await
                    .unwrap();
                sqlx::query("UPDATE dispatch_events SET payload_json=$2 WHERE dispatch_id=$1 AND to_status='cancelled'")
                    .bind(ATTEMPT).bind(result).execute(&pool).await.unwrap();
                fixture
                    .request
                    .cancelled_validation
                    .as_mut()
                    .unwrap()
                    .raw_result_sha256 = crate::db::campaigns::graph::digest(raw.as_bytes());
            }
            "missing-event" => {
                sqlx::query("DELETE FROM dispatch_events WHERE dispatch_id=$1")
                    .bind(ATTEMPT)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "provider-event" => {
                sqlx::query("UPDATE dispatch_events SET transition_source='provider_claim' WHERE dispatch_id=$1")
                .bind(ATTEMPT).execute(&pool).await.unwrap();
            }
            "conflicting-terminal" => {
                sqlx::query("INSERT INTO dispatch_events(dispatch_id,from_status,to_status,transition_source)
                VALUES($1,'dispatched','completed','watcher_streaming_final')")
                .bind(ATTEMPT).execute(&pool).await.unwrap();
            }
            "unknown-chronology" => {
                sqlx::query("UPDATE task_dispatches SET created_at=NULL WHERE id=$1")
                    .bind(ATTEMPT)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        unchanged_rejection(&pool, &fixture).await;
        pool.close().await;
        database.drop().await;
    }
}

#[tokio::test]
async fn cancelled_validator_replan_rejects_live_owners_execution_role_drift_and_business_publication_pg()
 {
    for boundary in [
        "working-owner",
        "observed-tool",
        "published-business",
        "different-parent",
        "executor-drift",
        "profile-drift",
        "role-drift",
        "accepted-validator",
        "approved-candidate",
        "metadata-drift",
        "newer-dispatch",
    ] {
        let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
        let pool = database.connect_and_migrate().await;
        let fixture = cancelled(&pool, true).await;
        let key = format!("{}:validate:{}", fixture.campaign.id, fixture.version);
        match boundary {
            "working-owner" => {
                sqlx::query(
                    "INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id)
                VALUES('still-owned','monitoring','codex','turn_active',$1)",
                )
                .bind(ATTEMPT)
                .execute(&pool)
                .await
                .unwrap();
            }
            "observed-tool" => {
                progress::tool_use(
                    Some(&pool),
                    Some(ATTEMPT),
                    Some("late-observation"),
                    "input-hash",
                )
                .await;
            }
            "published-business" => {
                sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('published-candidate','Work','ready','qwen')")
                    .execute(&pool).await.unwrap();
                sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,version,node_id,role,card_id,contract_digest)
                    VALUES('published-candidate',$1,$2,'produce','work','published-candidate',$3)")
                    .bind(&fixture.campaign.id).bind(fixture.version).bind("c".repeat(64)).execute(&pool).await.unwrap();
            }
            "different-parent" => {
                sqlx::query("UPDATE campaign_graph_versions SET parent_version=NULL WHERE campaign_id=$1 AND version=$2")
                .bind(&fixture.campaign.id).bind(fixture.version).execute(&pool).await.unwrap();
            }
            "executor-drift" => {
                immutable_card_rejection(
                    &pool,
                    &fixture,
                    "UPDATE kanban_cards SET assigned_agent_id='qwen' WHERE id=$1",
                )
                .await;
                // A terminal dispatch can legally retain a different executor;
                // the application must still refuse independent QA recovery.
                sqlx::query("UPDATE task_dispatches SET to_agent_id='qwen' WHERE id=$1")
                    .bind(ATTEMPT)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "profile-drift" => {
                sqlx::query("UPDATE campaign_graph_tasks SET execution_profile=jsonb_set(execution_profile,'{graph_version}','99') WHERE task_key=$1")
                .bind(&key).execute(&pool).await.unwrap();
            }
            "newer-dispatch" => {
                sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at)
                VALUES('newer-attempt',$1,'monitoring','implementation','failed','{}',NOW(),NOW())")
                .bind(store::card_id(&key)).execute(&pool).await.unwrap();
            }
            "role-drift" => {
                sqlx::query("UPDATE campaign_graph_tasks SET role='work' WHERE task_key=$1")
                    .bind(&key)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "accepted-validator" => {
                sqlx::query("UPDATE campaign_graph_tasks SET accepted=TRUE WHERE task_key=$1")
                    .bind(&key)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "approved-candidate" => {
                sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE campaign_id=$1 AND version=$2")
                    .bind(&fixture.campaign.id).bind(fixture.version).execute(&pool).await.unwrap();
            }
            "metadata-drift" => {
                immutable_card_rejection(
                    &pool,
                    &fixture,
                    "UPDATE kanban_cards SET metadata=jsonb_set(metadata,'{graph_task_key}','\"another-task\"') WHERE id=$1",
                )
                .await;
            }
            _ => unreachable!(),
        }
        if boundary != "metadata-drift" {
            unchanged_rejection(&pool, &fixture).await;
        }
        pool.close().await;
        database.drop().await;
    }
}

async fn retained_validator_history(pool: &PgPool, fixture: &CancelledFixture) -> Value {
    sqlx::query_scalar(
        "SELECT jsonb_build_object('task',to_jsonb(t),'card',to_jsonb(k),'dispatch',to_jsonb(d),
            'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.id) FROM dispatch_events e WHERE e.dispatch_id=d.id),
            'entries',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.id) FROM auto_queue_entries e WHERE e.kanban_card_id=k.id),
            'runs',(SELECT jsonb_agg(to_jsonb(r) ORDER BY r.id) FROM auto_queue_runs r WHERE r.id IN(
                SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=k.id)))
         FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id
         JOIN task_dispatches d ON d.kanban_card_id=k.id
         WHERE t.campaign_id=$1 AND t.task_key=$2 AND d.id=$3",
    )
    .bind(&fixture.campaign.id)
    .bind(format!("{}:validate:{}", fixture.campaign.id, fixture.version))
    .bind(ATTEMPT)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn cancelled_validator_replan_retires_unpublished_artifact_execute_at_paused_revision59_pg() {
    let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = database.connect_and_migrate().await;
    // Select the real refused root shape before the candidate and its QA profile
    // are frozen. No proposed business node has ever been published.
    let mut fixture =
        validator_attempt(&pool, true, Kind::Execute, Effect::ArtifactOnly, true).await;
    while fixture.campaign.revision < 59 {
        let input: campaigns::CampaignInput =
            serde_json::from_value(serde_json::to_value(&fixture.campaign).unwrap()).unwrap();
        fixture.campaign = campaigns::replace(
            &pool,
            &fixture.campaign.id,
            fixture.campaign.revision,
            input,
        )
        .await
        .unwrap();
    }
    fixture.request.expected_revision = fixture.campaign.revision;
    assert_eq!(fixture.campaign.revision, 59);
    assert_eq!(fixture.campaign.status, CampaignStatus::Paused);
    assert_eq!(fixture.graph.nodes[0].kind, Kind::Execute);
    assert_eq!(fixture.graph.nodes[0].effect, Effect::ArtifactOnly);
    assert!(fixture.graph.nodes[0].dependencies.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM campaign_graph_tasks WHERE campaign_id=$1 AND task_key=$2",
        )
        .bind(&fixture.campaign.id)
        .bind(&fixture.graph.nodes[0].task_key)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_goal_revisions")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    let before = canonical_inspection(store::status(&pool, &fixture.campaign.id).await.unwrap());
    let history = retained_validator_history(&pool, &fixture).await;
    assert!(history["dispatch"]["completed_at"].is_null());
    assert_eq!(history["dispatch"]["status"], "cancelled");
    assert_eq!(history["task"]["accepted"], false);
    let repair = replan::request(&pool, &fixture.campaign.id, &fixture.request)
        .await
        .unwrap();
    assert_eq!(repair.revision, 60);
    assert!(repair.goal_scope_preserved && repair.independent_validation_required);
    assert_eq!(repair.planner_agent_id, goal().planner);
    assert_eq!(
        replan::request(&pool, &fixture.campaign.id, &fixture.request)
            .await
            .unwrap()
            .revision,
        repair.revision
    );
    let after = canonical_inspection(store::status(&pool, &fixture.campaign.id).await.unwrap());
    for field in ["tasks", "goal", "active_version", "occurrences"] {
        assert_eq!(before[field], after[field], "{field}");
    }
    assert_eq!(
        before["control_budget"]["usage"],
        after["control_budget"]["usage"]
    );
    let mut expected_versions = before["versions"].clone();
    expected_versions[0]["status"] = json!("rejected");
    assert_eq!(after["versions"], expected_versions);
    for failure in before["failures"].as_array().unwrap() {
        assert!(after["failures"].as_array().unwrap().contains(failure));
    }
    assert_eq!(retained_validator_history(&pool, &fixture).await, history);
    assert!(after["hold_reason"].is_null());
    let paused = campaigns::get(&pool, &fixture.campaign.id).await.unwrap();
    assert_eq!(paused.status, CampaignStatus::Paused);
    assert_eq!(paused.revision, 60);
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&paused).unwrap()).unwrap();
    input.status = CampaignStatus::Active;
    let active = campaigns::replace(&pool, &paused.id, paused.revision, input)
        .await
        .unwrap();
    assert_eq!(active.revision, 61);
    control(
        &pool,
        &active.id,
        &goal(),
        "plan",
        Some(after["active_version"].as_i64().unwrap()),
        &repair.repair_failure_id,
        "Independently inspect the cancellation and propose a fresh corrected graph",
    )
    .await
    .unwrap();
    for node in &mut fixture.graph.nodes {
        node.task_key.push_str(":fresh-execute-retirement");
    }
    let next = store::stage(
        &pool,
        &active.id,
        &fixture.graph,
        Some(&repair.repair_failure_id),
    )
    .await
    .unwrap();
    assert_eq!(next, fixture.version + 1);
    assert!(
        store::activate(&pool, &active.id, next, active.revision)
            .await
            .unwrap_err()
            .to_string()
            .contains("only validated versions")
    );
    control(
        &pool,
        &active.id,
        &goal(),
        "validate",
        Some(next),
        &next.to_string(),
        "Independently validate the newly authored candidate before business work",
    )
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT assigned_agent_id FROM kanban_cards WHERE id=$1",
        )
        .bind(store::card_id(&format!("{}:validate:{next}", active.id)))
        .fetch_one(&pool)
        .await
        .unwrap(),
        Some(goal().verifier)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='work'",
        )
        .bind(&active.id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(retained_validator_history(&pool, &fixture).await, history);
    super::replan_tests::settle(&pool).await;
    pool.close().await;
    database.drop().await;
}

#[tokio::test]
async fn cancelled_validator_replan_artifact_execute_retirement_keeps_audit_external_and_legacy_boundaries_pg()
 {
    for boundary in [
        "missing-audit",
        "version-pin",
        "raw-pin",
        "published-business",
        "external-root",
        "legacy-failed",
    ] {
        let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
        let pool = database.connect_and_migrate().await;
        let mut fixture = validator_attempt(
            &pool,
            true,
            Kind::Execute,
            if boundary == "external-root" {
                Effect::External
            } else {
                Effect::ArtifactOnly
            },
            boundary != "legacy-failed",
        )
        .await;
        match boundary {
            "missing-audit" => fixture.request.cancelled_validation = None,
            "version-pin" => {
                fixture
                    .request
                    .cancelled_validation
                    .as_mut()
                    .unwrap()
                    .graph_version += 1;
            }
            "raw-pin" => {
                fixture
                    .request
                    .cancelled_validation
                    .as_mut()
                    .unwrap()
                    .raw_result_sha256 = "0".repeat(64);
            }
            "published-business" => {
                sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('published-candidate','Work','ready','qwen')")
                    .execute(&pool).await.unwrap();
                sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,version,node_id,role,card_id,contract_digest)
                    VALUES('published-candidate',$1,$2,'produce','work','published-candidate',$3)")
                    .bind(&fixture.campaign.id).bind(fixture.version).bind("c".repeat(64)).execute(&pool).await.unwrap();
            }
            "external-root" | "legacy-failed" => {}
            _ => unreachable!(),
        }
        let history = retained_validator_history(&pool, &fixture).await;
        let reason = unchanged_rejection(&pool, &fixture).await;
        if matches!(boundary, "external-root" | "legacy-failed") {
            assert_eq!(
                reason,
                "recovery roots must observe before external effects"
            );
        }
        assert_eq!(retained_validator_history(&pool, &fixture).await, history);
        pool.close().await;
        database.drop().await;
    }
}
