//! A native QA rejection permits fresh planning, never approval of its rejected DAG.
use super::*;
use crate::db::campaigns::graph::recovery::replan::{self, RejectedValidation, Replan};

const ATTEMPT: &str = "native-rejected-validator";

struct RejectedFixture {
    campaign: campaigns::Campaign,
    request: Replan,
    raw: String,
    graph: Graph,
    version: i64,
}

async fn rejected(pool: &PgPool, parent: bool) -> RejectedFixture {
    rejected_with_source(
        pool,
        parent,
        Some("campaign_native_receipt"),
        "campaign_native_receipt",
    )
    .await
}

async fn rejected_with_source(
    pool: &PgPool,
    parent: bool,
    source: Option<&str>,
    event_source: &str,
) -> RejectedFixture {
    let c = setup(pool).await;
    let mut proposed = graph();
    proposed.nodes[0].kind = Kind::Inspect;
    let mut source_failure = None;
    if parent {
        let version = store::stage(pool, &c.id, &proposed, None).await.unwrap();
        sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE campaign_id=$1 AND version=$2")
            .bind(&c.id).bind(version).execute(pool).await.unwrap();
        store::activate(pool, &c.id, version, c.revision)
            .await
            .unwrap();
        let node = &proposed.nodes[0];
        store::publish(
            pool,
            &c.id,
            &store::Publication {
                key: &node.task_key,
                node: &node.id,
                role: "work",
                version: Some(version),
                agent: &node.agent,
                title: &node.title,
                prompt: &node.details,
                digest: &store::task_digest(node).unwrap(),
            },
        )
        .await
        .unwrap();
        // Model an existing accepted parent contract. The new native QA turn
        // below must preserve it; this fixture does not claim new work acceptance.
        sqlx::query("UPDATE campaign_graph_tasks SET accepted=TRUE,observation='{}' WHERE campaign_id=$1 AND role='work'")
            .bind(&c.id).execute(pool).await.unwrap();
        let node = &proposed.nodes[1];
        let card = store::publish(
            pool,
            &c.id,
            &store::Publication {
                key: &node.task_key,
                node: &node.id,
                role: "work",
                version: Some(version),
                agent: &node.agent,
                title: &node.title,
                prompt: &node.details,
                digest: &store::task_digest(node).unwrap(),
            },
        )
        .await
        .unwrap();
        sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
            .bind(&card)
            .execute(pool)
            .await
            .unwrap();
        let source_receipt = json!({"summary":crate::db::campaigns::graph::result::CAPTURE_SUMMARY,
            "work_outcome":"artifact","completion_source":"campaign_native_receipt",
            "campaign_receipt":{"outcome":"blocked","failure_category":"plan","summary":"Parent contract needs a newly discovered source assertion",
                "observations":[{"command":"inspect parent source assertion","exit_code":0,"evidence":"Source correction required; historical accepted inspection retained"}]}});
        sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,created_at)
            VALUES('parent-source-rejection',$1,'monitoring','implementation','pending',NOW()-INTERVAL '3 seconds')")
            .bind(&card).execute(pool).await.unwrap();
        assert_eq!(
            crate::dispatch::set_dispatch_status_on_pg_async(
                pool,
                "parent-source-rejection",
                "completed",
                Some(&source_receipt),
                "campaign_native_receipt",
                Some(&["pending"]),
                true
            )
            .await
            .unwrap(),
            1
        );
        reconcile::observe(pool, &c.id, &goal(), None)
            .await
            .unwrap();
        source_failure = Some(
            sqlx::query_scalar::<_, String>(
                "SELECT id FROM campaign_graph_failures WHERE campaign_id=$1 AND task_key=$2",
            )
            .bind(&c.id)
            .bind(&node.task_key)
            .fetch_one(pool)
            .await
            .unwrap(),
        );
        super::replan_tests::settle(pool).await;
        proposed.nodes[1].task_key.push_str(":successor");
    }
    let version = store::stage(pool, &c.id, &proposed, source_failure.as_deref())
        .await
        .unwrap();
    control(
        pool,
        &c.id,
        &goal(),
        "validate",
        Some(version),
        &version.to_string(),
        "Independently inspect the exact candidate",
    )
    .await
    .unwrap();
    let key = format!("{}:validate:{version}", c.id);
    let card = store::card_id(&key);
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    let candidate_digest: String = sqlx::query_scalar(
        "SELECT digest FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2",
    )
    .bind(&c.id)
    .bind(version)
    .fetch_one(pool)
    .await
    .unwrap();
    let mut receipt = json!({"summary":crate::db::campaigns::graph::result::CAPTURE_SUMMARY,
        "work_outcome":"artifact",
        "campaign_receipt":{"outcome":"blocked","failure_category":"plan","summary":"Candidate omits a source-defined acceptance condition",
            "observations":[{"command":"read exact candidate and source","exit_code":0,"evidence":"Independent source inspection; no business execution"}]},
        "graph_validation":{"approved":false,"reason":"Require current deployed source evidence before business execution","digest":candidate_digest}});
    if let Some(source) = source {
        receipt["completion_source"] = json!(source);
    }
    crate::db::campaigns::graph::result::validate_dispatch(&receipt).unwrap();
    sqlx::query(
        "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,created_at)
        VALUES($1,$2,'monitoring','implementation','pending',NOW()-INTERVAL '3 seconds')",
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
            "completed",
            Some(&receipt),
            event_source,
            Some(&["pending"]),
            true
        )
        .await
        .unwrap(),
        1
    );
    progress::tool_use(
        Some(pool),
        Some(ATTEMPT),
        Some("independent-source-check"),
        "candidate-input",
    )
    .await;
    reconcile::observe(pool, &c.id, &goal(), None)
        .await
        .unwrap();
    reconcile::reject_failed_candidates(pool, &c.id)
        .await
        .unwrap();
    super::replan_tests::settle(pool).await;
    let current = campaigns::get(pool, &c.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let campaign = campaigns::replace(pool, &c.id, current.revision, input)
        .await
        .unwrap();
    let state = store::status(pool, &c.id).await.unwrap();
    let failure = state["failures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|failure| failure["task_key"] == key)
        .unwrap();
    assert_eq!(failure["category"], "plan");
    assert_eq!(state["versions"][0]["status"], "rejected");
    let raw: String = sqlx::query_scalar("SELECT result FROM task_dispatches WHERE id=$1")
        .bind(ATTEMPT)
        .fetch_one(pool)
        .await
        .unwrap();
    let mut budget = goal().budget;
    budget.max_dispatches = 20;
    budget.max_evolutions = 4;
    let request = Replan {
        cancelled_validation: None,
        expected_revision: campaign.revision, request_id: "native-rejected-qa-source-repair".into(),
        failure_id: failure["id"].as_str().unwrap().into(),
        evidence: "The retained independent QA rejected this exact source contract. Keep the rejected graph, raw result, accepted parent and lifetime usage; the existing planner must correct the acceptance gap and obtain fresh independent QA.".into(),
        budget: Some(budget), supersede_candidate: None, unattempted_effect: None, settled_effect: None,
        rejected_validation: Some(RejectedValidation {
            dispatch_id: ATTEMPT.into(), graph_version: version, graph_digest: candidate_digest,
            raw_result_sha256: crate::db::campaigns::graph::digest(raw.as_bytes()),
            contract_digest: failure["evidence"]["runtime"]["contract_digest"].as_str().unwrap().into(),
        }),
    };
    RejectedFixture {
        campaign,
        request,
        raw,
        graph: proposed,
        version,
    }
}

async fn unchanged_rejection(pool: &PgPool, f: &RejectedFixture) {
    let before = canonical_inspection(store::status(pool, &f.campaign.id).await.unwrap());
    assert!(
        replan::request(pool, &f.campaign.id, &f.request)
            .await
            .is_err()
    );
    assert_eq!(
        canonical_inspection(store::status(pool, &f.campaign.id).await.unwrap()),
        before
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_goal_revisions")
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn rejected_candidate_source_absent_native_event_repairs_without_rewriting_history_pg() {
    for event_source in ["watcher_streaming_final", "campaign_native_receipt"] {
        let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
        let pool = fixture.connect_and_migrate().await;
        let f = rejected_with_source(&pool, true, None, event_source).await;
        assert!(
            serde_json::from_str::<Value>(&f.raw)
                .unwrap()
                .get("completion_source")
                .is_none()
        );
        let before = canonical_inspection(store::status(&pool, &f.campaign.id).await.unwrap());
        let event_before: Value = sqlx::query_scalar("SELECT payload_json FROM dispatch_events
            WHERE dispatch_id=$1 AND to_status='completed' AND transition_source=$2 ORDER BY id LIMIT 1")
            .bind(ATTEMPT).bind(event_source).fetch_one(&pool).await.unwrap();
        let reply = replan::request(&pool, &f.campaign.id, &f.request)
            .await
            .unwrap();
        assert_eq!(reply.revision, f.campaign.revision + 1);
        assert!(reply.independent_validation_required && reply.goal_scope_preserved);
        let after = canonical_inspection(store::status(&pool, &f.campaign.id).await.unwrap());
        for field in [
            "versions",
            "tasks",
            "active_version",
            "occurrences",
            "observed_tool_calls",
            "elapsed_dispatch_seconds",
        ] {
            assert_eq!(after[field], before[field], "{field}");
        }
        for failure in before["failures"].as_array().unwrap() {
            assert!(after["failures"].as_array().unwrap().contains(failure));
        }
        let mut expected_goal = before["goal"].clone();
        expected_goal["budget"] = serde_json::to_value(f.request.budget.as_ref().unwrap()).unwrap();
        assert_eq!(after["goal"], expected_goal);
        assert_eq!(after["versions"][0]["status"], "rejected");
        assert_eq!(
            campaigns::get(&pool, &f.campaign.id).await.unwrap().status,
            CampaignStatus::Paused
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT result FROM task_dispatches WHERE id=$1")
                .bind(ATTEMPT)
                .fetch_one(&pool)
                .await
                .unwrap(),
            f.raw
        );
        assert_eq!(sqlx::query_scalar::<_, Value>("SELECT payload_json FROM dispatch_events
            WHERE dispatch_id=$1 AND to_status='completed' AND transition_source=$2 ORDER BY id LIMIT 1")
            .bind(ATTEMPT).bind(event_source).fetch_one(&pool).await.unwrap(), event_before);
        pool.close().await;
        fixture.drop().await;
    }
}

#[tokio::test]
async fn rejected_candidate_source_absence_never_waives_event_or_source_conflicts_pg() {
    for boundary in [
        "missing-event",
        "unknown-event-source",
        "content-mismatch",
        "source-conflict",
        "unknown-raw-source",
        "non-completed-event",
    ] {
        let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
        let pool = fixture.connect_and_migrate().await;
        let source = (boundary == "source-conflict").then_some("campaign_native_receipt");
        let mut f = rejected_with_source(&pool, true, source, "watcher_streaming_final").await;
        match boundary {
            "missing-event" => {
                sqlx::query("DELETE FROM dispatch_events WHERE dispatch_id=$1")
                    .bind(ATTEMPT)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "unknown-event-source" => {
                sqlx::query("UPDATE dispatch_events SET transition_source='operator_override' WHERE dispatch_id=$1")
                    .bind(ATTEMPT).execute(&pool).await.unwrap();
            }
            "content-mismatch" => {
                sqlx::query(
                    "UPDATE dispatch_events SET payload_json='{}'::jsonb WHERE dispatch_id=$1",
                )
                .bind(ATTEMPT)
                .execute(&pool)
                .await
                .unwrap();
            }
            "unknown-raw-source" => {
                // Corrupt only this disposable fixture, including its operator
                // hash and event content, to isolate the metadata source guard.
                let mut invalid: Value = serde_json::from_str(&f.raw).unwrap();
                invalid["completion_source"] = json!("operator_override");
                let raw = invalid.to_string();
                sqlx::query("UPDATE task_dispatches SET result=$2 WHERE id=$1")
                    .bind(ATTEMPT)
                    .bind(&raw)
                    .execute(&pool)
                    .await
                    .unwrap();
                sqlx::query("UPDATE dispatch_events SET payload_json=$2 WHERE dispatch_id=$1")
                    .bind(ATTEMPT)
                    .bind(&invalid)
                    .execute(&pool)
                    .await
                    .unwrap();
                f.request
                    .rejected_validation
                    .as_mut()
                    .unwrap()
                    .raw_result_sha256 = crate::db::campaigns::graph::digest(raw.as_bytes());
            }
            "non-completed-event" => {
                sqlx::query("UPDATE dispatch_events SET to_status='failed' WHERE dispatch_id=$1")
                    .bind(ATTEMPT)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "source-conflict" => {}
            _ => unreachable!(),
        }
        let raw_before: String =
            sqlx::query_scalar("SELECT result FROM task_dispatches WHERE id=$1")
                .bind(ATTEMPT)
                .fetch_one(&pool)
                .await
                .unwrap();
        unchanged_rejection(&pool, &f).await;
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT result FROM task_dispatches WHERE id=$1")
                .bind(ATTEMPT)
                .fetch_one(&pool)
                .await
                .unwrap(),
            raw_before,
            "{boundary}"
        );
        pool.close().await;
        fixture.drop().await;
    }
}

#[tokio::test]
async fn rejected_candidate_plan_repair_preserves_native_history_and_requires_new_qa_pg() {
    for parent in [false, true] {
        let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
        let pool = fixture.connect_and_migrate().await;
        let f = rejected(&pool, parent).await;
        store::hold(&pool, &f.campaign.id, "Plan evolution budget exhausted")
            .await
            .unwrap();
        let before = canonical_inspection(store::status(&pool, &f.campaign.id).await.unwrap());
        let reply = replan::request(&pool, &f.campaign.id, &f.request)
            .await
            .unwrap();
        assert!(reply.independent_validation_required && reply.goal_scope_preserved);
        assert_eq!(reply.planner_agent_id, goal().planner);
        assert_eq!(
            replan::request(&pool, &f.campaign.id, &f.request)
                .await
                .unwrap()
                .revision,
            reply.revision
        );
        let after = canonical_inspection(store::status(&pool, &f.campaign.id).await.unwrap());
        for field in [
            "versions",
            "tasks",
            "active_version",
            "occurrences",
            "observed_tool_calls",
            "elapsed_dispatch_seconds",
        ] {
            assert_eq!(after[field], before[field], "{field}");
        }
        assert!(
            after["failures"]
                .as_array()
                .unwrap()
                .contains(&before["failures"][0])
        );
        let mut expected_goal = before["goal"].clone();
        expected_goal["budget"] = serde_json::to_value(f.request.budget.as_ref().unwrap()).unwrap();
        assert_eq!(after["goal"], expected_goal);
        assert_eq!(after["versions"][0]["status"], "rejected");
        assert!(after["hold_reason"].is_null());
        assert_eq!(
            campaigns::get(&pool, &f.campaign.id).await.unwrap().status,
            CampaignStatus::Paused
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT result FROM task_dispatches WHERE id=$1")
                .bind(ATTEMPT)
                .fetch_one(&pool)
                .await
                .unwrap(),
            f.raw
        );
        let current = campaigns::get(&pool, &f.campaign.id).await.unwrap();
        let mut input: campaigns::CampaignInput =
            serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
        input.status = CampaignStatus::Active;
        let resumed = campaigns::replace(&pool, &f.campaign.id, current.revision, input)
            .await
            .unwrap();
        assert!(
            store::activate(&pool, &f.campaign.id, f.version, resumed.revision)
                .await
                .is_err()
        );
        let active = after["active_version"].as_i64();
        assert!(
            control(
                &pool,
                &f.campaign.id,
                &store::goal(&pool, &f.campaign.id).await.unwrap(),
                "plan",
                active,
                &reply.repair_failure_id,
                "Correct only the independently rejected acceptance gap"
            )
            .await
            .unwrap()
        );
        assert_eq!(sqlx::query_scalar::<_,String>("SELECT assigned_agent_id FROM kanban_cards
            WHERE id=(SELECT card_id FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='plan')")
            .bind(&f.campaign.id).fetch_one(&pool).await.unwrap(), "qwen");
        assert_eq!(
            sqlx::query_scalar::<_, bool>(
                "SELECT accepted FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='validate'"
            )
            .bind(&f.campaign.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            false
        );
        super::replan_tests::settle(&pool).await;
        pool.close().await;
        fixture.drop().await;
    }
}

#[tokio::test]
async fn rejected_candidate_plan_repair_rejects_identity_receipt_and_raw_drift_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let mut f = rejected(&pool, true).await;
    let original_audit =
        serde_json::to_value(f.request.rejected_validation.as_ref().unwrap()).unwrap();
    for (field, wrong) in [
        ("dispatch_id", json!("other")),
        ("graph_version", json!(f.version + 1)),
        ("graph_digest", json!("0".repeat(64))),
        ("contract_digest", json!("1".repeat(64))),
        ("raw_result_sha256", json!("2".repeat(64))),
    ] {
        let mut audit = original_audit.clone();
        audit[field] = wrong;
        f.request.rejected_validation = Some(serde_json::from_value(audit).unwrap());
        unchanged_rejection(&pool, &f).await;
    }
    f.request.rejected_validation = None;
    unchanged_rejection(&pool, &f).await;
    f.request.rejected_validation = Some(serde_json::from_value(original_audit).unwrap());
    let original_evidence: Value =
        sqlx::query_scalar("SELECT evidence FROM campaign_graph_failures WHERE id=$1")
            .bind(&f.request.failure_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let mut unknown_provider = original_evidence.clone();
    unknown_provider["runtime"]["provider"] = Value::Null;
    sqlx::query("UPDATE agents SET provider=NULL WHERE id='monitoring'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_failures SET evidence=$2 WHERE id=$1")
        .bind(&f.request.failure_id)
        .bind(&unknown_provider)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET observation=$2 WHERE task_key=$1")
        .bind(unknown_provider["task_key"].as_str().unwrap())
        .bind(&unknown_provider)
        .execute(&pool)
        .await
        .unwrap();
    // Two unknown provider identities do not authenticate a native validator.
    unchanged_rejection(&pool, &f).await;
    sqlx::query("UPDATE agents SET provider='opencode' WHERE id='monitoring'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_failures SET evidence=$2 WHERE id=$1")
        .bind(&f.request.failure_id)
        .bind(&original_evidence)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET observation=$2 WHERE task_key=$1")
        .bind(original_evidence["task_key"].as_str().unwrap())
        .bind(&original_evidence)
        .execute(&pool)
        .await
        .unwrap();
    let original: Value = serde_json::from_str(&f.raw).unwrap();
    for (pointer, wrong) in [
        ("/graph_validation/approved", json!(true)),
        ("/graph_validation/digest", json!("3".repeat(64))),
        ("/graph_validation/reason", json!("")),
        ("/campaign_receipt/outcome", json!("passed")),
        ("/campaign_receipt/failure_category", json!("unknown")),
        (
            "/campaign_receipt/summary",
            json!("Changed after native completion"),
        ),
    ] {
        let mut altered = original.clone();
        *altered.pointer_mut(pointer).unwrap() = wrong;
        let raw = altered.to_string();
        sqlx::query("UPDATE task_dispatches SET result=$2 WHERE id=$1")
            .bind(ATTEMPT)
            .bind(&raw)
            .execute(&pool)
            .await
            .unwrap();
        // Even an updated current operator pin cannot replace immutable native
        // completion content or convert an approval into a rejected-plan repair.
        f.request
            .rejected_validation
            .as_mut()
            .unwrap()
            .raw_result_sha256 = crate::db::campaigns::graph::digest(raw.as_bytes());
        unchanged_rejection(&pool, &f).await;
    }
    sqlx::query("UPDATE task_dispatches SET result=$2 WHERE id=$1")
        .bind(ATTEMPT)
        .bind(&f.raw)
        .execute(&pool)
        .await
        .unwrap();
    f.request
        .rejected_validation
        .as_mut()
        .unwrap()
        .raw_result_sha256 = crate::db::campaigns::graph::digest(f.raw.as_bytes());
    f.request.unattempted_effect = Some(replan::UnattemptedEffect {
        dispatch_id: ATTEMPT.into(),
        result_sha256: "4".repeat(64),
        audit_sha256: "5".repeat(64),
    });
    unchanged_rejection(&pool, &f).await;
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn rejected_candidate_plan_repair_rejects_later_candidates_published_work_and_unknowns_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let f = rejected(&pool, true).await;
    sqlx::query("INSERT INTO campaign_graph_versions(campaign_id,version,parent_version,digest,graph,status)
        SELECT campaign_id,version+1,version,'later-candidate',graph,'rejected' FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2")
        .bind(&f.campaign.id).bind(f.version).execute(&pool).await.unwrap();
    unchanged_rejection(&pool, &f).await;
    sqlx::query("DELETE FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2")
        .bind(&f.campaign.id)
        .bind(f.version + 1)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET parent_version=NULL WHERE campaign_id=$1 AND version=$2")
        .bind(&f.campaign.id).bind(f.version).execute(&pool).await.unwrap();
    unchanged_rejection(&pool, &f).await;
    sqlx::query(
        "UPDATE campaign_graph_versions SET parent_version=$3 WHERE campaign_id=$1 AND version=$2",
    )
    .bind(&f.campaign.id)
    .bind(f.version)
    .bind(f.version - 1)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('unexecuted-published-candidate','Legacy work','ready','monitoring')")
        .execute(&pool).await.unwrap();
    let node = &f.graph.nodes[1];
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,version,node_id,role,card_id,contract_digest)
        VALUES($1,$2,$3,$4,'work','unexecuted-published-candidate',$5)")
        .bind(&node.task_key).bind(&f.campaign.id).bind(f.version).bind(&node.id).bind(store::task_digest(node).unwrap()).execute(&pool).await.unwrap();
    unchanged_rejection(&pool, &f).await;
    sqlx::query("DELETE FROM campaign_graph_tasks WHERE card_id='unexecuted-published-candidate'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM kanban_cards WHERE id='unexecuted-published-candidate'")
        .execute(&pool)
        .await
        .unwrap();
    let card = store::card_id(&format!("{}:validate:{}", f.campaign.id, f.version));
    for status in [Some("unknown_owner"), None] {
        // Keep the native known-status and NOT NULL constraints enabled.
        let inserted = sqlx::query(
            "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status)
            VALUES('unknown-qa-sibling',$1,'monitoring','legacy',$2)",
        )
        .bind(&card)
        .bind(status)
        .execute(&pool)
        .await;
        assert!(
            inserted.is_err(),
            "native dispatch status constraints must reject {status:?}"
        );
    }
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at)
        VALUES('later-qa-attempt',$1,'monitoring','legacy','completed','{}',NOW(),NOW())")
        .bind(&card).execute(&pool).await.unwrap();
    unchanged_rejection(&pool, &f).await;
    sqlx::query("DELETE FROM task_dispatches WHERE id='later-qa-attempt'")
        .execute(&pool)
        .await
        .unwrap();
    for status in [Some("awaiting_user"), None] {
        sqlx::query(
            "INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id)
            VALUES('unsettled-rejected-owner','monitoring','opencode',$1,$2)",
        )
        .bind(status)
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
        unchanged_rejection(&pool, &f).await;
        sqlx::query("DELETE FROM sessions WHERE session_key='unsettled-rejected-owner'")
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO auto_queue_runs(id,agent_id,status) VALUES('rejected-qa-slot-run','monitoring','completed')")
        .execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO auto_queue_entries(id,run_id,kanban_card_id,agent_id,status)
        VALUES('rejected-qa-slot-entry','rejected-qa-slot-run',$1,'monitoring','done')",
    )
    .bind(&card)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO auto_queue_slots(agent_id,slot_index,assigned_run_id) VALUES('monitoring',8,'rejected-qa-slot-run')")
        .execute(&pool).await.unwrap();
    unchanged_rejection(&pool, &f).await;
    sqlx::query("DELETE FROM auto_queue_slots WHERE agent_id='monitoring' AND slot_index=8")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM auto_queue_entries WHERE id='rejected-qa-slot-entry'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM auto_queue_runs WHERE id='rejected-qa-slot-run'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO auto_queue_run_cleanup_tasks(run_ids,dispatch_ids) VALUES('{}',ARRAY[$1]::text[])")
        .bind(ATTEMPT).execute(&pool).await.unwrap();
    unchanged_rejection(&pool, &f).await;
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks WHERE $1=ANY(dispatch_ids)")
        .bind(ATTEMPT)
        .execute(&pool)
        .await
        .unwrap();
    let rollback = json!([{"card_id":card,"dispatch_id":ATTEMPT}]);
    sqlx::query(
        "INSERT INTO auto_queue_run_cleanup_tasks(run_ids,dispatch_ids,card_rollback_tasks)
        VALUES('{}','{}',$1)",
    )
    .bind(&rollback)
    .execute(&pool)
    .await
    .unwrap();
    unchanged_rejection(&pool, &f).await;
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks WHERE card_rollback_tasks=$1")
        .bind(&rollback)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO campaign_graph_failures(id,campaign_id,task_key,fingerprint,category,evidence)
        SELECT 'additional-unknown',campaign_id,task_key,'additional-unknown','unknown',evidence FROM campaign_graph_failures WHERE id=$1")
        .bind(&f.request.failure_id).execute(&pool).await.unwrap();
    unchanged_rejection(&pool, &f).await;
    sqlx::query("DELETE FROM campaign_graph_failures WHERE id='additional-unknown'")
        .execute(&pool)
        .await
        .unwrap();
    store::hold(
        &pool,
        &f.campaign.id,
        "Execution outcome requires reconciliation; automatic replay is disabled",
    )
    .await
    .unwrap();
    unchanged_rejection(&pool, &f).await;
    pool.close().await;
    fixture.drop().await;
}
