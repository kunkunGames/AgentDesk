//! Native PostgreSQL progression with explicit fixtures, never provider claims.
use super::*;
use crate::db::campaigns::graph::recovery::replan::settled_effect::{
    ConfiguredTarget, OperatorAttestation, SettledEffectBundle, SettledTarget, TargetState,
};
use crate::db::campaigns::graph::recovery::replan::{self, Replan, SettledExternalEffect};
use crate::db::campaigns::graph::{SettledEffectGate, learning as ledger};

fn pin(text: &str) -> String {
    crate::db::campaigns::graph::digest(text.as_bytes())
}

async fn paused(pool: &PgPool, id: &str, status: CampaignStatus) -> campaigns::Campaign {
    let c = campaigns::get(pool, id).await.unwrap();
    let mut input: campaigns::CampaignInput = serde_json::from_value(json!(c)).unwrap();
    input.status = status;
    campaigns::replace(pool, id, c.revision, input)
        .await
        .unwrap()
}

async fn held(pool: &PgPool) -> (campaigns::Campaign, Graph, String, Replan) {
    let c = setup(pool).await;
    let mut graph = graph();
    graph.nodes[0].effect = Effect::External;
    let version = store::stage(pool, &c.id, &graph, None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated'")
        .execute(pool)
        .await
        .unwrap();
    store::activate(pool, &c.id, version, c.revision)
        .await
        .unwrap();
    let node = &graph.nodes[0];
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
    let raw = json!({"summary":crate::db::campaigns::graph::result::CAPTURE_SUMMARY,
        "work_outcome":"artifact", "completion_source":"campaign_native_receipt",
        "campaign_receipt":{"outcome":"blocked","failure_category":"code",
          "summary":"Four configured targets changed; remaining target guard overflowed. Historical child exit is unavailable.",
          "observations":[{"command":"bounded original sync fixture","exit_code":1,"evidence":"Truthful partial effects; no historical success claimed"}]}}).to_string();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at)
        VALUES('partial-effect-dispatch',$1,'qwen','implementation','completed',$2,NOW()-INTERVAL '8 seconds',NOW())")
        .bind(&card).bind(&raw).execute(pool).await.unwrap();
    progress::tool_use(
        Some(pool),
        Some("partial-effect-dispatch"),
        Some("partial-tool"),
        "input-pin",
    )
    .await;
    reconcile::observe(pool, &c.id, &goal(), None)
        .await
        .unwrap();
    super::replan_tests::settle(pool).await;
    let c = paused(pool, &c.id, CampaignStatus::Paused).await;
    let state = canonical_inspection(store::status(pool, &c.id).await.unwrap());
    assert_eq!(state["failures"][0]["category"], "unknown");
    assert_eq!(
        state["failures"][0]["evidence"]["failure"]["category"],
        "code"
    );
    let configured_targets: Vec<_> = (0..7)
        .map(|i| ConfiguredTarget {
            id: format!("configured-target-{i}"),
            expected_sha256: pin(&format!("expected-target-{i}")),
        })
        .collect();
    let targets: Vec<_> = configured_targets
        .iter()
        .enumerate()
        .map(|(i, target)| SettledTarget {
            id: target.id.clone(),
            expected_sha256: target.expected_sha256.clone(),
            current_sha256: if i < 4 {
                target.expected_sha256.clone()
            } else {
                pin(&format!("unchanged-target-{i}"))
            },
            prior_evidence_sha256: pin(&format!("retained-before-after-target-{i}")),
            state: if i < 4 {
                TargetState::Converged
            } else {
                TargetState::PendingDelta
            },
        })
        .collect();
    let bundle = SettledEffectBundle {
        schema_version: 1,
        campaign_id: c.id.clone(),
        task_key: node.task_key.clone(),
        contract_digest: store::task_digest(node).unwrap(),
        goal_scope_sha256: ledger::scope_digest(&goal()).unwrap(),
        source_sha256: pin(
            "immutable source with seven configured destinations and exact mode rules",
        ),
        configuration_sha256: pin("immutable target configuration"),
        target_set_sha256: contract_digest(&configured_targets).unwrap(),
        configured_targets,
        observed_at: chrono::Utc::now().to_rfc3339(),
        targets,
    };
    let audit = SettledExternalEffect {
        dispatch_id: "partial-effect-dispatch".into(),
        result_sha256: pin(&raw),
        audit_sha256: contract_digest(&bundle).unwrap(),
        bundle,
        operator_attestation: OperatorAttestation {
            complete_target_coverage: true,
            current_state_verified: true,
            no_unknown_effects: true,
        },
    };
    let request = Replan {cancelled_validation:None,rejected_validation:None,expected_revision:c.revision,request_id:"settled-partial-source-correction".into(),
        failure_id:state["failures"][0]["id"].as_str().unwrap().into(),
        evidence:"Configured server administrator reviewed all seven exact source/config targets and complete state/log evidence. Missing original child exit remains missing. Require existing planner, fresh complete inventory and independent target QA before remaining deltas; never replay matching targets.".into(),
        budget:None,supersede_candidate:None,unattempted_effect:None,settled_effect:Some(audit)};
    (c, graph, raw, request)
}

fn refresh_pin(request: &mut Replan) {
    let audit = request.settled_effect.as_mut().unwrap();
    audit.audit_sha256 = contract_digest(&audit.bundle).unwrap();
}

fn corrected(audit: &SettledExternalEffect) -> Graph {
    let mut graph = graph();
    let mut inspect = graph.nodes[0].clone();
    inspect.id = "fresh-inspect".into();
    inspect.task_key = "repo:sha:fresh-settled-inspect".into();
    inspect.kind = Kind::Inspect;
    inspect.details = "Read-only inventory all seven original configured targets, immutable source/config/mode rules, retained partial evidence and every current owner. Do not claim the missing child exit. Write complete current target preconditions.".into();
    let mut verify = graph.nodes[1].clone();
    verify.id = "fresh-target-qa".into();
    verify.task_key = "repo:sha:fresh-settled-target-qa".into();
    verify.dependencies = [inspect.id.clone()].into();
    verify.inputs[0].dependency = inspect.id.clone();
    verify.details = "Independently verify full source-defined seven-target coverage and exact current state, expected modes, ownership and retained evidence before any remaining mutation.".into();
    graph.nodes[0].id = "remaining-delta".into();
    graph.nodes[0].task_key = "repo:sha:remaining-settled-delta".into();
    graph.nodes[0].effect = Effect::External;
    graph.nodes[0].dependencies = [verify.id.clone()].into();
    graph.nodes[0].inputs = vec![Input {
        dependency: verify.id.clone(),
        source: verify.outputs[0].path.clone(),
        destination: "input/target-qa.json".into(),
    }];
    graph.nodes[0].details = "Reinspect exact preconditions immediately before mutation. Matching targets invoke zero mutators; only remaining declared target deltas may execute. Write durable intent/log before effects and preserve true exits even if post-inventory fails.".into();
    graph.nodes[1].id = "remaining-final-qa".into();
    graph.nodes[1].task_key = "repo:sha:remaining-settled-final-qa".into();
    graph.nodes[1].dependencies = [graph.nodes[0].id.clone()].into();
    graph.nodes[1].inputs[0].dependency = graph.nodes[0].id.clone();
    graph.nodes.insert(0, verify.clone());
    graph.nodes.insert(0, inspect.clone());
    graph.settled_effect_gates = vec![SettledEffectGate {
        audit_sha256: audit.audit_sha256.clone(),
        inspection: inspect.id,
        verification: verify.id,
    }];
    graph.validate(&goal()).unwrap();
    graph
}

async fn assert_unchanged(pool: &PgPool, id: &str, before: &Value) {
    assert_eq!(
        &canonical_inspection(store::status(pool, id).await.unwrap()),
        before
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_reconciliations")
            .fetch_one(pool)
            .await
            .unwrap(),
        0
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
async fn settled_partial_effect_preserves_history_usage_and_publishes_native_planner_audit_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, raw, request) = held(&pool).await;
    let before = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("administrator")
    );
    let reply = replan::request_as_server_admin(&pool, &c.id, &request)
        .await
        .unwrap();
    assert!(reply.goal_scope_preserved && reply.independent_validation_required);
    let after = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    for field in [
        "goal",
        "tasks",
        "versions",
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
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_dispatches WHERE id='partial-effect-dispatch'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        raw
    );
    assert_eq!(
        replan::request_as_server_admin(&pool, &c.id, &request)
            .await
            .unwrap()
            .revision,
        reply.revision
    );
    paused(&pool, &c.id, CampaignStatus::Active).await;
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    let _ = tick(&pool, &engine, &c.id, None).await;
    let planner_key = format!("{}:plan:{}", c.id, reply.repair_failure_id);
    let card = store::card_id(&planner_key);
    let row = sqlx::query("SELECT description,assigned_agent_id FROM kanban_cards WHERE id=$1")
        .bind(card)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("assigned_agent_id"), "qwen");
    let prompt: String = row.get("description");
    assert!(prompt.contains(&request.settled_effect.as_ref().unwrap().audit_sha256));
    for marker in [
        "settled_effect_gates",
        "configured-target-6",
        "current_state_verified",
        "matching targets invoke no mutator",
    ] {
        assert!(prompt.contains(marker), "missing {marker}: {prompt}");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_tasks WHERE role='work'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "no effects before planning/QA"
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn settled_partial_effect_rejects_incomplete_stale_wrong_receipts_atomically_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, raw, mut request) = held(&pool).await;
    let before = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    let valid = request.settled_effect.as_ref().unwrap().clone();
    for case in [
        "subset",
        "duplicate",
        "wrong-target",
        "unknown-state",
        "stale",
        "future",
        "wrong-dispatch",
        "wrong-task",
        "wrong-goal",
        "wrong-result",
        "missing-attestation",
        "manifest-drift",
    ] {
        request.settled_effect = Some(valid.clone());
        let audit = request.settled_effect.as_mut().unwrap();
        match case {
            "subset" => {
                audit.bundle.targets.pop();
            }
            "duplicate" => {
                audit.bundle.targets[6] = audit.bundle.targets[0].clone();
            }
            "wrong-target" => {
                audit.bundle.targets[6].id = "unconfigured-target".into();
            }
            "unknown-state" => {
                audit.bundle.targets[0].state = TargetState::PendingDelta;
            }
            "stale" => {
                audit.bundle.observed_at =
                    (chrono::Utc::now() - chrono::Duration::hours(2)).to_rfc3339();
            }
            "future" => {
                audit.bundle.observed_at =
                    (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
            }
            "wrong-dispatch" => {
                audit.dispatch_id = "other-dispatch".into();
            }
            "wrong-task" => {
                audit.bundle.task_key = "other-task-key".into();
            }
            "wrong-goal" => {
                audit.bundle.goal_scope_sha256 = pin("different scope");
            }
            "wrong-result" => {
                audit.result_sha256 = pin("different retained result");
            }
            "missing-attestation" => {
                audit.operator_attestation.no_unknown_effects = false;
            }
            _ => {
                audit.bundle.target_set_sha256 = pin("different configured universe");
            }
        }
        refresh_pin(&mut request);
        assert!(
            replan::request_as_server_admin(&pool, &c.id, &request)
                .await
                .is_err(),
            "{case}"
        );
        assert_unchanged(&pool, &c.id, &before).await;
    }
    request.settled_effect = Some(valid.clone());
    request.unattempted_effect = Some(replan::UnattemptedEffect {
        dispatch_id: valid.dispatch_id.clone(),
        result_sha256: valid.result_sha256.clone(),
        audit_sha256: valid.audit_sha256.clone(),
    });
    assert!(
        replan::request_as_server_admin(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("mutually exclusive")
    );
    request.unattempted_effect = None;
    for category in ["plan", "unknown"] {
        let mut receipt: Value = serde_json::from_str(&raw).unwrap();
        receipt["campaign_receipt"]["failure_category"] = json!(category);
        let changed = receipt.to_string();
        sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='partial-effect-dispatch'")
            .bind(&changed)
            .execute(&pool)
            .await
            .unwrap();
        request.settled_effect.as_mut().unwrap().result_sha256 = pin(&changed);
        assert!(
            replan::request_as_server_admin(&pool, &c.id, &request)
                .await
                .is_err(),
            "{category}"
        );
        assert_unchanged(&pool, &c.id, &before).await;
    }
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn settled_partial_effect_rejects_unknown_owners_slots_cleanup_and_published_descendants_pg()
{
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, original, _, request) = held(&pool).await;
    let before = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    // Preserve native status constraints; unsupported states cannot be
    // manufactured by weakening the migrated schema.
    assert!(
        sqlx::query(
            "UPDATE task_dispatches SET status='running' WHERE id='partial-effect-dispatch'"
        )
        .execute(&pool)
        .await
        .is_err()
    );
    for status in [None, Some("unknown")] {
        assert!(
            sqlx::query("UPDATE task_dispatches SET status=$1 WHERE id='partial-effect-dispatch'")
                .bind(status)
                .execute(&pool)
                .await
                .is_err()
        );
    }
    for status in ["failed", "cancelled"] {
        sqlx::query("UPDATE task_dispatches SET status=$1 WHERE id='partial-effect-dispatch'")
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            replan::request_as_server_admin(&pool, &c.id, &request)
                .await
                .is_err()
        );
    }
    sqlx::query("UPDATE task_dispatches SET status='completed' WHERE id='partial-effect-dispatch'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id) VALUES('settled-owner','qwen','qwen','awaiting_user','partial-effect-dispatch')").execute(&pool).await.unwrap();
    for status in [Some("working"), Some("unknown")] {
        assert!(
            sqlx::query("UPDATE sessions SET status=$1 WHERE session_key='settled-owner'")
                .bind(status)
                .execute(&pool)
                .await
                .is_err()
        );
    }
    for status in [None, Some("awaiting_user"), Some("turn_active")] {
        sqlx::query("UPDATE sessions SET status=$1 WHERE session_key='settled-owner'")
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            replan::request_as_server_admin(&pool, &c.id, &request)
                .await
                .is_err()
        );
    }
    sqlx::query("UPDATE sessions SET status='disconnected',active_dispatch_id=NULL WHERE session_key='settled-owner'").execute(&pool).await.unwrap();
    let run: String = sqlx::query_scalar("SELECT run_id FROM auto_queue_entries LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO auto_queue_slots(agent_id,slot_index,assigned_run_id) VALUES('qwen',7,$1)",
    )
    .bind(&run)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        replan::request_as_server_admin(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("ownership")
    );
    sqlx::query("UPDATE auto_queue_slots SET assigned_run_id=NULL WHERE slot_index=7")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO auto_queue_run_cleanup_tasks(run_ids,dispatch_ids) VALUES('{}',ARRAY['partial-effect-dispatch'])").execute(&pool).await.unwrap();
    assert!(
        replan::request_as_server_admin(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("cleanup")
    );
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks")
        .execute(&pool)
        .await
        .unwrap();
    let child = &original.nodes[1];
    sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('settled-descendant','Check','ready','monitoring')").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,version,node_id,role,card_id,contract_digest) VALUES($1,$2,1,'verify','work','settled-descendant',$3)")
        .bind(&child.task_key).bind(&c.id).bind(store::task_digest(child).unwrap()).execute(&pool).await.unwrap();
    assert!(
        replan::request_as_server_admin(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("published")
    );
    sqlx::query("DELETE FROM campaign_graph_tasks WHERE card_id='settled-descendant'")
        .execute(&pool)
        .await
        .unwrap();
    // Obsolete sibling ownership is still owned by this campaign even if it
    // is not a descendant or part of the current business graph.
    sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('settled-sibling','Legacy control','done','monitoring')").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,version,node_id,role,card_id,contract_digest,observation) VALUES('repo:sha:legacy-sibling',$1,1,'legacy-sibling','plan','settled-sibling',$2,'{\"outcome\":\"explicit terminal fixture\"}')")
        .bind(&c.id).bind(pin("retained legacy control contract")).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,completed_at) VALUES('sibling-terminal','settled-sibling','monitoring','review','pending',NOW())").execute(&pool).await.unwrap();
    assert!(
        replan::request_as_server_admin(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("ownership")
    );
    sqlx::query("UPDATE task_dispatches SET status='completed' WHERE id='sibling-terminal'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id) VALUES('sibling-session','monitoring','opencode','awaiting_user','sibling-terminal')").execute(&pool).await.unwrap();
    for status in [None, Some("awaiting_user"), Some("turn_active")] {
        sqlx::query("UPDATE sessions SET status=$1 WHERE session_key='sibling-session'")
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            replan::request_as_server_admin(&pool, &c.id, &request)
                .await
                .is_err()
        );
    }
    sqlx::query("UPDATE sessions SET status='disconnected',active_dispatch_id=NULL WHERE session_key='sibling-session'").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO auto_queue_runs(id,repo,agent_id,status) VALUES('sibling-run','test/repo','monitoring','cancelled')").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO auto_queue_entries(id,run_id,kanban_card_id,agent_id,status) VALUES('sibling-entry','sibling-run','settled-sibling','monitoring','done')").execute(&pool).await.unwrap();
    for status in [None, Some("restoring"), Some("unknown")] {
        sqlx::query("UPDATE auto_queue_runs SET status=$1 WHERE id='sibling-run'")
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            replan::request_as_server_admin(&pool, &c.id, &request)
                .await
                .is_err()
        );
    }
    sqlx::query("UPDATE auto_queue_runs SET status='cancelled' WHERE id='sibling-run'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO auto_queue_slots(agent_id,slot_index,assigned_run_id) VALUES('monitoring',8,'sibling-run')").execute(&pool).await.unwrap();
    assert!(
        replan::request_as_server_admin(&pool, &c.id, &request)
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM auto_queue_slots WHERE agent_id='monitoring' AND slot_index=8")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO auto_queue_run_cleanup_tasks(run_ids,dispatch_ids) VALUES('{}',ARRAY['sibling-terminal'])").execute(&pool).await.unwrap();
    assert!(
        replan::request_as_server_admin(&pool, &c.id, &request)
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM auto_queue_entries WHERE id='sibling-entry'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM auto_queue_runs WHERE id='sibling-run'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_dispatches WHERE id='sibling-terminal'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM campaign_graph_tasks WHERE card_id='settled-sibling'")
        .execute(&pool)
        .await
        .unwrap();
    assert_unchanged(&pool, &c.id, &before).await;
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn settled_partial_effect_stage_activation_idempotency_recheck_gates_and_latest_result_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, raw, request) = held(&pool).await;
    let reply = replan::request_as_server_admin(&pool, &c.id, &request)
        .await
        .unwrap();
    paused(&pool, &c.id, CampaignStatus::Active).await;
    let valid = corrected(request.settled_effect.as_ref().unwrap());
    let mut no_gate = valid.clone();
    no_gate.settled_effect_gates.clear();
    assert!(
        store::stage(&pool, &c.id, &no_gate, Some(&reply.repair_failure_id))
            .await
            .unwrap_err()
            .to_string()
            .contains("audit-bound")
    );
    let mut wrong_gate = valid.clone();
    wrong_gate.settled_effect_gates[0].audit_sha256 = pin("unrelated audit");
    assert!(
        store::stage(&pool, &c.id, &wrong_gate, Some(&reply.repair_failure_id))
            .await
            .unwrap_err()
            .to_string()
            .contains("server-authenticated")
    );
    let mut ungated_effect = valid.clone();
    ungated_effect.nodes[2].dependencies.clear();
    ungated_effect.nodes[2].inputs.clear();
    ungated_effect.validate(&goal()).unwrap();
    assert!(
        store::stage(
            &pool,
            &c.id,
            &ungated_effect,
            Some(&reply.repair_failure_id)
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("every new external effect")
    );
    let version = store::stage(&pool, &c.id, &valid, Some(&reply.repair_failure_id))
        .await
        .unwrap();
    assert_eq!(
        store::stage(&pool, &c.id, &valid, Some(&reply.repair_failure_id))
            .await
            .unwrap(),
        version
    );
    sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE version=$1")
        .bind(version)
        .execute(&pool)
        .await
        .unwrap();
    // A changed retained native result re-pins the old obligation for every path.
    sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='partial-effect-dispatch'")
        .bind(format!("{raw} "))
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        replan::request_as_server_admin(&pool, &c.id, &request)
            .await
            .is_err()
    );
    assert!(
        store::stage(&pool, &c.id, &valid, Some(&reply.repair_failure_id))
            .await
            .is_err()
    );
    assert!(
        store::activate(
            &pool,
            &c.id,
            version,
            campaigns::get(&pool, &c.id).await.unwrap().revision
        )
        .await
        .is_err()
    );
    sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='partial-effect-dispatch'")
        .bind(&raw)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET accepted=TRUE WHERE role='work'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        store::activate(
            &pool,
            &c.id,
            version,
            campaigns::get(&pool, &c.id).await.unwrap().revision
        )
        .await
        .is_err()
    );
    sqlx::query("UPDATE campaign_graph_tasks SET accepted=FALSE WHERE role='work'")
        .execute(&pool)
        .await
        .unwrap();
    let card = store::card_id(&request.settled_effect.as_ref().unwrap().bundle.task_key);
    assert!(sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,created_at) VALUES('forbidden-partial-replay',$1,'qwen','implementation','pending',NOW())").bind(&card).execute(&pool).await.is_err());
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,created_at,completed_at,result) VALUES('later-partial-retry',$1,'qwen','implementation','completed',NOW(),NOW(),$2)").bind(&card).bind(&raw).execute(&pool).await.unwrap();
    assert!(
        replan::request_as_server_admin(&pool, &c.id, &request)
            .await
            .is_err()
    );
    assert!(
        store::activate(
            &pool,
            &c.id,
            version,
            campaigns::get(&pool, &c.id).await.unwrap().revision
        )
        .await
        .is_err()
    );
    sqlx::query("DELETE FROM task_dispatches WHERE id='later-partial-retry'")
        .execute(&pool)
        .await
        .unwrap();
    // Mutating a validated candidate's gate is still fenced at activation.
    sqlx::query("UPDATE campaign_graph_versions SET graph=$2 WHERE version=$1")
        .bind(version)
        .bind(json!(no_gate))
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        store::activate(
            &pool,
            &c.id,
            version,
            campaigns::get(&pool, &c.id).await.unwrap().revision
        )
        .await
        .is_err()
    );
    sqlx::query("UPDATE campaign_graph_versions SET graph=$2 WHERE version=$1")
        .bind(version)
        .bind(json!(valid))
        .execute(&pool)
        .await
        .unwrap();
    store::activate(
        &pool,
        &c.id,
        version,
        campaigns::get(&pool, &c.id).await.unwrap().revision,
    )
    .await
    .unwrap();
    // The same already-declared pending delta may continue after the fresh
    // gates are issued. A later, newly introduced external contract must obtain
    // another fresh pair instead of reusing the old target-state assertion.
    for node in &valid.nodes[..2] {
        store::publish(
            &pool,
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
    }
    let external = &valid.nodes[2];
    assert!(
        store::publish(
            &pool,
            &c.id,
            &store::Publication {
                key: &external.task_key,
                node: &external.id,
                role: "work",
                version: Some(version),
                agent: &external.agent,
                title: &external.title,
                prompt: &external.details,
                digest: &store::task_digest(external).unwrap(),
            }
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("accepted fresh inspection")
    );
    assert_eq!(
        store::stage(&pool, &c.id, &valid, Some(&reply.repair_failure_id))
            .await
            .unwrap(),
        version
    );
    let mut later = valid.clone();
    let mut effect = later.nodes[2].clone();
    effect.id = "later-delta".into();
    effect.task_key.push_str(":later");
    let mut qa = later.nodes[3].clone();
    qa.id = "later-final-qa".into();
    qa.task_key.push_str(":later");
    qa.dependencies = [effect.id.clone()].into();
    qa.inputs[0].dependency = effect.id.clone();
    later.nodes.extend([effect, qa]);
    later.validate(&goal()).unwrap();
    assert!(
        store::stage(&pool, &c.id, &later, Some(&reply.repair_failure_id))
            .await
            .unwrap_err()
            .to_string()
            .contains("newly introduced external contract")
    );
    assert_eq!(
        sqlx::query_scalar::<_, bool>(
            "SELECT accepted FROM campaign_graph_tasks WHERE role='work'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        false
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn settled_partial_effect_share_locks_native_result_and_fences_later_dispatch_insert_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, raw, request) = held(&pool).await;
    let audit = request.settled_effect.as_ref().unwrap();
    let original: Value =
        sqlx::query_scalar("SELECT evidence FROM campaign_graph_failures WHERE id=$1")
            .bind(&request.failure_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let mut locked = pool.begin().await.unwrap();
    replan::settled_effect::authenticated_request_proof(
        &mut locked,
        &c.id,
        &audit.bundle.task_key,
        &goal(),
        &original,
        audit,
        true,
    )
    .await
    .unwrap();
    let mut result_connection = pool.acquire().await.unwrap();
    sqlx::query("SET application_name='settled-result-race'")
        .execute(&mut *result_connection)
        .await
        .unwrap();
    let changed = format!("{raw} ");
    let update = tokio::spawn(async move {
        sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='partial-effect-dispatch'")
            .bind(changed)
            .execute(&mut *result_connection)
            .await
            .unwrap();
    });
    let mut dispatch_connection = pool.acquire().await.unwrap();
    sqlx::query("SET application_name='settled-dispatch-race'")
        .execute(&mut *dispatch_connection)
        .await
        .unwrap();
    let card = store::card_id(&audit.bundle.task_key);
    let insert = tokio::spawn(async move {
        sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,created_at,completed_at) VALUES('locked-later-dispatch',$1,'qwen','implementation','completed',NOW(),NOW())")
            .bind(card).execute(&mut *dispatch_connection).await.unwrap();
    });
    // Observe real PostgreSQL lock waits; scheduler delay is not a passing race.
    tokio::time::timeout(std::time::Duration::from_secs(5),async {
        loop {
            let waiting: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name IN ('settled-result-race','settled-dispatch-race') AND wait_event_type='Lock'")
                .fetch_one(&pool).await.unwrap();
            if waiting == 2 { break; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("both real write races must wait on retained-proof locks");
    assert!(!update.is_finished() && !insert.is_finished());
    locked.commit().await.unwrap();
    update.await.unwrap();
    insert.await.unwrap();
    assert!(
        replan::request_as_server_admin(&pool, &c.id, &request)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_reconciliations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    pool.close().await;
    fixture.drop().await;
}

async fn native_receipt(pool: &PgPool, card: &str, agent: &str, dispatch: &str, extra: Value) {
    let mut result = json!({"summary":crate::db::campaigns::graph::result::CAPTURE_SUMMARY,
        "work_outcome":"artifact","completion_source":"campaign_native_receipt",
        "campaign_receipt":{"outcome":"passed","summary":"Explicit independent source/current-state fixture",
            "observations":[{"command":"real fixture assertion and hash-verified artifact","exit_code":0,"evidence":"Fixture completed its declared check"}]}});
    for (key, value) in extra.as_object().unwrap() {
        result[key] = value.clone();
    }
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,completed_at) VALUES($1,$2,$3,'implementation','completed',$4,NOW())")
        .bind(dispatch).bind(card).bind(agent).bind(result.to_string()).execute(&mut *tx).await.unwrap();
    sqlx::query("UPDATE kanban_cards SET status='in_progress',latest_dispatch_id=$2 WHERE id=$1")
        .bind(card)
        .bind(dispatch)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn settled_partial_effect_native_plan_qa_artifact_progression_completes_once_without_old_acceptance_pg()
 {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, _, raw, request) = held(&pool).await;
    let reply = replan::request_as_server_admin(&pool, &c.id, &request)
        .await
        .unwrap();
    paused(&pool, &c.id, CampaignStatus::Active).await;
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    let _ = tick(&pool, &engine, &c.id, None).await;
    let planner = store::card_id(&format!("{}:plan:{}", c.id, reply.repair_failure_id));
    let graph = corrected(request.settled_effect.as_ref().unwrap());
    native_receipt(
        &pool,
        &planner,
        "qwen",
        "settled-native-plan",
        json!({"campaign_graph":graph}),
    )
    .await;
    let _planner_workspace = prepare_control_fixture(&pool, "settled-native-plan").await;
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let candidate = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(candidate["versions"][0]["status"], "candidate");
    assert_eq!(candidate["active_version"], 1);
    assert!(
        store::activate(
            &pool,
            &c.id,
            2,
            campaigns::get(&pool, &c.id).await.unwrap().revision
        )
        .await
        .is_err()
    );
    control(
        &pool,
        &c.id,
        &goal(),
        "validate",
        Some(2),
        "2",
        "Independent candidate QA",
    )
    .await
    .unwrap();
    let validator = store::card_id(&format!("{}:validate:2", c.id));
    native_receipt(&pool,&validator,"monitoring","settled-native-candidate-qa",json!({"graph_validation":{"digest":candidate["versions"][0]["digest"],"approved":true,"reason":"Audit-bound fresh target QA and only remaining deltas verified against exact source requirements"}})).await;
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    super::replan_tests::settle(&pool).await;
    store::activate(
        &pool,
        &c.id,
        2,
        campaigns::get(&pool, &c.id).await.unwrap().revision,
    )
    .await
    .unwrap();
    let mut manifests = BTreeMap::new();
    for node in &graph.nodes {
        // Native candidate/ownership gates already ran; this test executes the
        // real artifact handoff/capture/observer for every ordered business node.
        let workspace = tempfile::tempdir().unwrap();
        artifacts::materialize(
            &artifact_root().unwrap(),
            workspace.path(),
            node,
            &manifests,
        )
        .unwrap();
        std::fs::write(
            workspace.path().join(&node.outputs[0].path),
            json!({"coverage":7,"assertion":true,"node":node.id}).to_string(),
        )
        .unwrap();
        let card = store::publish(
            &pool,
            &c.id,
            &store::Publication {
                key: &node.task_key,
                node: &node.id,
                role: "work",
                version: Some(2),
                agent: &node.agent,
                title: &node.title,
                prompt: &node.details,
                digest: &store::task_digest(node).unwrap(),
            },
        )
        .await
        .unwrap();
        let prompt: String = sqlx::query_scalar("SELECT description FROM kanban_cards WHERE id=$1")
            .bind(&card)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(prompt.contains(&request.settled_effect.as_ref().unwrap().audit_sha256));
        assert!(prompt.contains("configured-target-6"));
        let dispatch = format!("settled-native-work-{}", node.id);
        native_receipt(&pool, &card, &node.agent, &dispatch, json!({})).await;
        sqlx::query("UPDATE task_dispatches SET context=$2 WHERE id=$1")
            .bind(&dispatch)
            .bind(json!({"worktree_path":workspace.path()}).to_string())
            .execute(&pool)
            .await
            .unwrap();
        reconcile::observe(&pool, &c.id, &goal(), None)
            .await
            .unwrap();
        let manifest: Value = sqlx::query_scalar(
            "SELECT artifact_manifest FROM campaign_graph_tasks WHERE task_key=$1 AND accepted",
        )
        .bind(&node.task_key)
        .fetch_one(&pool)
        .await
        .unwrap();
        artifacts::verify(&artifact_root().unwrap(), &manifest).unwrap();
        manifests.insert(node.id.clone(), manifest);
        reconcile::settle_accepted(&pool, &engine, &c.id)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM kanban_cards WHERE id=$1")
                .bind(&card)
                .fetch_one(&pool)
                .await
                .unwrap(),
            "done",
            "accepted artifacts settle through the canonical card transition"
        );
    }
    super::replan_tests::settle(&pool).await;
    tick(&pool, &engine, &c.id, None).await.unwrap();
    assert_eq!(
        campaigns::get(&pool, &c.id).await.unwrap().status,
        CampaignStatus::Completed
    );
    tick(&pool, &engine, &c.id, None).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM campaign_dag_samples WHERE campaign_id=$1"
        )
        .bind(&c.id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, bool>(
            "SELECT accepted FROM campaign_graph_tasks WHERE task_key=$1"
        )
        .bind(&request.settled_effect.as_ref().unwrap().bundle.task_key)
        .fetch_one(&pool)
        .await
        .unwrap(),
        false
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_dispatches WHERE id='partial-effect-dispatch'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        raw
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_dispatches WHERE kanban_card_id=$1"
        )
        .bind(store::card_id(
            &request.settled_effect.as_ref().unwrap().bundle.task_key
        ))
        .fetch_one(&pool)
        .await
        .unwrap(),
        1,
        "original effects never replayed"
    );
    pool.close().await;
    fixture.drop().await;
}
