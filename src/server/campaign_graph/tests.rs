use super::*;
use crate::db::campaigns::graph::{Effect, Format, Input, Kind, Output, Task, progress};
#[cfg(test)]
#[path = "budget_revision_tests.rs"]
mod budget_revision_tests;
#[cfg(test)]
#[path = "cancelled_validator_replan_tests.rs"]
mod cancelled_validator_replan_tests;
#[cfg(test)]
#[path = "effect_ownership_tests.rs"]
mod effect_ownership_tests;
#[cfg(test)]
#[path = "graph_card_identity_tests.rs"]
mod graph_card_identity_tests;
#[cfg(test)]
#[path = "initial_plan_replan_tests.rs"]
mod initial_plan_replan_tests;
#[cfg(test)]
#[path = "learning_tests.rs"]
mod learning_tests;
#[cfg(test)]
#[path = "planning_input_tests.rs"]
mod planning_input_tests;
#[cfg(test)]
#[path = "planning_ownership_tests.rs"]
mod planning_ownership_tests;
#[cfg(test)]
#[path = "planning_recovery_tests.rs"]
mod planning_recovery_tests;
#[cfg(test)]
#[path = "proposal_identity_tests.rs"]
mod proposal_identity_tests;
#[cfg(test)]
#[path = "recovery_tests.rs"]
mod recovery_tests;
#[cfg(test)]
#[path = "rejected_candidate_replan_tests.rs"]
mod rejected_candidate_replan_tests;
#[cfg(test)]
#[path = "replan_artifact_tests.rs"]
mod replan_artifact_tests;
#[cfg(test)]
#[path = "replan_effect_tests.rs"]
mod replan_effect_tests;
#[cfg(test)]
#[path = "replan_policy_tests.rs"]
mod replan_policy_tests;
#[cfg(test)]
#[path = "replan_settled_effect_tests.rs"]
mod replan_settled_effect_tests;
#[cfg(test)]
#[path = "replan_tests.rs"]
mod replan_tests;
#[cfg(test)]
#[path = "replan_validation_tests.rs"]
mod replan_validation_tests;
#[cfg(test)]
#[path = "result_transport_tests.rs"]
mod result_transport_tests;
#[cfg(test)]
#[path = "revision_tests.rs"]
mod revision_tests;
#[cfg(test)]
#[path = "schedule_budget_tests.rs"]
mod schedule_budget_tests;
#[cfg(test)]
#[path = "schedule_rollover_tests.rs"]
mod schedule_rollover_tests;
#[cfg(test)]
#[path = "validation_recovery_tests.rs"]
mod validation_recovery_tests;
#[cfg(test)]
#[path = "workflow_tests.rs"]
mod workflow_tests;

fn goal() -> Goal {
    serde_json::from_value(json!({"objective":"Generate and validate answer","success":["answer equals 42"],"constraints":["no external mutations"],"repo":"test/repo","planner":"qwen","verifier":"monitoring","agents":["qwen","monitoring"],"budget":{"max_dispatches":12,"max_elapsed_secs":600,"node_timeout_secs":120,"max_evolutions":2,"max_observed_tool_calls":20}})).unwrap()
}
fn graph() -> Graph {
    Graph {
        settled_effect_gates: vec![],
        nodes: vec![
            Task {
                id: "produce".into(),
                task_key: "repo:sha:produce".into(),
                title: "Produce".into(),
                agent: "qwen".into(),
                kind: Kind::Execute,
                effect: Effect::ArtifactOnly,
                details: "Write answer.json".into(),
                acceptance: vec!["JSON integer 42".into()],
                dependencies: Default::default(),
                inputs: vec![],
                outputs: vec![Output {
                    path: "answer.json".into(),
                    format: Format::Json,
                }],
            },
            Task {
                id: "verify".into(),
                task_key: "repo:sha:verify".into(),
                title: "Check".into(),
                agent: "monitoring".into(),
                kind: Kind::Verify,
                effect: Effect::ArtifactOnly,
                details: "Parse supplied answer and assert equals 42".into(),
                acceptance: vec!["real assertion".into()],
                dependencies: ["produce".into()].into(),
                inputs: vec![Input {
                    dependency: "produce".into(),
                    source: "answer.json".into(),
                    destination: "input/answer.json".into(),
                }],
                outputs: vec![Output {
                    path: "verified.json".into(),
                    format: Format::Json,
                }],
            },
        ],
    }
}
#[test]
fn graph_contract_rejects_cycles_missing_outputs_paths_and_self_verification() {
    let g = goal();
    let good = graph();
    good.validate(&g).unwrap();
    let mut bad = good.clone();
    bad.nodes[0].dependencies.insert("verify".into());
    assert!(bad.validate(&g).is_err());
    bad = good.clone();
    bad.nodes[1].inputs[0].source = "absent".into();
    assert!(bad.validate(&g).is_err());
    bad = good.clone();
    bad.nodes[1].inputs[0].destination = "../escape".into();
    assert!(bad.validate(&g).is_err());
    bad = good.clone();
    bad.nodes[1].agent = "qwen".into();
    assert!(bad.validate(&g).is_err());
    bad = good;
    bad.nodes[0].outputs.clear();
    assert!(bad.validate(&g).is_err());
}
#[test]
fn artifact_handoff_uses_frozen_bytes_rejects_corruption_and_conflicting_inputs() {
    let temp = tempfile::tempdir().unwrap();
    let a = temp.path().join("a");
    let b = temp.path().join("b");
    let root = temp.path().join("artifacts");
    std::fs::create_dir(&a).unwrap();
    std::fs::create_dir(&b).unwrap();
    std::fs::write(a.join("answer.json"), "42").unwrap();
    let graph = graph();
    let m = artifacts::capture(&a, &graph.nodes[0], &root).unwrap();
    std::fs::write(a.join("answer.json"), "99").unwrap();
    let manifests = [("produce".to_owned(), m.clone())].into();
    artifacts::materialize(&root, &b, &graph.nodes[1], &manifests).unwrap();
    assert_eq!(
        std::fs::read_to_string(b.join("input/answer.json")).unwrap(),
        "42"
    );
    artifacts::materialize(&root, &b, &graph.nodes[1], &manifests).unwrap();
    artifacts::verify_inputs(&root, &b, &graph.nodes[1], &manifests).unwrap();
    std::fs::write(b.join("input/answer.json"), "different work").unwrap();
    assert!(artifacts::verify_inputs(&root, &b, &graph.nodes[1], &manifests).is_err());
    assert!(artifacts::materialize(&root, &b, &graph.nodes[1], &manifests).is_err());
    std::fs::write(
        root.join(m["bundle"].as_str().unwrap()).join("answer.json"),
        "bad",
    )
    .unwrap();
    assert!(artifacts::verify(&root, &m).is_err());
}
#[test]
fn artifact_capture_accepts_file_boundary_and_rejects_one_extra_byte_without_publication() {
    use crate::db::campaigns::graph::MAX_ARTIFACT_FILE_BYTES;
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let mut task = graph().nodes.remove(0);
    task.outputs = vec![Output {
        path: "history.txt".into(),
        format: Format::Text,
    }];
    let file = std::fs::File::create(workspace.join("history.txt")).unwrap();
    file.set_len(MAX_ARTIFACT_FILE_BYTES).unwrap();
    let accepted = artifacts::capture(&workspace, &task, &temp.path().join("accepted")).unwrap();
    artifacts::verify(&temp.path().join("accepted"), &accepted).unwrap();
    file.set_len(MAX_ARTIFACT_FILE_BYTES + 1).unwrap();
    let rejected_root = temp.path().join("rejected");
    let error = artifacts::capture(&workspace, &task, &rejected_root)
        .unwrap_err()
        .to_string();
    assert!(error.contains("history.txt"), "{error}");
    assert!(error.contains("4194305 bytes"), "{error}");
    assert!(error.contains("maximum is 4194304 bytes"), "{error}");
    assert!(!rejected_root.exists(), "oversized output was published");
}
#[test]
fn artifact_capture_checks_total_bundle_boundary_before_publication() {
    use crate::db::campaigns::graph::MAX_ARTIFACT_FILE_BYTES;
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let mut task = graph().nodes.remove(0);
    task.outputs = (0..4)
        .map(|n| {
            let path = format!("part{n}.txt");
            std::fs::File::create(workspace.join(&path))
                .unwrap()
                .set_len(MAX_ARTIFACT_FILE_BYTES)
                .unwrap();
            Output {
                path,
                format: Format::Text,
            }
        })
        .collect();
    artifacts::capture(&workspace, &task, &temp.path().join("accepted")).unwrap();
    std::fs::write(workspace.join("extra.txt"), "x").unwrap();
    task.outputs.push(Output {
        path: "extra.txt".into(),
        format: Format::Text,
    });
    let rejected_root = temp.path().join("rejected");
    let error = artifacts::capture(&workspace, &task, &rejected_root)
        .unwrap_err()
        .to_string();
    assert!(error.contains("extra.txt"), "{error}");
    assert!(error.contains("16777217 bytes"), "{error}");
    assert!(error.contains("maximum is 16777216 bytes"), "{error}");
    assert!(!rejected_root.exists(), "oversized bundle was published");
}
#[test]
fn failure_classification_does_not_replay_unknown_and_redacts_credentials() {
    let (category, e) = observation::failure(
        "failed",
        &json!({"campaign_receipt":{"summary":"token=secret password:pw failure"}}),
    );
    assert_eq!(category, "unknown");
    assert!(!e.to_string().contains("secret"));
    assert!(!e.to_string().contains("pw"));
    assert!(!observation::receipt(&json!({"summary":"done"})));
    assert!(!observation::repeated(&vec!["x".into(); 2], 8));
    assert!(observation::repeated(&vec!["x".into(); 8], 8));
    let mut samples = vec!["x".into(); 8];
    samples[7] = "different".into();
    assert!(!observation::repeated(&samples, 8));
}
async fn setup(pool: &PgPool) -> campaigns::Campaign {
    sqlx::query("INSERT INTO agents(id,name,provider) VALUES('qwen','Qwen','qwen'),('monitoring','OpenCode','opencode')").execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO kv_meta(key,value) VALUES('runtime-config','{\"internalTasksEnabled\":true,\"campaignSuperiorAgentId\":\"qwen\"}')",
    )
    .execute(pool)
    .await
    .unwrap();
    let c = campaigns::create(
        pool,
        "native-goal".into(),
        serde_json::from_value(json!({"title":"Goal","status":"active","round":1,"nodes":[]}))
            .unwrap(),
    )
    .await
    .unwrap();
    store::enroll(pool, &c.id, c.revision, &goal())
        .await
        .unwrap();
    c
}

// Explicit provider fixtures must consume the same immutable input path as
// native intake. Keep the workspace alive through reconciliation.
async fn prepare_control_fixture(pool: &PgPool, dispatch: &str) -> tempfile::TempDir {
    let workspace = tempfile::tempdir().unwrap();
    let raw: Option<String> =
        sqlx::query_scalar("SELECT context::text FROM task_dispatches WHERE id=$1")
            .bind(dispatch)
            .fetch_one(pool)
            .await
            .unwrap();
    let mut context: Value = serde_json::from_str(raw.as_deref().unwrap_or("{}")).unwrap();
    assert!(context.is_object());
    context["worktree_path"] = json!(workspace.path());
    sqlx::query("UPDATE task_dispatches SET context=$2 WHERE id=$1")
        .bind(dispatch)
        .bind(context.to_string())
        .execute(pool)
        .await
        .unwrap();
    crate::services::campaign_control_input::prepare_input(pool, dispatch)
        .await
        .unwrap();
    workspace
}

fn canonical_inspection(mut state: Value) -> Value {
    // Canonical-state assertions retain counters and contracts, while a fresh
    // read timestamp correctly changes between otherwise identical reads.
    if let Some(budget) = state["control_budget"].as_object_mut() {
        budget.remove("observed_at");
    }
    state
}
#[tokio::test]
async fn graph_atomic_activation_publication_pause_and_budget_persistence_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    // The same native receipt/graph contract supports independent Codex agents.
    // Provider choice must not weaken structural or independent-verifier gates.
    sqlx::query("UPDATE agents SET provider='codex' WHERE id IN ('qwen','monitoring')")
        .execute(&pool)
        .await
        .unwrap();
    let codex = campaigns::create(
        &pool,
        "codex-goal".into(),
        serde_json::from_value(
            json!({"title":"Codex goal","status":"active","round":1,"nodes":[]}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    store::enroll(&pool, &codex.id, codex.revision, &goal())
        .await
        .unwrap();
    let mut self_verified = graph();
    self_verified.nodes[1].agent = self_verified.nodes[0].agent.clone();
    assert!(
        store::stage(&pool, &codex.id, &self_verified, None)
            .await
            .is_err()
    );
    store::stage(&pool, &codex.id, &graph(), None)
        .await
        .unwrap();
    let g = graph();
    let version = store::stage(&pool, &c.id, &g, None).await.unwrap();
    assert!(
        store::activate(&pool, &c.id, version, c.revision)
            .await
            .is_err()
    );
    sqlx::query("UPDATE campaign_graph_versions SET status='validated'")
        .execute(&pool)
        .await
        .unwrap();
    store::activate(&pool, &c.id, version, c.revision)
        .await
        .unwrap();
    assert!(
        store::activate(&pool, &c.id, version, c.revision)
            .await
            .is_err()
    );
    let n = &g.nodes[0];
    let digest = store::task_digest(n).unwrap();
    let p = store::Publication {
        key: &n.task_key,
        node: &n.id,
        role: "work",
        version: Some(version),
        agent: &n.agent,
        title: &n.title,
        prompt: &n.details,
        digest: &digest,
    };
    let (a, b) = tokio::join!(
        store::publish(&pool, &c.id, &p),
        store::publish(&pool, &c.id, &p)
    );
    assert_eq!(a.as_ref().unwrap(), b.as_ref().unwrap());
    let card = a.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM auto_queue_entries")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let wrong=sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('wrong-executor',$1,'monitoring','implementation','pending')").bind(&card).execute(&pool).await.unwrap_err();
    assert!(wrong.to_string().contains("enrolled native executor"));
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('attempt',$1,'qwen','implementation','pending')").bind(&card).execute(&pool).await.unwrap();
    assert!(sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('duplicate',$1,'qwen','implementation','pending')").bind(&card).execute(&pool).await.is_err());
    progress::tool_use(Some(&pool), Some("attempt"), Some("tool-1"), "hash").await;
    progress::tool_use(Some(&pool), Some("attempt"), Some("tool-1"), "hash").await;
    progress::tool_result(Some(&pool), Some("attempt"), Some("tool-1"), "outputhash").await;
    let reconnected = fixture.connect_and_migrate().await;
    assert_eq!(
        store::status(&reconnected, &c.id).await.unwrap()["observed_tool_calls"],
        1
    );
    let c = campaigns::get(&pool, &c.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    campaigns::replace(&pool, &c.id, c.revision, input)
        .await
        .unwrap();
    assert!(
        sqlx::query("UPDATE task_dispatches SET status='running' WHERE id='attempt'")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(store::publish(&pool, &c.id, &p).await.is_err());
    reconnected.close().await;
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn graph_failure_evolution_activation_and_rollback_preserve_effects_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let original = graph();
    let v1 = store::stage(&pool, &c.id, &original, None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated'")
        .execute(&pool)
        .await
        .unwrap();
    store::activate(&pool, &c.id, v1, c.revision).await.unwrap();
    let n = &original.nodes[0];
    let digest = store::task_digest(n).unwrap();
    let card = store::publish(
        &pool,
        &c.id,
        &store::Publication {
            key: &n.task_key,
            node: &n.id,
            role: "work",
            version: Some(v1),
            agent: &n.agent,
            title: &n.title,
            prompt: &n.details,
            digest: &digest,
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status,result,completed_at) VALUES('failed-attempt',$1,'implementation','failed',$2,NOW())")
        .bind(&card).bind(json!({"campaign_receipt":{"failure_category":"code","summary":"assertion: answer was 41"}}).to_string()).execute(&pool).await.unwrap();
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let state = store::status(&pool, &c.id).await.unwrap();
    let failure = state["failures"][0]["id"].as_str().unwrap();
    let mut repaired = original.clone();
    repaired.nodes[0].task_key = "repo:sha:produce-fixed".into();
    repaired.nodes[0].effect = Effect::External;
    repaired.nodes[0].details = "Write the corrected integer 42".into();
    repaired.nodes[1].task_key = "repo:sha:verify-fixed".into();
    assert!(store::stage(&pool, &c.id, &repaired, None).await.is_err());
    let v2 = store::stage(&pool, &c.id, &repaired, Some(failure))
        .await
        .unwrap();
    assert_eq!(
        store::stage(&pool, &c.id, &repaired, Some(failure))
            .await
            .unwrap(),
        v2
    );
    let c = campaigns::get(&pool, &c.id).await.unwrap();
    assert!(store::activate(&pool, &c.id, v2, c.revision).await.is_err());
    sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE version=$1")
        .bind(v2)
        .execute(&pool)
        .await
        .unwrap();
    store::activate(&pool, &c.id, v2, c.revision).await.unwrap();
    let c = campaigns::get(&pool, &c.id).await.unwrap();
    store::activate(&pool, &c.id, v1, c.revision).await.unwrap();
    assert!(
        sqlx::query("UPDATE task_dispatches SET status='pending' WHERE id='failed-attempt'")
            .execute(&pool)
            .await
            .is_err()
    );
    let c = campaigns::get(&pool, &c.id).await.unwrap();
    store::activate(&pool, &c.id, v2, c.revision).await.unwrap();
    let n = &repaired.nodes[0];
    let digest = store::task_digest(n).unwrap();
    store::publish(
        &pool,
        &c.id,
        &store::Publication {
            key: &n.task_key,
            node: &n.id,
            role: "work",
            version: Some(v2),
            agent: &n.agent,
            title: &n.title,
            prompt: &n.details,
            digest: &digest,
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET accepted=TRUE WHERE task_key=$1")
        .bind(&n.task_key)
        .execute(&pool)
        .await
        .unwrap();
    let c = campaigns::get(&pool, &c.id).await.unwrap();
    assert!(
        store::activate(&pool, &c.id, v1, c.revision).await.is_err(),
        "rollback cannot forget a completed effect"
    );
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["active_version"],
        v2
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn scheduled_goal_admission_is_atomic_coalesced_and_nonoverlapping_pg() {
    use crate::db::campaigns::graph::schedule;
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let mut g = goal();
    g.schedule = Some(schedule::Schedule {
        timezone: "Asia/Seoul".into(),
        daily_minutes: [0, 30].into(),
    });
    let now = chrono::DateTime::parse_from_rfc3339("2026-09-28T15:35:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    assert_eq!(
        g.schedule
            .as_ref()
            .unwrap()
            .latest(now)
            .unwrap()
            .unwrap()
            .to_rfc3339(),
        "2026-09-28T15:30:00+00:00"
    );
    sqlx::query("UPDATE campaign_execution_specs SET spec=$1,created_at=$2 WHERE campaign_id=$3")
        .bind(serde_json::to_value(&g).unwrap())
        .bind(now - chrono::Duration::hours(1))
        .bind(&c.id)
        .execute(&pool)
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        schedule::admit(&pool, &c.id, &g, now),
        schedule::admit(&pool, &c.id, &g, now)
    );
    let child = a.unwrap().unwrap();
    assert_eq!(b.unwrap().as_deref(), Some(child.as_str()));
    assert!(store::goal(&pool, &child).await.unwrap().schedule.is_none());
    assert!(
        schedule::admit(&pool, &c.id, &g, now + chrono::Duration::days(1))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_occurrences")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let child = campaigns::get(&pool, &child).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&child).unwrap()).unwrap();
    // Operator cancellation closes an occurrence without inventing completed work.
    input.status = CampaignStatus::Cancelled;
    campaigns::replace(&pool, &child.id, child.revision, input)
        .await
        .unwrap();
    assert!(
        schedule::admit(&pool, &c.id, &g, now + chrono::Duration::days(2))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_occurrences")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn captured_unverified_artifact_can_evolve_but_late_verification_fences_activation_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    let original = graph();
    let v1 = store::stage(&pool, &c.id, &original, None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated'")
        .execute(&pool)
        .await
        .unwrap();
    store::activate(&pool, &c.id, v1, c.revision).await.unwrap();
    for n in &original.nodes {
        let digest = store::task_digest(n).unwrap();
        store::publish(
            &pool,
            &c.id,
            &store::Publication {
                key: &n.task_key,
                node: &n.id,
                role: "work",
                version: Some(v1),
                agent: &n.agent,
                title: &n.title,
                prompt: &n.details,
                digest: &digest,
            },
        )
        .await
        .unwrap();
    }
    sqlx::query(
        "UPDATE campaign_graph_tasks SET accepted=TRUE,observation='{}' WHERE node_id='produce'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let verifier = store::card_id(&original.nodes[1].task_key);
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status,result,completed_at) VALUES('bad-check',$1,'implementation','failed',$2,NOW())")
        .bind(verifier).bind(json!({"campaign_receipt":{"failure_category":"code","summary":"generated code failed its behavioral assertion"}}).to_string()).execute(&pool).await.unwrap();
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let state = store::status(&pool, &c.id).await.unwrap();
    let failure = state["failures"][0]["id"].as_str().unwrap();
    let mut next = original.clone();
    for n in &mut next.nodes {
        n.task_key.push_str(":corrected");
    }
    let v2 = store::stage(&pool, &c.id, &next, Some(failure))
        .await
        .expect("unverified pure artifacts must be replaceable");
    sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE version=$1")
        .bind(v2)
        .execute(&pool)
        .await
        .unwrap();
    // Model a concurrent verifier settlement arriving between staging and activation.
    sqlx::query("UPDATE campaign_graph_tasks SET accepted=TRUE WHERE node_id='verify'")
        .execute(&pool)
        .await
        .unwrap();
    let c = campaigns::get(&pool, &c.id).await.unwrap();
    assert!(store::activate(&pool, &c.id, v2, c.revision).await.is_err());
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["active_version"],
        v1
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn cancelled_dispatch_lifetime_freezes_and_unknown_plan_is_held_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    control(&pool, &c.id, &goal(), "plan", None, "initial", GRAPH_SCHEMA)
        .await
        .unwrap();
    let card = store::card_id(&format!("{}:plan:initial", c.id));
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status,created_at,updated_at) VALUES('cancelled-plan',$1,'implementation','cancelled',NOW()-INTERVAL '1 hour',NOW()-INTERVAL '3590 seconds')")
        .bind(card).execute(&pool).await.unwrap();
    assert!((progress::elapsed_seconds(&pool, &c.id).await.unwrap() - 10.0).abs() < 0.01);
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    assert!(
        store::execution_prompt(&pool, "cancelled-plan")
            .await
            .unwrap()
            .unwrap()
            .contains(GRAPH_SCHEMA)
    );
    assert!(
        store::execution_prompt(&pool, "unbound-dispatch")
            .await
            .unwrap()
            .is_none()
    );
    let state = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(state["failures"][0]["category"], "unknown");
    assert!(
        state["hold_reason"]
            .as_str()
            .unwrap()
            .contains("reconciliation")
    );
    assert!((state["elapsed_dispatch_seconds"].as_f64().unwrap() - 10.0).abs() < 0.01);
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn held_terminal_failure_releases_owned_queue_after_restart_without_replay_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    control(&pool, &c.id, &goal(), "plan", None, "initial", GRAPH_SCHEMA)
        .await
        .unwrap();
    let card = store::card_id(&format!("{}:plan:initial", c.id));
    let run: String =
        sqlx::query_scalar("SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=$1")
            .bind(&card)
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query("UPDATE auto_queue_runs SET status='active' WHERE id=$1")
        .bind(&run)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO auto_queue_runs(id,agent_id,status) VALUES('unrelated-run','qwen','active')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO auto_queue_slots(agent_id,slot_index,assigned_run_id,assigned_thread_group) VALUES('qwen',0,$1,0),('qwen',1,'unrelated-run',0)").bind(&run).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('failed-plan',$1,'qwen','implementation','failed')").bind(&card).execute(&pool).await.unwrap();
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let before = store::status(&pool, &c.id).await.unwrap();
    assert!(before["hold_reason"].is_string());
    pool.close().await;

    let reconnected = fixture.connect_and_migrate().await;
    assert!(
        reconciliation_candidates(&reconnected)
            .await
            .unwrap()
            .contains(&c.id)
    );
    assert!(
        reconcile::cleanup_if_held(&reconnected, &c.id, None)
            .await
            .unwrap()
    );
    assert!(
        reconcile::cleanup_if_held(&reconnected, &c.id, None)
            .await
            .unwrap()
    );
    let after = store::status(&reconnected, &c.id).await.unwrap();
    assert_eq!(after["hold_reason"], before["hold_reason"]);
    assert_eq!(after["failures"], before["failures"]);
    assert_eq!(after["tasks"], before["tasks"]);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM auto_queue_runs WHERE id=$1")
            .bind(&run)
            .fetch_one(&reconnected)
            .await
            .unwrap(),
        "completed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM auto_queue_slots WHERE assigned_run_id=$1"
        )
        .bind(&run)
        .fetch_one(&reconnected)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT assigned_run_id FROM auto_queue_slots WHERE agent_id='qwen' AND slot_index=1"
        )
        .fetch_one(&reconnected)
        .await
        .unwrap(),
        "unrelated-run"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_dispatches WHERE kanban_card_id=$1"
        )
        .bind(&card)
        .fetch_one(&reconnected)
        .await
        .unwrap(),
        1
    );
    assert!(
        !reconciliation_candidates(&reconnected)
            .await
            .unwrap()
            .contains(&c.id)
    );
    reconnected.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn held_cleanup_preserves_queue_with_unrelated_card_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    control(&pool, &c.id, &goal(), "plan", None, "initial", GRAPH_SCHEMA)
        .await
        .unwrap();
    let card = store::card_id(&format!("{}:plan:initial", c.id));
    let run: String =
        sqlx::query_scalar("SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=$1")
            .bind(&card)
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query(
        "INSERT INTO kanban_cards(id,title,status) VALUES('unrelated-card','Preserve','ready')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO auto_queue_entries(id,run_id,kanban_card_id,agent_id,priority_rank) VALUES('unrelated-entry',$1,'unrelated-card','qwen',2)").bind(&run).execute(&pool).await.unwrap();
    store::hold(&pool, &c.id, "Reconcile unknown effects")
        .await
        .unwrap();
    assert!(
        reconcile::cleanup_if_held(&pool, &c.id, None)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM auto_queue_runs WHERE id=$1")
            .bind(&run)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "generated"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM auto_queue_entries WHERE id='unrelated-entry'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "pending"
    );
    pool.close().await;
    fixture.drop().await;
}
