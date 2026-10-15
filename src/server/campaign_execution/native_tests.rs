//! Real Linux files, protected checker children, PostgreSQL, queue and FSM.
//! Implementation output is a fixture, not a real model-completion claim.
use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;
use checks::{
    contract::{self, Review},
    native_tests::{answer, engine, fixture},
};
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

struct Fixture {
    _roots: Vec<tempfile::TempDir>,
    registry: PathBuf,
    contracts: BTreeMap<String, contract::Contract>,
    plan: plan::Plan,
    campaign: Campaign,
}

async fn setup(pool: &PgPool) -> Fixture {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    let input: CampaignInput=serde_json::from_value(json!({
        "title":"verified handoff", "description":"A to B plus independent C", "status":"active","round":1,
        "nodes":[
            {"id":"a","title":"Produce A","status":"pending","stage":"build","round":1,"acceptance":["answer is 42"]},
            {"id":"b","title":"Consume A","status":"pending","stage":"build","round":1,"dependencies":["a"],"acceptance":["answer is A plus 1"]},
            {"id":"c","title":"Independent C","status":"pending","stage":"build","round":1,"acceptance":["answer is 42"]}
        ]})).unwrap();
    let campaign = campaigns::create(pool, "handoff".into(), input)
        .await
        .unwrap();
    sqlx::query("INSERT INTO agents(id,name,provider,discord_channel_id,execution_requirements) VALUES('canary','canary','opencode','123','{\"execution_limits\":{\"max_elapsed_secs\":60,\"max_tool_calls\":16}}')")
        .execute(pool).await.unwrap();
    let mut roots = Vec::new();
    let mut contracts = BTreeMap::new();
    let mut nodes = BTreeMap::new();
    for id in ["a", "b", "c"] {
        let expected = if id == "b" {
            "int(pathlib.Path(sys.argv[1], 'input/a.txt').read_text()) + 1"
        } else {
            "42"
        };
        let script = format!(
            "import json,pathlib,sys\nprint(json.dumps({{'passed': int(pathlib.Path(sys.argv[1], 'answer.txt').read_text()) == {expected}}}))\n"
        );
        let (root, contract) = fixture(&script, Review::None);
        roots.push(root);
        contracts.insert(id.to_string(), contract);
        nodes.insert(
            id.to_string(),
            plan::Node {
                task_key: format!("canary-{id}"),
                check_template: id.into(),
                auto_rework: true,
                inputs: if id == "b" {
                    vec![plan::Input {
                        dependency: "a".into(),
                        source: "answer.txt".into(),
                        destination: "input/a.txt".into(),
                    }]
                } else {
                    vec![]
                },
            },
        );
    }
    let registry = roots[0].path().join("registry.json");
    fs::write(
        &registry,
        serde_json::to_vec(
            &contracts
                .iter()
                .map(|(id, c)| (id, &c.template))
                .collect::<BTreeMap<_, _>>(),
        )
        .unwrap(),
    )
    .unwrap();
    fs::set_permissions(&registry, fs::Permissions::from_mode(0o600)).unwrap();
    let plan = plan::Plan {
        graph_digest: contract::digest(&serde_json::to_vec(&plan::graph(&campaign)).unwrap()),
        repo: "fixture/24x7".into(),
        agent_id: "canary".into(),
        nodes,
    };
    Fixture {
        _roots: roots,
        registry,
        contracts,
        plan,
        campaign,
    }
}

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

async fn activate(engine: &PolicyEngine, plan: &plan::Plan, runs: Vec<String>) {
    for run in runs {
        let (status, axum::Json(response)) =
            crate::services::auto_queue::route::activate_with_bridge_pg(
                engine.clone(),
                crate::services::auto_queue::route::ActivateBody {
                    run_id: Some(run),
                    repo: Some(plan.repo.clone()),
                    agent_id: Some(plan.agent_id.clone()),
                    thread_group: None,
                    unified_thread: Some(false),
                    active_only: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(status, axum::http::StatusCode::OK, "{response}");
        println!("canonical activation: {response}");
    }
}

async fn finish_fixture(pool: &PgPool, engine: &PolicyEngine, f: &Fixture, id: &str, value: &str) {
    let card = plan::card_id(&f.plan.nodes[id].task_key);
    answer(&f.contracts[id], value);
    let updated=sqlx::query("UPDATE task_dispatches SET status='completed',result=$2,completed_at=NOW() WHERE kanban_card_id=$1 AND status IN ('pending','dispatched') AND dispatch_type IN ('implementation','rework')")
        .bind(&card).bind(format!("fixture implementation {id}={value}")).execute(pool).await.unwrap().rows_affected();
    assert_eq!(
        updated, 1,
        "must complete the dispatch created by canonical activation"
    );
    sqlx::query("UPDATE kanban_cards SET status='review',review_entered_at=NOW() WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    checks::test_drive(pool, engine, &card).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Linux root, Landlock, Python3 and PostgreSQL"]
async fn verified_files_supply_finite_batches_and_resume_once_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = setup(&pool).await;
    let engine = engine(f._roots[0].path(), pool.clone());
    let first = supply(&pool, "handoff", &f.plan, &f.registry)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(count(&pool, "SELECT count(*) FROM kanban_cards").await, 2);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM auto_queue_entries").await,
        2
    );
    assert_eq!(
        count(&pool, "SELECT count(*) FROM task_dispatches").await,
        0,
        "supplier never owns dispatch creation"
    );
    assert_eq!(
        supply(&pool, "handoff", &f.plan, &f.registry)
            .await
            .unwrap(),
        first
    );
    activate(&engine, &f.plan, first).await;
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM task_dispatches WHERE dispatch_type='implementation'"
        )
        .await,
        2
    );
    // Failure in A must not certify A or block the independent C branch.
    finish_fixture(&pool, &engine, &f, "a", "0").await;
    finish_fixture(&pool, &engine, &f, "c", "42").await;
    supply(&pool, "handoff", &f.plan, &f.registry)
        .await
        .unwrap();
    assert_eq!(count(&pool, "SELECT count(*) FROM kanban_cards").await, 2);
    let a = plan::card_id(&f.plan.nodes["a"].task_key);
    assert!(
        checks::completed_artifact(&pool, &a)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM task_dispatches WHERE dispatch_type='rework'"
        )
        .await,
        1,
        "the failed protected check must schedule one bounded repair through the existing FSM"
    );
    checks::test_drive(&pool, &engine, &a).await.unwrap();
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM task_dispatches WHERE dispatch_type='rework'"
        )
        .await,
        1
    );
    finish_fixture(&pool, &engine, &f, "a", "42").await;
    assert!(
        checks::completed_artifact(&pool, &a)
            .await
            .unwrap()
            .is_some()
    );
    let next = supply(&pool, "handoff", &f.plan, &f.registry)
        .await
        .unwrap();
    assert_eq!(
        count(&pool, "SELECT count(*) FROM auto_queue_runs").await,
        2
    );
    assert_eq!(
        count(&pool, "SELECT count(*) FROM auto_queue_entries").await,
        3
    );
    assert_eq!(
        fs::read(f.contracts["b"].template.workspace.join("input/a.txt")).unwrap(),
        b"42"
    );
    // Prove the bytes are the protected artifact, not A's now-mutable workspace.
    answer(&f.contracts["a"], "999");
    supply(&pool, "handoff", &f.plan, &f.registry)
        .await
        .unwrap();
    activate(&engine, &f.plan, next).await;
    // The canonical queue preserves its terminal-slot cleanup window across
    // finite batches. Exercise normal retry after that window, without altering
    // completion timestamps or releasing slots behind the queue's back.
    let b = plan::card_id(&f.plan.nodes["b"].task_key);
    tokio::time::timeout(std::time::Duration::from_secs(65), async {
        loop {
            let ready: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM task_dispatches WHERE kanban_card_id=$1 AND status IN ('pending','dispatched'))",
            )
            .bind(&b)
            .fetch_one(&pool)
            .await
            .unwrap();
            if ready {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let retry = supply(&pool, "handoff", &f.plan, &f.registry).await.unwrap();
            activate(&engine, &f.plan, retry).await;
        }
    })
    .await
    .expect("successor must activate after the canonical slot cleanup window");
    finish_fixture(&pool, &engine, &f, "b", "43").await;
    supply(&pool, "handoff", &f.plan, &f.registry)
        .await
        .unwrap();
    assert_eq!(
        campaigns::get(&pool, "handoff").await.unwrap().status,
        CampaignStatus::Completed
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM kanban_cards WHERE status='done'"
        )
        .await,
        3
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM kanban_audit_logs WHERE to_status='done'"
        )
        .await,
        3
    );
    drop(engine);
    pool.close().await;
    let reconnected = db.connect_and_migrate().await;
    assert!(
        supply(&reconnected, "handoff", &f.plan, &f.registry)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        count(&reconnected, "SELECT count(*) FROM auto_queue_entries").await,
        3
    );
    assert_eq!(
        count(
            &reconnected,
            "SELECT count(*) FROM kanban_audit_logs WHERE to_status='done'"
        )
        .await,
        3
    );
    reconnected.close().await;
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Linux root and PostgreSQL"]
async fn campaign_contract_pause_duplicate_and_writer_guards_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = setup(&pool).await;
    // Ledger completion never replaces a verified artifact.
    sqlx::query("UPDATE campaigns SET document=jsonb_set(document,'{nodes,0,status}','\"completed\"') WHERE id='handoff'")
        .execute(&pool).await.unwrap();
    let (left, right) = tokio::join!(
        supply(&pool, "handoff", &f.plan, &f.registry),
        supply(&pool, "handoff", &f.plan, &f.registry)
    );
    assert!(left.is_ok() || right.is_ok());
    // A revision race may defer one preparation; the next persisted sweep
    // must complete supply without replacing or duplicating the winning task.
    supply(&pool, "handoff", &f.plan, &f.registry)
        .await
        .unwrap();
    assert_eq!(count(&pool, "SELECT count(*) FROM kanban_cards").await, 2);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM auto_queue_entries").await,
        2
    );
    assert!(sqlx::query("UPDATE campaigns SET document=jsonb_set(document,'{nodes,0,details}','\"different work\"') WHERE id='handoff'").execute(&pool).await.is_err());
    let a = plan::card_id(&f.plan.nodes["a"].task_key);
    assert!(
        sqlx::query("UPDATE kanban_cards SET metadata='{}' WHERE id=$1")
            .bind(&a)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE agents SET execution_requirements='{}' WHERE id='canary'")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(sqlx::query("UPDATE campaigns SET document=jsonb_set(document,'{status}','\"completed\"') WHERE id='handoff'").execute(&pool).await.is_err());
    sqlx::query("UPDATE campaigns SET document=jsonb_set(document,'{status}','\"paused\"') WHERE id='handoff'")
        .execute(&pool).await.unwrap();
    assert!(
        supply(&pool, "handoff", &f.plan, &f.registry)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status) VALUES('paused',$1,'implementation','pending')")
        .bind(&a).execute(&pool).await.is_err());
    // Input paths, per-dependency real-file coverage and plan mutations fail closed.
    let mut bad = f.plan.clone();
    bad.nodes.get_mut("b").unwrap().inputs.clear();
    assert!(bad.validate(&f.campaign).is_err());
    bad = f.plan.clone();
    bad.nodes.get_mut("b").unwrap().inputs[0].destination = "../outside".into();
    assert!(bad.validate(&f.campaign).is_err());
    let occupied = inputs::prepare(&pool, &f.registry, &f.plan.nodes["a"], &BTreeMap::new()).await;
    assert!(
        occupied.is_err(),
        "supplied workspaces cannot be prepared again"
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM resource_locks WHERE holder_instance_id='protected-campaign-v1'"
        )
        .await,
        0
    );
    pool.close().await;
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Linux root, Landlock, Python3 and PostgreSQL"]
async fn campaign_rework_budget_exhausts_without_starving_independent_work_pg() {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let f = setup(&pool).await;
    let engine = engine(f._roots[0].path(), pool.clone());
    let runs = supply(&pool, "handoff", &f.plan, &f.registry)
        .await
        .unwrap();
    activate(&engine, &f.plan, runs).await;
    for value in ["0", "1", "2"] {
        finish_fixture(&pool, &engine, &f, "a", value).await;
    }
    finish_fixture(&pool, &engine, &f, "c", "42").await;
    let a = plan::card_id(&f.plan.nodes["a"].task_key);
    for _ in 0..3 {
        checks::test_drive(&pool, &engine, &a).await.unwrap();
        supply(&pool, "handoff", &f.plan, &f.registry)
            .await
            .unwrap();
    }
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM task_dispatches WHERE dispatch_type='rework'"
        )
        .await,
        2
    );
    assert_eq!(
        count(&pool, "SELECT count(*) FROM kanban_cards").await,
        2,
        "B must stay unsupplied"
    );
    let campaign = campaigns::get(&pool, "handoff").await.unwrap();
    let a = campaign.nodes.iter().find(|n| n.input.id == "a").unwrap();
    assert_eq!(a.input.status, NodeStatus::Blocked);
    assert!(a.input.blocker.is_some());
    assert_eq!(
        campaign
            .nodes
            .iter()
            .find(|n| n.input.id == "c")
            .unwrap()
            .input
            .status,
        NodeStatus::Completed
    );
    drop(engine);
    pool.close().await;
    db.drop().await;
}
