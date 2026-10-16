//! Disposable PG/provider fixtures exercise the native workflow boundaries.
//! These receipts are test inputs, not native/provider or business completion evidence.
use super::*;
use crate::db::campaigns::graph::{result, workflow};
use std::path::Path;
#[path = "finish_recovery_tests.rs"]
mod finish_recovery_tests;

fn graph_with_unpublished_suffix() -> Graph {
    let mut proposed = graph();
    let mut produce = proposed.nodes[0].clone();
    produce.id = "followup-produce".into();
    produce.task_key = "repo:sha:followup-produce".into();
    produce.dependencies = ["produce".into(), "verify".into()].into();
    produce.inputs = vec![
        Input {
            dependency: "produce".into(),
            source: "answer.json".into(),
            destination: "input/original.json".into(),
        },
        Input {
            dependency: "verify".into(),
            source: "verified.json".into(),
            destination: "input/verified.json".into(),
        },
    ];
    let mut verify = proposed.nodes[1].clone();
    verify.id = "followup-verify".into();
    verify.task_key = "repo:sha:followup-verify".into();
    verify.dependencies = [produce.id.clone()].into();
    verify.inputs[0].dependency = produce.id.clone();
    proposed.nodes.extend([produce, verify]);
    proposed.validate(&goal()).unwrap();
    proposed
}

fn fixture_receipt(summary: &str) -> Value {
    json!({"summary":result::CAPTURE_SUMMARY,"work_outcome":"artifact",
        "completion_source":"campaign_native_receipt",
        "campaign_receipt":{"outcome":"passed","summary":summary,
            "observations":[{"command":"validate explicit workflow fixture report","exit_code":0,
                "evidence":"Disposable fixture evidence; no live provider or external business execution"}]}})
}

async fn enable_manager(pool: &PgPool, id: &str) {
    let current = campaigns::get(pool, id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let paused = campaigns::replace(pool, id, current.revision, input)
        .await
        .unwrap();
    let revision = workflow::store::configure(pool, id, paused.revision, true)
        .await
        .unwrap();
    let current = campaigns::get(pool, id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
    input.status = CampaignStatus::Active;
    campaigns::replace(pool, id, revision, input).await.unwrap();
}

async fn publish_work(pool: &PgPool, id: &str, version: i64, node: &Task) -> String {
    store::publish(
        pool,
        id,
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
    .unwrap()
}

async fn terminal_fixture(
    pool: &PgPool,
    key: &str,
    dispatch: &str,
    value: &Value,
    workspace: Option<&Path>,
) -> Option<tempfile::TempDir> {
    result::validate_dispatch(value).unwrap();
    let card = store::card_id(key);
    let agent: String =
        sqlx::query_scalar("SELECT assigned_agent_id FROM kanban_cards WHERE id=$1")
            .bind(&card)
            .fetch_one(pool)
            .await
            .unwrap();
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    let context = workspace
        .map(|path| json!({"worktree_path":path}))
        .unwrap_or_else(|| json!({}));
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,context,created_at)
        VALUES($1,$2,$3,'implementation','pending',$4,NOW()-INTERVAL '1 second')")
        .bind(dispatch).bind(&card).bind(agent).bind(context.to_string()).execute(pool).await.unwrap();
    let control_workspace = if workspace.is_none() {
        Some(prepare_control_fixture(pool, dispatch).await)
    } else {
        None
    };
    assert_eq!(
        crate::dispatch::set_dispatch_status_on_pg_async(
            pool,
            dispatch,
            "completed",
            Some(value),
            "campaign_native_receipt",
            Some(&["pending"]),
            true
        )
        .await
        .unwrap(),
        1
    );
    control_workspace
}

async fn acknowledge_fixture_status_reactions(pool: &PgPool, id: &str, dispatches: &[&str]) {
    use crate::db::dispatches::outbox::{mark_dispatch_outbox_claimed_pg, mark_outbox_done_pg};

    // Only this disposable provider fixture's canonical terminal reactions are
    // acknowledged. No Discord delivery runs, and the retained row says so.
    let mut tx = pool.begin().await.unwrap();
    let rows: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(o) FROM dispatch_outbox o JOIN task_dispatches d ON d.id=o.dispatch_id
        JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id
        WHERE t.campaign_id=$1 AND d.id=ANY($2) AND d.status IN ('completed','failed','cancelled')
          AND o.action='status_reaction' ORDER BY o.dispatch_id,o.id FOR UPDATE OF o",
    )
    .bind(id)
    .bind(dispatches)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    let actual: Vec<&str> = rows
        .iter()
        .map(|row| row["dispatch_id"].as_str().unwrap())
        .collect();
    let mut expected = dispatches.to_vec();
    expected.sort_unstable();
    assert_eq!(
        actual, expected,
        "fixture reaction membership must be exact"
    );
    let mut claims = Vec::new();
    let owner = "fixture-workflow-status-reaction";
    for row in &rows {
        assert_eq!(row["status"], "pending");
        assert!(row["claim_owner"].is_null() && row["claimed_at"].is_null());
        let outbox_id = row["id"].as_i64().unwrap();
        let claimed_at = mark_dispatch_outbox_claimed_pg(&mut tx, outbox_id, owner)
            .await
            .unwrap();
        claims.push((outbox_id, claimed_at));
    }
    tx.commit().await.unwrap();
    let outcome = json!({"fixture":true,"external_delivery_performed":false,
        "reason":"Disposable workflow fixture acknowledges its terminal status reaction"});
    for (outbox_id, claimed_at) in claims {
        mark_outbox_done_pg(
            pool,
            outbox_id,
            "suppressed",
            &outcome.to_string(),
            owner,
            claimed_at,
        )
        .await
        .unwrap();
        let after: Value =
            sqlx::query_scalar("SELECT to_jsonb(o) FROM dispatch_outbox o WHERE id=$1")
                .bind(outbox_id)
                .fetch_one(pool)
                .await
                .unwrap();
        let before = rows.iter().find(|row| row["id"] == outbox_id).unwrap();
        for field in ["id", "dispatch_id", "action", "created_at", "retry_count"] {
            assert_eq!(
                after[field], before[field],
                "retained fixture reaction {field}"
            );
        }
        assert_eq!(after["status"], "done");
        assert_eq!(after["delivery_status"], "suppressed");
        assert_eq!(after["delivery_result"], outcome);
        assert!(after["claim_owner"].is_null() && after["claimed_at"].is_null());
    }
}

async fn accept_fixture_work(
    pool: &PgPool,
    id: &str,
    proposed: &Graph,
    node: &Task,
    dispatch: &str,
) {
    let root = artifact_root().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let mut manifests = BTreeMap::new();
    for input in &node.inputs {
        let producer = proposed
            .nodes
            .iter()
            .find(|producer| producer.id == input.dependency)
            .unwrap();
        let manifest: Value = sqlx::query_scalar(
            "SELECT artifact_manifest FROM campaign_graph_tasks WHERE task_key=$1 AND accepted",
        )
        .bind(&producer.task_key)
        .fetch_one(pool)
        .await
        .unwrap();
        manifests.insert(input.dependency.clone(), manifest);
    }
    artifacts::materialize(&root, workspace.path(), node, &manifests).unwrap();
    if node.kind == Kind::Verify {
        for input in &node.inputs {
            let actual: Value = serde_json::from_slice(
                &std::fs::read(workspace.path().join(&input.destination)).unwrap(),
            )
            .unwrap();
            assert_eq!(actual["answer"], 42);
        }
    }
    for output in &node.outputs {
        std::fs::write(
            workspace.path().join(&output.path),
            json!({"answer":42,"verified":node.kind==Kind::Verify}).to_string(),
        )
        .unwrap();
    }
    terminal_fixture(
        pool,
        &node.task_key,
        dispatch,
        &fixture_receipt("Explicit fixture output captured and checked"),
        Some(workspace.path()),
    )
    .await;
    reconcile::observe(pool, id, &goal(), None).await.unwrap();
    let manifest: Value = sqlx::query_scalar(
        "SELECT artifact_manifest FROM campaign_graph_tasks WHERE task_key=$1 AND accepted",
    )
    .bind(&node.task_key)
    .fetch_one(pool)
    .await
    .unwrap();
    artifacts::verify(&root, &manifest).unwrap();
}

async fn accepted_prefix(pool: &PgPool, managed: bool) -> (campaigns::Campaign, Graph, i64) {
    let initial = setup(pool).await;
    assert_eq!(
        store::status(pool, &initial.id).await.unwrap()["manager_enabled"],
        false
    );
    if managed {
        enable_manager(pool, &initial.id).await;
    }
    let proposed = graph_with_unpublished_suffix();
    let version = store::stage(pool, &initial.id, &proposed, None)
        .await
        .unwrap();
    // This is only the initial graph setup. The revision regression below drives
    // candidate approval through its real control receipt and observation path.
    sqlx::query(
        "UPDATE campaign_graph_versions SET status='validated' WHERE campaign_id=$1 AND version=$2",
    )
    .bind(&initial.id)
    .bind(version)
    .execute(pool)
    .await
    .unwrap();
    let current = campaigns::get(pool, &initial.id).await.unwrap();
    store::activate(pool, &initial.id, version, current.revision)
        .await
        .unwrap();
    for (index, node) in proposed.nodes.iter().take(2).enumerate() {
        publish_work(pool, &initial.id, version, node).await;
        accept_fixture_work(
            pool,
            &initial.id,
            &proposed,
            node,
            &format!("fixture-work-{index}"),
        )
        .await;
    }
    super::replan_tests::settle(pool).await;
    acknowledge_fixture_status_reactions(pool, &initial.id, &["fixture-work-0", "fixture-work-1"])
        .await;
    (
        campaigns::get(pool, &initial.id).await.unwrap(),
        proposed,
        version,
    )
}

async fn assert_sql_authority(pool: &PgPool, frontier: &workflow::Frontier) {
    let sql: Value = sqlx::query_scalar("SELECT agentdesk_workflow_authority_state($1)")
        .bind(&frontier.campaign_id)
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(sql, frontier.authority_state());
}

async fn manager_turn(pool: &PgPool, id: &str) -> (workflow::Frontier, String) {
    let frontier = workflow::store::snapshot(pool, id).await.unwrap();
    assert_sql_authority(pool, &frontier).await;
    assert!(matches!(
        manager::admit(pool, id, &goal()).await.unwrap(),
        manager::Admission::Owned
    ));
    let key = format!("{id}:manage:{}-1", frontier.digest().unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM campaign_graph_tasks WHERE task_key=$1 AND role='manage'"
        )
        .bind(&key)
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
    (frontier, key)
}

async fn propose_complete(pool: &PgPool, id: &str) -> (workflow::Frontier, String) {
    let (frontier, key) = manager_turn(pool, id).await;
    let mut receipt = fixture_receipt(
        "Manager fixture proposes verified goal completion, preserving unpublished work",
    );
    receipt["workflow_decision"] = json!({"snapshot_digest":frontier.digest().unwrap(),"action":"complete","reason":"The accepted independent fixture artifact verifies 42; remaining unpublished work is unnecessary"});
    let _workspace = terminal_fixture(pool, &key, "fixture-manager-complete", &receipt, None).await;
    reconcile::observe(pool, id, &goal(), None).await.unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT accepted FROM campaign_graph_tasks WHERE task_key=$1"
        )
        .bind(&key)
        .fetch_one(pool)
        .await
        .unwrap()
    );
    assert!(matches!(
        manager::admit(pool, id, &goal()).await.unwrap(),
        manager::Admission::Owned
    ));
    let finish_key = format!("{id}:finish:{}-goal", frontier.digest().unwrap());
    (frontier, finish_key)
}

async fn assess_goal(
    pool: &PgPool,
    id: &str,
    frontier: &workflow::Frontier,
    key: &str,
    approved: bool,
) -> Value {
    let verify = frontier
        .graph
        .nodes
        .iter()
        .find(|node| node.kind == Kind::Verify && frontier.accepted(&node.task_key))
        .unwrap();
    let manifest = &frontier.task(&verify.task_key).unwrap()["artifact_manifest"];
    let output = &verify.outputs[0].path;
    let root = artifact_root().unwrap();
    artifacts::verify(&root, manifest).unwrap();
    let actual: Value = serde_json::from_slice(
        &std::fs::read(root.join(manifest["bundle"].as_str().unwrap()).join(output)).unwrap(),
    )
    .unwrap();
    assert_eq!(actual["answer"], 42);
    assert_eq!(actual["verified"], true);
    let mut receipt = fixture_receipt("Independent fixture goal assessment is recorded truthfully");
    receipt["goal_validation"] = json!({"snapshot_digest":frontier.digest().unwrap(),"approved":approved,
        "reason":if approved {"Fixture assertion and accepted verification file cover every criterion"} else {"Fixture deliberately records the business criterion as unproved; reporting it does not approve completion"},
        "criteria":frontier.goal.success.iter().enumerate().map(|(index,_)|json!({"index":index,"met":approved,"observation":0,
            "task_key":verify.task_key,"path":output,"sha256":manifest["files"][output],"evidence":"Accepted verification file and explicit fixture assessment"})).collect::<Vec<_>>()});
    let _workspace = terminal_fixture(pool, key, "fixture-goal-assessment", &receipt, None).await;
    reconcile::observe(pool, id, &goal(), None).await.unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT accepted FROM campaign_graph_tasks WHERE task_key=$1"
        )
        .bind(key)
        .fetch_one(pool)
        .await
        .unwrap()
    );
    receipt
}

#[tokio::test]
async fn workflow_frozen_publication_is_once_and_stale_dispatch_is_rejected_pg() {
    let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = database.connect_and_migrate().await;
    let (campaign, proposed, version) = accepted_prefix(&pool, true).await;
    let frontier = workflow::store::snapshot(&pool, &campaign.id)
        .await
        .unwrap();
    let frozen = workflow::store::freeze(&pool, &campaign.id, &goal())
        .await
        .unwrap();
    let key = format!("{}:manage:{}-1", campaign.id, frontier.digest().unwrap());
    let prompt = "One explicit frozen workflow fixture";
    let digest = contract_digest(&prompt).unwrap();
    let publication = store::Publication {
        key: &key,
        node: "manage",
        role: "manage",
        version: Some(version),
        agent: "qwen",
        title: "Frozen manager fixture",
        prompt,
        digest: &digest,
    };
    let (first, second) = tokio::join!(
        store::publish_planning(&pool, &campaign.id, &publication, None, &frozen),
        store::publish_planning(&pool, &campaign.id, &publication, None, &frozen)
    );
    let card = first.unwrap();
    assert_eq!(second.unwrap(), card);
    assert_eq!(
        store::publish_planning(&pool, &campaign.id, &publication, None, &frozen)
            .await
            .unwrap(),
        card
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='manage'"
        )
        .bind(&campaign.id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM auto_queue_entries WHERE kanban_card_id=$1"
        )
        .bind(&card)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    let profile: Value =
        sqlx::query_scalar("SELECT execution_profile FROM campaign_graph_tasks WHERE task_key=$1")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        profile["workflow_authority_state"],
        frontier.authority_state()
    );
    assert_sql_authority(&pool, &frontier).await;
    assert_eq!(
        workflow::store::snapshot(&pool, &campaign.id)
            .await
            .unwrap()
            .digest()
            .unwrap(),
        frontier.digest().unwrap()
    );
    assert_eq!(
        workflow::retirement::settle_stale_unstarted(&pool, &campaign.id)
            .await
            .unwrap(),
        0,
        "a current frozen control must retain ownership"
    );
    // Publishing real remaining work changes the authoritative frontier, even
    // before a provider attempt exists. The old control must not first-start.
    publish_work(&pool, &campaign.id, version, &proposed.nodes[2]).await;
    let changed = workflow::store::snapshot(&pool, &campaign.id)
        .await
        .unwrap();
    assert_ne!(changed.digest().unwrap(), frontier.digest().unwrap());
    assert_sql_authority(&pool, &changed).await;
    let refused = sqlx::query(
        "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status)
        VALUES('stale-workflow-start',$1,'qwen','implementation','pending')",
    )
    .bind(&card)
    .execute(&pool)
    .await;
    let refusal = refused.unwrap_err().to_string();
    assert!(
        refusal.contains("Workflow control has lost its current goal, graph or result frontier"),
        "SQL dispatch guard must refuse the stale frontier: {refusal}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_dispatches WHERE id='stale-workflow-start'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    let before = store::status(&pool, &campaign.id).await.unwrap();
    assert!(planning::pending(&before));
    // Two retirement observers may race, but one stale history transition and
    // one canonical queue audit must commit; the other is an idempotent no-op.
    let (first, second) = tokio::join!(
        workflow::retirement::settle_stale_unstarted(&pool, &campaign.id),
        workflow::retirement::settle_stale_unstarted(&pool, &campaign.id)
    );
    assert_eq!(first.unwrap() + second.unwrap(), 1);
    assert_eq!(
        workflow::retirement::settle_stale_unstarted(&pool, &campaign.id)
            .await
            .unwrap(),
        0
    );
    let retired = store::status(&pool, &campaign.id).await.unwrap();
    assert!(!planning::pending(&retired));
    assert_eq!(retired["goal"], before["goal"]);
    assert_eq!(retired["active_version"], before["active_version"]);
    assert_eq!(retired["versions"], before["versions"]);
    assert!(before["budget_enforcement"].is_object());
    assert_eq!(retired["budget_enforcement"], before["budget_enforcement"]);
    assert_eq!(
        retired["tasks"].as_array().unwrap().len(),
        before["tasks"].as_array().unwrap().len()
    );
    let history = retired["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["task_key"] == key)
        .unwrap();
    assert_eq!(history["accepted"], false);
    assert!(history["artifact_manifest"].is_null());
    assert_eq!(history["execution_profile"], profile);
    assert_eq!(history["observation"]["outcome"], "superseded");
    assert_eq!(history["observation"]["unstarted"], true);
    assert_eq!(
        history["observation"]["snapshot_digest"],
        frontier.digest().unwrap()
    );
    assert_eq!(
        history["observation"]["original_execution_profile"],
        profile
    );
    assert_eq!(history["observation"]["changed"]["authority"], true);
    for work in before["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|task| task["role"] == "work")
    {
        assert_eq!(
            retired["tasks"]
                .as_array()
                .unwrap()
                .iter()
                .find(|task| task["task_key"] == work["task_key"])
                .unwrap(),
            work
        );
    }
    let queue: Value = sqlx::query_scalar("SELECT jsonb_build_object('entry',to_jsonb(e),'run',to_jsonb(r))
        FROM auto_queue_entries e JOIN auto_queue_runs r ON r.id=e.run_id WHERE e.kanban_card_id=$1")
        .bind(&card).fetch_one(&pool).await.unwrap();
    assert_eq!(queue["entry"]["status"], "skipped");
    assert_eq!(queue["entry"]["retry_count"], 0);
    assert!(queue["entry"]["dispatch_id"].is_null());
    assert!(queue["entry"]["slot_index"].is_null());
    assert!(queue["entry"]["dispatched_at"].is_null());
    assert!(queue["entry"]["completed_at"].is_string());
    assert_eq!(queue["run"]["status"], "completed");
    assert!(queue["run"]["completed_at"].is_string());
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM auto_queue_entry_transitions
        WHERE entry_id=$1 AND from_status='pending' AND to_status='skipped' AND trigger_source='workflow_stale_unstarted'")
        .bind(queue["entry"]["id"].as_str().unwrap()).fetch_one(&pool).await.unwrap(), 1);
    // The existing executor can observe its own newer work, then admit only
    // the ready independent verification. The old manager no longer fences it.
    accept_fixture_work(
        &pool,
        &campaign.id,
        &proposed,
        &proposed.nodes[2],
        "fixture-new-frontier-work",
    )
    .await;
    assert!(matches!(
        manager::admit(&pool, &campaign.id, &goal()).await.unwrap(),
        manager::Admission::VerifyOnly
    ));
    assert!(!planning::pending(
        &store::status(&pool, &campaign.id).await.unwrap()
    ));
    pool.close().await;
    database.drop().await;
}

#[tokio::test]
async fn workflow_success_revision_preserves_published_work_and_requires_new_qa_pg() {
    let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = database.connect_and_migrate().await;
    let (campaign, mut replacement, version) = accepted_prefix(&pool, true).await;
    let (frontier, key) = manager_turn(&pool, &campaign.id).await;
    replacement.nodes[2].task_key = "repo:new-input:followup-produce".into();
    replacement.nodes[2].details =
        "Use the newly verified input for the smallest remaining step".into();
    replacement.nodes[3].task_key = "repo:new-input:followup-verify".into();
    replacement.validate(&goal()).unwrap();
    let before = canonical_inspection(store::status(&pool, &campaign.id).await.unwrap());
    let mut receipt =
        fixture_receipt("Manager fixture adapts unpublished work after successful verification");
    receipt["workflow_decision"] = json!({"snapshot_digest":frontier.digest().unwrap(),"action":"revise","reason":"Actual accepted fixture output changes only the unpublished successor contract"});
    receipt["campaign_graph"] = serde_json::to_value(&replacement).unwrap();
    let _workspace = terminal_fixture(&pool, &key, "fixture-manager-revise", &receipt, None).await;
    reconcile::observe(&pool, &campaign.id, &goal(), None)
        .await
        .unwrap();
    let after = store::status(&pool, &campaign.id).await.unwrap();
    assert_eq!(after["active_version"], version);
    assert!(after["failures"].as_array().unwrap().is_empty());
    assert_eq!(after["versions"][0]["status"], "candidate");
    assert!(after["versions"][0]["failure_id"].is_null());
    assert_eq!(after["versions"][0]["manager_task_key"], key);
    for original in before["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|task| task["role"] == "work")
    {
        let retained = after["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|task| task["task_key"] == original["task_key"])
            .unwrap();
        assert_eq!(retained, original);
    }
    let candidate = after["versions"][0]["version"].as_i64().unwrap();
    let current = campaigns::get(&pool, &campaign.id).await.unwrap();
    assert!(
        store::activate(&pool, &campaign.id, candidate, current.revision)
            .await
            .is_err()
    );
    let suffix = candidate.to_string();
    assert!(
        control(
            &pool,
            &campaign.id,
            &goal(),
            "validate",
            Some(candidate),
            &suffix,
            "Independent fixture candidate QA"
        )
        .await
        .unwrap()
    );
    let qa_key = format!("{}:validate:{suffix}", campaign.id);
    let mut qa = fixture_receipt("Independent fixture QA approves the changed candidate only");
    qa["graph_validation"] = json!({"digest":after["versions"][0]["digest"],"approved":true,"reason":"Preserved accepted contracts and checked the changed successor fixture"});
    let _qa_workspace = terminal_fixture(&pool, &qa_key, "fixture-revision-qa", &qa, None).await;
    reconcile::observe(&pool, &campaign.id, &goal(), None)
        .await
        .unwrap();
    let current = campaigns::get(&pool, &campaign.id).await.unwrap();
    store::activate(&pool, &campaign.id, candidate, current.revision)
        .await
        .unwrap();
    let activated = store::status(&pool, &campaign.id).await.unwrap();
    assert_eq!(activated["active_version"], candidate);
    assert_eq!(activated["goal"], before["goal"]);
    assert!(activated["failures"].as_array().unwrap().is_empty());
    for node in replacement.nodes.iter().take(2) {
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT accepted FROM campaign_graph_tasks WHERE task_key=$1"
            )
            .bind(&node.task_key)
            .fetch_one(&pool)
            .await
            .unwrap()
        );
    }
    pool.close().await;
    database.drop().await;
}

#[tokio::test]
async fn workflow_goal_completion_waits_for_cleanup_then_skips_unpublished_suffix_pg() {
    let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = database.connect_and_migrate().await;
    let (campaign, proposed, _) = accepted_prefix(&pool, true).await;
    let (frontier, finish_key) = propose_complete(&pool, &campaign.id).await;
    let receipt = assess_goal(&pool, &campaign.id, &frontier, &finish_key, true).await;
    let before = campaigns::get(&pool, &campaign.id).await.unwrap();
    let goal_before = store::status(&pool, &campaign.id).await.unwrap()["goal"].clone();
    super::replan_tests::settle(&pool).await;
    // Canonical run settlement does not acknowledge the completed dispatch's
    // reaction. Prove delivery alone blocks before isolating the other owners.
    let unsettled: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM auto_queue_runs WHERE status NOT IN ('completed','failed','cancelled'))
        +(SELECT COUNT(*) FROM auto_queue_entries WHERE status NOT IN ('done','failed','user_cancelled','skipped'))
        +(SELECT COUNT(*) FROM task_dispatches WHERE status IN ('pending','dispatched','running'))
        +(SELECT COUNT(*) FROM auto_queue_slots WHERE assigned_run_id IS NOT NULL)
        +(SELECT COUNT(*) FROM sessions WHERE status NOT IN ('idle','disconnected'))
        +(SELECT COUNT(*) FROM dispatch_queue)
        +(SELECT COUNT(*) FROM auto_queue_run_cleanup_tasks)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        unsettled, 0,
        "standalone fixture outbox proof has no other owner"
    );
    assert!(
        !workflow::store::complete(&pool, &campaign.id, &finish_key)
            .await
            .unwrap()
    );
    assert_eq!(
        campaigns::get(&pool, &campaign.id).await.unwrap().revision,
        before.revision
    );
    acknowledge_fixture_status_reactions(
        &pool,
        &campaign.id,
        &["fixture-manager-complete", "fixture-goal-assessment"],
    )
    .await;
    let run: String =
        sqlx::query_scalar("SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=$1")
            .bind(store::card_id(&finish_key))
            .fetch_one(&pool)
            .await
            .unwrap();
    // Explicit fixture ownership remains observable; completion itself may not
    // cancel a run, delete a session, release a slot, or erase cleanup evidence.
    sqlx::query("UPDATE auto_queue_runs SET status='active' WHERE id=$1")
        .bind(&run)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        !workflow::store::complete(&pool, &campaign.id, &finish_key)
            .await
            .unwrap()
    );
    super::replan_tests::settle(&pool).await;
    sqlx::query("INSERT INTO sessions(session_key,agent_id,provider,status,active_dispatch_id)
        VALUES('fixture-workflow-owner','monitoring','opencode','awaiting_bg','fixture-goal-assessment')")
        .execute(&pool).await.unwrap();
    assert!(
        !workflow::store::complete(&pool, &campaign.id, &finish_key)
            .await
            .unwrap()
    );
    assert_eq!(
        campaigns::get(&pool, &campaign.id).await.unwrap().revision,
        before.revision
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sessions WHERE session_key='fixture-workflow-owner'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    sqlx::query("DELETE FROM sessions WHERE session_key='fixture-workflow-owner'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO auto_queue_slots(agent_id,slot_index,assigned_run_id) VALUES('monitoring',8,$1)")
        .bind(&run).execute(&pool).await.unwrap();
    assert!(
        !workflow::store::complete(&pool, &campaign.id, &finish_key)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT assigned_run_id FROM auto_queue_slots WHERE agent_id='monitoring' AND slot_index=8"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        Some(run.clone())
    );
    sqlx::query("DELETE FROM auto_queue_slots WHERE agent_id='monitoring' AND slot_index=8")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO auto_queue_run_cleanup_tasks(run_ids,dispatch_ids) VALUES(ARRAY[$1]::text[],ARRAY['fixture-goal-assessment']::text[])")
        .bind(&run).execute(&pool).await.unwrap();
    assert!(
        !workflow::store::complete(&pool, &campaign.id, &finish_key)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM auto_queue_run_cleanup_tasks WHERE 'fixture-goal-assessment'=ANY(dispatch_ids)"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks WHERE 'fixture-goal-assessment'=ANY(dispatch_ids)")
        .execute(&pool).await.unwrap();
    // Native cancel emits retain kanban_card_id, including when no run or
    // dispatch token remains. Completion must preserve this remaining delivery.
    let emit = json!({"dispatch_id":"retired-native-attempt","kanban_card_id":store::card_id(&finish_key),
        "from_status":"pending","to_agent_id":"monitoring","dispatch_type":"implementation","payload":null});
    let cleanup: i64 = sqlx::query_scalar(
        "INSERT INTO auto_queue_run_cleanup_tasks(run_ids,pending_emits)
        VALUES('{}',jsonb_build_array($1::jsonb)) RETURNING id",
    )
    .bind(&emit)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        !workflow::store::complete(&pool, &campaign.id, &finish_key)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, Value>(
            "SELECT pending_emits FROM auto_queue_run_cleanup_tasks WHERE id=$1"
        )
        .bind(cleanup)
        .fetch_one(&pool)
        .await
        .unwrap(),
        json!([emit])
    );
    sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks WHERE id=$1")
        .bind(cleanup)
        .execute(&pool)
        .await
        .unwrap();
    // Retained malformed cleanup is unknown ownership, not absence of debt.
    // Every payload stays intact; completion may neither erase it nor throw.
    for field in ["card_rollback_tasks", "pending_emits", "released_slots"] {
        let sql = format!(
            "INSERT INTO auto_queue_run_cleanup_tasks(run_ids,{field}) VALUES('{{}}','{{\"retained\":true}}') RETURNING id"
        );
        let cleanup: i64 = sqlx::query_scalar(&sql).fetch_one(&pool).await.unwrap();
        assert!(
            !workflow::store::complete(&pool, &campaign.id, &finish_key)
                .await
                .unwrap(),
            "{field}"
        );
        let sql = format!("SELECT {field} FROM auto_queue_run_cleanup_tasks WHERE id=$1");
        assert_eq!(
            sqlx::query_scalar::<_, Value>(&sql)
                .bind(cleanup)
                .fetch_one(&pool)
                .await
                .unwrap(),
            json!({"retained":true})
        );
        sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks WHERE id=$1")
            .bind(cleanup)
            .execute(&pool)
            .await
            .unwrap();
    }
    for (field, invalid) in [
        ("pending_emits", json!([{}])),
        ("pending_emits", json!([null])),
        ("released_slots", json!([{}])),
        (
            "released_slots",
            json!([{"agent_id":"monitoring","slot_index":"8"}]),
        ),
        ("card_rollback_tasks", json!([{}])),
        ("card_rollback_tasks", json!([{"card_id":"retained-card"}])),
    ] {
        let sql = format!(
            "INSERT INTO auto_queue_run_cleanup_tasks(run_ids,{field}) VALUES('{{}}',$1) RETURNING id"
        );
        let cleanup: i64 = sqlx::query_scalar(&sql)
            .bind(&invalid)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            !workflow::store::complete(&pool, &campaign.id, &finish_key)
                .await
                .unwrap(),
            "{field}: {invalid}"
        );
        let sql = format!("SELECT {field} FROM auto_queue_run_cleanup_tasks WHERE id=$1");
        assert_eq!(
            sqlx::query_scalar::<_, Value>(&sql)
                .bind(cleanup)
                .fetch_one(&pool)
                .await
                .unwrap(),
            invalid
        );
        sqlx::query("DELETE FROM auto_queue_run_cleanup_tasks WHERE id=$1")
            .bind(cleanup)
            .execute(&pool)
            .await
            .unwrap();
    }
    // Valid empty payloads carry no unknown ownership, even while the row's
    // unrelated maintenance bookkeeping has not yet removed it.
    let cleanup: i64 = sqlx::query_scalar(
        "INSERT INTO auto_queue_run_cleanup_tasks(run_ids) VALUES('{}') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        workflow::store::complete(&pool, &campaign.id, &finish_key)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM auto_queue_run_cleanup_tasks WHERE id=$1"
        )
        .bind(cleanup)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    let completed = campaigns::get(&pool, &campaign.id).await.unwrap();
    assert_eq!(completed.status, CampaignStatus::Completed);
    assert_eq!(completed.revision, before.revision + 1);
    for (index, node) in proposed.nodes.iter().enumerate() {
        let projected = completed
            .nodes
            .iter()
            .find(|projected| projected.input.id == node.id)
            .unwrap();
        assert_eq!(
            projected.input.status,
            if index < 2 {
                campaigns::NodeStatus::Completed
            } else {
                campaigns::NodeStatus::Skipped
            }
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_tasks WHERE role='work'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        store::status(&pool, &campaign.id).await.unwrap()["goal"],
        goal_before
    );
    let raw: String =
        sqlx::query_scalar("SELECT result FROM task_dispatches WHERE id='fixture-goal-assessment'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(serde_json::from_str::<Value>(&raw).unwrap(), receipt);
    pool.close().await;
    database.drop().await;
}

#[tokio::test]
async fn workflow_negative_goal_assessment_is_accepted_and_opens_a_new_frontier_pg() {
    let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = database.connect_and_migrate().await;
    let (campaign, _, _) = accepted_prefix(&pool, false).await;
    // Legacy enrollment remains opted out; accepted artifacts do not silently
    // switch its execution policy or admit a manager control.
    assert_eq!(
        store::status(&pool, &campaign.id).await.unwrap()["manager_enabled"],
        false
    );
    assert!(
        workflow::store::snapshot(&pool, &campaign.id)
            .await
            .is_err()
    );
    assert!(
        control(
            &pool,
            &campaign.id,
            &goal(),
            "manage",
            Some(1),
            "legacy-probe",
            "Legacy manager must stay disabled"
        )
        .await
        .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM campaign_graph_tasks WHERE role IN ('manage','finish')"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    enable_manager(&pool, &campaign.id).await;
    let (frontier, finish_key) = propose_complete(&pool, &campaign.id).await;
    assess_goal(&pool, &campaign.id, &frontier, &finish_key, false).await;
    assert!(
        workflow::store::complete(&pool, &campaign.id, &finish_key)
            .await
            .is_err()
    );
    assert_eq!(
        campaigns::get(&pool, &campaign.id).await.unwrap().status,
        CampaignStatus::Active
    );
    let changed = workflow::store::snapshot(&pool, &campaign.id)
        .await
        .unwrap();
    assert_ne!(changed.digest().unwrap(), frontier.digest().unwrap());
    assert_eq!(changed.unmet_goal_assessments.len(), 1);
    assert_eq!(
        changed.unmet_goal_assessments[0]["validation"]["approved"],
        false
    );
    assert_sql_authority(&pool, &changed).await;
    let (next, new_key) = manager_turn(&pool, &campaign.id).await;
    assert_eq!(next.digest().unwrap(), changed.digest().unwrap());
    assert_ne!(
        new_key,
        format!("{}:manage:{}-1", campaign.id, frontier.digest().unwrap())
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM campaign_graph_tasks WHERE role='manage'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        2
    );
    assert!(matches!(
        manager::admit(&pool, &campaign.id, &goal()).await.unwrap(),
        manager::Admission::Owned
    ));
    assert!(
        store::status(&pool, &campaign.id).await.unwrap()["failures"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    pool.close().await;
    database.drop().await;
}

async fn retirement_witness(pool: &PgPool, key: &str) -> Value {
    sqlx::query_scalar("WITH owned AS (SELECT e.* FROM auto_queue_entries e
        JOIN campaign_graph_tasks t ON t.card_id=e.kanban_card_id WHERE t.task_key=$1)
        SELECT jsonb_build_object('task',to_jsonb(t),'card',to_jsonb(k),
            'queues',(SELECT jsonb_agg(jsonb_build_object('entry',to_jsonb(e),'run',to_jsonb(r)) ORDER BY e.id)
                FROM owned e JOIN auto_queue_runs r ON r.id=e.run_id),
            'attempts',(SELECT jsonb_agg(to_jsonb(d) ORDER BY d.id) FROM task_dispatches d WHERE d.kanban_card_id=t.card_id),
            'slots',(SELECT jsonb_agg(to_jsonb(slot) ORDER BY slot.agent_id,slot.slot_index) FROM auto_queue_slots slot
                WHERE slot.assigned_run_id IN(SELECT run_id FROM owned)),
            'cleanup',(SELECT jsonb_agg(to_jsonb(cleanup) ORDER BY cleanup.id) FROM auto_queue_run_cleanup_tasks cleanup
                WHERE cleanup.run_ids && ARRAY(SELECT run_id FROM owned)),
            'transitions',(SELECT jsonb_agg(to_jsonb(audit) ORDER BY audit.id) FROM auto_queue_entry_transitions audit
                WHERE audit.entry_id IN(SELECT id FROM owned)))
        FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id WHERE t.task_key=$1")
        .bind(key).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn workflow_stale_controls_preserve_any_attempt_slot_cleanup_and_active_run_pg() {
    let database = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = database.connect_and_migrate().await;
    let (campaign, proposed, version) = accepted_prefix(&pool, true).await;
    let (frontier, finish) = propose_complete(&pool, &campaign.id).await;
    let frozen = workflow::store::freeze(&pool, &campaign.id, &goal())
        .await
        .unwrap();
    let mut controls = Vec::new();
    for case in ["attempt", "slot", "cleanup", "active", "unknown"] {
        let key = format!(
            "{}:manage:{}-{case}-fixture",
            campaign.id,
            frontier.digest().unwrap()
        );
        let prompt = format!("Explicit never-started retirement boundary fixture: {case}");
        let digest = contract_digest(&prompt).unwrap();
        let card = store::publish_planning(
            &pool,
            &campaign.id,
            &store::Publication {
                key: &key,
                node: "manage",
                role: "manage",
                version: Some(version),
                agent: "qwen",
                title: "Preserved workflow owner fixture",
                prompt: &prompt,
                digest: &digest,
            },
            None,
            &frozen,
        )
        .await
        .unwrap();
        let run: String =
            sqlx::query_scalar("SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=$1")
                .bind(&card)
                .fetch_one(&pool)
                .await
                .unwrap();
        match case {
            "attempt" => {
                // A different dispatch type and a NULL terminal timestamp still
                // constitute retained execution history, never absence evidence.
                sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status)
                    VALUES('fixture-retained-other-type',$1,'qwen','consultation','cancelled')")
                    .bind(&card).execute(&pool).await.unwrap();
            }
            "slot" => {
                sqlx::query(
                    "INSERT INTO auto_queue_slots(agent_id,slot_index,assigned_run_id)
                    VALUES('qwen',19,$1)",
                )
                .bind(&run)
                .execute(&pool)
                .await
                .unwrap();
            }
            "cleanup" => {
                sqlx::query(
                    "INSERT INTO auto_queue_run_cleanup_tasks(run_ids) VALUES(ARRAY[$1]::text[])",
                )
                .bind(&run)
                .execute(&pool)
                .await
                .unwrap();
            }
            "active" => {
                sqlx::query("UPDATE auto_queue_runs SET status='active' WHERE id=$1")
                    .bind(&run)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "unknown" => {
                sqlx::query("UPDATE auto_queue_runs SET status='restoring' WHERE id=$1")
                    .bind(&run)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        controls.push((key.clone(), retirement_witness(&pool, &key).await));
    }
    assert_eq!(
        workflow::retirement::settle_stale_unstarted(&pool, &campaign.id)
            .await
            .unwrap(),
        0,
        "unchanged authority cannot justify retirement"
    );
    publish_work(&pool, &campaign.id, version, &proposed.nodes[2]).await;
    assert_eq!(
        workflow::retirement::settle_stale_unstarted(&pool, &campaign.id)
            .await
            .unwrap(),
        1,
        "only the exclusive unstarted finish may retire"
    );
    assert_eq!(
        workflow::retirement::settle_stale_unstarted(&pool, &campaign.id)
            .await
            .unwrap(),
        0
    );
    for (key, original) in controls {
        assert_eq!(
            retirement_witness(&pool, &key).await,
            original,
            "retirement must preserve unknown or started ownership: {key}"
        );
    }
    let retired_finish = retirement_witness(&pool, &finish).await;
    assert_eq!(retired_finish["task"]["accepted"], false);
    assert_eq!(
        retired_finish["task"]["observation"]["outcome"],
        "superseded"
    );
    assert_eq!(retired_finish["queues"][0]["entry"]["status"], "skipped");
    assert_eq!(retired_finish["queues"][0]["run"]["status"], "completed");
    assert!(
        planning::pending(&store::status(&pool, &campaign.id).await.unwrap()),
        "retained unknown owners must continue to fence planning"
    );
    pool.close().await;
    database.drop().await;
}
