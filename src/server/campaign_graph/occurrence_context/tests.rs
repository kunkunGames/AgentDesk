//! Retained-ledger cursors and actual control/work publication, never model approval.
use super::*;
use crate::db::campaigns::{
    self, Campaign,
    graph::{Graph, store},
};

fn goal() -> Goal {
    serde_json::from_value(json!({"objective":"Inspect and truthfully report the current target",
        "success":["Real report and independent checks"],
        "constraints":["Use only enrolled Codex CLI executors: coder for planning/work and qa for independent verification"],
        "repo":"test/repo","planner":"coder","verifier":"qa","agents":["coder","qa"],
        "budget":{"max_dispatches":20,"max_elapsed_secs":900,"node_timeout_secs":120,
            "max_evolutions":2,"max_observed_tool_calls":300}})).unwrap()
}

async fn setup(pool: &PgPool) {
    sqlx::query("INSERT INTO agents(id,name,provider) VALUES('coder','Coder','codex'),('qa','QA','codex'),('codex','Superior','codex')")
        .execute(pool).await.unwrap();
    sqlx::query("INSERT INTO kv_meta(key,value) VALUES('runtime-config','{\"internalTasksEnabled\":true,\"campaignSuperiorAgentId\":\"codex\",\"campaignDagTemplateMinSamples\":10}')")
        .execute(pool).await.unwrap();
}

async fn campaign(pool: &PgPool, id: &str) -> Campaign {
    let c = campaigns::create(
        pool,
        id.into(),
        serde_json::from_value(json!({"title":id,"status":"active","round":1,"nodes":[]})).unwrap(),
    )
    .await
    .unwrap();
    store::enroll(pool, id, c.revision, &goal()).await.unwrap();
    c
}

fn slot(n: i64) -> DateTime<Utc> {
    "2026-10-03T08:00:00+09:00"
        .parse::<DateTime<Utc>>()
        .unwrap()
        + chrono::Duration::days(n)
}

async fn occurrence(pool: &PgPool, parent: &str, id: &str, n: i64) -> Campaign {
    // Native admission inherits the parent's existing family, rather than
    // enrolling the child as an unrelated root before adding its lineage.
    let parent_goal = store::goal(pool, parent).await.unwrap();
    let family = crate::db::campaigns::graph::learning::family(pool, parent, &parent_goal)
        .await
        .unwrap();
    let mut child_goal = parent_goal;
    child_goal.schedule = None;
    child_goal.observation_window = Some(slot(n).to_rfc3339());
    let mut tx = pool.begin().await.unwrap();
    campaigns::create_on_tx(
        &mut tx,
        id.into(),
        serde_json::from_value(json!({"title":id,"status":"active","round":1,"nodes":[]})).unwrap(),
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO campaign_execution_specs(campaign_id,spec) VALUES($1,$2)")
        .bind(id)
        .bind(serde_json::to_value(child_goal).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO campaign_graph_occurrences(campaign_id,scheduled_at,child_id) VALUES($1,$2,$3)")
        .bind(parent).bind(slot(n)).bind(id).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO campaign_dag_members(campaign_id,family_id) VALUES($1,$2)")
        .bind(id)
        .bind(family)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    campaigns::get(pool, id).await.unwrap()
}

fn context_from_prompt(prompt: &str) -> Value {
    let marker = "Frozen native occurrence context (canonical retained ledger at publication):\n";
    let raw = prompt.split_once(marker).unwrap().1.lines().next().unwrap();
    serde_json::from_str(raw).unwrap()
}

#[tokio::test]
async fn retained_occurrence_context_survives_recent_thirty_limit_retry_and_other_parent_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    setup(&pool).await;
    let parent = campaign(&pool, "cursor-owner").await;
    let other = campaign(&pool, "independent-owner").await;
    // Reverse insertion deliberately differs from scheduled order.
    for n in (0..31).rev() {
        occurrence(&pool, &parent.id, &format!("cursor-child-{n:02}"), n).await;
    }
    let other_child = occurrence(&pool, &other.id, "independent-first", 30).await;
    let first = load(&pool, "cursor-child-00").await.unwrap().unwrap();
    let thirty_first = load(&pool, "cursor-child-30").await.unwrap().unwrap();
    assert_eq!(first.zero_based_absolute_ordinal, 0);
    assert_eq!(thirty_first.zero_based_absolute_ordinal, 30);
    assert_eq!(first.scheduled_slot, slot(0));
    assert_eq!(thirty_first.parent_campaign_id, parent.id);
    assert_eq!(thirty_first.child_campaign_id, "cursor-child-30");
    assert_eq!(
        load(&pool, &other_child.id)
            .await
            .unwrap()
            .unwrap()
            .zero_based_absolute_ordinal,
        0
    );
    assert!(load(&pool, &parent.id).await.unwrap().is_none());
    assert!(load(&pool, "not-a-child").await.unwrap().is_none());
    let api = store::status(&pool, &parent.id).await.unwrap();
    assert_eq!(api["occurrences"].as_array().unwrap().len(), 30);
    assert!(
        !api["occurrences"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["child_id"] == first.child_campaign_id)
    );
    let before_goal = store::goal(&pool, &parent.id).await.unwrap();
    occurrence(&pool, &parent.id, "cursor-child-31", 31).await;
    let before_families: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM campaign_dag_families")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        before_families, 2,
        "native children inherit their two parents' families"
    );
    let reopened = fixture.connect_and_migrate().await;
    assert_eq!(
        load(&reopened, "cursor-child-00").await.unwrap(),
        Some(first)
    );
    assert_eq!(
        load(&reopened, "cursor-child-30").await.unwrap(),
        Some(thirty_first)
    );
    assert_eq!(
        serde_json::to_value(store::goal(&reopened, &parent.id).await.unwrap()).unwrap(),
        serde_json::to_value(before_goal).unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_dag_families")
            .fetch_one(&reopened)
            .await
            .unwrap(),
        before_families
    );
    // One canonical child cannot be rebound to another parent or slot.
    assert!(sqlx::query("INSERT INTO campaign_graph_occurrences(campaign_id,scheduled_at,child_id) VALUES($1,$2,'cursor-child-30')")
        .bind(&other.id).bind(slot(31)).execute(&reopened).await.is_err());
    reopened.close().await;
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn occurrence_control_and_work_publication_pin_cursor_and_actual_bootstrap_authority_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    setup(&pool).await;
    let parent = campaign(&pool, "publication-owner").await;
    let child = occurrence(&pool, &parent.id, "publication-first", 0).await;
    let child_goal = store::goal(&pool, &child.id).await.unwrap();
    let goal_before = serde_json::to_value(&child_goal).unwrap();
    assert!(
        crate::server::campaign_graph::control(
            &pool,
            &child.id,
            &child_goal,
            "plan",
            None,
            "initial",
            "Author the bounded DAG"
        )
        .await
        .unwrap()
    );
    let key = format!("{}:plan:initial", child.id);
    let prompt: String = sqlx::query_scalar("SELECT k.description FROM kanban_cards k JOIN campaign_graph_tasks t ON t.card_id=k.id WHERE t.task_key=$1")
        .bind(&key).fetch_one(&pool).await.unwrap();
    let context = context_from_prompt(&prompt);
    assert_eq!(context["parent_campaign_id"], parent.id);
    assert_eq!(context["child_campaign_id"], child.id);
    assert_eq!(context["zero_based_absolute_ordinal"], 0);
    assert!(prompt.contains("durable bootstrap_plan assignment"));
    assert!(prompt.contains("\"assigned_agent_id\":\"codex\""));
    let actual: Value = sqlx::query_scalar("SELECT jsonb_build_object('agent',a.agent_id,'purpose',a.purpose,'card_agent',k.assigned_agent_id) FROM campaign_dag_control_assignments a JOIN campaign_graph_tasks t ON t.task_key=a.task_key JOIN kanban_cards k ON k.id=t.card_id WHERE a.task_key=$1")
        .bind(&key).fetch_one(&pool).await.unwrap();
    assert_eq!(
        actual,
        json!({"agent":"codex","purpose":"bootstrap_plan","card_agent":"codex"})
    );
    occurrence(&pool, &parent.id, "publication-later", 1).await;
    crate::server::campaign_graph::control(
        &pool,
        &child.id,
        &child_goal,
        "plan",
        None,
        "initial",
        "A retry must not replace the published prompt",
    )
    .await
    .unwrap();
    let retained: String = sqlx::query_scalar("SELECT k.description FROM kanban_cards k JOIN campaign_graph_tasks t ON t.card_id=k.id WHERE t.task_key=$1")
        .bind(&key).fetch_one(&pool).await.unwrap();
    assert_eq!(retained, prompt);
    let graph: Graph = serde_json::from_value(json!({"nodes":[
        {"id":"inspect","task_key":"publication:first:inspect","title":"Inspect","agent":"coder",
         "kind":"inspect","effect":"artifact_only","details":"Write report.json",
         "acceptance":["Truthful report"],"dependencies":[],"inputs":[],
         "outputs":[{"path":"report.json","format":"json"}]},
        {"id":"verify","task_key":"publication:first:verify","title":"Independent QA","agent":"qa",
         "kind":"verify","effect":"artifact_only","details":"Verify supplied report",
         "acceptance":["Independent check"],"dependencies":["inspect"],
         "inputs":[{"dependency":"inspect","source":"report.json","destination":"input/report.json"}],
         "outputs":[{"path":"verified.json","format":"json"}]}]})).unwrap();
    let version = store::stage(&pool, &child.id, &graph, None).await.unwrap();
    sqlx::query(
        "UPDATE campaign_graph_versions SET status='validated' WHERE campaign_id=$1 AND version=$2",
    )
    .bind(&child.id)
    .bind(version)
    .execute(&pool)
    .await
    .unwrap();
    let current = campaigns::get(&pool, &child.id).await.unwrap();
    store::activate(&pool, &child.id, version, current.revision)
        .await
        .unwrap();
    let node = &graph.nodes[0];
    // Materialize the actual canonical empty manifest of this first node.
    let workspace = tempfile::tempdir().unwrap();
    let artifacts = tempfile::tempdir().unwrap();
    let input_manifest = crate::services::campaign_artifacts::materialize(
        artifacts.path(),
        workspace.path(),
        node,
        &std::collections::BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(input_manifest, json!({}));
    let work = work_prompt(&pool, &child.id, &child_goal, node, &input_manifest)
        .await
        .unwrap();
    let card = store::publish(
        &pool,
        &child.id,
        &store::Publication {
            key: &node.task_key,
            node: &node.id,
            role: "work",
            version: Some(version),
            agent: &node.agent,
            title: &node.title,
            prompt: &work,
            digest: &store::task_digest(node).unwrap(),
        },
    )
    .await
    .unwrap();
    let actual_work: String =
        sqlx::query_scalar("SELECT description FROM kanban_cards WHERE id=$1")
            .bind(card)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(context_from_prompt(&actual_work), context);
    assert_eq!(
        serde_json::to_value(store::goal(&pool, &child.id).await.unwrap()).unwrap(),
        goal_before
    );
    assert_eq!(
        store::status(&pool, &parent.id).await.unwrap()["dag_learning"]["sample_count"],
        0
    );
    pool.close().await;
    fixture.drop().await;
}

#[test]
fn ordinary_goal_has_no_invented_occurrence_or_bootstrap_context() {
    let original = "Original ordinary task contract".to_owned();
    assert_eq!(append(original.clone(), None).unwrap(), original);
    let text = control_authority("ordinary", &goal(), "validate", "qa", None).unwrap();
    assert!(text.contains("\"bootstrap_authority\":null"));
    assert!(text.contains("\"assigned_agent_id\":\"qa\""));
    assert!(!text.contains("durable bootstrap_plan assignment"));
}

#[test]
fn bootstrap_explanation_uses_actual_assignment_without_changing_goal_roles() {
    let goal = goal();
    let original = serde_json::to_value(&goal).unwrap();
    let authority = ControlAssignment {
        family_id: "actual-family".into(),
        agent_id: "codex".into(),
        purpose: "bootstrap_plan",
        input_manifest: None,
    };
    let text = control_authority("first-child", &goal, "plan", "codex", Some(&authority)).unwrap();
    assert!(text.contains("\"family_id\":\"actual-family\""));
    assert!(text.contains("\"business_executors\":[\"coder\",\"qa\"]"));
    assert!(text.contains("Goal.planner names the ongoing planner"));
    assert_eq!(serde_json::to_value(&goal).unwrap(), original);
    assert!(control_authority("first-child", &goal, "plan", "coder", Some(&authority)).is_err());
    assert!(
        control_authority("first-child", &goal, "validate", "codex", Some(&authority)).is_err()
    );
}

#[test]
fn truthful_report_receipt_does_not_relabel_failed_business_checks() {
    let dir = tempfile::tempdir().unwrap();
    let report = json!({"business_outcome":"left_open","command":"exact-head test",
        "tested_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "raw_exit_code":1,"unavailable":["generated catalog"],"unreviewed_scope":["runtime behavior"]});
    let path = dir.path().join("report.json");
    std::fs::write(&path, serde_json::to_vec(&report).unwrap()).unwrap();
    let parsed: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(parsed["raw_exit_code"], 1);
    assert_eq!(parsed["tested_sha"], report["tested_sha"]);
    let receipt = json!({"campaign_receipt":{"outcome":"passed","summary":"The assigned truthful report was written and checked; business remains left open",
        "observations":[{"command":"Rust fixture read and independently assert report raw exit and tested SHA","exit_code":0,
            "evidence":"Actual JSON read/assertions preserved raw_exit_code=1 and unavailable generated catalog"}]}});
    assert!(crate::server::campaign_graph::observation::receipt(
        &receipt
    ));
    crate::db::campaigns::graph::result::validate_dispatch(&receipt).unwrap();
    let mut failed_check = receipt.clone();
    failed_check["campaign_receipt"]["observations"][0] = json!({"command":"exact-head test",
        "exit_code":1,"evidence":"The test actually failed"});
    assert!(!crate::server::campaign_graph::observation::receipt(
        &failed_check
    ));
    // A well-typed failing result is captureable, but has no acceptance.
    crate::db::campaigns::graph::result::validate_dispatch(&failed_check).unwrap();
    failed_check["campaign_receipt"]["outcome"] = json!("blocked");
    assert!(!crate::server::campaign_graph::observation::receipt(
        &failed_check
    ));
    crate::db::campaigns::graph::result::validate_dispatch(&failed_check).unwrap();
    failed_check["campaign_receipt"]["observations"][0]["exit_code"] = json!("passed");
    assert!(crate::db::campaigns::graph::result::validate_dispatch(&failed_check).is_err());
    let prompt = crate::server::campaign_graph::contract_prompt(&goal()).unwrap();
    assert!(prompt.contains("its own declared acceptance is fulfilled"));
    assert!(prompt.contains("never relabel the failing check as passed"));
    assert!(prompt.contains("acceptance requires checks to pass"));
    assert!(prompt.contains("setup at the exact assigned head"));
}
