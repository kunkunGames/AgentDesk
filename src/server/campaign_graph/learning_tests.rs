//! Real PostgreSQL contracts with explicit fixture receipts, not model acceptance.
use super::*;
use crate::db::campaigns::graph::learning::{
    self as ledger,
    policy::Policy,
    template::{Decision, Review, Template},
};

async fn superior(pool: &PgPool, n: u32) {
    sqlx::query("INSERT INTO agents(id,name,provider) VALUES('codex','Codex','codex'),('brain','Brain','codex') ON CONFLICT DO NOTHING")
        .execute(pool).await.unwrap();
    sqlx::query("UPDATE kv_meta SET value=$1 WHERE key='runtime-config'")
        .bind(json!({"internalTasksEnabled":true,"campaignSuperiorAgentId":"codex","campaignDagTemplateMinSamples":n}).to_string())
        .execute(pool).await.unwrap();
}
async fn occurrence(pool: &PgPool, root: &str, id: &str, ordinal: i64) -> campaigns::Campaign {
    let c = campaigns::create(
        pool,
        id.into(),
        serde_json::from_value(json!({"title":id,"status":"active","round":1,"nodes":[]})).unwrap(),
    )
    .await
    .unwrap();
    store::enroll(pool, id, c.revision, &goal()).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_occurrences(campaign_id,scheduled_at,child_id) VALUES($1,NOW()+($2::bigint*INTERVAL '1 minute'),$3)")
        .bind(root).bind(ordinal).bind(id).execute(pool).await.unwrap();
    ledger::family(pool, id, &goal()).await.unwrap();
    c
}
fn definition() -> Template {
    let mut g = graph();
    for n in &mut g.nodes {
        n.task_key = "{{task_key}}".into();
        n.agent = if n.kind == Kind::Verify {
            "{{verifier}}"
        } else {
            "{{planner}}"
        }
        .into();
    }
    g.nodes[0].details =
        "Freshly inspect {{repo}} for {{window}}; {{objective}}; write answer.json".into();
    Template {
        schema_version: 1,
        name: "Fresh bounded answer workflow".into(),
        description: "Reusable independently verified fixture plan".into(),
        graph: g,
    }
}
async fn complete_fixture(pool: &PgPool, c: &campaigns::Campaign) -> Graph {
    complete_after_failure(pool, c, None).await
}
async fn complete_after_failure(
    pool: &PgPool,
    c: &campaigns::Campaign,
    failure: Option<&str>,
) -> Graph {
    let mut g = graph();
    for n in &mut g.nodes {
        n.task_key = format!("{}:{}", c.id, n.id);
    }
    let v = store::stage(pool, &c.id, &g, failure).await.unwrap();
    sqlx::query(
        "UPDATE campaign_graph_versions SET status='validated' WHERE campaign_id=$1 AND version=$2",
    )
    .bind(&c.id)
    .bind(v)
    .execute(pool)
    .await
    .unwrap();
    store::activate(pool, &c.id, v, c.revision).await.unwrap();
    for n in &g.nodes {
        let card = store::publish(
            pool,
            &c.id,
            &store::Publication {
                key: &n.task_key,
                node: &n.id,
                role: "work",
                version: Some(v),
                agent: &n.agent,
                title: &n.title,
                prompt: &n.details,
                digest: &store::task_digest(n).unwrap(),
            },
        )
        .await
        .unwrap();
        let dispatch = format!("fixture-{}-{}", c.id, n.id);
        // The native observer's accepted state is the fixture input to learning.
        // This test deliberately makes no provider execution claim.
        sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result) VALUES($1,$2,$3,'implementation','completed',$4)")
            .bind(&dispatch).bind(&card).bind(&n.agent).bind(json!({"campaign_receipt":{"outcome":"passed","summary":"explicit learning fixture"}}).to_string()).execute(pool).await.unwrap();
        sqlx::query("UPDATE kanban_cards SET status='done' WHERE id=$1")
            .bind(card)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("UPDATE campaign_graph_tasks SET accepted=TRUE,observation=$2,artifact_manifest=$3 WHERE task_key=$1")
            .bind(&n.task_key).bind(json!({"dispatch_id":dispatch,"outcome":"accepted"}))
            .bind(json!({"bundle":"fixture-bounded-artifact","files":{n.outputs[0].path.clone():"fixture-hash"}})).execute(pool).await.unwrap();
    }
    let fresh = campaigns::get(pool, &c.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&fresh).unwrap()).unwrap();
    input.status = CampaignStatus::Completed;
    for n in &mut input.nodes {
        n.status = campaigns::NodeStatus::Completed;
    }
    campaigns::replace(pool, &c.id, fresh.revision, input)
        .await
        .unwrap();
    g
}

#[tokio::test]
async fn completed_repaired_graph_collects_despite_retained_failed_revision_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    superior(&pool, 10).await;
    let original = graph();
    let v1 = store::stage(&pool, &c.id, &original, None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE campaign_id=$1")
        .bind(&c.id)
        .execute(&pool)
        .await
        .unwrap();
    store::activate(&pool, &c.id, v1, c.revision).await.unwrap();
    let node = &original.nodes[0];
    let card = store::publish(
        &pool,
        &c.id,
        &store::Publication {
            key: &node.task_key,
            node: &node.id,
            role: "work",
            version: Some(v1),
            agent: &node.agent,
            title: &node.title,
            prompt: &node.details,
            digest: &store::task_digest(node).unwrap(),
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status,result,completed_at) VALUES('retained-failure',$1,'implementation','failed',$2,NOW())")
        .bind(&card).bind(json!({"campaign_receipt":{"failure_category":"code","summary":"explicit failed artifact fixture"}}).to_string()).execute(&pool).await.unwrap();
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let state = store::status(&pool, &c.id).await.unwrap();
    let failure = state["failures"][0]["id"].as_str().unwrap();
    let current = campaigns::get(&pool, &c.id).await.unwrap();
    let repaired = complete_after_failure(&pool, &current, Some(failure)).await;
    let key = &repaired.nodes[1].task_key;
    let manifest: Value =
        sqlx::query_scalar("SELECT artifact_manifest FROM campaign_graph_tasks WHERE task_key=$1")
            .bind(key)
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query("UPDATE campaign_graph_tasks SET artifact_manifest=NULL WHERE task_key=$1")
        .bind(key)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        !reconciliation_candidates(&pool)
            .await
            .unwrap()
            .contains(&c.id),
        "a completed label cannot replace captured current-node evidence"
    );
    sqlx::query("UPDATE campaign_graph_tasks SET artifact_manifest=$2 WHERE task_key=$1")
        .bind(key)
        .bind(manifest)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        reconciliation_candidates(&pool)
            .await
            .unwrap()
            .contains(&c.id),
        "failed tasks from a retired revision must not hide a verified active graph"
    );
    assert!(
        ledger::samples::collect(&pool, &c.id, &goal())
            .await
            .unwrap()
    );
    assert!(
        !ledger::samples::collect(&pool, &c.id, &goal())
            .await
            .unwrap()
    );
    assert!(
        !reconciliation_candidates(&pool)
            .await
            .unwrap()
            .contains(&c.id)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM campaign_graph_failures WHERE campaign_id=$1"
        )
        .bind(&c.id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT accepted FROM campaign_graph_tasks WHERE task_key=$1"
        )
        .bind(&node.task_key)
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    pool.close().await;
    fixture.drop().await;
}

#[test]
fn campaign_dag_policy_defaults_bounds_and_scope_are_explicit() {
    let p = Policy::from_value(&Value::Null).unwrap();
    assert_eq!((p.superior_agent.as_str(), p.min_samples), ("codex", 10));
    assert_eq!(
        Policy::from_value(
            &json!({"campaignSuperiorAgentId":"brain","campaignDagTemplateMinSamples":3})
        )
        .unwrap()
        .min_samples,
        3
    );
    for value in [
        json!({"campaignDagTemplateMinSamples":0}),
        json!({"campaignDagTemplateMinSamples":101}),
        json!({"campaignDagTemplateMinSamples":2.5}),
        json!({"campaignDagTemplateMinSamples":"10"}),
        json!({"campaignSuperiorAgentId":"../agent"}),
    ] {
        assert!(Policy::from_value(&value).is_err());
    }
    let mut changed = goal();
    let digest = ledger::scope_digest(&changed).unwrap();
    changed.budget.max_dispatches += 1;
    assert_eq!(digest, ledger::scope_digest(&changed).unwrap());
    changed.success.push("a different required result".into());
    assert_ne!(digest, ledger::scope_digest(&changed).unwrap());
}

#[tokio::test]
async fn bootstrap_superior_then_enrolled_planner_preserves_published_contracts_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let root = setup(&pool).await;
    superior(&pool, 10).await;
    let first = occurrence(&pool, &root.id, "first", 1).await;
    control(
        &pool,
        &first.id,
        &goal(),
        "plan",
        None,
        "initial",
        GRAPH_SCHEMA,
    )
    .await
    .unwrap();
    let assigned:String=sqlx::query_scalar("SELECT k.assigned_agent_id FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id WHERE t.campaign_id=$1")
        .bind(&first.id).fetch_one(&pool).await.unwrap();
    assert_eq!(assigned, "codex");
    let first_card: String =
        sqlx::query_scalar("SELECT card_id FROM campaign_graph_tasks WHERE campaign_id=$1")
            .bind(&first.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    // The DB permits the assigned superior control task although it is not a business executor.
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('superior-start',$1,'codex','implementation','pending')")
        .bind(&first_card).execute(&pool).await.unwrap();
    let illegal=sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('wrong-start',$1,'brain','implementation','pending')")
        .bind(&first_card).execute(&pool).await;
    assert!(illegal.is_err());
    sqlx::query("UPDATE kv_meta SET value=$1 WHERE key='runtime-config'")
        .bind(json!({"internalTasksEnabled":true,"campaignSuperiorAgentId":"brain"}).to_string())
        .execute(&pool)
        .await
        .unwrap();
    control(
        &pool,
        &first.id,
        &goal(),
        "plan",
        None,
        "initial",
        "changed prompt must not replace the old card",
    )
    .await
    .unwrap();
    let preserved: String =
        sqlx::query_scalar("SELECT assigned_agent_id FROM kanban_cards WHERE id=$1")
            .bind(first_card)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(preserved, "codex");
    let second = occurrence(&pool, &root.id, "second", 2).await;
    control(
        &pool,
        &second.id,
        &goal(),
        "plan",
        None,
        "initial",
        GRAPH_SCHEMA,
    )
    .await
    .unwrap();
    let assigned:String=sqlx::query_scalar("SELECT k.assigned_agent_id FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id WHERE t.campaign_id=$1")
        .bind(&second.id).fetch_one(&pool).await.unwrap();
    assert_eq!(assigned, "qwen");
    let mut forbidden = graph();
    forbidden.nodes[0].agent = "codex".into();
    assert!(forbidden.validate(&goal()).is_err());
    let old_family = ledger::family(&pool, &root.id, &goal()).await.unwrap();
    let mut new_scope = goal();
    new_scope
        .success
        .push("additional independent result".into());
    sqlx::query("UPDATE campaign_execution_specs SET spec=$2 WHERE campaign_id=$1")
        .bind(&root.id)
        .bind(serde_json::to_value(&new_scope).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    let third = occurrence(&pool, &root.id, "new-scope-first", 3).await;
    sqlx::query("UPDATE campaign_execution_specs SET spec=$2 WHERE campaign_id=$1")
        .bind(&third.id)
        .bind(serde_json::to_value(&new_scope).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    control(
        &pool,
        &third.id,
        &new_scope,
        "plan",
        None,
        "initial",
        GRAPH_SCHEMA,
    )
    .await
    .unwrap();
    let new_family = ledger::family(&pool, &third.id, &new_scope).await.unwrap();
    assert_ne!(old_family, new_family);
    let assigned:String=sqlx::query_scalar("SELECT k.assigned_agent_id FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id WHERE t.campaign_id=$1")
        .bind(&third.id).fetch_one(&pool).await.unwrap();
    assert_eq!(assigned, "brain");
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn verified_occurrences_count_once_and_template_requires_superior_decision_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let root = setup(&pool).await;
    superior(&pool, 2).await;
    let first = occurrence(&pool, &root.id, "sample-first", 1).await;
    complete_fixture(&pool, &first).await;
    // Claimed completion with a missing independently verified output cannot count.
    sqlx::query("UPDATE campaign_graph_tasks SET artifact_manifest=NULL WHERE campaign_id=$1 AND node_id='verify'")
        .bind(&first.id).execute(&pool).await.unwrap();
    assert!(
        !ledger::samples::collect(&pool, &first.id, &goal())
            .await
            .unwrap()
    );
    sqlx::query("UPDATE campaign_graph_tasks SET artifact_manifest=$2 WHERE campaign_id=$1 AND node_id='verify'")
        .bind(&first.id).bind(json!({"bundle":"fixture-verified-output","files":{"verified.json":"fixture"}})).execute(&pool).await.unwrap();
    assert!(
        ledger::samples::collect(&pool, &first.id, &goal())
            .await
            .unwrap()
    );
    assert!(
        !ledger::samples::collect(&pool, &first.id, &goal())
            .await
            .unwrap()
    );
    assert!(
        ledger::reviews::prepare(&pool, &root.id, &goal())
            .await
            .unwrap()
            .is_none()
    );
    let second = occurrence(&pool, &root.id, "sample-second", 2).await;
    complete_fixture(&pool, &second).await;
    assert!(
        ledger::samples::collect(&pool, &second.id, &goal())
            .await
            .unwrap()
    );
    let pending = ledger::reviews::prepare(&pool, &root.id, &goal())
        .await
        .unwrap()
        .unwrap();
    let repeated = ledger::reviews::prepare(&pool, &root.id, &goal())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.id, repeated.id);
    assert_eq!(pending.agent_id, "codex");
    assert_eq!(
        ledger::inspection(&pool, &root.id).await.unwrap()["sample_count"],
        2
    );
    let decision = Review {
        sample_digest: pending.sample_digest.clone(),
        decision: Decision::Promote,
        reason: "Two independently verified fixture outcomes support this bounded structure".into(),
        template: Some(definition()),
    };
    let mut invalid = serde_json::to_value(&decision).unwrap();
    invalid["sample_digest"] = json!("wrong");
    assert!(
        ledger::reviews::accept(&pool, &pending.task_key, &invalid)
            .await
            .is_err()
    );
    invalid = serde_json::to_value(&decision).unwrap();
    invalid["template"]["graph"]["nodes"][1]["agent"] = json!("{{planner}}");
    assert!(
        ledger::reviews::accept(&pool, &pending.task_key, &invalid)
            .await
            .is_err()
    );
    assert!(ledger::inspection(&pool, &root.id).await.unwrap()["active_template"].is_null());
    let valid = serde_json::to_value(&decision).unwrap();
    ledger::reviews::accept(&pool, &pending.task_key, &valid)
        .await
        .unwrap();
    ledger::reviews::accept(&pool, &pending.task_key, &valid)
        .await
        .unwrap();
    let mut changed = valid.clone();
    changed["reason"] = json!("changed");
    assert!(
        ledger::reviews::accept(&pool, &pending.task_key, &changed)
            .await
            .is_err()
    );
    let next = occurrence(&pool, &root.id, "sample-next", 3).await;
    assert!(learning::initial(&pool, &next.id, &goal()).await.unwrap());
    let state = store::status(&pool, &next.id).await.unwrap();
    assert_eq!(state["versions"][0]["status"], "candidate");
    assert!(state["active_version"].is_null());
    assert!(state["tasks"].as_array().unwrap().is_empty());
    let resolved: Graph = serde_json::from_value(state["versions"][0]["graph"].clone()).unwrap();
    assert_eq!(resolved.nodes[0].agent, "qwen");
    assert_eq!(resolved.nodes[1].agent, "monitoring");
    assert!(
        resolved
            .nodes
            .iter()
            .all(|n| !n.task_key.starts_with("sample-first"))
    );
    assert!(
        store::activate(&pool, &next.id, 1, next.revision)
            .await
            .is_err()
    );
    assert!(
        ledger::reviews::prepare(&pool, &root.id, &goal())
            .await
            .unwrap()
            .is_none()
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn deferred_template_review_waits_for_another_configured_sample_batch_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let root = setup(&pool).await;
    superior(&pool, 1).await;
    let first = occurrence(&pool, &root.id, "defer-first", 1).await;
    complete_fixture(&pool, &first).await;
    ledger::samples::collect(&pool, &first.id, &goal())
        .await
        .unwrap();
    let pending = ledger::reviews::prepare(&pool, &root.id, &goal())
        .await
        .unwrap()
        .unwrap();
    ledger::reviews::accept(&pool,&pending.task_key,&json!({"sample_digest":pending.sample_digest,"decision":"defer","reason":"Need a second distinct outcome","template":null})).await.unwrap();
    assert!(
        ledger::reviews::prepare(&pool, &root.id, &goal())
            .await
            .unwrap()
            .is_none()
    );
    let second = occurrence(&pool, &root.id, "defer-second", 2).await;
    complete_fixture(&pool, &second).await;
    ledger::samples::collect(&pool, &second.id, &goal())
        .await
        .unwrap();
    let next = ledger::reviews::prepare(&pool, &root.id, &goal())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(next.id, pending.id);
    assert_eq!(next.snapshot["sample_count"], 2);
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn default_tenth_sample_claim_is_atomic_and_review_failure_keeps_lower_planning_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let root = setup(&pool).await;
    superior(&pool, 10).await;
    for n in 1..=10 {
        let child = occurrence(&pool, &root.id, &format!("threshold-{n}"), n).await;
        complete_fixture(&pool, &child).await;
        ledger::samples::collect(&pool, &child.id, &goal())
            .await
            .unwrap();
        if n < 10 {
            assert!(
                ledger::reviews::prepare(&pool, &root.id, &goal())
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
    let pinned_goal = goal();
    let (first, second) = tokio::join!(
        ledger::reviews::prepare(&pool, &root.id, &pinned_goal),
        ledger::reviews::prepare(&pool, &root.id, &pinned_goal)
    );
    let first = first.unwrap().unwrap();
    let second = second.unwrap().unwrap();
    assert_eq!(first.id, second.id);
    assert_eq!(first.snapshot["threshold"], 10);
    ledger::reviews::fail(
        &pool,
        &first.task_key,
        "Explicit fixture reviewer unavailable",
    )
    .await
    .unwrap();
    assert!(
        ledger::reviews::prepare(&pool, &root.id, &goal())
            .await
            .unwrap()
            .is_none()
    );
    // The first plan had already been assigned; removing the superior from
    // current policy must not prevent subsequent enrolled-agent planning.
    sqlx::query("UPDATE campaign_dag_families SET bootstrap_campaign_id='threshold-1'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE kv_meta SET value=$1 WHERE key='runtime-config'")
        .bind(json!({"internalTasksEnabled":true,"campaignSuperiorAgentId":"missing-superior","campaignDagTemplateMinSamples":1}).to_string()).execute(&pool).await.unwrap();
    let child = occurrence(&pool, &root.id, "lower-after-review-failure", 11).await;
    control(
        &pool,
        &child.id,
        &goal(),
        "plan",
        None,
        "initial",
        GRAPH_SCHEMA,
    )
    .await
    .unwrap();
    learning::review(&pool, &root.id, &goal()).await.unwrap();
    assert!(store::status(&pool, &root.id).await.unwrap()["hold_reason"].is_null());
    let assigned:String=sqlx::query_scalar("SELECT k.assigned_agent_id FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id WHERE t.campaign_id=$1")
        .bind(&child.id).fetch_one(&pool).await.unwrap();
    assert_eq!(assigned, "qwen");
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn frozen_superior_review_input_and_native_receipt_promote_once_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let root = setup(&pool).await;
    superior(&pool, 1).await;
    let child = occurrence(&pool, &root.id, "frozen-sample", 1).await;
    complete_fixture(&pool, &child).await;
    ledger::samples::collect(&pool, &child.id, &goal())
        .await
        .unwrap();
    learning::review(&pool, &root.id, &goal()).await.unwrap();
    let pending = ledger::reviews::prepare(&pool, &root.id, &goal())
        .await
        .unwrap()
        .unwrap();
    let card = store::card_id(&pending.task_key);
    let workspace = tempfile::tempdir().unwrap();
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,context) VALUES('frozen-review',$1,'codex','implementation','dispatched',$2)")
        .bind(&card).bind(json!({"worktree_path":workspace.path().to_string_lossy()})).execute(&pool).await.unwrap();
    crate::services::campaign_control_input::prepare_input(&pool, "frozen-review")
        .await
        .unwrap();
    let input = workspace.path().join(ledger::template::INPUT_FILE);
    let original = std::fs::read(&input).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&original).unwrap()["sample_digest"],
        pending.sample_digest
    );
    let result = json!({"campaign_receipt":{"outcome":"passed","summary":"Explicit fixture decision","observations":[{"command":"compare frozen fixture graphs","exit_code":0,"evidence":"all fixture scopes and independent verifier roles checked"}]},
        "dag_template_review":{"sample_digest":pending.sample_digest,"decision":"promote","reason":"Bounded reusable fixture workflow","template":definition()}});
    std::fs::write(&input, b"{}").unwrap();
    assert!(
        learning::accept_review(
            &pool,
            &pending.task_key,
            &result,
            json!({"worktree_path":workspace.path().to_string_lossy()})
        )
        .await
        .is_err()
    );
    assert!(ledger::inspection(&pool, &root.id).await.unwrap()["active_template"].is_null());
    std::fs::write(&input, &original).unwrap();
    sqlx::query("UPDATE task_dispatches SET status='completed',result=$1 WHERE id='frozen-review'")
        .bind(result.to_string())
        .execute(&pool)
        .await
        .unwrap();
    reconcile::observe(&pool, &root.id, &goal(), None)
        .await
        .unwrap();
    reconcile::observe(&pool, &root.id, &goal(), None)
        .await
        .unwrap();
    let state = ledger::inspection(&pool, &root.id).await.unwrap();
    assert_eq!(state["active_template"]["version"], 1);
    assert_eq!(state["reviews"][0]["status"], "promoted");
    let next = occurrence(&pool, &root.id, "template-rejected-next", 2).await;
    assert!(learning::initial(&pool, &next.id, &goal()).await.unwrap());
    control(
        &pool,
        &next.id,
        &goal(),
        "validate",
        Some(1),
        "1",
        "independently check instantiated template",
    )
    .await
    .unwrap();
    let validator = store::card_id(&format!("{}:validate:1", next.id));
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&validator)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result) VALUES('template-rejection',$1,'monitoring','implementation','completed',$2)")
        .bind(&validator).bind(json!({"campaign_receipt":{"outcome":"blocked","summary":"Fixture template rejected","failure_category":"plan"},"graph_validation":{"approved":false,"reason":"Needs a fresh check"}}).to_string()).execute(&pool).await.unwrap();
    reconcile::observe(&pool, &next.id, &goal(), None)
        .await
        .unwrap();
    reconcile::reject_failed_candidates(&pool, &next.id)
        .await
        .unwrap();
    assert!(ledger::inspection(&pool, &root.id).await.unwrap()["active_template"].is_null());
    assert!(!learning::initial(&pool, &next.id, &goal()).await.unwrap());
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn active_template_reviews_repeat_each_batch_keep_and_deduplicate_versions_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let root = setup(&pool).await;
    superior(&pool, 2).await;
    for i in 1..=8 {
        let c = occurrence(&pool, &root.id, &format!("continuous-{i}"), i).await;
        complete_fixture(&pool, &c).await;
        ledger::samples::collect(&pool, &c.id, &goal())
            .await
            .unwrap();
        let review = ledger::reviews::prepare(&pool, &root.id, &goal())
            .await
            .unwrap();
        if i % 2 == 1 {
            assert!(review.is_none());
            continue;
        }
        let review = review.unwrap();
        if i > 2 {
            assert!(review.snapshot["active_template"].is_object());
            assert_eq!(review.snapshot["previous_sample_count"], i - 2);
            assert!(
                ledger::reviews::instantiate(&pool, &root.id, &goal())
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        let mut template = definition();
        if i >= 6 {
            template.description = "Improved independently verified description".into();
        }
        let decision = json!({"sample_digest":review.sample_digest,"decision":if i==4 {"keep"}else{"promote"},"reason":"Verified batch compared with the frozen existing template","template":if i==4 {serde_json::Value::Null}else{serde_json::to_value(template).unwrap()}});
        ledger::reviews::accept(&pool, &review.task_key, &decision)
            .await
            .unwrap();
        ledger::reviews::accept(&pool, &review.task_key, &decision)
            .await
            .unwrap();
        let state = ledger::inspection(&pool, &root.id).await.unwrap();
        assert_eq!(
            state["active_template"]["version"],
            if i < 6 { 1 } else { 2 }
        );
        assert_eq!(state["new_samples_since_review"], 0);
        assert_eq!(state["samples_until_review"], 2);
        let status: String =
            sqlx::query_scalar("SELECT status FROM campaign_dag_reviews WHERE id=$1")
                .bind(&review.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, if i == 4 || i == 8 { "kept" } else { "promoted" });
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM campaign_dag_templates")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    let mut held = None;
    for i in 9..=14 {
        let c = occurrence(&pool, &root.id, &format!("continuous-{i}"), i).await;
        complete_fixture(&pool, &c).await;
        ledger::samples::collect(&pool, &c.id, &goal())
            .await
            .unwrap();
        if i >= 10 {
            let pending = ledger::reviews::prepare(&pool, &root.id, &goal())
                .await
                .unwrap()
                .unwrap();
            if let Some(first) = &held {
                let first: &ledger::reviews::Pending = first;
                assert_eq!(pending.id, first.id);
                assert_eq!(pending.sample_digest, first.sample_digest);
            } else {
                held = Some(pending);
            }
        }
    }
    for through in [10, 12, 14] {
        let pending = if through == 10 {
            held.take().unwrap()
        } else {
            ledger::reviews::prepare(&pool, &root.id, &goal())
                .await
                .unwrap()
                .unwrap()
        };
        assert_eq!(pending.snapshot["sample_count"], through);
        let ids: Vec<_> = pending.snapshot["samples"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["campaign_id"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            ids,
            vec![
                format!("continuous-{}", through - 1),
                format!("continuous-{through}")
            ]
        );
        ledger::reviews::accept(&pool,&pending.task_key,&json!({"sample_digest":pending.sample_digest,"decision":"keep","reason":"Each delayed cohort is preserved without consuming another batch","template":null})).await.unwrap();
    }
    let state = ledger::inspection(&pool, &root.id).await.unwrap();
    assert_eq!(state["last_review_sample_count"], 14);
    assert_eq!(state["new_samples_since_review"], 0);
    assert_eq!(state["samples_until_review"], 2);
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn template_review_rejects_changed_base_and_missing_verified_effect_class_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let root = setup(&pool).await;
    superior(&pool, 1).await;
    let c = occurrence(&pool, &root.id, "review-base-first", 1).await;
    complete_fixture(&pool, &c).await;
    let first = ledger::reviews::prepare(&pool, &root.id, &goal())
        .await
        .unwrap()
        .unwrap();
    let mut bad = definition();
    bad.graph.nodes[0].kind = Kind::Inspect;
    assert!(ledger::reviews::accept(&pool,&first.task_key,&json!({"sample_digest":first.sample_digest,"decision":"promote","reason":"Cannot erase executed workflow","template":bad})).await.is_err());
    ledger::reviews::accept(&pool,&first.task_key,&json!({"sample_digest":first.sample_digest,"decision":"promote","reason":"Verified workflow preserved","template":definition()})).await.unwrap();
    let c = occurrence(&pool, &root.id, "review-base-second", 2).await;
    complete_fixture(&pool, &c).await;
    let next = ledger::reviews::prepare(&pool, &root.id, &goal())
        .await
        .unwrap()
        .unwrap();
    let template = next.snapshot["active_template"]["id"].as_str().unwrap();
    ledger::reviews::block(
        &pool,
        template,
        &root.id,
        "fresh occurrence independently rejected this template",
    )
    .await
    .unwrap();
    assert!(ledger::reviews::accept(&pool,&next.task_key,&json!({"sample_digest":next.sample_digest,"decision":"keep","reason":"Stale acceptance must not revive blocked template","template":null})).await.is_err());
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn scheduled_windows_preserve_family_and_verified_samples_without_backfill_pg() {
    use crate::db::campaigns::graph::schedule::{self, activation};
    use chrono::Timelike;
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let root = setup(&pool).await;
    superior(&pool, 2).await;
    complete_fixture(&pool, &root).await;
    ledger::samples::collect(&pool, &root.id, &goal())
        .await
        .unwrap();
    let original = ledger::inspection(&pool, &root.id).await.unwrap();
    let now = chrono::Utc::now();
    let future = now + chrono::Duration::minutes(2);
    let schedule = schedule::Schedule {
        timezone: "UTC".into(),
        daily_minutes: [(future.hour() * 60 + future.minute()) as u16].into(),
    };
    let current = campaigns::get(&pool, &root.id).await.unwrap();
    let request = activation::Activation {
        expected_revision: current.revision,
        request_id: "recurrence-v1".into(),
        evidence:
            "Enable the independently verified parent while preserving its single prior sample"
                .into(),
        schedule,
        occurrence_budget: None,
        activate: true,
    };
    // Completed fixture cards deliberately omit the dispatcher lifecycle.
    // Activation must refuse those unsettled queues before their terminal fixture state.
    assert!(
        activation::activate(&pool, &root.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("unsettled queue")
    );
    sqlx::query("UPDATE auto_queue_runs SET status='completed' WHERE id IN (SELECT e.run_id FROM auto_queue_entries e JOIN campaign_graph_tasks t ON t.card_id=e.kanban_card_id WHERE t.campaign_id=$1)")
        .bind(&root.id).execute(&pool).await.unwrap();
    let revision = activation::activate(&pool, &root.id, &request)
        .await
        .unwrap();
    assert_eq!(
        activation::activate(&pool, &root.id, &request)
            .await
            .unwrap(),
        revision
    );
    let parent_goal = store::goal(&pool, &root.id).await.unwrap();
    assert!(
        schedule::admit(&pool, &root.id, &parent_goal, now)
            .await
            .unwrap()
            .is_none()
    );
    let child = schedule::admit(&pool, &root.id, &parent_goal, future)
        .await
        .unwrap()
        .unwrap();
    let child_goal = store::goal(&pool, &child).await.unwrap();
    assert_eq!(
        ledger::scope_digest(&parent_goal).unwrap(),
        ledger::scope_digest(&child_goal).unwrap()
    );
    assert_eq!(parent_goal.objective, child_goal.objective);
    assert!(child_goal.observation_window.is_some());
    let c = campaigns::get(&pool, &child).await.unwrap();
    complete_fixture(&pool, &c).await;
    assert!(
        ledger::samples::collect(&pool, &child, &child_goal)
            .await
            .unwrap()
    );
    let state = ledger::inspection(&pool, &root.id).await.unwrap();
    assert_eq!(state["family_id"], original["family_id"]);
    assert_eq!(state["sample_count"], 2);
    assert_eq!(
        ledger::inspection(&pool, &child).await.unwrap()["recurring"],
        true
    );
    assert!(
        ledger::reviews::prepare(&pool, &root.id, &parent_goal)
            .await
            .unwrap()
            .is_some()
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn missing_prerequisite_fences_planner_and_schedule_activation_preserves_hold_pg() {
    use crate::db::campaigns::graph::{
        preflight::Prerequisite,
        schedule::{Schedule, activation},
    };
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    superior(&pool, 10).await;
    let mut g = goal();
    g.prerequisites = vec![Prerequisite::Binary {
        name: "agentdesk-definitely-absent-prerequisite".into(),
    }];
    sqlx::query("UPDATE campaign_execution_specs SET spec=$2 WHERE campaign_id=$1")
        .bind(&c.id)
        .bind(serde_json::to_value(&g).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    tick(&pool, &engine, &c.id, None).await.unwrap();
    let state = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(state["preflight"]["ready"], false);
    assert_eq!(
        state["preflight"]["checks"][0]["reason_code"],
        "binary_unavailable"
    );
    assert!(state["tasks"].as_array().unwrap().is_empty());
    assert!(state["hold_reason"].is_null());
    let current = campaigns::get(&pool, &c.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let paused = campaigns::replace(&pool, &c.id, current.revision, input)
        .await
        .unwrap();
    store::hold(&pool, &c.id, "Unknown effects need operator reconciliation")
        .await
        .unwrap();
    let request = activation::Activation {
        expected_revision: paused.revision,
        request_id: "cannot-clear-hold".into(),
        evidence: "Scheduling must preserve the operator's existing hold".into(),
        schedule: Schedule {
            timezone: "UTC".into(),
            daily_minutes: [0, 360, 720, 1080].into(),
        },
        occurrence_budget: None,
        activate: true,
    };
    assert!(
        activation::activate(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("cannot clear")
    );
    assert_eq!(
        campaigns::get(&pool, &c.id).await.unwrap().status,
        CampaignStatus::Paused
    );
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["hold_reason"],
        "Unknown effects need operator reconciliation"
    );
    assert!(store::goal(&pool, &c.id).await.unwrap().schedule.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM campaign_schedule_revisions")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    pool.close().await;
    fixture.drop().await;
}

// Budget fixtures use durable claims/assignments and canonical DB transitions.
// Synthetic historical receipts are explicit inputs, not provider execution proof.
async fn review_budget_fixture(
    pool: &PgPool,
    owner: &str,
    goal: &Goal,
    name: &str,
    ordinal: i64,
    seconds_ago: i64,
    policy_mode: &str,
) -> ledger::reviews::Pending {
    let family = ledger::family(pool, owner, goal).await.unwrap();
    let claimed_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT clock_timestamp()-($1::bigint*INTERVAL '1 second')")
            .bind(seconds_ago)
            .fetch_one(pool)
            .await
            .unwrap();
    let mut snapshot = json!({"schema_version":1,"goal":goal,"active_template":null,"samples":[],
        "execution_policy":ledger::budget::ReviewPolicy::freeze(goal,claimed_at).unwrap()});
    if policy_mode == "missing" {
        snapshot.as_object_mut().unwrap().remove("execution_policy");
    }
    if policy_mode == "invalid" {
        snapshot["execution_policy"]["max_dispatches"] = json!(2);
    }
    let id = format!("budget-review-{name}");
    let key = format!("{owner}:template_review:{name}");
    let digest = contract_digest(&snapshot).unwrap();
    sqlx::query("INSERT INTO campaign_dag_reviews(id,family_id,owner_campaign_id,task_key,superior_agent_id,sample_count,threshold,sample_digest,snapshot,created_at)
        VALUES($1,$2,$3,$4,'codex',$5,10,$6,$7,$8)")
        .bind(&id).bind(&family).bind(owner).bind(&key).bind(ordinal)
        .bind(&digest).bind(&snapshot).bind(claimed_at).execute(pool).await.unwrap();
    ledger::reviews::Pending {
        id,
        family_id: family,
        task_key: key,
        agent_id: "codex".into(),
        sample_digest: digest,
        snapshot,
    }
}
fn frozen_review_manifest(review: &ledger::reviews::Pending) -> Value {
    let staging = tempfile::tempdir().unwrap();
    let mut snapshot = review.snapshot.clone();
    snapshot["sample_digest"] = json!(review.sample_digest);
    std::fs::write(
        staging.path().join(ledger::template::INPUT_FILE),
        serde_json::to_vec(&snapshot).unwrap(),
    )
    .unwrap();
    artifacts::capture_outputs(
        staging.path(),
        &[Output {
            path: ledger::template::INPUT_FILE.into(),
            format: Format::Json,
        }],
        &artifact_root().unwrap(),
    )
    .unwrap()
}
async fn review_budget_card(
    pool: &PgPool,
    owner: &str,
    review: &ledger::reviews::Pending,
    real_publication: bool,
) -> String {
    if real_publication {
        let manifest = frozen_review_manifest(review);
        return store::publish_control(
            pool,
            owner,
            &store::Publication {
                key: &review.task_key,
                node: "template_review",
                role: "template_review",
                version: None,
                agent: &review.agent_id,
                title: "Bounded optional review fixture",
                prompt: "Read frozen review evidence",
                digest: &review.sample_digest,
            },
            &ledger::ControlAssignment {
                family_id: review.family_id.clone(),
                agent_id: review.agent_id.clone(),
                purpose: "template_review",
                input_manifest: Some(manifest),
            },
        )
        .await
        .unwrap();
    }
    // Historical or already-expired claims cannot be published through the
    // live admission API; insert preserved upgrade input to exercise its guard.
    let card = store::card_id(&review.task_key);
    sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES($1,'Preserved review fixture','review','codex')")
        .bind(&card).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,node_id,role,contract_digest,card_id)
        VALUES($1,$2,'template_review','template_review',$3,$4)")
        .bind(&review.task_key).bind(owner).bind(&review.sample_digest).bind(&card).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO campaign_dag_control_assignments(task_key,family_id,agent_id,purpose)
        VALUES($1,$2,'codex','template_review')",
    )
    .bind(&review.task_key)
    .bind(&review.family_id)
    .execute(pool)
    .await
    .unwrap();
    card
}
async fn review_budget_tools(pool: &PgPool, dispatch: &str, count: i64) {
    sqlx::query("INSERT INTO campaign_graph_progress(dispatch_id,event_id,fingerprint)
        SELECT $1,'event-'||n,'fixture-'||n FROM generate_series(1,$2::bigint) n ON CONFLICT DO NOTHING")
        .bind(dispatch).bind(count).execute(pool).await.unwrap();
}
async fn recurring_budget_goal(
    pool: &PgPool,
    owner: &str,
) -> (Goal, chrono::DateTime<chrono::Utc>) {
    use chrono::Timelike;
    let window = chrono::Utc::now() + chrono::Duration::minutes(2);
    let mut g = goal();
    g.schedule = Some(crate::db::campaigns::graph::schedule::Schedule {
        timezone: "UTC".into(),
        daily_minutes: [(window.hour() * 60 + window.minute()) as u16].into(),
    });
    sqlx::query("UPDATE campaign_execution_specs SET spec=$2 WHERE campaign_id=$1")
        .bind(owner)
        .bind(json!(g))
        .execute(pool)
        .await
        .unwrap();
    (g, window)
}

#[tokio::test]
async fn recurring_review_history_preserves_raw_usage_and_admits_finite_next_work_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    superior(&pool, 10).await;
    let (g, window) = recurring_budget_goal(&pool, &c.id).await;
    // More historical review tasks, time and tools than all ordinary limits.
    for n in 1..=13 {
        let r = review_budget_fixture(
            &pool,
            &c.id,
            &g,
            &format!("history-{n}"),
            n * 10,
            10000,
            "valid",
        )
        .await;
        let card = review_budget_card(&pool, &c.id, &r, false).await;
        let dispatch = format!("history-{n}");
        sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,created_at,completed_at)
            VALUES($1,$2,'codex','implementation','completed',NOW()-INTERVAL '4000 seconds',NOW()-INTERVAL '1 second')")
            .bind(&dispatch).bind(card).execute(&pool).await.unwrap();
        review_budget_tools(&pool, &dispatch, 30).await;
        ledger::reviews::fail(&pool, &r.task_key, "Retained historical review failure")
            .await
            .unwrap();
        sqlx::query("UPDATE campaign_graph_tasks SET observation='{\"outcome\":\"fixture_terminal\"}' WHERE task_key=$1")
            .bind(&r.task_key).execute(&pool).await.unwrap();
    }
    let before = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(before["tasks"].as_array().unwrap().len(), 13);
    assert_eq!(before["observed_tool_calls"], 390);
    assert!(before["elapsed_dispatch_seconds"].as_f64().unwrap() > 50000.0);
    assert_eq!(before["budget_enforcement"]["published_tasks"], 0);
    assert_eq!(before["budget_enforcement"]["observed_tool_calls"], 0);
    assert_eq!(
        before["budget_enforcement"]["scope"],
        "recurring_parent_ordinary_lifetime"
    );
    reconcile::observe(&pool, &c.id, &g, None).await.unwrap();
    assert!(store::status(&pool, &c.id).await.unwrap()["hold_reason"].is_null());
    let next = review_budget_fixture(&pool, &c.id, &g, "after-history", 140, 0, "valid").await;
    let card = review_budget_card(&pool, &c.id, &next, true).await;
    assert_eq!(next.snapshot["execution_policy"]["max_dispatches"], 1);
    assert_eq!(next.snapshot["execution_policy"]["max_elapsed_secs"], 120);
    assert_eq!(
        next.snapshot["execution_policy"]["max_observed_tool_calls"],
        20
    );
    sqlx::query(
        "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status)
        VALUES('one-review',$1,'codex','implementation','pending')",
    )
    .bind(&card)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        sqlx::query(
            "INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status)
        VALUES('replayed-review',$1,'codex','rework','pending')"
        )
        .bind(&card)
        .execute(&pool)
        .await
        .is_err()
    );
    let child = crate::db::campaigns::graph::schedule::admit(&pool, &c.id, &g, window)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store::goal(&pool, &child)
            .await
            .unwrap()
            .budget
            .max_dispatches,
        g.budget.max_dispatches
    );
    assert!(store::goal(&pool, &child).await.unwrap().schedule.is_none());
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["observed_tool_calls"],
        before["observed_tool_calls"]
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn ordinary_labels_and_nonrecurring_review_costs_still_enforce_budget_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    superior(&pool, 10).await;
    let (g, _) = recurring_budget_goal(&pool, &c.id).await;
    // A review-looking role without a durable claim/assignment stays ordinary.
    sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('label-only','Label only','review','codex')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO campaign_graph_tasks(task_key,campaign_id,node_id,role,contract_digest,card_id,observation)
        VALUES('label-only',$1,'template_review','template_review','fixture','label-only','{}')")
        .bind(&c.id).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,completed_at)
        VALUES('label-usage','label-only','codex','implementation','completed',NOW())")
        .execute(&pool).await.unwrap();
    review_budget_tools(&pool, "label-usage", 20).await;
    reconcile::observe(&pool, &c.id, &g, None).await.unwrap();
    let held = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(held["budget_enforcement"]["observed_tool_calls"], 20);
    assert!(
        held["hold_reason"]
            .as_str()
            .unwrap()
            .contains("Cumulative execution")
    );
    // Keep nonrecurring and occurrence lifetime publication semantics.
    let c2 = campaigns::create(
        &pool,
        "nonrec-budget".into(),
        serde_json::from_value(json!({"title":"Nonrec","status":"active","round":1,"nodes":[]}))
            .unwrap(),
    )
    .await
    .unwrap();
    let mut single = goal();
    single.budget.max_dispatches = 3;
    store::enroll(&pool, &c2.id, c2.revision, &single)
        .await
        .unwrap();
    for n in 1..=3 {
        let r = review_budget_fixture(
            &pool,
            &c2.id,
            &single,
            &format!("nonrec-{n}"),
            n,
            0,
            "valid",
        )
        .await;
        review_budget_card(&pool, &c2.id, &r, true).await;
        ledger::reviews::fail(&pool, &r.task_key, "Retained review fixture")
            .await
            .unwrap();
    }
    let fourth = review_budget_fixture(&pool, &c2.id, &single, "nonrec-4", 4, 0, "valid").await;
    let result = store::publish_control(
        &pool,
        &c2.id,
        &store::Publication {
            key: &fourth.task_key,
            node: "template_review",
            role: "template_review",
            version: None,
            agent: "codex",
            title: "Fourth",
            prompt: "bounded",
            digest: &fourth.sample_digest,
        },
        &ledger::ControlAssignment {
            family_id: fourth.family_id.clone(),
            agent_id: "codex".into(),
            purpose: "template_review",
            input_manifest: Some(frozen_review_manifest(&fourth)),
        },
    )
    .await;
    assert!(result.unwrap_err().to_string().contains("budget exhausted"));
    assert_eq!(
        ledger::budget::ordinary_usage(&pool, &c2.id, &single)
            .await
            .unwrap()
            .published_tasks,
        3
    );
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["hold_reason"],
        held["hold_reason"]
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn unpublished_paused_review_deadline_and_legacy_policy_fail_closed_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    superior(&pool, 10).await;
    let expired =
        review_budget_fixture(&pool, &c.id, &goal(), "unpublished", 10, 121, "valid").await;
    let old_snapshot = expired.snapshot.clone();
    let mut input: campaigns::CampaignInput = serde_json::from_value(json!(c)).unwrap();
    input.status = CampaignStatus::Paused;
    campaigns::replace(&pool, &c.id, c.revision, input)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE kv_meta SET value='{\"internalTasksEnabled\":false}' WHERE key='runtime-config'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        reconciliation_candidates(&pool)
            .await
            .unwrap()
            .contains(&c.id)
    );
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    tick(&pool, &engine, &c.id, None).await.unwrap();
    let saved: Value =
        sqlx::query_scalar("SELECT to_jsonb(r) FROM campaign_dag_reviews r WHERE task_key=$1")
            .bind(&expired.task_key)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(saved["status"], "failed");
    assert_eq!(saved["snapshot"], old_snapshot);
    assert!(
        saved["reason"]
            .as_str()
            .unwrap()
            .contains("deadline elapsed")
    );
    // Invalid and absent legacy policy never inherit the revised live budget.
    for (index, mode) in ["missing", "invalid"].into_iter().enumerate() {
        let legacy =
            review_budget_fixture(&pool, &c.id, &goal(), mode, 20 + index as i64, 0, mode).await;
        let before = legacy.snapshot.clone();
        ledger::budget::expire_pending(&pool, &c.id).await.unwrap();
        let row: Value =
            sqlx::query_scalar("SELECT to_jsonb(r) FROM campaign_dag_reviews r WHERE task_key=$1")
                .bind(&legacy.task_key)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(row["status"], "failed");
        assert_eq!(row["snapshot"], before);
        assert!(
            row["reason"]
                .as_str()
                .unwrap()
                .contains("no valid frozen finite policy")
        );
    }
    assert!(
        store::status(&pool, &c.id).await.unwrap()["tasks"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn frozen_review_first_dispatch_and_claim_identity_are_guarded_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    superior(&pool, 10).await;
    let expired =
        review_budget_fixture(&pool, &c.id, &goal(), "expired-card", 10, 121, "valid").await;
    let card = review_budget_card(&pool, &c.id, &expired, false).await;
    let original: Value =
        sqlx::query_scalar("SELECT to_jsonb(r) FROM campaign_dag_reviews r WHERE task_key=$1")
            .bind(&expired.task_key)
            .fetch_one(&pool)
            .await
            .unwrap();
    let denied=sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('late-first',$1,'codex','implementation','pending')").bind(&card).execute(&pool).await.unwrap_err();
    assert!(denied.to_string().contains("deadline has elapsed"));
    ledger::budget::expire_pending(&pool, &c.id).await.unwrap();
    assert!(sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('failed-first',$1,'codex','implementation','dispatched')").bind(&card).execute(&pool).await.unwrap_err().to_string().contains("terminal"));
    for statement in [
        "UPDATE campaign_dag_reviews SET snapshot=snapshot-'execution_policy' WHERE task_key=$1",
        "UPDATE campaign_dag_reviews SET created_at=NOW() WHERE task_key=$1",
        "UPDATE campaign_dag_reviews SET sample_count=11 WHERE task_key=$1",
        "UPDATE campaign_dag_reviews SET superior_agent_id='brain' WHERE task_key=$1",
    ] {
        assert!(
            sqlx::query(statement)
                .bind(&expired.task_key)
                .execute(&pool)
                .await
                .unwrap_err()
                .to_string()
                .contains("immutable")
        );
    }
    let saved: Value =
        sqlx::query_scalar("SELECT to_jsonb(r) FROM campaign_dag_reviews r WHERE task_key=$1")
            .bind(&expired.task_key)
            .fetch_one(&pool)
            .await
            .unwrap();
    for field in [
        "snapshot",
        "created_at",
        "sample_count",
        "superior_agent_id",
        "sample_digest",
    ] {
        assert_eq!(saved[field], original[field]);
    }
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT assigned_agent_id FROM kanban_cards WHERE id=$1")
            .bind(&card)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "codex"
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn finite_review_tool_boundary_cancels_once_and_preserves_history_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    superior(&pool, 10).await;
    let (g, _) = recurring_budget_goal(&pool, &c.id).await;
    let r = review_budget_fixture(&pool, &c.id, &g, "tool-boundary", 10, 0, "valid").await;
    let card = review_budget_card(&pool, &c.id, &r, true).await;
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status) VALUES('tool-boundary',$1,'codex','implementation','dispatched')")
        .bind(&card).execute(&pool).await.unwrap();
    sqlx::query("UPDATE auto_queue_entries SET status='dispatched',dispatch_id='tool-boundary' WHERE kanban_card_id=$1")
        .bind(&card).execute(&pool).await.unwrap();
    review_budget_tools(&pool, "tool-boundary", 19).await;
    assert!(
        ledger::budget::expire_pending(&pool, &c.id)
            .await
            .unwrap()
            .is_empty()
    );
    review_budget_tools(&pool, "tool-boundary", 20).await;
    assert!(
        learning::reconcile_review_budgets(&pool, &c.id, None)
            .await
            .unwrap()
    );
    assert!(
        learning::reconcile_review_budgets(&pool, &c.id, None)
            .await
            .unwrap()
    );
    reconcile::observe(&pool, &c.id, &g, None).await.unwrap();
    let saved: Value =
        sqlx::query_scalar("SELECT to_jsonb(r) FROM campaign_dag_reviews r WHERE task_key=$1")
            .bind(&r.task_key)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(saved["status"], "failed");
    assert_eq!(saved["snapshot"], r.snapshot);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM task_dispatches WHERE id='tool-boundary'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "cancelled"
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM dispatch_events WHERE dispatch_id='tool-boundary' AND to_status='cancelled'").fetch_one(&pool).await.unwrap(),1);
    let state = store::status(&pool, &c.id).await.unwrap();
    assert_eq!(state["observed_tool_calls"], 20);
    assert_eq!(state["budget_enforcement"]["observed_tool_calls"], 0);
    assert!(state["hold_reason"].is_null());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM auto_queue_run_cleanup_tasks")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn ontime_review_completion_observed_late_uses_canonical_end_stamp_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    superior(&pool, 10).await;
    for (index, offset, missing) in [(1, 119, false), (2, 121, false), (3, 0, true)] {
        let r = review_budget_fixture(
            &pool,
            &c.id,
            &goal(),
            &format!("end-{index}"),
            index * 10,
            130,
            "valid",
        )
        .await;
        let card = review_budget_card(&pool, &c.id, &r, false).await;
        let start: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT created_at FROM campaign_dag_reviews WHERE task_key=$1")
                .bind(&r.task_key)
                .fetch_one(&pool)
                .await
                .unwrap();
        let end = (!missing).then_some(start + chrono::Duration::seconds(offset));
        sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,created_at,completed_at) VALUES($1,$2,'codex','implementation','completed',$3,$4)")
            .bind(format!("end-{index}")).bind(card).bind(start).bind(end).execute(&pool).await.unwrap();
        review_budget_tools(&pool, &format!("end-{index}"), 20).await;
        let expired = ledger::budget::expire_pending(&pool, &c.id).await.unwrap();
        if index == 1 {
            assert!(expired.is_empty());
            ledger::reviews::accept(&pool,&r.task_key,&json!({"sample_digest":r.sample_digest,"decision":"defer","reason":"On-time explicit fixture receipt observed after restart","template":null})).await.unwrap();
        } else {
            assert_eq!(expired, vec![r.task_key.clone()]);
        }
        let status: String =
            sqlx::query_scalar("SELECT status FROM campaign_dag_reviews WHERE task_key=$1")
                .bind(&r.task_key)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, if index == 1 { "deferred" } else { "failed" });
    }
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn expired_review_never_masks_provider_unknown_or_existing_hold_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    superior(&pool, 10).await;
    let (g, _) = recurring_budget_goal(&pool, &c.id).await;
    for (index, category) in ["unknown", "infrastructure"].into_iter().enumerate() {
        let r = review_budget_fixture(
            &pool,
            &c.id,
            &g,
            &format!("unknown-{index}"),
            10 + index as i64,
            121,
            "valid",
        )
        .await;
        let card = review_budget_card(&pool, &c.id, &r, false).await;
        let result = json!({"campaign_receipt":{"outcome":"blocked","failure_category":category,"summary":"Explicit unresolved native fixture outcome"}});
        sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result) VALUES($1,$2,'codex','implementation','failed',$3)")
            .bind(format!("unknown-{index}")).bind(card).bind(result.to_string()).execute(&pool).await.unwrap();
        ledger::budget::expire_pending(&pool, &c.id).await.unwrap();
        reconcile::observe(&pool, &c.id, &g, None).await.unwrap();
        let saved: String =
            sqlx::query_scalar("SELECT category FROM campaign_graph_failures WHERE task_key=$1")
                .bind(&r.task_key)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(saved, category);
    }
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["hold_reason"],
        "Execution outcome requires reconciliation; automatic replay is disabled"
    );
    store::hold(&pool, &c.id, "Existing operator effect hold")
        .await
        .unwrap();
    learning::reconcile_review_budgets(&pool, &c.id, None)
        .await
        .unwrap();
    assert_eq!(
        store::status(&pool, &c.id).await.unwrap()["hold_reason"],
        "Existing operator effect hold"
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn failed_review_next_ten_distinct_samples_are_required_after_fail_and_defer_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    superior(&pool, 10).await;
    for n in 1..=30 {
        let child = occurrence(&pool, &c.id, &format!("next-ten-{n}"), n).await;
        complete_fixture(&pool, &child).await;
        assert!(
            ledger::samples::collect(&pool, &child.id, &goal())
                .await
                .unwrap()
        );
        assert!(
            !ledger::samples::collect(&pool, &child.id, &goal())
                .await
                .unwrap()
        );
        let claim = ledger::reviews::prepare(&pool, &c.id, &goal())
            .await
            .unwrap();
        if n % 10 != 0 {
            assert!(claim.is_none(), "sample {n} cannot replenish a batch");
            continue;
        }
        let r = claim.unwrap();
        assert_eq!(r.snapshot["sample_count"], n);
        assert_eq!(r.snapshot["previous_sample_count"], n - 10);
        assert_eq!(r.snapshot["samples"].as_array().unwrap().len(), 10);
        if n == 10 {
            ledger::reviews::fail(&pool, &r.task_key, "Explicit failed reviewer fixture")
                .await
                .unwrap();
        } else {
            ledger::reviews::accept(&pool,&r.task_key,&json!({"sample_digest":r.sample_digest,"decision":"defer","reason":"Wait exactly one next distinct sample batch","template":null})).await.unwrap();
        }
        assert!(
            ledger::reviews::prepare(&pool, &c.id, &goal())
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(
        ledger::inspection(&pool, &c.id).await.unwrap()["sample_count"],
        30
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_dag_reviews")
            .fetch_one(&pool)
            .await
            .unwrap(),
        3
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn failed_review_active_owners_fence_but_idle_terminal_history_does_not_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let c = setup(&pool).await;
    superior(&pool, 10).await;
    let r = review_budget_fixture(&pool, &c.id, &goal(), "idle-history", 10, 0, "valid").await;
    let card = review_budget_card(&pool, &c.id, &r, false).await;
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,thread_id,completed_at) VALUES('idle-history',$1,'codex','implementation','completed','989801',NOW())")
        .bind(&card).execute(&pool).await.unwrap();
    sqlx::query("UPDATE kanban_cards SET active_thread_id='989801',channel_thread_map='{\"codex\":\"989801\"}' WHERE id=$1")
        .bind(&card).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO sessions(session_key,provider,status,cwd,thread_channel_id) VALUES('idle-review','codex','idle',$1,'989801')")
        .bind(format!("/isolated/card-{card}")).execute(&pool).await.unwrap();
    ledger::reviews::fail(&pool, &r.task_key, "Invalid explicit fixture receipt")
        .await
        .unwrap();
    assert!(!ledger::budget::unsettled(&pool, &r.task_key).await.unwrap());
    assert!(
        learning::reconcile_review_budgets(&pool, &c.id, None)
            .await
            .unwrap()
    );
    sqlx::query("UPDATE sessions SET status='awaiting_user' WHERE session_key='idle-review'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(ledger::budget::unsettled(&pool, &r.task_key).await.unwrap());
    assert!(
        !learning::reconcile_review_budgets(&pool, &c.id, None)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM sessions WHERE session_key='idle-review'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "awaiting_user"
    );
    sqlx::query("UPDATE sessions SET status='idle' WHERE session_key='idle-review'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        learning::reconcile_review_budgets(&pool, &c.id, None)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM task_dispatches WHERE id='idle-history'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "completed"
    );
    pool.close().await;
    fixture.drop().await;
}
