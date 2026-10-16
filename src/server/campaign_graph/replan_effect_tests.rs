use super::*;
use crate::db::campaigns::graph::recovery::replan::{self, Replan, UnattemptedEffect};

async fn held_external(pool: &PgPool) -> (campaigns::Campaign, Graph, String, Replan) {
    let c = setup(pool).await;
    let mut original = graph();
    original.nodes[0].effect = Effect::External;
    let version = store::stage(pool, &c.id, &original, None).await.unwrap();
    sqlx::query("UPDATE campaign_graph_versions SET status='validated'")
        .execute(pool)
        .await
        .unwrap();
    store::activate(pool, &c.id, version, c.revision)
        .await
        .unwrap();
    let n = &original.nodes[0];
    let card = store::publish(
        pool,
        &c.id,
        &store::Publication {
            key: &n.task_key,
            node: &n.id,
            role: "work",
            version: Some(version),
            agent: &n.agent,
            title: &n.title,
            prompt: &n.details,
            digest: &store::task_digest(n).unwrap(),
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&card)
        .execute(pool)
        .await
        .unwrap();
    let raw = json!({
        "summary": crate::db::campaigns::graph::result::CAPTURE_SUMMARY,
        "work_outcome": "artifact", "completion_source": "campaign_native_receipt",
        "campaign_receipt": {"outcome":"blocked", "failure_category":"plan",
            "summary":"Guard rejected the source/capture contract before any effect was attempted",
            "observations":[{"command":"evaluate exact action guard", "exit_code":0,
                "evidence":"Guard denied; command/effect audit and remote state retained"}]}
    })
    .to_string();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,created_at,completed_at)
        VALUES('unattempted-dispatch',$1,'qwen','implementation','completed',$2,NOW()-INTERVAL '8 seconds',NOW())")
        .bind(&card).bind(&raw).execute(pool).await.unwrap();
    progress::tool_use(
        Some(pool),
        Some("unattempted-dispatch"),
        Some("guard-tool"),
        "input-hash",
    )
    .await;
    reconcile::observe(pool, &c.id, &goal(), None)
        .await
        .unwrap();
    super::replan_tests::settle(pool).await;
    let current = campaigns::get(pool, &c.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let c = campaigns::replace(pool, &c.id, current.revision, input)
        .await
        .unwrap();
    let state = canonical_inspection(store::status(pool, &c.id).await.unwrap());
    let failure = &state["failures"][0];
    assert_eq!(failure["category"], "unknown");
    assert_eq!(
        failure["evidence"]["external_effect_requires_reconciliation"],
        true
    );
    let request = Replan {cancelled_validation:None,
        rejected_validation: None,
        expected_revision:c.revision, request_id:"audited-unattempted-effect-repair".into(),
        failure_id:failure["id"].as_str().unwrap().into(),
        evidence:"Operator audited every retained command, exact driver source and current remote state: the action was never attempted. Preserve the blocked dispatch and require fresh planner inspection and independent QA before any effect.".into(),
        budget:None, supersede_candidate:None,
        settled_effect:None,
        unattempted_effect:Some(UnattemptedEffect {dispatch_id:"unattempted-dispatch".into(),
            result_sha256:crate::db::campaigns::graph::digest(raw.as_bytes()),
            audit_sha256:crate::db::campaigns::graph::digest(b"Retained independently reviewable operator command/effect audit")}),
    };
    (c, original, raw, request)
}

#[tokio::test]
async fn audited_unattempted_effect_replan_preserves_unknown_history_and_requires_new_planner_qa_pg()
 {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, original, raw, mut request) = held_external(&pool).await;
    let before = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    let audit = request.unattempted_effect.take().unwrap();
    assert!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("external effects")
    );
    request.unattempted_effect = Some(audit.clone());
    let reply = replan::request(&pool, &c.id, &request).await.unwrap();
    assert_eq!(reply.planner_agent_id, "qwen");
    assert!(reply.independent_validation_required && reply.goal_scope_preserved);
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
    assert_eq!(after["hold_reason"], Value::Null);
    assert!(
        after["failures"]
            .as_array()
            .unwrap()
            .contains(&before["failures"][0])
    );
    let repair = after["failures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["id"] == reply.repair_failure_id)
        .unwrap();
    assert_eq!(repair["evidence"]["unattempted_effect"], json!(audit));
    assert_eq!(
        repair["evidence"]["original_failure"],
        before["failures"][0]["evidence"]
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT result FROM task_dispatches WHERE id='unattempted-dispatch'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        raw
    );
    assert_eq!(
        replan::request(&pool, &c.id, &request)
            .await
            .unwrap()
            .revision,
        reply.revision
    );
    request.unattempted_effect.as_mut().unwrap().audit_sha256 = "a".repeat(64);
    assert!(replan::request(&pool, &c.id, &request).await.is_err());

    let paused = campaigns::get(&pool, &c.id).await.unwrap();
    assert_eq!(paused.status, CampaignStatus::Paused);
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&paused).unwrap()).unwrap();
    input.status = CampaignStatus::Active;
    campaigns::replace(&pool, &c.id, paused.revision, input)
        .await
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.policies.dir = root.path().join("empty-policies");
    std::fs::create_dir(&config.policies.dir).unwrap();
    let engine = PolicyEngine::new_with_pg(&config, Some(pool.clone())).unwrap();
    let _ = tick(&pool, &engine, &c.id, None).await;
    let planner_key = format!("{}:plan:{}", c.id, reply.repair_failure_id);
    let planner = store::card_id(&planner_key);
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT assigned_agent_id FROM kanban_cards WHERE id=$1"
        )
        .bind(&planner)
        .fetch_one(&pool)
        .await
        .unwrap()
        .as_deref(),
        Some("qwen")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_tasks WHERE role='work'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    super::replan_tests::settle(&pool).await;
    let mut corrected = original;
    corrected.nodes[0]
        .task_key
        .push_str(":new-audited-contract");
    corrected.nodes[0]
        .details
        .push_str(" Reconcile exact prior non-attempt before freshly inspecting any action.");
    sqlx::query("UPDATE kanban_cards SET status='in_progress' WHERE id=$1")
        .bind(&planner)
        .execute(&pool)
        .await
        .unwrap();
    let proposal = json!({"summary":crate::db::campaigns::graph::result::CAPTURE_SUMMARY,
        "work_outcome":"artifact", "completion_source":"campaign_native_receipt",
        "campaign_receipt":{"outcome":"passed", "summary":"New source-backed plan",
        "observations":[{"command":"inspect audited failure", "exit_code":0, "evidence":"Prior non-attempt retained"}]},
        "campaign_graph":corrected});
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,result,completed_at)
        VALUES('audited-repair-plan',$1,'qwen','implementation','completed',$2,NOW())")
        .bind(&planner).bind(proposal.to_string()).execute(&pool).await.unwrap();
    let _control_workspace = prepare_control_fixture(&pool, "audited-repair-plan").await;
    reconcile::observe(&pool, &c.id, &goal(), None)
        .await
        .unwrap();
    let proposed = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    assert_eq!(proposed["versions"][0]["status"], "candidate", "{proposed}");
    assert_eq!(proposed["active_version"], 1);
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
        "Independent QA",
    )
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT assigned_agent_id FROM kanban_cards WHERE id=$1"
        )
        .bind(store::card_id(&format!("{}:validate:2", c.id)))
        .fetch_one(&pool)
        .await
        .unwrap()
        .as_deref(),
        Some("monitoring")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_tasks WHERE role='work'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    pool.close().await;
    fixture.drop().await;
}

#[tokio::test]
async fn unattempted_effect_audit_rejects_stale_receipts_missing_provenance_unknown_attempts_and_descendants_pg()
 {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    let (c, graph, raw, mut request) = held_external(&pool).await;
    let before = canonical_inspection(store::status(&pool, &c.id).await.unwrap());
    let valid = request.unattempted_effect.as_ref().unwrap().clone();
    for bad in [
        UnattemptedEffect {
            dispatch_id: "different-dispatch".into(),
            ..valid.clone()
        },
        UnattemptedEffect {
            result_sha256: "0".repeat(64),
            ..valid.clone()
        },
        UnattemptedEffect {
            audit_sha256: "unverified".into(),
            ..valid.clone()
        },
    ] {
        request.unattempted_effect = Some(bad);
        assert!(replan::request(&pool, &c.id, &request).await.is_err());
        assert_eq!(
            canonical_inspection(store::status(&pool, &c.id).await.unwrap()),
            before
        );
    }
    request.unattempted_effect = Some(valid.clone());
    for change in [
        "missing-native-provenance",
        "passed-receipt",
        "unknown-receipt",
        "malformed-envelope",
    ] {
        let mut value: Value = serde_json::from_str(&raw).unwrap();
        match change {
            "missing-native-provenance" => {
                value.as_object_mut().unwrap().remove("completion_source");
            }
            "passed-receipt" => {
                value["campaign_receipt"]["outcome"] = json!("passed");
            }
            "unknown-receipt" => {
                value["campaign_receipt"]["failure_category"] = json!("unknown");
            }
            _ => {
                value["forged_effect_approval"] = json!(true);
            }
        }
        let changed = value.to_string();
        sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='unattempted-dispatch'")
            .bind(&changed)
            .execute(&pool)
            .await
            .unwrap();
        request.unattempted_effect.as_mut().unwrap().result_sha256 =
            crate::db::campaigns::graph::digest(changed.as_bytes());
        assert!(
            replan::request(&pool, &c.id, &request).await.is_err(),
            "{change}"
        );
        assert_eq!(
            canonical_inspection(store::status(&pool, &c.id).await.unwrap()),
            before
        );
    }
    sqlx::query("UPDATE task_dispatches SET result=$1 WHERE id='unattempted-dispatch'")
        .bind(&raw)
        .execute(&pool)
        .await
        .unwrap();
    request.unattempted_effect = Some(valid);
    sqlx::query("UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{failure,category}','\"unknown\"') WHERE id=$1")
        .bind(&request.failure_id).execute(&pool).await.unwrap();
    assert!(replan::request(&pool, &c.id, &request).await.is_err());
    sqlx::query("UPDATE campaign_graph_failures SET evidence=jsonb_set(evidence,'{failure,category}','\"plan\"') WHERE id=$1")
        .bind(&request.failure_id).execute(&pool).await.unwrap();
    // Build the adversarial state of a descendant already published before
    // reconciliation; restore the original hold before exercising admission.
    sqlx::query("UPDATE campaign_execution_specs SET hold_reason=NULL WHERE campaign_id=$1")
        .bind(&c.id)
        .execute(&pool)
        .await
        .unwrap();
    let paused = campaigns::get(&pool, &c.id).await.unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&paused).unwrap()).unwrap();
    input.status = CampaignStatus::Active;
    let active = campaigns::replace(&pool, &c.id, paused.revision, input)
        .await
        .unwrap();
    let mut input: campaigns::CampaignInput =
        serde_json::from_value(serde_json::to_value(&active).unwrap()).unwrap();
    input.status = CampaignStatus::Paused;
    let paused_input = input;
    let child = &graph.nodes[1];
    store::publish(
        &pool,
        &c.id,
        &store::Publication {
            key: &child.task_key,
            node: &child.id,
            role: "work",
            version: Some(1),
            agent: &child.agent,
            title: &child.title,
            prompt: &child.details,
            digest: &store::task_digest(child).unwrap(),
        },
    )
    .await
    .unwrap();
    super::replan_tests::settle(&pool).await;
    store::hold(&pool, &c.id, before["hold_reason"].as_str().unwrap())
        .await
        .unwrap();
    let paused = campaigns::replace(&pool, &c.id, active.revision, paused_input)
        .await
        .unwrap();
    request.expected_revision = paused.revision;
    let rejection = replan::request(&pool, &c.id, &request)
        .await
        .unwrap_err()
        .to_string();
    assert!(rejection.contains("already published"), "{rejection}");
    assert!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM campaign_graph_reconciliations")
            .fetch_one(&pool)
            .await
            .unwrap()
            == 0
    );
    assert_eq!(
        canonical_inspection(store::status(&pool, &c.id).await.unwrap())["hold_reason"],
        before["hold_reason"]
    );
    pool.close().await;
    fixture.drop().await;
}
