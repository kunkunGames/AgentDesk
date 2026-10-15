use super::*;
use anyhow::ensure;

/// Reconcile both fresh and pre-upgrade blocked validator receipts. A terminal
/// plan rejection must not leave a candidate occupying the next-version gate.
pub(super) async fn reject_failed_candidates(pool: &PgPool, id: &str) -> Result<()> {
    let templates:Vec<Option<String>>=sqlx::query_scalar("UPDATE campaign_graph_versions v SET status='rejected'
        WHERE v.campaign_id=$1 AND v.status='candidate' AND EXISTS(
            SELECT 1 FROM campaign_graph_tasks t JOIN campaign_graph_failures f ON f.task_key=t.task_key
            WHERE t.campaign_id=v.campaign_id AND t.version=v.version AND t.role='validate'
              AND NOT t.accepted AND f.category='plan') RETURNING template_id")
        .bind(id).fetch_all(pool).await?;
    for template in templates.into_iter().flatten() {
        crate::db::campaigns::graph::learning::reviews::block(
            pool,
            &template,
            id,
            "Independent graph validation rejected the instantiated template",
        )
        .await?;
    }
    Ok(())
}

/// Read the canonical validator reason without rewriting captured failure
/// history. Older failure envelopes retained only a generic receipt summary.
pub(super) async fn planning_evidence(pool: &PgPool, failure: &Value) -> Result<Value> {
    let mut evidence = failure["evidence"].clone();
    // A delayed control failure may be newer than the source correction that
    // retired its candidate. Supply that audited input without rewriting either
    // historical failure or claiming the correction is business approval.
    let correction: Option<Value> = sqlx::query_scalar(
        "SELECT repair.evidence FROM campaign_graph_failures repair
         JOIN campaign_graph_failures captured ON captured.campaign_id=repair.campaign_id
         WHERE captured.id=$1 AND repair.evidence ? 'operator_replan'
         ORDER BY repair.created_at DESC,repair.id DESC LIMIT 1",
    )
    .bind(failure["id"].as_str())
    .fetch_optional(pool)
    .await?;
    if let Some(correction) = correction {
        evidence["operator_source_correction"] = correction;
    }
    let raw: Option<String> = sqlx::query_scalar(
        "SELECT d.result::text
        FROM task_dispatches d JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id
        WHERE d.id=$1 AND t.task_key=$2 AND t.role='validate'",
    )
    .bind(evidence["dispatch_id"].as_str())
    .bind(failure["task_key"].as_str())
    .fetch_optional(pool)
    .await?
    .flatten();
    let result = decode(raw);
    if let Some(reason) = result["graph_validation"]["reason"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
    {
        evidence["validation_reason"] = Value::String(observation::redact(reason));
    }
    Ok(evidence)
}

pub(super) async fn cleanup_if_held(
    pool: &PgPool,
    id: &str,
    health_registry: Option<std::sync::Arc<crate::services::discord::health::HealthRegistry>>,
) -> Result<bool> {
    let held = sqlx::query_scalar::<_, bool>(
        "SELECT hold_reason IS NOT NULL FROM campaign_execution_specs WHERE campaign_id=$1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    .unwrap_or(false);
    if !held {
        return Ok(false);
    }
    let runs = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT e.run_id FROM campaign_graph_tasks t
         JOIN auto_queue_entries e ON e.kanban_card_id=t.card_id
         JOIN auto_queue_runs r ON r.id=e.run_id
         WHERE t.campaign_id=$1 AND r.status IN ('generated','active','paused')
         AND NOT EXISTS(SELECT 1 FROM auto_queue_entries other
             LEFT JOIN campaign_graph_tasks owner ON owner.card_id=other.kanban_card_id
             WHERE other.run_id=r.id AND owner.campaign_id IS DISTINCT FROM $1)",
    )
    .bind(id)
    .fetch_all(pool)
    .await?;
    for run in runs {
        // Preserve failure/hold evidence. The canonical lifecycle owns process
        // cancellation, the durable cleanup job and eventual slot release.
        crate::services::auto_queue::cancel_run::end_run_with_pg(
            health_registry.clone(),
            pool,
            &run,
        )
        .await
        .map_err(anyhow::Error::msg)?;
    }
    Ok(true)
}

async fn fail(
    pool: &PgPool,
    id: &str,
    key: &str,
    dispatch: &str,
    category: &str,
    evidence: Value,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    // Admission/publication take this owner first. Failure, observation and its
    // safety hold become visible in one commit, including after a restart.
    sqlx::query("SELECT id FROM campaigns WHERE id=$1 FOR UPDATE")
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query("SELECT campaign_id FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE")
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    let external:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN campaign_graph_versions v ON v.campaign_id=t.campaign_id AND v.version=t.version,jsonb_array_elements(v.graph->'nodes') n WHERE t.task_key=$1 AND t.role='work' AND n->>'task_key'=t.task_key AND COALESCE(n->>'effect','external')<>'artifact_only')")
        .bind(key).fetch_one(&mut *tx).await?;
    // A provider's error label does not prove that an external action had no effect.
    let category = if external && matches!(category, "code" | "plan") {
        "unknown"
    } else {
        category
    };
    let fingerprint = evidence["fingerprint"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or(contract_digest(&evidence)?);
    let fid = crate::db::campaigns::graph::digest(format!("{id}:{key}:{fingerprint}").as_bytes());
    let runtime:Value=sqlx::query_scalar("SELECT jsonb_build_object('role',t.role,'graph_version',t.version,'contract_digest',t.contract_digest,'artifact_manifest',t.artifact_manifest,'provider',a.provider,'agent_id',d.to_agent_id,'dispatch_status',d.status,'observed_tool_calls',(SELECT COUNT(*) FROM campaign_graph_progress p WHERE p.dispatch_id=d.id)) FROM campaign_graph_tasks t LEFT JOIN task_dispatches d ON d.id=$2 LEFT JOIN agents a ON a.id=d.to_agent_id WHERE t.task_key=$1")
        .bind(key).bind(dispatch).fetch_one(&mut *tx).await?;
    let is_review: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t WHERE t.task_key=$1 AND ({}))",
        crate::db::campaigns::graph::learning::budget::REVIEW_TASK_SQL
    ))
    .bind(key)
    .fetch_one(&mut *tx)
    .await?;
    let evidence = json!({"dispatch_id":dispatch,"dispatch_reference":format!("/api/dispatches/{dispatch}"),"task_key":key,"runtime":runtime,"external_effect_requires_reconciliation":external,"failure":evidence});
    sqlx::query("INSERT INTO campaign_graph_failures(id,campaign_id,task_key,fingerprint,category,evidence) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(task_key,fingerprint) DO NOTHING")
        .bind(fid).bind(id).bind(key).bind(&fingerprint).bind(category).bind(&evidence).execute(&mut *tx).await?;
    sqlx::query(
        "UPDATE campaign_graph_tasks SET observation=$2 WHERE task_key=$1 AND observation IS NULL",
    )
    .bind(key)
    .bind(evidence)
    .execute(&mut *tx)
    .await?;
    let repeats: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM campaign_graph_failures WHERE campaign_id=$1 AND fingerprint=$2",
    )
    .bind(id)
    .bind(fingerprint)
    .fetch_one(&mut *tx)
    .await?;
    let hold = if matches!(category, "unknown" | "infrastructure") {
        Some("Execution outcome requires reconciliation; automatic replay is disabled")
    } else if !is_review && repeats > 1 {
        Some("The same failure recurred after evolution; reconcile cause before any new attempt")
    } else {
        None
    };
    if let Some(reason) = hold {
        sqlx::query("UPDATE campaign_execution_specs SET hold_reason=$2 WHERE campaign_id=$1
            AND (hold_reason IS NULL OR hold_reason IN ('Plan evolution budget exhausted',
              'Cumulative execution time or observed tool budget exhausted; retained across restart and graph revisions'))")
            .bind(id).bind(reason).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}
pub(super) async fn observe(
    pool: &PgPool,
    id: &str,
    g: &Goal,
    health_registry: Option<std::sync::Arc<crate::services::discord::health::HealthRegistry>>,
) -> Result<()> {
    let usage = crate::db::campaigns::graph::learning::budget::ordinary_usage(pool, id, g).await?;
    let elapsed = usage.elapsed_dispatch_seconds;
    let observed_tools = usage.observed_tool_calls;
    if elapsed > f64::from(g.budget.max_elapsed_secs)
        || g.budget
            .max_observed_tool_calls
            .is_some_and(|limit| observed_tools >= i64::from(limit))
    {
        store::hold_budget(pool, id).await?;
        let runs:Vec<String>=sqlx::query_scalar("SELECT DISTINCT e.run_id FROM auto_queue_entries e JOIN campaign_graph_tasks t ON t.card_id=e.kanban_card_id JOIN auto_queue_runs r ON r.id=e.run_id WHERE t.campaign_id=$1 AND r.status IN ('active','generated','paused')").bind(id).fetch_all(pool).await?;
        for run in runs {
            crate::services::auto_queue::cancel_run::end_run_with_pg(
                health_registry.clone(),
                pool,
                &run,
            )
            .await
            .map_err(anyhow::Error::msg)?;
        }
    }
    let tasks=sqlx::query("SELECT t.*,k.status AS card_status FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id WHERE t.campaign_id=$1 AND t.observation IS NULL ORDER BY t.created_at LIMIT 64").bind(id).fetch_all(pool).await?;
    for task in tasks {
        let card: String = task.get("card_id");
        let key: String = task.get("task_key");
        let role: String = task.get("role");
        let dispatch=sqlx::query("SELECT id,status,result::text AS result,context::text AS context,EXTRACT(EPOCH FROM (NOW()-created_at))::float8 AS age FROM task_dispatches WHERE kanban_card_id=$1 AND dispatch_type IN ('implementation','rework') ORDER BY created_at DESC LIMIT 1").bind(&card).fetch_optional(pool).await?;
        let Some(d) = dispatch else {
            continue;
        };
        let did: String = d.get("id");
        let status: String = d.get("status");
        let result = decode(d.get("result"));
        if let Some(reason) =
            crate::db::campaigns::graph::learning::budget::known_policy_failure(pool, &key).await?
        {
            if matches!(status.as_str(), "pending" | "dispatched" | "running") {
                continue;
            }
            // A deadline does not explain a pre-existing provider failure.
            // Only a canonical policy cancellation, or an otherwise valid
            // passed receipt outside its policy, supports known failure.
            let controller_cancel: bool = status == "cancelled"
                && result["reason"] == "campaign_review_policy_failed"
                && sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM dispatch_events WHERE dispatch_id=$1
                    AND transition_source='cancel_dispatch' AND to_status='cancelled'
                    AND payload_json->>'reason'='campaign_review_policy_failed')",
                )
                .bind(&did)
                .fetch_one(pool)
                .await?;
            let passed_receipt = status == "completed"
                && observation::receipt(&result)
                && crate::db::campaigns::graph::result::validate_dispatch(&result).is_ok()
                && result["campaign_receipt"]["failure_category"].is_null();
            if controller_cancel || passed_receipt {
                fail(pool,id,&key,&did,"plan",json!({"reason":reason,"policy_failure":true,"terminal_dispatch_status":status})).await?;
                continue;
            }
        }
        if matches!(status.as_str(), "pending" | "dispatched" | "running") {
            let samples:Vec<String>=sqlx::query_scalar("SELECT fingerprint||':'||output_digest FROM campaign_graph_progress WHERE dispatch_id=$1 AND output_digest IS NOT NULL ORDER BY recorded_at DESC LIMIT 8").bind(&did).fetch_all(pool).await?;
            let stalled = observation::repeated(&samples, 8);
            let captured_review: bool = sqlx::query_scalar(&format!(
                "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t WHERE t.task_key=$1 AND ({}))",
                crate::db::campaigns::graph::learning::budget::REVIEW_TASK_SQL
            ))
            .bind(&key)
            .fetch_one(pool)
            .await?;
            if (!captured_review && d.get::<f64, _>("age") > f64::from(g.budget.node_timeout_secs))
                || stalled
            {
                let runs:Vec<String>=sqlx::query_scalar("SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=$1 AND status IN ('pending','dispatched')").bind(&card).fetch_all(pool).await?;
                if captured_review {
                    crate::db::campaigns::graph::learning::reviews::fail(pool,&key,"Review execution stalled; actual execution outcome requires reconciliation").await?;
                    crate::services::auto_queue::cancel_run::cancel_exclusive_card_runs_with_pg(
                        health_registry.clone(),
                        pool,
                        &card,
                        &runs,
                        "campaign_review_stalled",
                    )
                    .await
                    .map_err(anyhow::Error::msg)?;
                } else {
                    for run in runs {
                        crate::services::auto_queue::cancel_run::end_run_with_pg(
                            health_registry.clone(),
                            pool,
                            &run,
                        )
                        .await
                        .map_err(anyhow::Error::msg)?;
                    }
                }
                fail(pool,id,&key,&did,"unknown",json!({"reason":if stalled{"repeated identical tool arguments and results; cancellation requested, effects require reconciliation"}else{"node deadline exceeded; cancellation requested, effects require reconciliation"},"graph_version":task.get::<Option<i64>,_>("version")})).await?;
            }
            continue;
        }
        if status != "completed" || !observation::receipt(&result) {
            let (category, evidence) = observation::failure(&status, &result);
            if role == "template_review" {
                crate::db::campaigns::graph::learning::reviews::fail(
                    pool,
                    &key,
                    &evidence.to_string(),
                )
                .await?;
            }
            fail(pool, id, &key, &did, category, evidence).await?;
            continue;
        }
        // Receipt and artifact validation provide completion authority. Waiting
        // for generic PR review here created a circular dependency on drift recovery.
        // Preserve operator cancellation or other states rather than reviving them.
        if !matches!(
            task.get::<String, _>("card_status").as_str(),
            "in_progress" | "review" | "done"
        ) {
            continue;
        }
        let version: Option<i64> = task.get("version");
        if role == "validate"
            && crate::db::campaigns::graph::result::validate_dispatch(&result).is_ok()
            && let Some(version) = version
            && super::planning::observe_retired_validation(
                pool,
                id,
                &key,
                &did,
                version,
                &result["graph_validation"],
            )
            .await?
        {
            continue;
        }
        let accepted:Result<Value>=async {
            crate::db::campaigns::graph::result::validate_dispatch(&result)?;
            super::manager::validate_role(&role,&result)?;
            ensure!(role=="template_review" || result["dag_template_review"].is_null(),"unexpected template decision for task role");
            match role.as_str() {
                "plan"=>{
                    crate::services::campaign_control_input::verify_planning_input(pool,&key,&decode(d.get("context"))).await?;
                    let graph:Graph=serde_json::from_value(result["campaign_graph"].clone())?;
                    let failure=if version.is_some(){key.rsplit(':').next()}else{None};
                    let staged=store::stage(pool,id,&graph,failure).await?;
                    let planner:Option<String>=sqlx::query_scalar("SELECT assigned_agent_id FROM kanban_cards WHERE id=$1")
                        .bind(&card).fetch_one(pool).await?;
                    sqlx::query("UPDATE campaign_graph_versions SET planner_agent_id=$3 WHERE campaign_id=$1 AND version=$2 AND planner_agent_id IS NULL")
                        .bind(id).bind(staged).bind(planner).execute(pool).await?;
                    Ok(Value::Null)
                },
                "validate"=>{
                    let version=version.ok_or_else(||anyhow::anyhow!("missing candidate version"))?;
                    let validation=&result["graph_validation"];
                    ensure!(validation["approved"]==true && validation["reason"].as_str().is_some_and(|s|!s.trim().is_empty()),"independent graph validation rejected");
                    let changed=sqlx::query("UPDATE campaign_graph_versions SET status='validated' WHERE campaign_id=$1 AND version=$2 AND status='candidate' AND digest=$3")
                        .bind(id).bind(version).bind(validation["digest"].as_str().unwrap_or("")).execute(pool).await?.rows_affected();
                    ensure!(changed==1 || sqlx::query_scalar::<_,bool>("SELECT status IN ('validated','active','superseded') FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2 AND digest=$3").bind(id).bind(version).bind(validation["digest"].as_str().unwrap_or("")).fetch_optional(pool).await?.unwrap_or(false),"candidate digest or validation state differs");
                    Ok(Value::Null)
                },
                "work"=>{
                    let graph:Value=sqlx::query_scalar("SELECT graph FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2").bind(id).bind(version).fetch_one(pool).await?;
                    let graph:Graph=serde_json::from_value(graph)?;
                    let node=graph.nodes.iter().find(|n|n.task_key==key).cloned().ok_or_else(||anyhow::anyhow!("task absent from pinned graph"))?;
                    let context=decode(d.get("context"));
                    let workspace=context["worktree_path"].as_str().ok_or_else(||anyhow::anyhow!("canonical dispatch workspace missing"))?.to_owned();
                    let mut manifests=BTreeMap::new();
                    for input in &node.inputs {
                        let producer=graph.nodes.iter().find(|n| n.id==input.dependency).ok_or_else(|| anyhow::anyhow!("missing producer"))?;
                        let manifest:Value=sqlx::query_scalar("SELECT artifact_manifest FROM campaign_graph_tasks WHERE task_key=$1 AND accepted").bind(&producer.task_key).fetch_one(pool).await?;
                        manifests.insert(input.dependency.clone(),manifest);
                    }
                    let root=artifact_root()?;
                    tokio::task::spawn_blocking(move|| {
                        let workspace=std::path::Path::new(&workspace);
                        artifacts::verify_inputs(&root,workspace,&node,&manifests)?;
                        artifacts::capture(workspace,&node,&root)
                    }).await?
                },
                "manage"|"finish"=>{
                    crate::services::campaign_control_input::verify_planning_input(pool,&key,&decode(d.get("context"))).await?;
                    crate::db::campaigns::graph::workflow::store::accept(pool,id,&key,&did,&result).await?;
                    Ok(Value::Null)
                },
                "template_review"=>{
                    learning::accept_review(pool,&key,&result,decode(d.get("context"))).await?;
                    Ok(Value::Null)
                },
                _=>anyhow::bail!("unknown task role"),
            }
        }.await;
        match accepted {
            Ok(manifest) => {
                sqlx::query("UPDATE campaign_graph_tasks SET accepted=TRUE,observation=$2,artifact_manifest=$3 WHERE task_key=$1 AND observation IS NULL")
                    .bind(&key).bind(json!({"dispatch_id":did,"outcome":"accepted","graph_version":version})).bind(manifest).execute(pool).await?;
            }
            Err(error) => {
                if role == "template_review" {
                    crate::db::campaigns::graph::learning::reviews::fail(
                        pool,
                        &key,
                        &error.to_string(),
                    )
                    .await?;
                }
                fail(pool,id,&key,&did,if role=="work"{"code"}else{"plan"},json!({"error":observation::redact(&error.to_string()),"graph_version":version,"fingerprint":contract_digest(&(role.as_str(),observation::redact(&error.to_string())))?})).await?;
                if role == "validate" {
                    sqlx::query("UPDATE campaign_graph_versions SET status='rejected' WHERE campaign_id=$1 AND version=$2 AND status='candidate'").bind(id).bind(version).execute(pool).await?;
                    let template:Option<String>=sqlx::query_scalar("SELECT template_id FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2")
                        .bind(id).bind(version).fetch_optional(pool).await?.flatten();
                    if let Some(template) = template {
                        crate::db::campaigns::graph::learning::reviews::block(
                            pool,
                            &template,
                            id,
                            &error.to_string(),
                        )
                        .await?;
                    }
                }
            }
        }
    }
    // Project observed state back onto the existing Campaign/Kanban view.
    let c = campaigns::get(pool, id).await?;
    if c.status != CampaignStatus::Active {
        return Ok(());
    }
    let states=sqlx::query("SELECT t.node_id,t.card_id,t.accepted,t.observation,EXISTS(SELECT 1 FROM task_dispatches d WHERE d.kanban_card_id=t.card_id AND d.status IN ('dispatched','running')) AS running FROM campaign_graph_tasks t JOIN campaign_execution_specs s ON s.campaign_id=t.campaign_id JOIN campaign_graph_versions v ON v.campaign_id=s.campaign_id AND v.version=s.active_version WHERE t.campaign_id=$1 AND t.role='work' AND EXISTS(SELECT 1 FROM jsonb_array_elements(v.graph->'nodes') n WHERE n->>'task_key'=t.task_key) ORDER BY t.created_at").bind(id).fetch_all(pool).await?;
    let mut input: campaigns::CampaignInput = serde_json::from_value(serde_json::to_value(&c)?)?;
    let mut changed = false;
    for node in &mut input.nodes {
        if let Some(row) = states
            .iter()
            .rev()
            .find(|r| r.get::<String, _>("node_id") == node.id)
        {
            let card: String = row.get("card_id");
            let status = if row.get::<bool, _>("accepted") {
                campaigns::NodeStatus::Completed
            } else if row.get::<Option<Value>, _>("observation").is_some() {
                campaigns::NodeStatus::Blocked
            } else if row.get::<bool, _>("running") {
                campaigns::NodeStatus::Running
            } else {
                campaigns::NodeStatus::Pending
            };
            if node.status != status || node.session_id.as_deref() != Some(&card) {
                node.status = status;
                node.session_id = Some(card);
                changed = true;
            }
        }
    }
    if changed {
        campaigns::replace(pool, id, c.revision, input).await?;
    }
    Ok(())
}

/// Settle accepted tasks through the existing FSM, including its audit, hooks,
/// queue synchronization and cleanup. Replaying a tick is harmless after done.
pub(super) async fn settle_accepted(pool: &PgPool, engine: &PolicyEngine, id: &str) -> Result<()> {
    let cards:Vec<String>=sqlx::query_scalar("SELECT t.card_id FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id WHERE t.campaign_id=$1 AND t.accepted AND k.status<>'done' ORDER BY t.created_at")
        .bind(id).fetch_all(pool).await?;
    for card in cards {
        crate::kanban::transition_status_with_opts_pg_only(
            pool,
            engine,
            &card,
            "done",
            "campaign_graph_completion",
            crate::engine::transition::ForceIntent::None,
        )
        .await?;
    }
    Ok(())
}
