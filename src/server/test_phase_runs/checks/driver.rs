use super::super::{TestPhaseRunCompleteRequest, TestPhaseRunStartRequest, lifecycle};
use super::{capture, contract, process, snapshot, store};
use crate::engine::PolicyEngine;
use serde_json::{Value, json};
use sqlx::PgPool;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

static IN_FLIGHT: AtomicBool = AtomicBool::new(false);
struct Flight;
impl Drop for Flight {
    fn drop(&mut self) {
        IN_FLIGHT.store(false, Ordering::Release);
    }
}

/// Admission is quick; neither QuickJS nor the policy-tick advisory lock waits
/// for a compiler/checker. Restart reconciliation uses the same persisted rows.
pub(crate) fn kick(pool: PgPool, engine: PolicyEngine) {
    if IN_FLIGHT.swap(true, Ordering::AcqRel) {
        return;
    }
    tokio::spawn(async move {
        let _flight = Flight;
        if let Err(error) = sweep(&pool, &engine).await {
            tracing::warn!(%error,"managed check reconciliation failed");
        }
    });
}

pub(super) async fn sweep(pool: &PgPool, engine: &PolicyEngine) -> anyhow::Result<()> {
    let cards = store::pending_cards(pool).await?;
    for card in cards {
        if let Err(error) = drive(pool, engine, &card).await {
            tracing::warn!(%card, %error, "managed check reconciliation failed");
        }
    }
    Ok(())
}

pub(super) async fn drive(pool: &PgPool, engine: &PolicyEngine, card: &str) -> anyhow::Result<()> {
    let Some(mut work) = store::load(pool, card).await? else {
        return Ok(());
    };
    match drive_checks(pool, &mut work).await {
        Ok(true) => wake(pool, engine, &work).await,
        Ok(false) => Ok(()),
        Err(error) => {
            store::unverifiable(pool, &work, &error.to_string()).await?;
            Ok(())
        }
    }
}

async fn drive_checks(pool: &PgPool, work: &mut store::Work) -> anyhow::Result<bool> {
    work.contract.validate().map_err(anyhow::Error::msg)?;
    // A done dispatch/EOF does not prove writer quiescence. Retained managed
    // leases (including unknown and expired) block candidate capture.
    let busy:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_execution_leases WHERE resource_key=$1 AND (managed OR expires_at>clock_timestamp()))")
        .bind(work.contract.template.workspace.to_string_lossy().as_ref()).fetch_one(pool).await?;
    if busy {
        store::mark_wait(pool, work, "verification_await_execution_quiescence").await?;
        return Ok(false);
    }
    let previous = work.state.as_ref().filter(|s| {
        s["dispatch_id"] == work.dispatch
            && s["dispatch_result"] == json!(work.result)
            && s["contract_digest"] == work.contract.digest
            && s["pipeline_stage_id"] == json!(work.stage)
    });
    // Capture/configuration failures require operator correction and a new
    // implementation attempt. They are visible failures, not an endless
    // await_checks loop; checker retries have their own finite receipt budget.
    if previous.is_some_and(|s| s["outcome"] == "unverifiable" && s["candidate_digest"].is_null()) {
        // Keep the visible hold and advance its observation timestamp, as with
        // other waits. A bounded sweep must rotate past permanently rejected
        // captures instead of repeatedly selecting the same oldest 16 cards.
        store::mark_wait(pool, work, "verification_unverifiable").await?;
        return Ok(false);
    }
    let candidate = if let Some(state) = previous {
        snapshot::Candidate {
            digest: state["candidate_digest"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing candidate digest"))?
                .into(),
            path: state["candidate_path"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing candidate path"))?
                .into(),
        }
    } else {
        let count:i64=sqlx::query_scalar("SELECT COUNT(DISTINCT required_capabilities->>'dispatch_id') FROM test_phase_runs WHERE card_id=$1 AND idempotency_key LIKE 'managed-check:%'")
            .bind(&work.card).fetch_one(pool).await?;
        anyhow::ensure!(
            count < i64::from(work.contract.template.max_candidates),
            "verification candidate budget exhausted"
        );
        capture::freeze(pool, work).await?
    };
    let mut state = previous
        .cloned()
        .unwrap_or_else(|| store::state(work, &candidate));
    // Restart may reuse a receipt, but must still have its protected inputs.
    let path = candidate.path.clone();
    let digest = candidate.digest.clone();
    let checks = work.contract.template.checks.clone();
    let valid = tokio::task::spawn_blocking(move || {
        snapshot::verify(&path, &digest)?;
        for check in checks.values() {
            process::validate_check(check)?;
        }
        Ok::<_, String>(())
    })
    .await?;
    valid.map_err(anyhow::Error::msg)?;
    // Save the exact candidate before any process can start, for crash recovery.
    if !store::save(pool, work, &state).await? {
        return Ok(false);
    }
    work.state = Some(state.clone());
    let mut outcome = "passed";
    for (id, check) in &work.contract.template.checks {
        let result = run_check(pool, work, &candidate, id, check).await?;
        state["runs"][id] = json!(result.0);
        match result.1.as_str() {
            "passed" => {}
            "running" => {
                outcome = "running";
                break;
            }
            "failed" => {
                outcome = "failed";
                break;
            }
            _ => {
                outcome = "unverifiable";
                break;
            }
        }
    }
    state["outcome"] = json!(outcome);
    if !store::save(pool, work, &state).await? {
        return Ok(false);
    }
    work.state = Some(state);
    Ok(true)
}

async fn run_check(
    pool: &PgPool,
    work: &store::Work,
    candidate: &snapshot::Candidate,
    id: &str,
    check: &contract::Check,
) -> anyhow::Result<(String, String)> {
    let identity = contract::digest(&serde_json::to_vec(&(
        &work.card,
        &work.dispatch,
        &work.result,
        &work.contract.digest,
        &candidate.digest,
        id,
    ))?);
    for attempt in 0..work.contract.template.max_check_attempts {
        let key = format!("managed-check:{identity}:{attempt}");
        store::expire(pool, &key).await?;
        let request: TestPhaseRunStartRequest = serde_json::from_value(
            json!({"phase_key":format!("managed-check:{id}"),"head_sha":candidate.digest,"idempotency_key":key,
            "card_id":work.card,"repo_id":work.repo,"required_capabilities":{"dispatch_id":work.dispatch,"contract_digest":work.contract.digest,"check_id":id},
            "resource_lock_key":key,"holder_instance_id":"protected-command-v1","holder_job_id":uuid::Uuid::new_v4().to_string(),"ttl_secs":check.timeout_secs+60}),
        )?;
        let start = lifecycle::start(pool, &request)
            .await
            .map_err(anyhow::Error::msg)?;
        let Some(run) = start.run else {
            return Ok((key, "running".into()));
        };
        let run = if start.started {
            let evidence = execute(pool, work, candidate, check).await;
            let mut evidence = evidence
                .unwrap_or_else(|e| json!({"outcome":"unverifiable","reason":e.to_string()}));
            evidence["collector"] = json!("protected-command-v1");
            evidence["contract_digest"] = json!(work.contract.digest);
            evidence["dispatch_id"] = json!(work.dispatch);
            evidence["check_id"] = json!(id);
            let status = match evidence["outcome"].as_str() {
                Some("passed") => "passed",
                Some("cancelled") => "canceled",
                _ => "failed",
            };
            let complete: TestPhaseRunCompleteRequest = serde_json::from_value(
                json!({"phase_key":run.phase_key,"head_sha":run.head_sha,"idempotency_key":key,
                "resource_lock_key":run.resource_lock_key,"holder_instance_id":run.holder_instance_id,"holder_job_id":run.holder_job_id,
                "status":status,"evidence":evidence,"release_lock":true}),
            )?;
            lifecycle::complete(pool, &complete)
                .await
                .map_err(anyhow::Error::msg)?
                .run
        } else {
            run
        };
        if run.status == "running" {
            return Ok((key, "running".into()));
        }
        let result = run.evidence["outcome"].as_str().unwrap_or("unverifiable");
        if result != "unverifiable" || attempt + 1 == work.contract.template.max_check_attempts {
            return Ok((key, result.into()));
        }
    }
    anyhow::bail!("invalid empty check attempt budget")
}

async fn execute(
    pool: &PgPool,
    work: &store::Work,
    candidate: &snapshot::Candidate,
    check: &contract::Check,
) -> anyhow::Result<Value> {
    anyhow::ensure!(
        store::current(pool, work).await?,
        "candidate attempt was replaced before check launch"
    );
    let identity = crate::services::platform::managed_process::inspect_workspace(
        &work.contract.template.workspace,
    )?;
    let check = check.clone();
    let path = candidate.path.clone();
    let digest = candidate.digest.clone();
    let cancelled = Arc::new(AtomicBool::new(false));
    let child_cancel = cancelled.clone();
    struct CancelOnDrop(Arc<AtomicBool>);
    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let _cancel_on_drop = CancelOnDrop(cancelled.clone());
    let mut task = tokio::task::spawn_blocking(move || {
        process::run(&check, &path, &digest, identity, child_cancel)
    });
    loop {
        tokio::select! {
            result=&mut task=>return result?.map_err(anyhow::Error::msg),
            _=tokio::time::sleep(std::time::Duration::from_millis(250))=> {
                if !store::current(pool,work).await.unwrap_or(false){cancelled.store(true,Ordering::Release);}
            }
        }
    }
}

async fn wake(pool: &PgPool, engine: &PolicyEngine, work: &store::Work) -> anyhow::Result<()> {
    match store::disposition(pool, &work.card)
        .await
        .map_err(anyhow::Error::msg)?
        .as_str()
    {
        "ready_to_commit" => {
            crate::kanban::transition_status_with_opts_pg_only(
                pool,
                engine,
                &work.card,
                "done",
                "managed_checks",
                crate::engine::transition::ForceIntent::None,
            )
            .await?;
        }
        "run_local_review" => {
            crate::kanban::resume_verified_review(pool, engine, &work.card).await?;
        }
        wait @ ("await_checks" | "await_human") => {
            if wait == "await_human" && super::rework::schedule(pool, work).await? {
                return Ok(());
            }
            store::mark_wait(pool, work, &format!("verification_{wait}")).await?;
        }
        _ => {}
    }
    Ok(())
}
