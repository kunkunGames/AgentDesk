//! Explicit operator reconciliation preserves failed attempts and requires QA.
mod artifact;
mod captured;
mod descendants;
pub(crate) mod finish;
mod planning;
mod proposal;
pub(crate) mod replan;
mod transport;
mod validation;
use super::store;
use anyhow::{Result, ensure};
use serde::Deserialize;
use sqlx::{PgPool, Row, types::Json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reconcile {
    pub expected_revision: i64,
    pub failure_id: String,
    pub evidence: String,
}

pub async fn reconcile(pool: &PgPool, id: &str, request: &Reconcile) -> Result<i64> {
    ensure!(
        !request.evidence.trim().is_empty() && request.evidence.len() <= 8000,
        "bounded reconciliation evidence is required"
    );
    ensure!(
        crate::services::settings::internal_tasks_enabled(pool)
            .await
            .map_err(anyhow::Error::msg)?,
        "internal task admission disabled"
    );
    let mut tx = pool.begin().await?;
    let campaign: Json<crate::db::campaigns::Campaign> =
        sqlx::query_scalar("SELECT document FROM campaigns WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    let campaign = campaign.0;
    ensure!(
        matches!(
            campaign.status,
            crate::db::campaigns::CampaignStatus::Active
                | crate::db::campaigns::CampaignStatus::Paused
        ),
        "reconciliation requires an active or paused campaign"
    );
    if let Some(previous) = sqlx::query("SELECT campaign_id,evidence,candidate_version FROM campaign_graph_reconciliations WHERE failure_id=$1")
        .bind(&request.failure_id).fetch_optional(&mut *tx).await? {
        ensure!(previous.get::<String,_>("campaign_id")==id && previous.get::<String,_>("evidence")==request.evidence, "failure already reconciled with different evidence");
        return Ok(previous.try_get("candidate_version")?);
    }
    ensure!(
        campaign.revision == request.expected_revision,
        "campaign revision conflict"
    );
    let state=sqlx::query("SELECT spec,active_version,hold_reason FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE")
        .bind(id).fetch_one(&mut *tx).await?;
    let goal: super::Goal = serde_json::from_value(state.get("spec"))?;
    let version: Option<i64> = state.try_get("active_version")?;
    let failure=sqlx::query("SELECT f.task_key,f.category,f.evidence,t.accepted,t.role,t.version FROM campaign_graph_failures f JOIN campaign_graph_tasks t ON t.task_key=f.task_key WHERE f.id=$1 AND f.campaign_id=$2")
        .bind(&request.failure_id).bind(id).fetch_one(&mut *tx).await?;
    let category: String = failure.get("category");
    let role: String = failure.get("role");
    let evidence: serde_json::Value = failure.get("evidence");
    let identity_repair = category == "plan"
        && role == "plan"
        && version.is_some()
        && failure.get::<Option<i64>, _>("version") == version
        && evidence["failure"]["error"] == "a published task key cannot acquire a new contract";
    let transport_repair = category == "plan"
        && role == "plan"
        && version.is_none()
        && super::result::captured_schema_failure(&evidence["failure"]["error"]);
    let hold: Option<String> = state.get("hold_reason");
    ensure!(
        hold.as_deref()
            == Some("Execution outcome requires reconciliation; automatic replay is disabled")
            || ((identity_repair || transport_repair)
                && hold.as_deref() == Some("Plan evolution budget exhausted")),
        "hold is not a reconcilable execution failure"
    );
    ensure!(
        matches!(category.as_str(), "unknown" | "infrastructure")
            || identity_repair
            || transport_repair,
        "captured failure has no safe reconciliation contract"
    );
    ensure!(
        !failure.get::<bool, _>("accepted"),
        "only unaccepted held execution may be reconciled"
    );
    let unresolved:i64=sqlx::query_scalar("SELECT COUNT(*) FROM campaign_graph_failures f WHERE f.campaign_id=$1 AND f.category IN ('unknown','infrastructure') AND NOT EXISTS(SELECT 1 FROM campaign_graph_reconciliations r WHERE r.failure_id=f.id)")
        .bind(id).fetch_one(&mut *tx).await?;
    ensure!(
        unresolved
            == if identity_repair || transport_repair {
                0
            } else {
                1
            },
        "additional unknown failures need reconciliation"
    );
    ensure!(!sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN task_dispatches d ON d.kanban_card_id=t.card_id WHERE t.campaign_id=$1 AND (d.status IN ('pending','dispatched','running') OR EXISTS(SELECT 1 FROM sessions s WHERE s.active_dispatch_id=d.id AND s.status IN ('working','turn_active','awaiting_bg'))))")
        .bind(id).fetch_one(&mut *tx).await?, "campaign still owns active execution");
    ensure!(!sqlx::query_scalar::<_,bool>("WITH owned AS (SELECT DISTINCT r.id,r.status FROM campaign_graph_tasks t JOIN auto_queue_entries e ON e.kanban_card_id=t.card_id JOIN auto_queue_runs r ON r.id=e.run_id WHERE t.campaign_id=$1) SELECT EXISTS(SELECT 1 FROM owned WHERE status IN ('generated','active','paused')) OR EXISTS(SELECT 1 FROM auto_queue_run_cleanup_tasks WHERE run_ids && ARRAY(SELECT id FROM owned))")
        .bind(id).fetch_one(&mut *tx).await?, "campaign cleanup is not complete");
    let elapsed = super::progress::elapsed_seconds(pool, id).await?;
    ensure!(
        elapsed < f64::from(goal.budget.max_elapsed_secs),
        "elapsed budget exhausted"
    );
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM campaign_graph_tasks WHERE campaign_id=$1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    ensure!(
        count + 2 < i64::from(goal.budget.max_dispatches),
        "insufficient dispatch budget for validation and work"
    );
    let tools:i64=sqlx::query_scalar("SELECT COUNT(*) FROM campaign_graph_progress p JOIN task_dispatches d ON d.id=p.dispatch_id JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id WHERE t.campaign_id=$1")
        .bind(id).fetch_one(&mut *tx).await?;
    ensure!(
        goal.budget
            .max_observed_tool_calls
            .is_none_or(|limit| tools < i64::from(limit)),
        "observed tool budget exhausted"
    );
    let key: String = failure.get("task_key");
    let graph = match role.as_str() {
        "work" => artifact::candidate(&mut tx, id, version, &key, request).await?,
        "plan" if identity_repair => {
            proposal::candidate(
                &mut tx,
                id,
                &goal,
                version.ok_or_else(|| anyhow::anyhow!("missing active graph"))?,
                &key,
                &evidence,
            )
            .await?
        }
        "plan" if transport_repair => {
            transport::candidate(&mut tx, id, &key, &evidence, request).await?
        }
        "plan" => {
            ensure!(
                version.is_none(),
                "planning recovery requires no active business graph"
            );
            ensure!(
                failure.get::<String, _>("category") == "infrastructure",
                "unknown planning effects require separate effect reconciliation"
            );
            planning::candidate(&mut tx, id, &key, &failure.get("evidence"), request).await?
        }
        "validate" => {
            ensure!(
                version.is_none(),
                "validation recovery requires no active business graph"
            );
            validation::candidate(&mut tx, id, &key, &evidence, request).await?
        }
        _ => anyhow::bail!("held role has no safe reconciliation contract"),
    };
    graph.validate(&goal)?;
    sqlx::query("INSERT INTO campaign_graph_reconciliations(failure_id,campaign_id,expected_revision,evidence) VALUES($1,$2,$3,$4)")
        .bind(&request.failure_id).bind(id).bind(request.expected_revision).bind(&request.evidence).execute(&mut *tx).await?;
    sqlx::query("UPDATE campaign_execution_specs SET hold_reason=NULL WHERE campaign_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    let candidate =
        store::stage_on_tx(&mut tx, id, &goal, &graph, Some(&request.failure_id)).await?;
    sqlx::query(
        "UPDATE campaign_graph_reconciliations SET candidate_version=$2 WHERE failure_id=$1",
    )
    .bind(&request.failure_id)
    .bind(candidate)
    .execute(&mut *tx)
    .await?;
    crate::db::campaigns::replace_on_tx(
        &mut tx,
        id,
        campaign.revision,
        serde_json::from_value(serde_json::to_value(&campaign)?)?,
    )
    .await?;
    tx.commit().await?;
    Ok(candidate)
}
