//! Reuse the existing native publication, artifact handoff and queue owner.
use super::*;
use crate::db::campaigns::graph::{Format, Output, learning as ledger, schema};
use crate::services::campaign_control_input::review_input;
use anyhow::ensure;

pub(super) async fn reconcile_review_budgets(
    pool: &PgPool,
    id: &str,
    health_registry: Option<std::sync::Arc<crate::services::discord::health::HealthRegistry>>,
) -> Result<bool> {
    ledger::budget::expire_pending(pool, id).await?;
    let mut ready = true;
    for key in ledger::budget::failed_keys(pool, id).await? {
        let (card, runs) = ledger::budget::review_card_runs(pool, &key).await?;
        crate::services::auto_queue::cancel_run::cancel_exclusive_card_runs_with_pg(
            health_registry.clone(),
            pool,
            &card,
            &runs,
            "campaign_review_policy_failed",
        )
        .await
        .map_err(anyhow::Error::msg)?;
        crate::services::auto_queue::runtime::stop_failed_review_turns_pg(
            health_registry.as_deref(),
            pool,
            &key,
        )
        .await
        .map_err(anyhow::Error::msg)?;
        ready &= !ledger::budget::unsettled(pool, &key).await?;
    }
    Ok(ready && !ledger::budget::failed_owners_unsettled(pool, id).await?)
}

pub(super) async fn review(pool: &PgPool, id: &str, goal: &Goal) -> Result<()> {
    let family = ledger::family(pool, id, goal).await?;
    let owner: String =
        sqlx::query_scalar("SELECT root_campaign_id FROM campaign_dag_families WHERE id=$1")
            .bind(&family)
            .fetch_one(pool)
            .await?;
    if owner != id {
        return Ok(());
    }
    let pending = ledger::reviews::prepare(pool, id, goal).await;
    let pending = match pending {
        Ok(pending) => {
            sqlx::query("UPDATE campaign_dag_families SET review_wait_reason=NULL WHERE id=$1 AND review_wait_reason IS NOT NULL")
                .bind(&family).execute(pool).await?;
            pending
        }
        Err(error) => {
            // Promotion is optional work. Surface its gate without stopping
            // admission of ordinary occurrences using their enrolled planner.
            sqlx::query("UPDATE campaign_dag_families SET review_wait_reason=$2 WHERE id=$1")
                .bind(&family)
                .bind(super::observation::redact(&error.to_string()))
                .execute(pool)
                .await?;
            return Ok(());
        }
    };
    let Some(review) = pending else { return Ok(()) };
    if let Err(error) = publish_review(pool, id, goal, &review).await {
        ledger::reviews::fail(
            pool,
            &review.task_key,
            &super::observation::redact(&error.to_string()),
        )
        .await?;
    }
    Ok(())
}
async fn publish_review(
    pool: &PgPool,
    id: &str,
    goal: &Goal,
    review: &ledger::reviews::Pending,
) -> Result<()> {
    if sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE task_key=$1)",
    )
    .bind(&review.task_key)
    .fetch_one(pool)
    .await?
    {
        return Ok(());
    }
    let mut snapshot = review.snapshot.clone();
    snapshot["sample_digest"] = json!(review.sample_digest);
    let root = artifact_root()?;
    let manifest = tokio::task::spawn_blocking(move || {
        let staging = tempfile::tempdir()?;
        std::fs::write(
            staging.path().join(ledger::template::INPUT_FILE),
            serde_json::to_vec(&snapshot)?,
        )?;
        artifacts::capture_outputs(
            staging.path(),
            &[Output {
                path: ledger::template::INPUT_FILE.into(),
                format: Format::Json,
            }],
            &root,
        )
    })
    .await??;
    let authority = ledger::ControlAssignment {
        family_id: review.family_id.clone(),
        agent_id: review.agent_id.clone(),
        purpose: "template_review",
        input_manifest: Some(manifest),
    };
    let prompt = format!(
        "{}\n{}\nFrozen sample digest: {}.\nTemplate/result JSON Schema:\n{}",
        contract_prompt(goal)?,
        ledger::template::instructions(),
        review.sample_digest,
        serde_json::to_string(&schema::document()?["result"])?
    );
    store::publish_control(
        pool,
        id,
        &store::Publication {
            key: &review.task_key,
            node: "template_review",
            role: "template_review",
            version: None,
            agent: &review.agent_id,
            title: &format!("[{id}] DAG template promotion review"),
            prompt: &prompt,
            digest: &contract_digest(&prompt)?,
        },
        &authority,
    )
    .await?;
    Ok(())
}

pub(super) async fn initial(pool: &PgPool, id: &str, goal: &Goal) -> Result<bool> {
    let Some((template, graph)) = ledger::reviews::instantiate(pool, id, goal).await? else {
        return Ok(false);
    };
    let mut tx = pool.begin().await?;
    store::lock(&mut tx, id).await?;
    let version = store::stage_on_tx(&mut tx, id, goal, &graph, None).await?;
    sqlx::query("UPDATE campaign_graph_versions SET template_id=$3 WHERE campaign_id=$1 AND version=$2 AND template_id IS NULL")
        .bind(id).bind(version).bind(template).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn accept_review(
    pool: &PgPool,
    key: &str,
    result: &Value,
    context: Value,
) -> Result<()> {
    let manifest:Value=sqlx::query_scalar("SELECT input_manifest FROM campaign_dag_control_assignments WHERE task_key=$1 AND purpose='template_review'")
        .bind(key).fetch_one(pool).await?;
    let workspace = context["worktree_path"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("review workspace unavailable"))?
        .to_owned();
    let root = artifact_root()?;
    let (inputs, manifests) = review_input(manifest);
    tokio::task::spawn_blocking(move || {
        artifacts::verify_input_files(&root, std::path::Path::new(&workspace), &inputs, &manifests)
    })
    .await??;
    ensure!(
        result["campaign_graph"].is_null() && result["graph_validation"].is_null(),
        "template review cannot submit a business graph or graph approval"
    );
    ledger::reviews::accept(pool, key, &result["dag_template_review"]).await
}
