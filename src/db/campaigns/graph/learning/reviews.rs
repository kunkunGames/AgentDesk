//! Review claims and promotion decisions are durable and idempotent.
use super::{
    policy::Policy,
    samples,
    template::{Decision, Review, Template},
};
use crate::db::campaigns::graph::{Goal, Graph, contract_digest};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

#[derive(Clone, Debug)]
pub struct Pending {
    pub id: String,
    pub family_id: String,
    pub task_key: String,
    pub agent_id: String,
    pub sample_digest: String,
    pub snapshot: Value,
}
fn pending(row: &sqlx::postgres::PgRow) -> Pending {
    Pending {
        id: row.get("id"),
        family_id: row.get("family_id"),
        task_key: row.get("task_key"),
        agent_id: row.get("superior_agent_id"),
        sample_digest: row.get("sample_digest"),
        snapshot: row.get("snapshot"),
    }
}

pub async fn prepare(pool: &PgPool, id: &str, goal: &Goal) -> Result<Option<Pending>> {
    let family = super::family(pool, id, goal).await?;
    samples::collect_family(pool, &family).await?;
    let policy = Policy::load(pool).await?;
    let mut tx = pool.begin().await?;
    let owner: String = sqlx::query_scalar(
        "SELECT root_campaign_id FROM campaign_dag_families WHERE id=$1 FOR UPDATE",
    )
    .bind(&family)
    .fetch_one(&mut *tx)
    .await?;
    if owner != id {
        return Ok(None);
    }
    let unsettled: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM campaign_dag_reviews review
        JOIN campaign_graph_tasks t ON t.task_key=review.task_key
        WHERE review.family_id=$1 AND review.status='failed' AND ({}) AND ({}))",
        super::budget::REVIEW_TASK_SQL,
        super::budget::UNSETTLED_TASK_SQL
    ))
    .bind(&family)
    .fetch_one(&mut *tx)
    .await?;
    if unsettled {
        // A ledger failure alone cannot relinquish an actual execution owner.
        return Ok(None);
    }
    if let Some(row) =
        sqlx::query("SELECT * FROM campaign_dag_reviews WHERE family_id=$1 AND status='pending'")
            .bind(&family)
            .fetch_optional(&mut *tx)
            .await?
    {
        return Ok(Some(pending(&row)));
    }
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM campaign_dag_samples WHERE family_id=$1")
            .bind(&family)
            .fetch_one(&mut *tx)
            .await?;
    let prior: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(sample_count),0) FROM campaign_dag_reviews WHERE family_id=$1",
    )
    .bind(&family)
    .fetch_one(&mut *tx)
    .await?;
    if count - prior < i64::from(policy.min_samples) {
        return Ok(None);
    }
    let reviewed_count = prior + i64::from(policy.min_samples);
    super::policy::eligible_agent(pool, &policy.superior_agent).await?;
    let samples:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(s) FROM campaign_dag_samples s WHERE family_id=$1 ORDER BY sequence ASC LIMIT $2 OFFSET $3")
        .bind(&family).bind(i64::from(policy.min_samples)).bind(prior).fetch_all(&mut *tx).await?;
    let failures:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('campaign_id',f.campaign_id,'category',f.category,'task_key',f.task_key,'created_at',f.created_at,'evidence',f.evidence)
        FROM campaign_graph_failures f JOIN campaign_dag_members m ON m.campaign_id=f.campaign_id
        WHERE m.family_id=$1 ORDER BY f.created_at DESC LIMIT 50")
        .bind(&family).fetch_all(&mut *tx).await?;
    let active: Option<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(t) FROM campaign_dag_templates t WHERE family_id=$1 AND status='active'",
    )
    .bind(&family)
    .fetch_optional(&mut *tx)
    .await?;
    let baseline: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('campaign_id',m.campaign_id,'graph_versions',(SELECT count(*) FROM campaign_graph_versions v WHERE v.campaign_id=m.campaign_id),'failure_counts',(SELECT jsonb_object_agg(category,n) FROM(SELECT category,count(*) n FROM campaign_graph_failures f WHERE f.campaign_id=m.campaign_id GROUP BY category)z)) FROM campaign_dag_members m WHERE m.family_id=$1 ORDER BY m.created_at DESC LIMIT 100")
        .bind(&family).fetch_all(&mut *tx).await?;
    let claimed_at = sqlx::query_scalar("SELECT NOW()")
        .fetch_one(&mut *tx)
        .await?;
    let execution_policy = super::budget::ReviewPolicy::freeze(goal, claimed_at)?;
    let snapshot = json!({"schema_version":1,"family_id":family,"goal":goal,"scope":super::scope(goal),"execution_policy":execution_policy,
        "sample_count":reviewed_count,"available_sample_count":count,"previous_sample_count":prior,"threshold":policy.min_samples,
        "samples":samples,"active_template":active,"quality_baseline":baseline,"failure_history":failures});
    let digest = contract_digest(&snapshot)?;
    // Full bounded evidence is a frozen input artifact, not an oversized prompt.
    ensure!(
        serde_json::to_vec(&snapshot)?.len() as u64
            <= crate::db::campaigns::graph::MAX_ARTIFACT_FILE_BYTES,
        "template review snapshot exceeds the native 4 MiB file limit; lower the configured review batch size"
    );
    let review = format!(
        "review-{}",
        contract_digest(&(&family, reviewed_count, &digest))?
    );
    let key = format!("{id}:template_review:{review}");
    let row=sqlx::query("INSERT INTO campaign_dag_reviews(id,family_id,owner_campaign_id,task_key,superior_agent_id,sample_count,threshold,sample_digest,snapshot)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *")
        .bind(review).bind(family).bind(id).bind(key).bind(policy.superior_agent).bind(reviewed_count)
        .bind(i64::from(policy.min_samples)).bind(digest).bind(snapshot).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(Some(pending(&row)))
}

pub async fn accept(pool: &PgPool, task_key: &str, value: &Value) -> Result<()> {
    let decision: Review = serde_json::from_value(value.clone())?;
    ensure!(
        !decision.reason.trim().is_empty() && decision.reason.len() <= 8000,
        "bounded template decision reason required"
    );
    ensure!(
        matches!(
            (&decision.decision, &decision.template),
            (Decision::Promote, Some(_)) | (Decision::Keep | Decision::Defer, None)
        ),
        "promotion requires a template; deferral must not include one"
    );
    let mut tx = pool.begin().await?;
    let family: String =
        sqlx::query_scalar("SELECT family_id FROM campaign_dag_reviews WHERE task_key=$1")
            .bind(task_key)
            .fetch_one(&mut *tx)
            .await?;
    sqlx::query("SELECT id FROM campaign_dag_families WHERE id=$1 FOR UPDATE")
        .bind(family)
        .fetch_one(&mut *tx)
        .await?;
    let row = sqlx::query("SELECT * FROM campaign_dag_reviews WHERE task_key=$1 FOR UPDATE")
        .bind(task_key)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        decision.sample_digest == row.get::<String, _>("sample_digest"),
        "template review sample digest differs"
    );
    let family: String = row.get("family_id");
    let snapshot: Value = row.get("snapshot");
    let snapshot_goal: Goal = serde_json::from_value(snapshot["goal"].clone())?;
    let current: Value =
        sqlx::query_scalar("SELECT spec FROM campaign_execution_specs WHERE campaign_id=$1")
            .bind(row.get::<String, _>("owner_campaign_id"))
            .fetch_one(&mut *tx)
            .await?;
    let current: Goal = serde_json::from_value(current)?;
    ensure!(
        super::scope_digest(&current)? == super::scope_digest(&snapshot_goal)?,
        "template review goal scope changed"
    );
    if row.get::<String, _>("status") != "pending" {
        if let Some(saved) = row.get::<Option<String>, _>("decision_digest") {
            ensure!(
                saved == contract_digest(&decision)?,
                "template decision is already settled differently"
            );
            return Ok(());
        }
        let expected = match decision.decision {
            Decision::Promote => "promoted",
            Decision::Keep => "kept",
            Decision::Defer => "deferred",
        };
        ensure!(
            row.get::<String, _>("status") == expected
                && row.get::<Option<String>, _>("reason").as_deref() == Some(&decision.reason),
            "template decision is already settled differently"
        );
        if let Some(template) = &decision.template {
            let digest: String =
                sqlx::query_scalar("SELECT digest FROM campaign_dag_templates WHERE review_id=$1")
                    .bind(row.get::<String, _>("id"))
                    .fetch_one(&mut *tx)
                    .await?;
            ensure!(
                digest == contract_digest(template)?,
                "template decision retry changed the definition"
            );
        }
        return Ok(());
    }
    super::budget::admit_decision(&mut tx, task_key, &snapshot, row.get("created_at")).await?;
    let active: Option<Value> = sqlx::query_scalar("SELECT to_jsonb(t) FROM campaign_dag_templates t WHERE family_id=$1 AND status='active' FOR UPDATE")
        .bind(&family).fetch_optional(&mut *tx).await?;
    let frozen = &snapshot["active_template"];
    ensure!(
        active.as_ref().map(|a| (&a["id"], &a["digest"]))
            == frozen
                .as_object()
                .map(|_| (&frozen["id"], &frozen["digest"])),
        "active template changed after review snapshot; fresh review required"
    );
    let decision_digest = contract_digest(&decision)?;
    let status = match decision.decision {
        Decision::Keep => {
            ensure!(active.is_some(), "keep requires an active template");
            "kept"
        }
        Decision::Defer => {
            ensure!(
                decision.template.is_none(),
                "deferred review cannot publish a template"
            );
            "deferred"
        }
        Decision::Promote => {
            let template = decision
                .template
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("promotion requires a template definition"))?;
            let samples = snapshot["samples"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("review samples missing"))?;
            ensure!(
                samples.len() as i64 >= row.get::<i64, _>("threshold"),
                "review has insufficient verified samples"
            );
            for sample in samples {
                let observed: Graph = serde_json::from_value(sample["graph"].clone())?;
                for node in &observed.nodes {
                    ensure!(
                        template
                            .graph
                            .nodes
                            .iter()
                            .any(|candidate| candidate.kind == node.kind
                                && candidate.effect == node.effect),
                        "template drops a verified task kind or business-effect class"
                    );
                }
                let goal: Goal = serde_json::from_value(sample["goal"].clone())?;
                template.instantiate(
                    &goal,
                    &family,
                    sample["campaign_id"].as_str().unwrap_or(""),
                    "validation-window",
                )?;
            }
            template.instantiate(
                &snapshot_goal,
                &family,
                "fresh-template-validation",
                "future-window",
            )?;
            let next: i64 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(version),0)+1 FROM campaign_dag_templates WHERE family_id=$1",
            )
            .bind(&family)
            .fetch_one(&mut *tx)
            .await?;
            let digest = contract_digest(template)?;
            let identifier = format!("template-{}", contract_digest(&(&family, next, &digest))?);
            if active.as_ref().is_some_and(|a| a["digest"] == digest) {
                "kept"
            } else {
                sqlx::query("UPDATE campaign_dag_templates SET status='retired' WHERE family_id=$1 AND status='active'")
                .bind(&family).execute(&mut *tx).await?;
                sqlx::query("INSERT INTO campaign_dag_templates(id,family_id,version,schema_version,document,digest,review_id,status)
                VALUES($1,$2,$3,1,$4,$5,$6,'active')")
                .bind(identifier).bind(&family).bind(next).bind(serde_json::to_value(template)?)
                .bind(digest).bind(row.get::<String,_>("id")).execute(&mut *tx).await?;
                "promoted"
            }
        }
    };
    sqlx::query("UPDATE campaign_dag_reviews SET status=$2,reason=$3,decision_digest=$4,decided_at=NOW() WHERE id=$1 AND status='pending'")
        .bind(row.get::<String,_>("id")).bind(status).bind(decision.reason).bind(decision_digest).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn fail(pool: &PgPool, key: &str, reason: &str) -> Result<()> {
    sqlx::query("UPDATE campaign_dag_reviews SET status='failed',reason=$2,decided_at=NOW() WHERE task_key=$1 AND status='pending'")
        .bind(key).bind(reason.chars().take(8000).collect::<String>()).execute(pool).await?;
    Ok(())
}

pub async fn instantiate(pool: &PgPool, id: &str, goal: &Goal) -> Result<Option<(String, Graph)>> {
    let family = super::family(pool, id, goal).await?;
    let row = sqlx::query(
        "SELECT id,document FROM campaign_dag_templates WHERE family_id=$1 AND status='active'",
    )
    .bind(&family)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else { return Ok(None) };
    let window: Option<String> = sqlx::query_scalar(
        "SELECT scheduled_at::text FROM campaign_graph_occurrences WHERE child_id=$1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    let resolved = serde_json::from_value::<Template>(row.get("document"))
        .map_err(anyhow::Error::from)
        .and_then(|template| {
            template.instantiate(
                goal,
                &family,
                id,
                window.as_deref().unwrap_or("manual occurrence"),
            )
        });
    match resolved {
        Ok(graph) => Ok(Some((row.get("id"), graph))),
        Err(error) => {
            block(pool, &row.get::<String, _>("id"), id, &error.to_string()).await?;
            Ok(None)
        }
    }
}
pub async fn block(pool: &PgPool, template: &str, campaign: &str, reason: &str) -> Result<()> {
    sqlx::query("UPDATE campaign_dag_templates SET status='blocked',blocked_reason=$3,blocked_by_campaign_id=$2,blocked_at=NOW() WHERE id=$1 AND status='active'")
        .bind(template).bind(campaign).bind(reason.chars().take(8000).collect::<String>()).execute(pool).await?;
    Ok(())
}
