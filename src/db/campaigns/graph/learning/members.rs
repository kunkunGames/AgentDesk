use super::{policy::Policy, scope, scope_digest};
use crate::db::campaigns::graph::{Goal, contract_digest};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

#[derive(Clone, Debug)]
pub struct ControlAssignment {
    pub family_id: String,
    pub agent_id: String,
    pub purpose: &'static str,
    pub input_manifest: Option<Value>,
}

pub async fn family(pool: &PgPool, id: &str, goal: &Goal) -> Result<String> {
    let mut tx = pool.begin().await?;
    let parent: Option<String> =
        sqlx::query_scalar("SELECT campaign_id FROM campaign_graph_occurrences WHERE child_id=$1")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
    let root = parent.as_deref().unwrap_or(id);
    // A child's scope was pinned when admitted, even if later root budgets change.
    if parent.is_some() {
        if let Some(found) = sqlx::query_scalar::<_, String>(
            "SELECT family_id FROM campaign_dag_members WHERE campaign_id=$1",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        {
            return Ok(found);
        }
    }
    let root_goal = if parent.is_some() {
        let raw: Value =
            sqlx::query_scalar("SELECT spec FROM campaign_execution_specs WHERE campaign_id=$1")
                .bind(root)
                .fetch_one(&mut *tx)
                .await?;
        serde_json::from_value(raw)?
    } else {
        goal.clone()
    };
    let digest = scope_digest(&root_goal)?;
    let family = format!("dag-{}", contract_digest(&(root, &digest))?);
    // Existing planning attempts retain their executor and are not replayed at upgrade.
    let bootstrap: Option<String> = sqlx::query_scalar(
        "SELECT t.campaign_id FROM campaign_graph_tasks t WHERE t.role='plan'
         AND NOT EXISTS(SELECT 1 FROM campaign_dag_families WHERE root_campaign_id=$1) AND
         (t.campaign_id=$1 OR t.campaign_id IN (SELECT child_id FROM campaign_graph_occurrences WHERE campaign_id=$1))
         ORDER BY t.created_at,t.task_key LIMIT 1",
    ).bind(root).fetch_optional(&mut *tx).await?;
    sqlx::query("INSERT INTO campaign_dag_families(id,root_campaign_id,scope_digest,scope,bootstrap_campaign_id)
        VALUES($1,$2,$3,$4,$5) ON CONFLICT(root_campaign_id,scope_digest) DO NOTHING")
        .bind(&family).bind(root).bind(digest).bind(scope(&root_goal)).bind(bootstrap).execute(&mut *tx).await?;
    sqlx::query(
        "INSERT INTO campaign_dag_members(campaign_id,family_id) VALUES($1,$2)
        ON CONFLICT(campaign_id) DO UPDATE SET family_id=EXCLUDED.family_id",
    )
    .bind(root)
    .bind(&family)
    .execute(&mut *tx)
    .await?;
    if parent.is_some() {
        sqlx::query("INSERT INTO campaign_dag_members(campaign_id,family_id) VALUES($1,$2) ON CONFLICT DO NOTHING")
            .bind(id).bind(&family).execute(&mut *tx).await?;
    }
    sqlx::query("INSERT INTO campaign_dag_members(campaign_id,family_id)
        SELECT child_id,$2 FROM campaign_graph_occurrences WHERE campaign_id=$1 ON CONFLICT DO NOTHING")
        .bind(root).bind(&family).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(family)
}

pub async fn initial_assignment(
    pool: &PgPool,
    id: &str,
    goal: &Goal,
) -> Result<Option<ControlAssignment>> {
    let family_id = family(pool, id, goal).await?;
    let policy = Policy::load(pool).await?;
    let mut tx = pool.begin().await?;
    let row = sqlx::query(
        "SELECT bootstrap_campaign_id FROM campaign_dag_families WHERE id=$1 FOR UPDATE",
    )
    .bind(&family_id)
    .fetch_one(&mut *tx)
    .await?;
    let bootstrap: Option<String> = row.get("bootstrap_campaign_id");
    if bootstrap.as_deref().is_some_and(|owner| owner != id) {
        return Ok(None);
    }
    if sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_versions WHERE campaign_id=$1)",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?
    {
        return Ok(None);
    }
    // Only bootstrap needs the superior. Subsequent occurrences keep working
    // with their enrolled agents if the superior is temporarily unavailable.
    super::policy::eligible_agent(pool, &policy.superior_agent).await?;
    sqlx::query("UPDATE campaign_dag_families SET bootstrap_campaign_id=$2 WHERE id=$1 AND bootstrap_campaign_id IS NULL")
        .bind(&family_id).bind(id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(Some(ControlAssignment {
        family_id,
        agent_id: policy.superior_agent,
        purpose: "bootstrap_plan",
        input_manifest: None,
    }))
}

pub async fn inspection(pool: &PgPool, id: &str) -> Result<Value> {
    let policy = Policy::load(pool).await?;
    let member: Option<String> =
        sqlx::query_scalar("SELECT family_id FROM campaign_dag_members WHERE campaign_id=$1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    let Some(family) = member else {
        return Ok(json!({"schema_version":1,
        "superior_agent_id":policy.superior_agent,"min_samples":policy.min_samples,"sample_count":0,
        "family_id":null,"active_template":null,"reviews":[]}));
    };
    let samples: i64 =
        sqlx::query_scalar("SELECT count(*) FROM campaign_dag_samples WHERE family_id=$1")
            .bind(&family)
            .fetch_one(pool)
            .await?;
    let active: Option<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(t) FROM campaign_dag_templates t WHERE family_id=$1 AND status='active'",
    )
    .bind(&family)
    .fetch_optional(pool)
    .await?;
    let reviews:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('id',id,'sample_count',sample_count,'threshold',threshold,'status',status,'reason',reason,'superior_agent_id',superior_agent_id,'created_at',created_at,'decided_at',decided_at) FROM campaign_dag_reviews WHERE family_id=$1 ORDER BY created_at DESC LIMIT 20")
        .bind(&family).fetch_all(pool).await?;
    let prior: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(sample_count),0) FROM campaign_dag_reviews WHERE family_id=$1",
    )
    .bind(&family)
    .fetch_one(pool)
    .await?;
    let new_samples = samples.saturating_sub(prior);
    let bootstrap: Option<String> =
        sqlx::query_scalar("SELECT bootstrap_campaign_id FROM campaign_dag_families WHERE id=$1")
            .bind(&family)
            .fetch_one(pool)
            .await?;
    let waiting: Option<String> =
        sqlx::query_scalar("SELECT review_wait_reason FROM campaign_dag_families WHERE id=$1")
            .bind(&family)
            .fetch_one(pool)
            .await?;
    ensure!(samples >= 0, "invalid sample count");
    Ok(
        json!({"schema_version":1,"family_id":family,"superior_agent_id":policy.superior_agent,
        "bootstrap_campaign_id":bootstrap,"min_samples":policy.min_samples,"sample_count":samples,
        "active_template":active,"reviews":reviews,"review_wait_reason":waiting,
        "last_review_sample_count":prior,"new_samples_since_review":new_samples,
        "samples_until_review":i64::from(policy.min_samples).saturating_sub(new_samples),
        "recurring":sqlx::query_scalar::<_,bool>("SELECT s.spec ? 'schedule' FROM campaign_execution_specs s JOIN campaign_dag_families f ON f.root_campaign_id=s.campaign_id WHERE f.id=$1").bind(&family).fetch_one(pool).await?}),
    )
}
