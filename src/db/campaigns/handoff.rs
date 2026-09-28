//! Atomic publication of a finite batch into the canonical Auto-Queue.
//! Completion remains exclusively owned by the protected checks and card FSM.
use super::{Campaign, CampaignStatus};
use serde_json::Value;
use sqlx::{PgPool, Row, types::Json};

pub(crate) struct PreparedCard {
    pub id: String,
    pub node_id: String,
    pub title: String,
    pub description: String,
    pub agent_id: String,
    pub repo: String,
    pub metadata: Value,
    pub contract: Value,
    pub capture_key: String,
    pub capture_job: String,
}

/// Files and contracts are prepared under capture reservations before this
/// short transaction. A restart either sees the entire batch or no batch.
pub(crate) async fn publish(
    pool: &PgPool,
    expected: &Campaign,
    cards: &[PreparedCard],
) -> anyhow::Result<Option<String>> {
    if cards.is_empty() {
        return Ok(None);
    }
    let mut tx = pool.begin().await?;
    let current: Json<Campaign> =
        sqlx::query_scalar("SELECT document FROM campaigns WHERE id=$1 FOR UPDATE")
            .bind(&expected.id)
            .fetch_one(&mut *tx)
            .await?;
    anyhow::ensure!(
        current.0.revision == expected.revision && current.0.status == CampaignStatus::Active,
        "campaign changed or stopped during preparation"
    );
    // One logical task owns one card across campaigns and batches. Sort lock
    // acquisition independently of the operator's node ordering.
    let mut ordered: Vec<_> = cards.iter().collect();
    ordered.sort_by(|a, b| a.id.cmp(&b.id));
    for card in &ordered {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('campaign-task'),hashtext($1))")
            .bind(&card.id)
            .execute(&mut *tx)
            .await?;
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM kanban_cards WHERE id=$1)")
                .bind(&card.id)
                .fetch_one(&mut *tx)
                .await?;
        anyhow::ensure!(!exists, "logical task {} is already supplied", card.node_id);
        let reservation = sqlx::query("SELECT holder_job_id, expires_at>clock_timestamp() AS live FROM resource_locks WHERE lock_key=$1 FOR UPDATE")
            .bind(&card.capture_key).fetch_optional(&mut *tx).await?;
        anyhow::ensure!(
            reservation.is_some_and(|r| r.get::<String, _>("holder_job_id") == card.capture_job
                && r.get::<bool, _>("live")),
            "input preparation reservation expired"
        );
        let workspace = card.contract["template"]["workspace"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("prepared card has no workspace"))?;
        let reused: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM kanban_cards WHERE verification_contract->'template'->>'workspace'=$1)")
            .bind(workspace).fetch_one(&mut *tx).await?;
        anyhow::ensure!(!reused, "campaign tasks require a dedicated workspace");
    }
    let run = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO auto_queue_runs(id,repo,agent_id,status,ai_model,ai_rationale,max_concurrent_threads,thread_group_count) VALUES($1,$2,$3,'generated','protected-campaign-v1','finite verified-input batch',$4,$5)")
        .bind(&run).bind(&cards[0].repo).bind(&cards[0].agent_id)
        .bind(cards.len().min(4) as i64).bind(cards.len() as i64).execute(&mut *tx).await?;
    for (index, card) in cards.iter().enumerate() {
        sqlx::query("INSERT INTO kanban_cards(id,title,description,status,assigned_agent_id,repo_id,metadata,verification_contract) VALUES($1,$2,$3,'backlog',$4,$5,$6,$7)")
            .bind(&card.id).bind(&card.title).bind(&card.description).bind(&card.agent_id)
            .bind(&card.repo).bind(&card.metadata).bind(&card.contract).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO auto_queue_entries(id,run_id,kanban_card_id,agent_id,priority_rank,thread_group,batch_phase,reason) VALUES($1,$2,$3,$4,$5,$5,0,'verified campaign inputs')")
            .bind(uuid::Uuid::new_v4().to_string()).bind(&run).bind(&card.id)
            .bind(&card.agent_id).bind(index as i64 + 1).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(Some(run))
}
