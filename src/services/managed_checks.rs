//! Shared protected check contracts and dispatch binding, below the server layer.
pub(crate) mod contract;
use serde_json::{Value, json};
use sqlx::PgPool;

/// Pin canonical dispatch creation to the workspace enrolled by the protected
/// supplier. A caller's worktree hint cannot redirect a managed campaign task.
pub(crate) async fn bind_campaign_dispatch(
    pool: &PgPool,
    card: &str,
    agent: &str,
    context: &mut Value,
) -> anyhow::Result<bool> {
    use sqlx::Row;
    let Some(row)=sqlx::query("SELECT verification_contract,assigned_agent_id FROM kanban_cards WHERE id=$1 AND metadata->>'managed_campaign'='true'")
        .bind(card).fetch_optional(pool).await? else {return Ok(false);};
    anyhow::ensure!(
        row.get::<Option<String>, _>("assigned_agent_id").as_deref() == Some(agent),
        "campaign dispatch assignment changed"
    );
    let contract: contract::Contract = serde_json::from_value(row.get("verification_contract"))?;
    contract.validate().map_err(anyhow::Error::msg)?;
    let path = contract.template.workspace;
    anyhow::ensure!(
        path.is_dir() && std::fs::canonicalize(&path)? == path,
        "campaign workspace is missing or aliased"
    );
    let obj = context
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("dispatch context must be an object"))?;
    obj.insert("worktree_path".into(), json!(path));
    obj.insert("managed_campaign".into(), json!(true));
    Ok(true)
}
