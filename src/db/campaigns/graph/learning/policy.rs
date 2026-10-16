//! Live operator policy. Published control assignments retain their agent.
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::PgPool;

pub const DEFAULT_MIN_SAMPLES: u32 = 10;
pub const DEFAULT_SUPERIOR_AGENT: &str = "codex";
pub const MAX_MIN_SAMPLES: u32 = 100;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub superior_agent: String,
    pub min_samples: u32,
}
impl Policy {
    pub fn from_value(value: &Value) -> Result<Self> {
        let superior_agent = match value.get("campaignSuperiorAgentId") {
            None => DEFAULT_SUPERIOR_AGENT,
            Some(v) => v
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("campaignSuperiorAgentId must be a string"))?,
        };
        super::super::super::validate_id(superior_agent)?;
        let min_samples = match value.get("campaignDagTemplateMinSamples") {
            None => u64::from(DEFAULT_MIN_SAMPLES),
            Some(v) => v.as_u64().ok_or_else(|| {
                anyhow::anyhow!("campaignDagTemplateMinSamples must be a positive integer")
            })?,
        };
        ensure!(
            (1..=u64::from(MAX_MIN_SAMPLES)).contains(&min_samples),
            "campaignDagTemplateMinSamples must be 1..={MAX_MIN_SAMPLES}"
        );
        Ok(Self {
            superior_agent: superior_agent.to_owned(),
            min_samples: min_samples as u32,
        })
    }
    pub async fn load(pool: &PgPool) -> Result<Self> {
        let raw: Option<String> = sqlx::query_scalar(
            "SELECT value FROM kv_meta WHERE key='runtime-config' AND (expires_at IS NULL OR expires_at>NOW())",
        ).fetch_optional(pool).await?;
        let value = raw
            .map(|v| serde_json::from_str(&v))
            .transpose()?
            .unwrap_or(Value::Null);
        Self::from_value(&value)
    }
}
pub async fn eligible_agent(pool: &PgPool, id: &str) -> Result<()> {
    let provider: Option<String> = sqlx::query_scalar("SELECT provider FROM agents WHERE id=$1")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    ensure!(
        provider
            .as_deref()
            .is_some_and(|p| matches!(p, "codex" | "qwen" | "opencode")),
        "superior agent {id} must be an existing native Codex/Qwen/OpenCode agent"
    );
    Ok(())
}
