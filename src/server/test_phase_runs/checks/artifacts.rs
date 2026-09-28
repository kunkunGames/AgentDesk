//! The only handoff source is a completed, still-verifiable protected candidate.
//! Model text, ledger status and a caller-supplied digest are not certificates.
use super::{contract::Contract, snapshot};
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug, Serialize)]
pub(crate) struct VerifiedArtifact {
    card_id: String,
    dispatch_id: String,
    contract_digest: String,
    candidate_digest: String,
    candidate_path: PathBuf,
}

impl VerifiedArtifact {
    pub(crate) fn card_id(&self) -> &str {
        &self.card_id
    }

    pub(crate) fn digest(&self) -> &str {
        &self.candidate_digest
    }

    /// Read only the protected frozen tree, never the mutable implementation workspace.
    pub(crate) fn files(&self) -> Result<BTreeMap<String, Vec<u8>>, String> {
        snapshot::verify(&self.candidate_path, &self.candidate_digest)?;
        let files = snapshot::read_tree(&self.candidate_path)?;
        let manifest: BTreeMap<_, _> = files
            .iter()
            .map(|(name, bytes)| (name, super::contract::digest(bytes)))
            .collect();
        if super::contract::digest(&serde_json::to_vec(&manifest).map_err(|e| e.to_string())?)
            != self.candidate_digest
        {
            return Err("verified artifact changed while reading".into());
        }
        Ok(files)
    }
}

/// Reuse the completion predicate with the original review state. The ordinary
/// disposition returns `completed` immediately for a done card, which alone
/// would miss a revoked receipt or a newer implementation attempt.
pub(crate) async fn completed_artifact(
    pool: &PgPool,
    card: &str,
) -> anyhow::Result<Option<VerifiedArtifact>> {
    let row = sqlx::query(
        "SELECT verification_contract,verification_state FROM kanban_cards k \
         WHERE id=$1 AND status='done' AND verification_contract IS NOT NULL \
         AND agentdesk_check_disposition(jsonb_populate_record(k,'{\"status\":\"review\"}'::jsonb))='ready_to_commit'",
    )
    .bind(card)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let contract: Contract = serde_json::from_value(row.get("verification_contract"))?;
    contract.validate().map_err(anyhow::Error::msg)?;
    let state: Value = row.get("verification_state");
    let field = |name: &str| -> anyhow::Result<String> {
        Ok(state[name]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("completed artifact lacks {name}"))?
            .to_owned())
    };
    let digest = field("candidate_digest")?;
    anyhow::ensure!(
        digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid completed artifact digest"
    );
    let path = PathBuf::from(field("candidate_path")?);
    anyhow::ensure!(
        path == contract.template.artifact_root.join(&digest),
        "completed artifact path differs from protected contract"
    );
    let artifact = VerifiedArtifact {
        card_id: card.to_owned(),
        dispatch_id: field("dispatch_id")?,
        contract_digest: contract.digest,
        candidate_digest: digest,
        candidate_path: path,
    };
    let to_verify = artifact.clone();
    tokio::task::spawn_blocking(move || to_verify.files())
        .await?
        .map_err(anyhow::Error::msg)?;
    Ok(Some(artifact))
}
