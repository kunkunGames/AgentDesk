//! Prepare immutable dependency files under the existing writer/capture fence.
mod publication;
use super::plan::Node;
use crate::server::{
    resource_locks::{self, ResourceLockRequest},
    test_phase_runs::checks::{self, contract, snapshot},
};
use sqlx::PgPool;
use std::{collections::BTreeMap, path::Path};

pub(super) struct Prepared {
    pub contract: contract::Contract,
    pub key: String,
    pub job: String,
    pub manifest: serde_json::Value,
}

pub(super) async fn release(pool: &PgPool, key: &str, job: &str) -> anyhow::Result<()> {
    resource_locks::release_resource_lock(pool, key, "protected-campaign-v1", job)
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(())
}

pub(super) async fn prepare(
    pool: &PgPool,
    registry: &Path,
    node: &Node,
    artifacts: &BTreeMap<String, checks::VerifiedArtifact>,
) -> anyhow::Result<Prepared> {
    let registry = registry.to_owned();
    let template_name = node.check_template.clone();
    let template =
        tokio::task::spawn_blocking(move || contract::load_template(&registry, &template_name))
            .await?
            .map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        template.review != contract::Review::Local,
        "campaign execution currently supports protected checks with none/human review; Git review requires a separate verified adapter"
    );
    let key = format!("managed-capture:{}", template.workspace.display());
    let job = uuid::Uuid::new_v4().to_string();
    let lock = resource_locks::acquire_resource_lock(
        pool,
        &ResourceLockRequest {
            lock_key: key.clone(),
            holder_instance_id: "protected-campaign-v1".into(),
            holder_job_id: job.clone(),
            ttl_secs: Some(60),
            metadata: None,
        },
    )
    .await
    .map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        lock.acquired,
        "campaign workspace preparation is already owned"
    );
    let occupied = sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM kanban_cards WHERE verification_contract->'template'->>'workspace'=$1)")
        .bind(template.workspace.to_string_lossy().as_ref()).fetch_one(pool).await;
    match occupied {
        Ok(false) => {}
        other => {
            release(pool, &key, &job).await?;
            anyhow::bail!("campaign workspace is already assigned or cannot be checked: {other:?}");
        }
    }
    let artifacts = artifacts.clone();
    let node = node.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut inputs = BTreeMap::new();
        let mut manifest = BTreeMap::new();
        // Keep only one source tree plus the bounded selected input set in memory.
        let mut input_bytes = 0usize;
        for (dependency, artifact) in &artifacts {
            let tree = artifact.files().map_err(anyhow::Error::msg)?;
            for input in node.inputs.iter().filter(|i| &i.dependency == dependency) {
                let bytes = tree
                    .get(&input.source)
                    .ok_or_else(|| anyhow::anyhow!("verified dependency lacks {}", input.source))?;
                anyhow::ensure!(
                    !template.allowed_changes.contains(&input.destination),
                    "dependency inputs must not be writable outputs in the check contract"
                );
                input_bytes = input_bytes
                    .checked_add(bytes.len())
                    .ok_or_else(|| anyhow::anyhow!("input size overflow"))?;
                anyhow::ensure!(
                    input_bytes <= 32 * 1024 * 1024,
                    "campaign inputs exceed 32 MiB"
                );
                anyhow::ensure!(
                    inputs
                        .insert(input.destination.clone(), bytes.clone())
                        .is_none(),
                    "duplicate input path"
                );
                manifest.insert(
                    input.destination.clone(),
                    serde_json::json!({
                    "card_id":artifact.card_id(),"candidate_digest":artifact.digest(),
                    "source":input.source,"sha256":contract::digest(bytes)}),
                );
            }
        }
        anyhow::ensure!(
            inputs.len() == node.inputs.len(),
            "missing verified dependency input"
        );
        publication::install(&template.workspace, &template.artifact_root, &inputs)?;
        let contract = snapshot::enroll(template).map_err(anyhow::Error::msg)?;
        Ok::<_, anyhow::Error>((contract, serde_json::to_value(manifest)?))
    })
    .await;
    match result {
        Ok(Ok((contract, manifest))) => Ok(Prepared {
            contract,
            key,
            job,
            manifest,
        }),
        other => {
            release(pool, &key, &job).await?;
            match other {
                Ok(Err(e)) => Err(e),
                Err(e) => Err(e.into()),
                _ => unreachable!(),
            }
        }
    }
}
