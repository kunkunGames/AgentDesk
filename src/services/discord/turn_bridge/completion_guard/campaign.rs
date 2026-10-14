//! Transport a native graph result through the existing completion authority.
//! This records provider output; graph/artifact acceptance is a later operation.
use super::*;

pub(super) async fn complete_if_enrolled(
    shared: &Arc<super::super::super::SharedData>,
    dispatch_id: &str,
    adk_cwd: Option<&str>,
) -> bool {
    let Some(pool) = shared.pg_pool.as_ref() else {
        return false;
    };
    let enrolled = sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN task_dispatches d ON d.kanban_card_id=t.card_id WHERE d.id=$1)")
        .bind(dispatch_id).fetch_one(pool).await;
    match enrolled {
        Ok(false) => return false,
        Err(error) => {
            tracing::warn!(%dispatch_id,%error,"cannot determine Campaign completion ownership");
            return true;
        }
        Ok(true) => {}
    }
    let receipt = match read_pinned_result(pool, dispatch_id, adk_cwd).await {
        Ok(receipt) => receipt,
        Err(error) => {
            fail_dispatch_with_retry(
                shared.api_port,
                Some(dispatch_id),
                &format!("Campaign completion receipt rejected: {error}"),
            )
            .await;
            return true;
        }
    };
    let result = async {
        let engine = shared
            .policy
            .engine
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("completion engine unavailable"))?;
        crate::dispatch::finalize_dispatch_with_backends(
            engine,
            dispatch_id,
            "campaign_native_receipt",
            Some(&receipt),
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let _ = queue_dispatch_followup_with_handles(
            shared.pg_pool.as_ref(),
            dispatch_id,
            "campaign_native_receipt",
        )
        .await;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = result {
        fail_dispatch_with_retry(
            shared.api_port,
            Some(dispatch_id),
            &format!("Campaign completion receipt rejected: {error}"),
        )
        .await;
    }
    true
}

/// Both turn-end and streaming finalization must use the same pinned receipt.
/// An error cannot fall through to ordinary prose-based completion.
pub(in crate::services::discord) async fn native_campaign_result(
    pool: &sqlx::PgPool,
    dispatch_id: &str,
    adk_cwd: Option<&str>,
) -> anyhow::Result<Option<serde_json::Value>> {
    let enrolled = sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN task_dispatches d ON d.kanban_card_id=t.card_id WHERE d.id=$1)")
        .bind(dispatch_id).fetch_one(pool).await?;
    if !enrolled {
        return Ok(None);
    }
    Ok(Some(read_pinned_result(pool, dispatch_id, adk_cwd).await?))
}

async fn read_pinned_result(
    pool: &sqlx::PgPool,
    dispatch_id: &str,
    adk_cwd: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    let workspace = adk_cwd
        .ok_or_else(|| anyhow::anyhow!("Campaign workspace unavailable"))?
        .to_owned();
    let context: Option<String> =
        sqlx::query_scalar("SELECT context FROM task_dispatches WHERE id=$1")
            .bind(dispatch_id)
            .fetch_one(pool)
            .await?;
    let context: serde_json::Value = serde_json::from_str(&context.unwrap_or_else(|| "{}".into()))?;
    let context = if let Some(s) = context.as_str() {
        serde_json::from_str(s)?
    } else {
        context
    };
    let canonical = context["worktree_path"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("dispatch has no canonical workspace"))?
        .to_owned();
    let created_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT created_at FROM task_dispatches WHERE id=$1")
            .bind(dispatch_id)
            .fetch_one(pool)
            .await?;
    let receipt = tokio::task::spawn_blocking(move || -> anyhow::Result<serde_json::Value> {
        use anyhow::ensure;
        let root = std::path::Path::new(&workspace).canonicalize()?;
        ensure!(
            root == std::path::Path::new(&canonical).canonicalize()?,
            "completion workspace differs from dispatch"
        );
        ensure!(
            tracked_change_summary(Some(&workspace)).is_none(),
            "commit tracked changes before completing a graph task"
        );
        let modified: chrono::DateTime<chrono::Utc> =
            std::fs::symlink_metadata(root.join(".agentdesk-campaign-result.json"))?
                .modified()?
                .into();
        ensure!(modified >= created_at, "receipt predates its dispatch");
        read_result(&root)
    })
    .await??;
    Ok(receipt)
}

fn read_result(root: &std::path::Path) -> anyhow::Result<serde_json::Value> {
    use anyhow::ensure;
    let path = root.join(".agentdesk-campaign-result.json");
    let meta = std::fs::symlink_metadata(&path)?;
    ensure!(
        meta.is_file() && !meta.file_type().is_symlink(),
        "invalid Campaign result file"
    );
    let limit = crate::db::campaigns::graph::receipt::MAX_RESULT_FILE_BYTES;
    ensure!(
        meta.len() > 0 && meta.len() <= limit,
        "Campaign result file has {} bytes; required 1..{} UTF-8 bytes",
        meta.len(),
        limit
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(meta.nlink() == 1, "hardlinked Campaign receipt rejected");
    }
    let bytes = std::fs::read(path)?;
    ensure!(
        bytes.len() as u64 == meta.len(),
        "Campaign receipt changed during read"
    );
    let proposed: serde_json::Value = serde_json::from_slice(&bytes)?;
    crate::db::campaigns::graph::receipt::validate_envelope(&proposed)?;
    crate::db::campaigns::graph::schema::validate_result(&proposed)?;
    let mut result = proposed;
    // Structured provider evidence does not imply business acceptance.
    result["work_outcome"] = serde_json::json!("artifact");
    result["summary"] = serde_json::json!(crate::db::campaigns::graph::result::CAPTURE_SUMMARY);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_receipt_preserves_structured_result_and_rejects_authority_fields() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join(".agentdesk-campaign-result.json");
        std::fs::write(
            &path,
            r#"{"campaign_receipt":{"outcome":"blocked","summary":"blocked"},"campaign_graph":{"nodes":[]}}"#,
        )
        .unwrap();
        let result = read_result(root.path()).unwrap();
        assert_eq!(result["campaign_receipt"]["outcome"], "blocked");
        assert!(result["campaign_graph"].is_object());
        std::fs::write(&path, r#"{"campaign_receipt":{},"status":"completed"}"#).unwrap();
        assert!(read_result(root.path()).is_err());

        let limit = crate::db::campaigns::graph::receipt::MAX_RESULT_FILE_BYTES as usize;
        let mut boundary =
            serde_json::json!({"campaign_receipt":{"outcome":"blocked","summary":""}});
        let header_bytes = boundary.to_string().len();
        boundary["campaign_receipt"]["summary"] =
            serde_json::json!("x".repeat(limit - header_bytes));
        let bytes = serde_json::to_vec(&boundary).unwrap();
        assert_eq!(bytes.len(), limit);
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            read_result(root.path()).unwrap()["campaign_receipt"]["outcome"],
            "blocked"
        );
        boundary["campaign_receipt"]["summary"] =
            serde_json::json!("x".repeat(limit - header_bytes + 1));
        std::fs::write(&path, boundary.to_string()).unwrap();
        let error = read_result(root.path()).unwrap_err().to_string();
        assert!(
            error.contains("262145 bytes") && error.contains("262144"),
            "{error}"
        );

        // UTF-8 file bytes, rather than character count, govern Korean receipts.
        boundary["campaign_receipt"]["summary"] =
            serde_json::json!("한".repeat((limit - header_bytes) / 3 + 1));
        assert!(boundary.to_string().chars().count() < limit);
        std::fs::write(&path, boundary.to_string()).unwrap();
        assert!(read_result(root.path()).is_err());
    }

    #[test]
    fn native_receipt_capture_round_trips_schema_and_rejects_malformed_fields() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join(".agentdesk-campaign-result.json");
        let proposed = serde_json::json!({
            "campaign_receipt": {
                "outcome": "passed", "summary": "Planning inputs inspected",
                "observations": [{"command": "inspect", "exit_code": 0, "evidence": "fixture inputs"}]
            },
            "campaign_graph": {"nodes": []}
        });
        std::fs::write(&path, proposed.to_string()).unwrap();
        let mut captured = read_result(root.path()).unwrap();
        assert_eq!(captured["campaign_graph"], proposed["campaign_graph"]);
        assert_eq!(captured["campaign_receipt"], proposed["campaign_receipt"]);
        assert_eq!(captured["work_outcome"], "artifact");
        captured["completion_source"] = serde_json::json!("campaign_native_receipt");
        crate::db::campaigns::graph::result::validate_dispatch(&captured).unwrap();
        assert!(crate::db::campaigns::graph::schema::validate_result(&captured).is_err());

        for invalid in [
            serde_json::json!({"campaign_receipt": {"outcome": "passed", "summary": 42}}),
            serde_json::json!({"campaign_receipt": {"outcome": "blocked", "summary": "blocked"},
                "graph_validation": {"digest": "d", "approved": "true", "reason": "checked"}}),
        ] {
            std::fs::write(&path, invalid.to_string()).unwrap();
            assert!(read_result(root.path()).is_err());
        }
    }
}
