//! Materialize frozen superior-review inputs before provider execution.
use crate::db::campaigns::graph::{Input, learning::template::INPUT_FILE};
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;

pub(crate) fn review_input(manifest: Value) -> (Vec<Input>, BTreeMap<String, Value>) {
    file_input(manifest, INPUT_FILE)
}

fn file_input(manifest: Value, file: &str) -> (Vec<Input>, BTreeMap<String, Value>) {
    (
        vec![Input {
            dependency: "template-context".into(),
            source: file.into(),
            destination: file.into(),
        }],
        [("template-context".into(), manifest)].into(),
    )
}

pub(crate) async fn prepare_input(pool: &PgPool, dispatch: &str) -> Result<()> {
    let row = sqlx::query("SELECT t.role,
          COALESCE(ca.input_manifest,CASE WHEN t.role IN ('plan','manage','finish') THEN t.execution_profile->'control_input_manifest' END) AS input_manifest,
          d.context::text AS context
        FROM campaign_graph_tasks t LEFT JOIN campaign_dag_control_assignments ca ON ca.task_key=t.task_key
        JOIN task_dispatches d ON d.kanban_card_id=t.card_id WHERE d.id=$1
          AND (ca.input_manifest IS NOT NULL OR (t.role IN ('plan','manage','finish') AND t.execution_profile->'control_input_manifest' IS NOT NULL))")
        .bind(dispatch).fetch_optional(pool).await?;
    let Some(row) = row else { return Ok(()) };
    let context: Option<String> = row.get("context");
    let mut context: Value = serde_json::from_str(context.as_deref().unwrap_or("null"))?;
    if let Some(encoded) = context.as_str() {
        context = serde_json::from_str(encoded)?;
    }
    let workspace = context["worktree_path"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("control input workspace unavailable"))?
        .to_owned();
    let file = match row.get::<String, _>("role").as_str() {
        "plan" => crate::db::campaigns::graph::planning_input::INPUT_FILE,
        "manage" | "finish" => crate::db::campaigns::graph::workflow::INPUT_FILE,
        _ => INPUT_FILE,
    };
    let manifest: Value = row.get("input_manifest");
    validate_input(&manifest, file)?;
    let (inputs, manifests) = file_input(manifest, file);
    let root = crate::config::runtime_root()
        .ok_or_else(|| anyhow::anyhow!("runtime root unavailable"))?
        .join("artifacts/campaign-graphs");
    tokio::task::spawn_blocking(move || {
        super::campaign_artifacts::materialize_inputs(
            &root,
            std::path::Path::new(&workspace),
            &inputs,
            &manifests,
        )
    })
    .await??;
    Ok(())
}

fn validate_input(manifest: &Value, file: &str) -> Result<()> {
    ensure!(
        manifest["files"]
            .as_object()
            .is_some_and(|files| files.len() == 1 && files.get(file).is_some_and(Value::is_string)),
        "control input must contain only its canonical frozen file"
    );
    Ok(())
}

pub(crate) async fn verify_planning_input(pool: &PgPool, key: &str, context: &Value) -> Result<()> {
    let row = sqlx::query("SELECT execution_profile,role FROM campaign_graph_tasks WHERE task_key=$1 AND role IN ('plan','manage','finish')")
    .bind(key)
    .fetch_optional(pool)
    .await?
    ;
    let Some(row) = row else {
        return Ok(());
    };
    let Some(profile) = row.get::<Option<Value>, _>("execution_profile") else {
        return Ok(());
    };
    let Some(manifest) = profile.get("control_input_manifest").cloned() else {
        return Ok(());
    };
    ensure!(
        profile["control_input_digest"] == crate::db::campaigns::graph::contract_digest(&manifest)?,
        "frozen planning input manifest differs"
    );
    let file = if row.get::<String, _>("role") == "plan" {
        crate::db::campaigns::graph::planning_input::INPUT_FILE
    } else {
        crate::db::campaigns::graph::workflow::INPUT_FILE
    };
    validate_input(&manifest, file)?;
    let workspace = context["worktree_path"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("planning input workspace unavailable"))?
        .to_owned();
    let root = crate::config::runtime_root()
        .ok_or_else(|| anyhow::anyhow!("runtime root unavailable"))?
        .join("artifacts/campaign-graphs");
    let (inputs, manifests) = file_input(manifest, file);
    tokio::task::spawn_blocking(move || {
        super::campaign_artifacts::verify_input_files(
            &root,
            std::path::Path::new(&workspace),
            &inputs,
            &manifests,
        )
    })
    .await??;
    Ok(())
}
