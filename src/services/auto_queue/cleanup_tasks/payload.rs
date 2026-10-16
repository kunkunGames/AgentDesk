//! Durable cleanup payload decoding keeps malformed ownership evidence fail-closed.

use sqlx::Row;

use super::RunCleanupTask;

pub(super) fn parse_card_rollback_dispatch_id(
    item: &serde_json::Value,
) -> Result<Option<String>, String> {
    match item.get("dispatch_id") {
        Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(dispatch_id)) => Ok(Some(dispatch_id.clone())),
        None => Err("missing dispatch_id".to_string()),
        Some(_) => Err("invalid dispatch_id: expected null or string".to_string()),
    }
}

pub(super) fn task_from_row(row: &sqlx::postgres::PgRow) -> Result<RunCleanupTask, String> {
    let released_slots: serde_json::Value = row
        .try_get("released_slots")
        .map_err(|error| format!("decode auto-queue cleanup released slots: {error}"))?;
    let pending_emits: serde_json::Value = row
        .try_get("pending_emits")
        .map_err(|error| format!("decode auto-queue cleanup pending emits: {error}"))?;
    let card_rollback_tasks_json: serde_json::Value = row
        .try_get("card_rollback_tasks")
        .map_err(|error| format!("decode auto-queue cleanup card rollback tasks: {error}"))?;
    let card_rollback_tasks: Vec<(String, Option<String>)> = card_rollback_tasks_json
        .as_array()
        .ok_or_else(|| "card_rollback_tasks is not an array".to_string())?
        .iter()
        .map(|item| {
            let card_id = item
                .get("card_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "missing or invalid card_id".to_string())?
                .to_string();
            let dispatch_id = parse_card_rollback_dispatch_id(item)?;
            Ok((card_id, dispatch_id))
        })
        .collect::<Result<Vec<_>, String>>()
        .map_err(|error| format!("parse auto-queue cleanup card rollback tasks: {error}"))?;
    Ok(RunCleanupTask {
        id: row
            .try_get("id")
            .map_err(|error| format!("decode auto-queue cleanup id: {error}"))?,
        run_ids: row
            .try_get("run_ids")
            .map_err(|error| format!("decode auto-queue cleanup run ids: {error}"))?,
        dispatch_ids: row
            .try_get("dispatch_ids")
            .map_err(|error| format!("decode auto-queue cleanup dispatch ids: {error}"))?,
        released_slots: serde_json::from_value(released_slots)
            .map_err(|error| format!("parse auto-queue cleanup released slots: {error}"))?,
        pending_emits: serde_json::from_value(pending_emits)
            .map_err(|error| format!("parse auto-queue cleanup pending emits: {error}"))?,
        emitted: row
            .try_get("emitted")
            .map_err(|error| format!("decode auto-queue cleanup emitted flag: {error}"))?,
        card_rollback_tasks,
        card_rollback_source: row
            .try_get("card_rollback_source")
            .map_err(|error| format!("decode auto-queue cleanup card rollback source: {error}"))?,
    })
}
