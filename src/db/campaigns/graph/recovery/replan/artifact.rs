//! A lost artifact result permits new planning, never receipt acceptance or replay.
use super::Effect;
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{Row, postgres::PgRow};

pub(super) fn terminal(
    failure: &PgRow,
    effect: &Effect,
    evidence: &Value,
    dispatch: &PgRow,
) -> Result<()> {
    ensure!(
        effect == &Effect::ArtifactOnly
            && evidence["external_effect_requires_reconciliation"] == false
            && matches!(
                failure.get::<String, _>("category").as_str(),
                "unknown" | "infrastructure"
            )
            && matches!(
                dispatch.get::<String, _>("status").as_str(),
                "failed" | "cancelled"
            ),
        "only a terminal unaccepted artifact execution failure may request fresh planning"
    );
    // A failed transport must not conceal an already captured successful result.
    // Keep every raw/error/proposal byte as history; this path accepts none of it.
    let raw: Option<String> = dispatch.get("result");
    if let Some(raw) = raw
        && let Ok(mut captured) = serde_json::from_str::<Value>(&raw)
    {
        if let Some(encoded) = captured.as_str()
            && let Ok(value) = serde_json::from_str(encoded)
        {
            captured = value;
        }
        ensure!(
            !matches!(
                captured["campaign_receipt"]["outcome"].as_str(),
                Some("passed" | "no_work")
            ),
            "a captured successful native result requires separate reconciliation"
        );
    }
    Ok(())
}
