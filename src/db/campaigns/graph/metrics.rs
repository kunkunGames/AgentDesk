//! Versioned execution profiles and observed provider facts remain distinct.
use anyhow::Result;
use serde_json::{Value, json};
use sqlx::PgPool;

pub async fn inspection(pool: &PgPool, id: &str) -> Result<Value> {
    let roles: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('role',t.role,'status',d.status,'dispatches',count(*),'elapsed_seconds',COALESCE(sum(GREATEST(0,extract(epoch from(COALESCE(d.completed_at,CASE WHEN d.status IN('completed','failed','cancelled','rejected') THEN d.updated_at ELSE now() END)-d.created_at)))),0)) FROM campaign_graph_tasks t JOIN task_dispatches d ON d.kanban_card_id=t.card_id WHERE t.campaign_id=$1 AND d.dispatch_type IN('implementation','rework') GROUP BY t.role,d.status ORDER BY t.role,d.status")
        .bind(id).fetch_all(pool).await?;
    let profiles: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('task_key',t.task_key,'role',t.role,'accepted',t.accepted,'profile',t.execution_profile,'observed_provider',o.provider,'observed_model',o.model,'observed_usage',o.usage) FROM campaign_graph_tasks t LEFT JOIN task_dispatches d ON d.id=t.observation->>'dispatch_id' LEFT JOIN campaign_dispatch_observations o ON o.dispatch_id=d.id WHERE t.campaign_id=$1 ORDER BY t.created_at DESC LIMIT 200")
        .bind(id).fetch_all(pool).await?;
    let failures: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('category',category,'count',count(*)) FROM campaign_graph_failures WHERE campaign_id=$1 GROUP BY category ORDER BY category")
        .bind(id).fetch_all(pool).await?;
    Ok(
        json!({"schema_version":1,"time_basis":"dispatch_lifetime","cost":null,"roles":roles,"profiles":profiles,"failure_counts":failures}),
    )
}

/// Called only after the watcher proves the pinned turn still owns dispatch.
/// Accept provider control events, never models/usage inside assistant prose.
pub fn control_observation(value: &Value) -> Option<Value> {
    let source =
        if value["type"] == "result" || (value["type"] == "system" && value["subtype"] == "init") {
            value
        } else if value["type"] == "assistant" && value["message"]["model"].is_string() {
            &value["message"]
        } else {
            return None;
        };
    let model = source.get("model").and_then(Value::as_str).filter(|s| {
        !s.is_empty()
            && s.len() <= 160
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.:/".contains(&b))
    });
    let mut usage = serde_json::Map::new();
    if let Some(raw) = source.get("usage") {
        for key in [
            "input_tokens",
            "output_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
        ] {
            if let Some(n) = raw
                .get(key)
                .and_then(Value::as_u64)
                .filter(|n| *n <= 1_000_000_000)
            {
                usage.insert(key.into(), json!(n));
            }
        }
    }
    if model.is_none() && usage.is_empty() {
        return None;
    }
    Some(json!({"model":model,"usage":if usage.is_empty(){Value::Null}else{json!(usage)}}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prose_cannot_report_a_model_or_billing_and_missing_usage_stays_unknown() {
        assert!(
            control_observation(
                &json!({"type":"assistant","model":"fake","usage":{"input_tokens":999}})
            )
            .is_none()
        );
        let o = control_observation(
            &json!({"type":"system","subtype":"init","model":"verified-model"}),
        )
        .unwrap();
        assert!(o["usage"].is_null());
        let o = control_observation(
            &json!({"type":"result","usage":{"input_tokens":12,"output_tokens":8,"price":100}}),
        )
        .unwrap();
        assert_eq!(o["usage"]["output_tokens"], 8);
        assert!(o["usage"].get("price").is_none());
    }
}
