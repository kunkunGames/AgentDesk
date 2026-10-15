use super::super::{EndpointDoc, body_param, ep, path_param};
use serde_json::json;

pub(super) fn endpoints() -> Vec<EndpointDoc> {
    vec![
        ep("GET", "/api/campaigns/{id}/operations", "campaigns",
            "Inspect native_operations_v1 routine adoption and the latest 20 run outcomes. Source rows stay paused and the Campaign owns admission. This profile exposes native routine lifecycle evidence with managed_execution_v1:false. Agent-authored graph execution is inspected via /execution.")
            .with_params([("id", path_param("Campaign ID."))])
            .with_example(json!({"path":{"id":"release-a"}}), json!({"profile":"native_operations_v1","definition":null,"runs":[],"managed_execution_v1":false})),
        ep("POST", "/api/campaigns/{id}/operations", "campaigns",
            "Atomically adopt an eligible quiescent Qwen/OpenCode routine into native_operations_v1. Requires enabled routine runtime, campaign revision and exact source update time. Leaves the source paused. Detached, already adopted or running sources are rejected. Adoption reuses the routine lifecycle; goal-driven DAG generation requires native /execution enrollment.")
            .with_params([
                ("id", path_param("Campaign ID.")),
                ("routine_id", body_param("string", true, "Eligible source routine ID.")),
                ("node_id", body_param("string", true, "Campaign node identity.")),
                ("expected_revision", body_param("integer", true, "Current campaign revision.")),
                ("expected_routine_updated_at", body_param("string", true, "Exact observed RFC3339 source update timestamp.")),
                ("next_due_at", body_param("string", true, "RFC3339 next admission time.")),
            ]),
    ]
}
