use super::super::{EndpointDoc, body_param, ep, path_param};
use serde_json::json;

pub(super) fn endpoints() -> Vec<EndpointDoc> {
    vec![
        ep("GET", "/api/managed-checks/{id}", "kanban",
            "Inspect a card's protected verification contract, state and completion disposition. Requires the configured server admin bearer and canonical PostgreSQL. A model-provided verdict is not a protected passing receipt.")
            .with_params([("id", path_param("Kanban card ID."))])
            .with_example(json!({"path":{"id":"card-id"}}), json!({"contract":null,"state":null,"disposition":"unmanaged"})),
        ep("POST", "/api/managed-checks/{id}/enroll", "kanban",
            "Enroll a card using a named entry in the protected AGENTDESK_CHECK_REGISTRY. Requires the server admin bearer. The request cannot provide commands or passing receipts; the server snapshots the protected template and binds its immutable digest.")
            .with_params([
                ("id", path_param("Kanban card ID.")),
                ("template", body_param("string", true, "Existing protected registry template name.")),
            ]),
        ep("POST", "/api/managed-checks/{id}/approve", "kanban",
            "Record candidate-specific human approval only after protected checks pass for a human-review contract. Requires the server admin bearer and exact candidate, dispatch and contract digests. Stale identity or incomplete checks return 409; canonical completion remains owned by the checker and card FSM.")
            .with_params([
                ("id", path_param("Kanban card ID.")),
                ("candidate_digest", body_param("string", true, "Exact current candidate digest.")),
                ("dispatch_id", body_param("string", true, "Dispatch that produced the checked candidate.")),
                ("contract_digest", body_param("string", true, "Exact immutable verification contract digest.")),
                ("reason", body_param("string", true, "Nonempty operator approval reason, at most 1000 UTF-8 bytes.")),
            ]),
    ]
}
