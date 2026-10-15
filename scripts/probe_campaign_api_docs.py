"""Verify native API discovery against the HTTP response of the canary build."""
from probe_test_phase_lifecycle import require


def check(service, report):
    status, docs = service.request("docs?format=flat")
    require(status == 200, "native API documentation unavailable")
    endpoints = {(entry["method"], entry["path"]): entry
                 for entry in docs["endpoints"]}
    campaign = "/api/campaigns/{id}"
    required = {
        ("GET", campaign + "/execution"),
        ("POST", campaign + "/execution"),
        ("POST", campaign + "/execution/activate"),
        ("POST", campaign + "/execution/reconcile"),
        ("POST", campaign + "/execution/revise"),
        ("POST", campaign + "/execution/budget"),
        ("GET", campaign + "/operations"),
        ("POST", campaign + "/operations"),
        ("GET", "/api/managed-checks/{id}"),
        ("POST", "/api/managed-checks/{id}/enroll"),
        ("POST", "/api/managed-checks/{id}/approve"),
    }
    require(required <= endpoints.keys(),
            f"native API documentation missing: {sorted(required - endpoints.keys())}")
    for identity in required:
        entry = endpoints[identity]
        require(bool(entry["description"]), f"missing API contract: {identity}")
        require(entry["params"]["id"]["location"] == "path",
                f"missing API path identity: {identity}")
    revision = endpoints[("POST", campaign + "/execution/revise")]
    for field in ["expected_revision", "request_id", "evidence", "goal"]:
        parameter = revision["params"][field]
        require(parameter["required"] and parameter["location"] == "body",
                f"goal revision documentation lost required field: {field}")
    budget = endpoints[("POST", campaign + "/execution/budget")]
    for field in ["expected_revision", "request_id", "evidence", "budget"]:
        parameter = budget["params"][field]
        require(parameter["required"] and parameter["location"] == "body",
                f"budget revision documentation lost required field: {field}")
    report["checks"].append("native_campaign_and_managed_check_api_discovery")
