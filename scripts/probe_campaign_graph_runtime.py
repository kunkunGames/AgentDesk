"""Native HTTP/storage canary. Deliberately does not claim provider execution."""
from datetime import datetime, timedelta, timezone
import copy
from probe_test_phase_lifecycle import require


def check(service, sql, report, *, revise=False):
    sql("INSERT INTO agents(id,name,provider) VALUES"
        "('graph-qwen','Graph Qwen','qwen'),('graph-opencode','Graph OpenCode','opencode')")
    identifier = "native-graph-contract-canary"
    status, created = service.request("campaigns", {
        "id": identifier, "title": "Native graph contract canary",
        "status": "active", "round": 1, "nodes": [],
    })
    require(status == 201, "graph canary Campaign creation failed")
    future = datetime.now(timezone.utc) + timedelta(hours=2)
    body = {"expected_revision": created["campaign"]["revision"], "goal": {
        "objective": "Verify native goal persistence without executing an agent",
        "success": ["Goal and schedule survive restart"],
        "constraints": ["No provider execution or external effects"],
        "repo": "canary/repository", "planner": "graph-qwen", "verifier": "graph-opencode",
        "agents": ["graph-opencode", "graph-qwen"],
        "budget": {"max_dispatches": 6, "max_elapsed_secs": 300,
                   "node_timeout_secs": 60, "max_evolutions": 1, "max_observed_tool_calls": None},
        "schedule": {"timezone": "UTC", "daily_minutes": [future.hour * 60 + future.minute]},
    }}
    path = "campaigns/" + identifier + "/execution"
    status, state = service.request(path)
    require(status == 200 and state["enrolled"] is False, "unenrolled state is not explicit")
    limits = {"max_file_bytes": 4194304, "max_bundle_bytes": 16777216,
              "max_outputs_per_task": 16, "max_inputs_per_task": 32}
    require(state["artifact_limits"] == limits, "native handoff limits unavailable before enrollment")
    status, _ = service.request(path, body)
    require(status == 409, "internal graph execution must be disabled by default")
    status, _ = service.request("settings/runtime-config", {"internalTasksEnabled": True}, method="PUT")
    require(status == 200, "internal task opt-in failed")
    status, _ = service.request(path, body)
    require(status == 200, "native graph enrollment failed")
    service.stop()
    service.start()
    status, state = service.request(path)
    require(status == 200 and state["enrolled"] is True, "graph goal lost across restart")
    require(state["artifact_limits"] == limits, "native handoff limits lost across restart")
    report["checks"].append("native_artifact_limits_discovery_and_restart")
    require(state["goal"] == body["goal"], "stored graph contract changed")
    require(state["occurrence_budget"] == body["goal"]["budget"],
            "occurrence policy was not pinned at enrollment or did not survive restart")
    require(not state["tasks"] and not state["occurrences"], "future schedule executed prematurely")
    report["checks"].extend([
        "graph_http_requires_explicit_internal_opt_in",
        "native_graph_goal_and_schedule_survive_restart",
        "future_schedule_does_not_admit_historical_work",
    ])
    if revise:
        correction = {"expected_revision": created["campaign"]["revision"],
                      "request_id": "native-scope-correction",
                      "evidence": "Canary verifies audited goal correction without provider work",
                      "goal": copy.deepcopy(body["goal"])}
        correction["goal"]["constraints"].append("Preserve every configured business destination")
        status, _ = service.request(path + "/revise", correction, authenticated=False)
        require(status == 401, "goal revision accepted unauthenticated access")
        status, _ = service.request(path + "/revise", correction)
        require(status == 409, "goal revision accepted an active campaign")
        status, current = service.request("campaigns/" + identifier)
        require(status == 200, "could not read current campaign revision")
        doc = current["campaign"]
        update = {k: doc[k] for k in ["title", "description", "status", "round", "nodes"]}
        update.update(status="paused", expected_revision=doc["revision"])
        status, paused = service.request("campaigns/" + identifier, update, method="PUT")
        require(status == 200, "canary campaign pause failed")
        correction["expected_revision"] = paused["campaign"]["revision"]
        status, revised = service.request(path + "/revise", correction)
        require(status == 200 and revised["independent_validation_required"], "goal correction failed")
        status, repeated = service.request(path + "/revise", correction)
        require(status == 200 and repeated == revised, "goal correction is not idempotent")
        service.stop()
        service.start()
        status, state = service.request(path)
        require(status == 200 and state["goal"] == correction["goal"], "revised goal lost at restart")
        require(len(state["goal_revisions"]) == 1, "revision history missing or duplicated")
        require(state["goal_revisions"][0]["previous_goal"] == body["goal"], "original goal not retained")
        require(not state["tasks"] and not state["occurrences"], "correction admitted unintended work")
        report["checks"].append("audited_goal_revision_auth_pause_idempotency_and_restart")
        status, current = service.request("campaigns/" + identifier)
        require(status == 200, "could not read budget correction revision")
        increase = {"expected_revision": current["campaign"]["revision"],
                    "request_id": "native-finite-budget-correction",
                    "evidence": "Canary verifies finite increase without changing published scope",
                    "budget": copy.deepcopy(state["goal"]["budget"])}
        increase["budget"].update(max_dispatches=8, max_evolutions=2)
        increase["budget"]["max_elapsed_secs"] *= 2
        status, _ = service.request(path + "/budget", increase, authenticated=False)
        require(status == 401, "budget increase accepted unauthenticated access")
        status, revised_budget = service.request(path + "/budget", increase)
        require(status == 200 and revised_budget["contracts_preserved"], "finite budget increase failed")
        status, repeated = service.request(path + "/budget", increase)
        require(status == 200 and repeated == revised_budget, "budget increase is not idempotent")
        service.stop()
        service.start()
        status, after_budget = service.request(path)
        require(status == 200, "budget inspection unavailable after restart")
        expected = copy.deepcopy(state["goal"])
        expected["budget"] = increase["budget"]
        require(after_budget["goal"] == expected, "budget change altered immutable scope")
        require(after_budget["occurrence_budget"] == body["goal"]["budget"],
                "parent budget increase enlarged recurring business policy")
        for field in ["tasks", "versions", "failures", "observed_tool_calls", "elapsed_dispatch_seconds"]:
            require(after_budget[field] == state[field], f"budget change lost retained {field}")
        require(len(after_budget["goal_revisions"]) == 2, "budget history missing or duplicated")
        report["checks"].append("finite_budget_revision_auth_contracts_usage_idempotency_and_restart")
        report["checks"].append("native_parent_budget_increase_preserves_occurrence_policy_across_restart")
