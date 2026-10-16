"""Revise a manual goal without rewriting history or resetting its finite budget.

Only quiescent, artifact-only predecessors are eligible. The caller must pause
the predecessor and prove provider/queue quiescence before enrolling the result.
External-effect recovery remains the native reconciliation workflow's concern.
Dispatch inputs cover every implementation/rework attempt in the canonical
execution metrics. A task's latest observation cannot attest an earlier attempt;
predecessors needing historical observation recovery remain in the native path.
"""
from collections import Counter
import copy
import math


def verified_publications(state, tasks, dispatches):
    usage, metrics = state.get("budget_enforcement", {}), state.get("metrics", {})
    if type(usage) is not dict or type(metrics) is not dict:
        raise ValueError("Canonical publication and dispatch accounting are required")
    published = usage.get("published_tasks")
    if (usage.get("scope") != "campaign_lifetime" or type(published) is not int
            or published != len(state["tasks"]) or len(tasks) != published):
        raise ValueError("Complete canonical published-task accounting is required")
    if (type(metrics.get("schema_version")) is not int or metrics["schema_version"] != 1
            or metrics.get("time_basis") != "dispatch_lifetime"
            or type(metrics.get("roles")) is not list):
        raise ValueError("Canonical dispatch-lifetime metrics are required")
    expected = {}
    for row in metrics["roles"]:
        if type(row) is not dict:
            raise ValueError("Invalid canonical dispatch group")
        role, status, count = row.get("role"), row.get("status"), row.get("dispatches")
        if (type(role) is not str or not role or type(status) is not str or not status
                or type(count) is not int or not 0 < count <= 2**63 - 1
                or (role, status) in expected):
            raise ValueError("Invalid canonical dispatch group")
        expected[role, status] = count
    actual = Counter()
    for dispatch in dispatches:
        if dispatch.get("dispatch_type") not in {"implementation", "rework"}:
            raise ValueError("Implementation/rework dispatch inventory is required")
        task = tasks.get(dispatch["card"])
        if task is None:
            raise ValueError("Dispatch is outside the predecessor task inventory")
        actual[task["role"], dispatch["status"]] += 1
    if actual != expected:
        raise ValueError("Complete canonical dispatch-attempt accounting is required")
    return published


def successor_goal(predecessor, state, dispatches, desired, *, successor_id,
                   verified_tool_calls):
    if predecessor["status"] != "paused" or not state.get("enrolled"):
        raise ValueError("Predecessor must be paused and enrolled")
    old = state["goal"]
    if old.get("schedule") or desired.get("schedule"):
        raise ValueError("Manual occurrence succession cannot replace periodic parents")
    if not successor_id or successor_id == predecessor["id"]:
        raise ValueError("Successor needs a distinct identity")
    if old["repo"] != desired["repo"] or old["agents"] != desired["agents"]:
        raise ValueError("Scope correction cannot change repository or executors")
    if type(verified_tool_calls) is not int or verified_tool_calls < state["observed_tool_calls"]:
        raise ValueError("Verified provider tool usage is required; missing telemetry is not zero usage")
    tasks = {t["card_id"]: t for t in state["tasks"]}
    if len({d["id"] for d in dispatches}) != len(dispatches):
        raise ValueError("Duplicate dispatch accounting")
    published = verified_publications(state, tasks, dispatches)
    for dispatch in dispatches:
        if dispatch["status"] != "completed":
            raise ValueError("Only completed dispatches can be carried into a successor")
        task = tasks.get(dispatch["card"])
        if not task or not task.get("observation"):
            raise ValueError("Every dispatch needs its canonical captured observation")
        observation = task["observation"]
        if observation.get("dispatch_id") != dispatch["id"]:
            raise ValueError("Every dispatch needs its own canonical captured observation")
        if observation.get("external_effect_requires_reconciliation"):
            raise ValueError("Unreconciled external effect cannot be superseded")
        if task["role"] == "work":
            versions = [v for v in state["versions"] if v["version"] == task["version"]]
            nodes = [n for v in versions for n in v["graph"]["nodes"]
                     if n["task_key"] == task["task_key"]]
            if len(nodes) != 1 or nodes[0].get("effect") != "artifact_only":
                raise ValueError("Dispatched external or unknown effects must remain pinned")
    if set(tasks) != {d["card"] for d in dispatches}:
        raise ValueError("Unaccounted admitted task remains")
    budget = copy.deepcopy(old["budget"])
    elapsed = state["elapsed_dispatch_seconds"]
    if not math.isfinite(elapsed) or elapsed < 0:
        raise ValueError("Invalid elapsed accounting")
    used_evolutions = sum(bool(v.get("failure_id")) for v in state["versions"])
    # Native max_dispatches counts admitted logical tasks, not provider retries.
    budget["max_dispatches"] -= published
    budget["max_elapsed_secs"] -= math.ceil(elapsed)
    budget["max_evolutions"] -= used_evolutions
    if budget.get("max_observed_tool_calls") is not None:
        budget["max_observed_tool_calls"] -= verified_tool_calls
    if (budget["max_dispatches"] < 3 or budget["max_elapsed_secs"] < 60
            or budget["max_evolutions"] < 0
            or (budget.get("max_observed_tool_calls") is not None
                and budget["max_observed_tool_calls"] < 1)):
        raise ValueError("Insufficient remaining predecessor budget")
    budget["node_timeout_secs"] = min(budget["node_timeout_secs"], budget["max_elapsed_secs"])
    result = copy.deepcopy(desired)
    result["budget"] = budget
    result["constraints"] += [
        "This goal corrects the immutable scope of " + predecessor["id"]
        + ". Its failures and observations remain authoritative history. Inspect /api/campaigns/"
        + predecessor["id"] + "/execution and the referenced artifacts before planning. "
        "Independently reconcile existing target state before external effects. "
        "Do not infer no prior effects from absent daily markers.",
        "Use successor campaign identity " + successor_id
        + " in new logical task keys. Reuse predecessor evidence only after verifying its content and freshness; "
        "do not claim predecessor work is independently accepted. The finite budget already deducts predecessor usage.",
    ]
    return result
