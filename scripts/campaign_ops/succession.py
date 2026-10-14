"""Revise a manual goal without rewriting history or resetting its finite budget.

Only quiescent, artifact-only predecessors are eligible. The caller must pause
the predecessor and prove provider/queue quiescence before enrolling the result.
External-effect recovery remains the native reconciliation workflow's concern.
"""
import copy
import math


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
    if not isinstance(verified_tool_calls, int) or verified_tool_calls < state["observed_tool_calls"]:
        raise ValueError("Verified provider tool usage is required; missing telemetry is not zero usage")
    tasks = {t["card_id"]: t for t in state["tasks"]}
    if len({d["id"] for d in dispatches}) != len(dispatches):
        raise ValueError("Duplicate dispatch accounting")
    for dispatch in dispatches:
        if dispatch["status"] != "completed":
            raise ValueError("Only completed dispatches can be carried into a successor")
        task = tasks.get(dispatch["card"])
        if not task or not task.get("observation"):
            raise ValueError("Every dispatch needs its canonical captured observation")
        observation = task["observation"]
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
    budget["max_dispatches"] -= len(dispatches)
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
