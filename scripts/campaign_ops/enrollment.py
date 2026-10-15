"""Translate source intent into immutable native goals, never into fixed DAGs."""
from .workflows import OBJECTIVES, SCRIPT_REFS, SLOTS, SOURCES


def goal(job, root):
    if job not in SOURCES:
        raise ValueError("Only the six historically eligible routines may be enrolled")
    home = root.parent.parent
    source = (root / "routines" if job == "health" else home / ".adk/local-routines") / SCRIPT_REFS[job]
    planner = "monitoring" if job in ("health", "monolith", "agentdesk-merge") else "qwen"
    operations = (
        "Use the documented skill-sync operations for the existing configured provider destinations and every target in "
        + str(home / ".codex/skills/skill-sync/config/mirror_targets.json")
        + ". Preserve their allowlists and existing authentication; do not add repositories, widen credentials, or bypass guards. "
        "Treat each configured mirror as a separately attributable effect and independently verify its commit/push or unchanged state. "
        "Backup acceptance is verified configured mirror contents, source provenance and remote commit evidence. "
        "Inspect and reuse the underlying sync/regression/mirror functions in DAG nodes; keep the daily duplicate guard and disable routing/memory writes."
        if job == "skills" else
        "For GitHub PR operations use the existing repository-scoped helper "
        + str(home / ".adk/local-routines/bin/routine-gh.py") + "."
    )
    return {
        "objective": OBJECTIVES[job] + " Read " + str(source)
        + " as business requirements. Analyze current observations yourself and generate the node graph; do not execute the old routine as a single task. "
        + operations,
        "success": ["Current observations determine actionable tasks or an independently verified no-work conclusion",
                    "Real artifacts and independent checks validate acceptance before downstream effects",
                    "External actions have an attributable target and current-head evidence"]
                    + (["Every configured skill mirror and provider destination is accounted for; required targets cannot be silently omitted",
                        "Real eligible skill regressions and independent rendered-content checks pass; failures remain explicit",
                        "Configured mirror backup contents and remote commits are independently verified, or all unchanged targets have fresh no-work evidence"] if job == "skills" else []),
        "constraints": ["Use only the enrolled Qwen CLI and OpenCode CLI executors",
                        "Preserve completed effects, in-flight ownership and unknown outcomes; reconcile before replay",
                        "No credential discovery, permission widening, service deployment, runtime DB writes or unrequested external messages",
                        ("Scope this occurrence to the existing skill source, configured provider destinations and all configured mirror targets; no admin bypass, force push or branch deletion"
                         if job == "skills" else "At most one PR or bounded target per occurrence; no admin bypass, force push or branch deletion"),
                        ("Use the assigned internal repository workspace for execution and artifacts. This does not remove configured business destinations: include the existing provider targets selected by the source routine's --workspace argument, without changing cwd or adding destinations"
                         if job == "skills" else "Use the assigned internal workspace; source routine paths must not override the configured repository")],
        "repo": "kunkunGames/monolith" if job == "monolith" else "kunkunGames/AgentDesk",
        "planner": planner, "verifier": "qwen" if planner == "monitoring" else "monitoring",
        "agents": ["monitoring", "qwen"],
        "budget": {"max_dispatches": 20, "max_elapsed_secs": 1800 if job == "health" else 10800,
                   "node_timeout_secs": 600 if job == "health" else 1800,
                   "max_evolutions": 8, "max_observed_tool_calls": 300},
        "schedule": {"timezone": "Asia/Seoul", "daily_minutes": sorted(h * 60 + m for h, m in SLOTS[job])},
    }


def enroll(api, root, jobs, *, apply=False):
    jobs = list(dict.fromkeys(jobs))
    if not jobs or any(job not in SOURCES for job in jobs):
        raise ValueError("Unknown or empty eligible routine selection")
    # The allowlist comes from the original active-routine snapshot. Current
    # paused state does not authorize importing historically paused routines.
    rows = {r["id"]: r for r in api.get("/api/routines")["routines"]}
    for job in jobs:
        row = rows.get(SOURCES[job])
        if not row or row.get("status") != "paused" or row.get("in_flight_run_id"):
            raise ValueError("Source must be quiescent and remain paused: " + job)
    if api.get("/api/settings/runtime-config")["current"].get("internalTasksEnabled") is not True:
        raise ValueError("Enable the explicit internalTasksEnabled opt-in first")
    campaigns = {c["id"]: c for c in api.campaigns()}
    result = []
    for job in jobs:
        identifier = "ops-goal-" + job
        desired = goal(job, root)
        existing = campaigns.get(identifier)
        if existing and existing["description"] != "native-goal-v1; source=" + SOURCES[job]:
            raise ValueError("Campaign ID belongs to another owner: " + identifier)
        state = api.get("/api/campaigns/" + identifier + "/execution") if existing else {"enrolled": False}
        if state["enrolled"]:
            if state["goal"] != desired:
                raise ValueError("Existing goal differs; do not overwrite its execution contract: " + identifier)
            result.append({"campaign": identifier, "status": "already_enrolled"})
            continue
        if not apply:
            result.append({"campaign": identifier, "status": "validated_payload", "goal": desired})
            continue
        if existing is None:
            existing = api.call("POST", "/api/campaigns", {
                "id": identifier, "title": "Operations · " + job,
                "description": "native-goal-v1; source=" + SOURCES[job],
                "status": "active", "round": 1, "nodes": [],
            })["campaign"]
        if existing["status"] != "active":
            raise ValueError("Do not reactivate a paused or terminal goal: " + identifier)
        api.call("POST", "/api/campaigns/" + identifier + "/execution", {
            "expected_revision": existing["revision"], "goal": desired,
        })
        verified = api.get("/api/campaigns/" + identifier + "/execution")
        if not verified["enrolled"] or verified["goal"] != desired:
            raise RuntimeError("Persisted goal differs from the submitted contract: " + identifier)
        result.append({"campaign": identifier, "status": "enrolled", "source_remains_paused": SOURCES[job]})
    return result
