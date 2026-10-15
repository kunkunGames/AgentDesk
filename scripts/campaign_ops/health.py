"""Fresh native health evidence with explicit coverage and temporal comparison.

This is a read-only business helper, not a scheduler or completion owner.
Raw provider/session/authentication content is never included in its output.
"""
import argparse
import datetime as dt
import hashlib
import json
import pathlib
import re
from urllib.parse import quote

from .client import Api

HEALTHY_RELAY_STATES = {"healthy", "active_foreground_stream", "explicit_background_work"}
FAULT_RELAY_STATES = {
    "tmux_alive_relay_dead", "stale_thread_proof", "orphan_pending_token",
    "unpaired_active_token", "queue_blocked",
}
MAX_DISPATCHES = 1000
MAX_REPORT_BYTES = 4 * 1024 * 1024


def utc_now():
    return dt.datetime.now(dt.timezone.utc)


def timestamp(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Health comparison timestamps require an explicit timezone")
    return parsed


def digest(value):
    data = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(data).hexdigest()


def project_mailbox(row, observer_dispatches):
    # The primary classifier is authoritative. The old shadow verdict and a
    # lone attached/desynced flag are diagnostics, not incident authority.
    dispatch = row.get("session_active_dispatch_id")
    return {
        "channel_id": row.get("channel_id"), "dispatch_id": dispatch,
        "agent_id": row.get("session_agent_id"),
        "observer_owned": dispatch in observer_dispatches,
        "relay_state": row.get("relay_stall_state"),
        "shadow_verdict": row.get("stall_shadow_verdict"),
        "watcher_attached": row.get("watcher_attached"),
        "inflight_present": row.get("inflight_state_present"),
        "active_dispatch_present": row.get("active_dispatch_present"),
        "turn_status": row.get("agent_turn_status"),
    }


def collect(api, campaign_id, task_key=None, clock=utc_now):
    started = clock()
    paths = []

    def get(path, **query):
        value = api.get(path, **query)
        paths.append({"path": path, "query": query, "read_at": clock().isoformat()})
        return value

    execution = get("/api/campaigns/" + quote(campaign_id, safe="") + "/execution")
    if execution.get("enrolled") is not True:
        raise ValueError("Health evidence requires a canonical enrolled campaign")
    tasks = execution.get("tasks", [])
    if task_key and not any(t.get("task_key") == task_key for t in tasks):
        raise ValueError("Observer task is not a member of the canonical campaign")
    cards = {t.get("card_id") for t in tasks if not task_key or t.get("task_key") == task_key}
    inventory, coverage = [], []
    for status in ("pending", "dispatched", "running"):
        page = get("/api/dispatches", status=status, limit=MAX_DISPATCHES)
        rows = page.get("dispatches")
        if not isinstance(rows, list):
            raise ValueError("Native dispatch inventory is not a list")
        coverage.append({"status": status, "count": len(rows),
                         "complete": len(rows) < MAX_DISPATCHES,
                         "limit": MAX_DISPATCHES})
        inventory.extend({k: row.get(k) for k in (
            "id", "kanban_card_id", "to_agent_id", "status", "created_at", "updated_at",
        )} for row in rows)
    owned = {r["id"] for r in inventory if r.get("kanban_card_id") in cards}
    detail = get("/api/health/detail")
    release = detail.get("release_source", {})
    provenance = release.get("declared_build_provenance", {})
    doctor = detail.get("latest_startup_doctor", {})
    source = {
        "deployed_repo_head": release.get("deployed_repo_head"),
        "binary_sha256": provenance.get("binary_sha256"),
        "source_manifest_sha256": provenance.get("source_manifest_sha256"),
    }
    snapshot = {
        "schema_version": 1, "observed_at": clock().isoformat(),
        "collection_started_at": started.isoformat(), "campaign_id": campaign_id,
        "observer_task_key": task_key, "release": source,
        "scope": "current native Hub health, primary relay classifiers and nonterminal dispatch inventory",
        "coverage": coverage, "reads": paths,
        "core": {"status": detail.get("status"), "fully_recovered": detail.get("fully_recovered"),
                 "db": detail.get("db"), "doctor_failed": doctor.get("failed_count"),
                 "disk_low": detail.get("disk_low")},
        "mailboxes": [project_mailbox(row, owned) for row in detail.get("mailboxes", [])],
        "dispatches": inventory,
        "campaign": {"hold_reason": execution.get("hold_reason"),
                     "accepted_work_tasks": sum(t.get("role") == "work" and t.get("accepted") is True for t in tasks),
                     "failed_attempt_count": len(execution.get("failures", [])),
                     "active_version": execution.get("active_version")},
        "diagnostics": {
            "startup_warned": doctor.get("warned_count"),
            "audit_truncated": detail.get("active_session_audit", {}).get("truncated"),
            "dispatch_outbox": {k: detail.get("dispatch_outbox", {}).get(k) for k in (
                "pending", "retrying", "permanent_failures", "oldest_pending_age")},
        },
        "limitations": [
            "Private provider work is unobserved; no private idle/absence assertion is made.",
            "Historical saved links, shadow diagnostics and earlier failed attempts do not alone establish a current operational fault or a new universal approval gate.",
            "Unknown outcomes remain in their canonical owner; this report cannot reconcile them, replay work or grant business acceptance.",
        ],
    }
    snapshot["evidence_digest"] = digest(snapshot)
    return snapshot


def candidates(snapshot):
    core = snapshot["core"]
    result = {}
    for key, bad in (("status", core.get("status") in ("unhealthy", "degraded")),
                     ("db", core.get("db") is False),
                     ("fully_recovered", core.get("fully_recovered") is False),
                     ("doctor_failed", isinstance(core.get("doctor_failed"), int) and core["doctor_failed"] > 0),
                     ("disk_low", core.get("disk_low") is True)):
        if bad:
            result["core:" + key] = {"target": "native_hub", "reason": key, "observed": core.get(key)}
    for row in snapshot.get("mailboxes", []):
        state = row.get("relay_state")
        if state in FAULT_RELAY_STATES:
            # A self-owned real stall still qualifies; only healthy progress
            # and legacy shadow disagreement are excluded from alarm authority.
            identity = (row.get("channel_id"), row.get("dispatch_id"), state)
            result["relay:" + digest(identity)] = {
                "target": row.get("channel_id"), "dispatch_id": row.get("dispatch_id"),
                "reason": state, "observer_owned": row.get("observer_owned"),
            }
    return result


def assess(current, previous=None):
    def intact(snapshot):
        content = {k: v for k, v in snapshot.items() if k != "evidence_digest"}
        return snapshot.get("schema_version") == 1 and snapshot.get("evidence_digest") == digest(content)

    if not intact(current) or (previous is not None and not intact(previous)):
        raise ValueError("Health evidence changed after collection")
    core = current["core"]
    reasons = []
    if not all(c.get("complete") is True for c in current.get("coverage", [])) or len(current.get("coverage", [])) != 3:
        reasons.append("nonterminal_dispatch_inventory_incomplete")
    if (core.get("status") not in {"healthy", "degraded", "unhealthy"}
            or any(type(core.get(k)) is not bool for k in ("db", "fully_recovered", "disk_low"))
            or type(core.get("doctor_failed")) is not int):
        reasons.append("required_native_health_evidence_missing")
    release = current.get("release", {})
    if any(not isinstance(release.get(key), str) or re.fullmatch(pattern, release[key]) is None
           for key, pattern in (("deployed_repo_head", r"[0-9a-f]{40}"),
                                ("binary_sha256", r"[0-9a-f]{64}"),
                                ("source_manifest_sha256", r"[0-9a-f]{64}"))):
        reasons.append("required_deployment_identity_missing")
    if any(m.get("relay_state") not in HEALTHY_RELAY_STATES | FAULT_RELAY_STATES for m in current.get("mailboxes", [])):
        reasons.append("primary_relay_classification_unavailable")
    now = candidates(current)
    confirmed, resolved = [], []
    if previous is not None:
        elapsed = (timestamp(current["observed_at"]) - timestamp(previous["observed_at"])).total_seconds()
        if elapsed < 30:
            reasons.append("comparison_requires_30_seconds_of_separate_observation")
        if (current["campaign_id"] != previous["campaign_id"] or current["release"] != previous["release"]
                or current.get("source_files") != previous.get("source_files")):
            reasons.append("comparison_scope_or_deployment_changed")
        prior = candidates(previous)
        if not reasons:
            confirmed = [v for k, v in now.items() if k in prior]
            resolved = [v for k, v in prior.items() if k not in now]
    if reasons:
        conclusion = "blocked"
    elif now:
        conclusion = "observed_fault" if confirmed else "requires_recheck"
    else:
        conclusion = "no_current_fault_in_declared_native_scope"
    return {"schema_version": 1, "conclusion": conclusion, "coverage_gates": reasons,
            "fault_candidates": list(now.values()), "confirmed_faults": confirmed,
            "resolved_observations": resolved, "evidence_digest": current["evidence_digest"],
            "business_accepted": False,
            "next_step": "Independent QA must collect fresh evidence, inspect the scope and verify original acceptance; this helper never completes a campaign."}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path.home() / ".adk/release")
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--task-key")
    parser.add_argument("--previous", type=pathlib.Path)
    args = parser.parse_args(argv)
    try:
        current = collect(Api(args.root), args.campaign, args.task_key)
        current["source_files"] = {
            "collector_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
            "working_watchdog_sha256": hashlib.sha256(
                (args.root / "routines/monitoring/working-watchdog.js").read_bytes()).hexdigest(),
        }
        current["evidence_digest"] = digest({k: v for k, v in current.items() if k != "evidence_digest"})
        previous = None
        if args.previous is not None:
            if args.previous.stat().st_size > MAX_REPORT_BYTES:
                raise ValueError("Previous health evidence exceeds the artifact limit")
            value = json.loads(args.previous.read_text())
            previous = value.get("snapshot", value)
        result = {"snapshot": current, "assessment": assess(current, previous)}
        output = json.dumps(result, ensure_ascii=False, indent=2)
        if len(output.encode()) > MAX_REPORT_BYTES:
            raise ValueError("Health evidence exceeds the artifact limit")
        print(output)
    except Exception as error:
        # Native transport exceptions may contain provider output. Keep the
        # unavailability fact while leaving that text with its existing owner.
        print(json.dumps({"schema_version": 1, "available": False,
                          "error_type": type(error).__name__, "business_accepted": False}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
