#!/usr/bin/env python3
"""Probe the first W3 boundary through the real Hub, without model credentials.

An authenticated active A -> B / independent C campaign must supply A/C to
existing cards, dispatches and queue entries. A protected checker control proves
the Hub's reconciliation is alive. A timeout is a failed prerequisite, not E1
success. Even a pass here would not prove execution or predecessor file use.
"""
import argparse
import json
from pathlib import Path
import time

from probe_managed_checks import worker
from probe_managed_checks_operations import answer, seed, state
from probe_test_phase_lifecycle import host, require


def exercise(service, database, template, artifact):
    goal_id = "campaign-supply-canary"
    nodes = [dict(id=name, title=f"Task {name}", status="pending", stage="build", round=1,
                  dependencies=["a"] if name == "b" else []) for name in ("a", "b", "c")]
    status, created = service.request("campaigns", {
        "id": goal_id, "title": "Approved W3 supply canary", "status": "active", "round": 1, "nodes": nodes,
    })
    require(status == 201, f"campaign HTTP creation failed: {status}")
    seed(service, database, "supply_control")
    answer(template, "42")
    started = time.monotonic()
    observations = []
    supplied = False
    try:
        while time.monotonic() - started < 95:
            require(service.process.poll() is None, "Hub exited during campaign supply")
            data = json.loads(database.query(
                "SELECT json_build_object("
                "'cards',(SELECT count(*) FROM kanban_cards WHERE id<>'supply_control'),"
                "'ready_nodes',(SELECT COALESCE(json_agg(metadata->>'node_id' ORDER BY metadata->>'node_id'),'[]'::json) "
                "FROM kanban_cards WHERE metadata->>'campaign_id'='campaign-supply-canary'),"
                "'dispatches',(SELECT count(*) FROM task_dispatches WHERE kanban_card_id<>'supply_control'),"
                "'queue_entries',(SELECT count(*) FROM auto_queue_entries),"
                "'runs',(SELECT count(*) FROM auto_queue_runs))"
            )[0])
            data.update(elapsed_seconds=round(time.monotonic() - started, 3),
                        checker_control=state(database, "supply_control")["status"])
            observations.append(data)
            if (data["cards"] == data["dispatches"] == data["queue_entries"] == 2 and data["runs"] == 1
                    and data["ready_nodes"] == ["a", "c"] and data["checker_control"] == "done"):
                supplied = True
                break
            time.sleep(1)
    finally:
        status, final = service.request(f"campaigns/{goal_id}")
        (artifact / "campaign-observation.json").write_text(json.dumps({
            "scope": "pre-execution campaign supply; no provider/agent configured; no E1 claim",
            "created": created, "final_http_status": status, "final": final,
            "expected_ready_nodes": ["a", "c"], "expected_waiting_node": "b",
            "observations": observations,
        }, indent=2) + "\n")
    require(observations[-1]["checker_control"] == "done", "checker control did not reconcile")
    require(supplied, "active Campaign remained a ledger: A/C cards, dispatches and queue entries were not supplied within 95 seconds")
    return ["checker_control_completed", "campaign_ready_root_supply"], observations[-1], {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-container", default="adk24x7-native-build-20260926")
    parser.add_argument("--postgres-container", default="adk24x7-native-test-20260926")
    parser.add_argument("--pg-port", type=int, default=44620)
    parser.add_argument("--binary", default="/target/debug/agentdesk")
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--database", help=argparse.SUPPRESS)
    args = parser.parse_args()
    return worker(args, exercise_fn=exercise) if args.worker else host(args, worker_script=Path(__file__).name)


if __name__ == "__main__":
    raise SystemExit(main())
