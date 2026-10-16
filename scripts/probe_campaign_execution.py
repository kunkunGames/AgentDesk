#!/usr/bin/env python3
"""Run a protected A -> B / independent C goal through the real Hub tick.

Uses real files, checker children, automatic bounded rework, queue and FSM.
Implementation output is injected fixture input; this is not a real-LLM or
24-hour acceptance claim. The shared harness owns cleanup and binary evidence.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import time

from probe_managed_checks import worker
from probe_test_phase_lifecycle import digest, host, require


def exercise(service, database, template, artifact):
    checks = []
    root = service.root
    goal = "campaign-execution-canary"
    nodes = [dict(id=name, title=f"Task {name}", status="pending", stage="build", round=1,
                  details="Write answer.txt using the acceptance criteria.",
                  acceptance=["answer is predecessor A plus 1" if name == "b" else "answer is 42"],
                  dependencies=["a"] if name == "b" else []) for name in ("a", "b", "c")]
    payload = dict(id=goal, title="Protected file handoff", description="A to B and independent C",
                   status="active", round=1, nodes=nodes)
    status, created = service.request("campaigns", payload)
    require(status == 201, f"campaign creation failed: {status}")
    graph = {key: payload[key] for key in ("title", "description", "round")}
    graph["nodes"] = {n["id"]: {key: n[key] for key in
                               ("title", "stage", "round", "details", "acceptance", "dependencies")} for n in nodes}
    graph_digest = hashlib.sha256(json.dumps(graph, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    templates = {}
    for name in ("a", "b", "c"):
        item = copy.deepcopy(template)
        workspace = root / f"workspace-{name}"
        artifacts = root / f"artifacts-{name}"
        workspace.mkdir(mode=0o700)
        os.chown(workspace, 65534, 65534)
        artifacts.mkdir()
        item.update(workspace=str(workspace), artifact_root=str(artifacts))
        if name == "b":
            checker = root / "check-b.py"
            checker.write_text("import json,pathlib,sys\nr=pathlib.Path(sys.argv[1])\nprint(json.dumps({'passed':int((r/'answer.txt').read_text()) == int((r/'input/a.txt').read_text())+1}))\n")
            item["checks"]["answer"].update(wrapper=str(checker), wrapper_sha256=digest(checker))
        templates[name] = item
    service.check_registry.write_text(json.dumps(templates))
    service.check_registry.chmod(0o600)
    plan = dict(graph_digest=graph_digest, repo="fixture/24x7", agent_id="campaign_canary", nodes={
        name: dict(task_key=f"canary-{name}", check_template=name, auto_rework=True,
                   inputs=[dict(dependency="a", source="answer.txt", destination="input/a.txt")] if name == "b" else [])
        for name in ("a", "b", "c")})
    registry = root / "campaigns.json"
    registry.write_text(json.dumps({goal: plan}))
    registry.chmod(0o600)
    service.stop()
    config_path = root / "config/agentdesk.yaml"
    config = json.loads(config_path.read_text())
    config["agents"] = [dict(id="campaign_canary", name="campaign canary", provider="opencode")]
    config_path.write_text(json.dumps(config))
    service.campaign_registry = registry
    service.start()
    # The configured roster must survive every boot. The fixture sets finite
    # dispatch requirements after the canonical agent synchronization.
    database.query("UPDATE agents SET discord_channel_id='123',execution_requirements='{\"execution_limits\":{\"max_elapsed_secs\":60,\"max_tool_calls\":16}}' WHERE id='campaign_canary'")

    def rows():
        return json.loads(database.query("SELECT COALESCE(json_agg(json_build_object('id',id,'node',metadata->>'node_id','status',status,'contract',verification_contract,'state',verification_state)),'[]'::json) FROM kanban_cards WHERE metadata->>'campaign_id'='campaign-execution-canary'")[0])

    # Finishing a predecessor, completing its queue run, releasing the slot,
    # and activating the next finite batch can cross separate 30-second ticks.
    # Allow five ticks without bypassing the real slot lifecycle in the fixture.
    def wait(predicate, description, timeout=150):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            require(service.process.poll() is None, "Hub exited")
            current = rows()
            if predicate(current):
                return current
            time.sleep(0.5)
        raise RuntimeError(f"timeout: {description}; cards={rows()}")

    current = wait(lambda cards: len(cards) == 2 and all(c["status"] not in ("backlog", "requested", "ready") for c in cards), "A/C supply and canonical activation")
    require(sorted(c["node"] for c in current) == ["a", "c"], "B was supplied without predecessor evidence")
    require(database.query("SELECT count(*) FROM auto_queue_runs") == ["1"], "root batch was duplicated")
    checks.append("protected_goal_supplies_roots_through_existing_queue")

    def finish(name, value):
        card = next(c for c in rows() if c["node"] == name)
        path = Path(templates[name]["workspace"]) / "answer.txt"
        path.write_text(value)
        os.chown(path, 65534, 65534)
        # Fixture completion of the dispatch the product created; no SQL creates
        # implementation/rework dispatches and no SQL marks a card done.
        changed = database.query(f"UPDATE task_dispatches SET status='completed',result='fixture output {name}={value}',completed_at=NOW() WHERE kanban_card_id='{card['id']}' AND status IN ('pending','dispatched') AND dispatch_type IN ('implementation','rework') RETURNING id")
        require(len(changed) == 1, "expected exactly one canonical implementation attempt")
        database.query(f"UPDATE kanban_cards SET status='review',review_entered_at=NOW() WHERE id='{card['id']}'")

    finish("a", "0")
    finish("c", "42")
    wait(lambda cards: any(c["node"] == "c" and c["status"] == "done" for c in cards)
         and database.query("SELECT count(*) FROM task_dispatches WHERE dispatch_type='rework'") == ["1"],
         "independent completion and automatic repair")
    require(len(rows()) == 2, "failed A incorrectly supplied B")
    checks.append("failed_check_schedules_one_rework_while_independent_c_completes")
    finish("a", "42")
    current = wait(lambda cards: len(cards) == 3 and any(c["node"] == "b" and c["status"] not in ("backlog", "requested", "ready") for c in cards), "verified A materialization and B activation")
    require(database.query("SELECT count(*) FROM auto_queue_runs") == ["2"], "successor must use a new finite batch")
    b = next(c for c in current if c["node"] == "b")
    input_path = Path(templates["b"]["workspace"]) / "input/a.txt"
    require(input_path.read_text() == "42", "B did not receive A's actual file")
    require(b["contract"]["baseline"]["input/a.txt"] == hashlib.sha256(b"42").hexdigest(), "input absent from immutable baseline")
    (Path(templates["a"]["workspace"]) / "answer.txt").write_text("999")
    checks.append("verified_predecessor_bytes_materialized_and_frozen_in_successor_contract")
    finish("b", "43")
    wait(lambda cards: len(cards) == 3 and all(c["status"] == "done" for c in cards), "all protected checks and FSM completion")
    end = time.monotonic() + 50
    while time.monotonic() < end:
        if database.query("SELECT document->>'status' FROM campaigns WHERE id='campaign-execution-canary'") == ["completed"]:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("completed cards did not reconcile the Campaign ledger")
    require(database.query("SELECT count(*) FROM kanban_audit_logs WHERE to_status='done'") == ["3"], "completion audit was duplicated")
    checks.append("actual_file_consumption_checks_drive_all_three_fsm_completions")
    service.stop()
    service.start()
    time.sleep(32)
    require(database.query("SELECT count(*) FROM auto_queue_entries") == ["3"], "restart duplicated queue entries")
    require(database.query("SELECT count(*) FROM task_dispatches WHERE dispatch_type='rework'") == ["1"], "restart duplicated repair")
    require(database.query("SELECT count(*) FROM kanban_audit_logs WHERE to_status='done'") == ["3"], "restart duplicated completion")
    checks.append("restart_and_next_tick_preserve_exactly_once_supply_rework_and_completion")
    detail = dict(scope="seeded implementation; real Hub scheduling, files, protected checker, queue and FSM; no real LLM", cards=rows(), input_sha256=digest(input_path))
    artifact.joinpath("campaign-evidence.json").write_text(json.dumps(detail, indent=2) + "\n")
    return checks, detail, {}


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
    return worker(args, exercise_fn=exercise, timeout_seconds=510) if args.worker else host(args, worker_script=Path(__file__).name, timeout_seconds=540)


if __name__ == "__main__":
    raise SystemExit(main())
