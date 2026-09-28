#!/usr/bin/env python3
"""Verify automatic managed checks through a disposable Hub and real HTTP.

Implementation completion is seeded fixture input; the service itself must
freeze the files, run the protected checker and commit the FSM transition.
No model, external messaging, production database or installed service is used.
"""
import argparse
import ctypes
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import tempfile
import time

from probe_test_phase_lifecycle import Service, digest, host, require


class FixtureDb:
    """Use the build image's existing libpq without installing a Python driver."""
    def __init__(self, database, port):
        require(database.startswith("phase_canary_") and database.isidentifier(), "invalid fixture database")
        require(port == 44620, "fixture port required")
        self.lib = ctypes.CDLL("libpq.so.5")
        for name, args, result in [
            ("PQconnectdb", [ctypes.c_char_p], ctypes.c_void_p),
            ("PQstatus", [ctypes.c_void_p], ctypes.c_int),
            ("PQfinish", [ctypes.c_void_p], None),
            ("PQexec", [ctypes.c_void_p, ctypes.c_char_p], ctypes.c_void_p),
            ("PQresultStatus", [ctypes.c_void_p], ctypes.c_int),
            ("PQresultErrorMessage", [ctypes.c_void_p], ctypes.c_char_p),
            ("PQntuples", [ctypes.c_void_p], ctypes.c_int),
            ("PQgetvalue", [ctypes.c_void_p, ctypes.c_int, ctypes.c_int], ctypes.c_char_p),
            ("PQclear", [ctypes.c_void_p], None),
        ]:
            function = getattr(self.lib, name)
            function.argtypes, function.restype = args, result
        self.connection = self.lib.PQconnectdb(
            f"host=127.0.0.1 port={port} dbname={database} user=postgres password=postgres connect_timeout=5".encode()
        )
        require(self.connection and self.lib.PQstatus(self.connection) == 0, "fixture connection failed")

    def query(self, statement, *, allow_error=False):
        result = self.lib.PQexec(self.connection, statement.encode())
        require(result, "fixture query returned no result")
        try:
            status = self.lib.PQresultStatus(result)
            if status not in (1, 2):
                if allow_error:
                    return None
                raise RuntimeError(self.lib.PQresultErrorMessage(result).decode(errors="replace"))
            return [self.lib.PQgetvalue(result, row, 0).decode() for row in range(self.lib.PQntuples(result))]
        finally:
            self.lib.PQclear(result)

    def close(self):
        self.lib.PQfinish(self.connection)


def exercise(service, database, template, artifact):
    checks = []
    database.query("INSERT INTO kanban_cards(id,title,status) VALUES('managed-http','HTTP fixture','backlog')")
    route = "managed-checks/managed-http"
    status, _ = service.request(route, authenticated=False)
    require(status == 401, "managed administration must require authentication")
    checks.append("admin_authentication_required")
    status, enrolled = service.request(route + "/enroll", {"template": "answer"})
    require(status == 200, f"enrollment failed: {enrolled}")
    (Path(template["workspace"]) / "answer.txt").write_text("42")
    os.chown(Path(template["workspace"]) / "answer.txt", 65534, 65534)
    database.query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status,result) VALUES('fixture-work','managed-http','implementation','completed','fixture implementation output')")
    database.query("UPDATE kanban_cards SET status='review',latest_dispatch_id='fixture-work',review_entered_at=NOW() WHERE id='managed-http'")
    require(database.query("UPDATE kanban_cards SET status='done' WHERE id='managed-http'", allow_error=True) is None,
            "raw force bypassed pending checks")
    checks.append("raw_completion_waits_for_actual_checks")

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if database.query("SELECT status FROM kanban_cards WHERE id='managed-http'") == ["done"]:
            break
        require(service.process.poll() is None, "service exited before its automatic check")
        time.sleep(0.25)
    else:
        raise RuntimeError("30-second reconciliation tick did not complete the managed card")
    status, detail = service.request(route)
    require(status == 200, "managed state inspection failed")
    status, rows = service.request("cluster/test-phase-runs")
    require(status == 200 and len(rows["runs"]) == 1, "expected exactly one server-started check")
    receipt = rows["runs"][0]
    require(receipt["status"] == "passed" and receipt["evidence"]["process_reaped"] is True
            and receipt["evidence"]["report"]["passed"] is True, "no real terminal checker evidence")
    require(receipt["evidence"]["contract_digest"] == enrolled["contract_digest"], "receipt has a different contract")
    checks.append("automatic_tick_runs_checker_and_commits_done")
    require(database.query("SELECT count(*) FROM kanban_audit_logs WHERE card_id='managed-http' AND to_status='done'") == ["1"],
            "completion audit is missing or duplicated")
    checks.append("completion_and_audit_recorded_once")

    forged = {"phase_key": receipt["phase_key"], "head_sha": receipt["head_sha"],
              "idempotency_key": receipt["idempotency_key"], "status": "passed", "evidence": {"forged": True}}
    status, _ = service.request("cluster/test-phase-runs/upsert", forged)
    require(status == 400, "public phase API accepted reserved collector evidence")
    checks.append("public_receipt_forgery_rejected")
    frozen = Path(detail["state"]["candidate_path"])
    require(frozen.joinpath("answer.txt").read_text() == "42", "frozen artifact has wrong contents")
    shutil.copytree(frozen, artifact / "candidate")
    checks.append("frozen_artifact_preserved_with_receipt")

    database.query("INSERT INTO kanban_cards(id,title,status) VALUES('human-http','human approval fixture','backlog')")
    human_route = "managed-checks/human-http"
    status, human_enrolled = service.request(human_route + "/enroll", {"template": "answer-human"})
    require(status == 200, "human contract enrollment failed")
    database.query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status,result) VALUES('human-work','human-http','implementation','completed','fixture human output')")
    database.query("UPDATE kanban_cards SET status='review',latest_dispatch_id='human-work',review_entered_at=NOW() WHERE id='human-http'")
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        status, human = service.request(human_route)
        if status == 200 and human.get("state") and human["state"].get("outcome") == "passed":
            break
        require(service.process.poll() is None, "service exited before human check")
        time.sleep(0.25)
    else:
        raise RuntimeError("human candidate checks did not run")
    require(human["disposition"] == "await_human", "passing checks bypassed mandatory human approval")
    require(database.query("SELECT status FROM kanban_cards WHERE id='human-http'") == ["review"], "human wait was auto-completed")
    approval = {"candidate_digest": human["state"]["candidate_digest"], "dispatch_id": "human-work",
                "contract_digest": human_enrolled["contract_digest"], "reason": "isolated HTTP fixture inspection"}
    status, _ = service.request(human_route + "/approve", approval, authenticated=False)
    require(status == 401, "anonymous approval accepted")
    status, _ = service.request(human_route + "/approve", dict(approval, candidate_digest="stale-candidate"))
    require(status == 409, "stale candidate approval accepted")
    checks.append("human_wait_requires_authenticated_current_candidate_approval")
    status, _ = service.request(human_route + "/approve", approval)
    require(status == 200, "current human approval failed")
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if database.query("SELECT status FROM kanban_cards WHERE id='human-http'") == ["done"]:
            break
        time.sleep(0.25)
    else:
        raise RuntimeError("approval did not wake completion")
    require(database.query("SELECT verification_state->>'approval_authority' FROM kanban_cards WHERE id='human-http'") == ["server_admin"],
            "approval authority was not persisted")
    checks.append("approval_wakes_fsm_and_records_authority")
    status, rows = service.request("cluster/test-phase-runs")
    require(status == 200 and len(rows["runs"]) == 2, "expected one check per candidate")
    service.stop()
    service.start()
    # Wait through another actual reconciliation tick, not just readiness.
    time.sleep(32)
    status, again = service.request("cluster/test-phase-runs")
    require(status == 200 and again["runs"] == rows["runs"], "restart reran or changed completed checks")
    require(database.query("SELECT status FROM kanban_cards WHERE id='managed-http'") == ["done"], "restart lost completion")
    require(database.query("SELECT status FROM kanban_cards WHERE id='human-http'") == ["done"], "restart lost approved completion")
    require(database.query("SELECT count(*) FROM kanban_audit_logs WHERE card_id='human-http' AND to_status='done'") == ["1"],
            "human completion was missing or duplicated")
    checks.append("restart_and_next_tick_do_not_duplicate_completion")
    return checks, detail, receipt


def worker(args, *, exercise_fn=exercise, checker_source=None, timeout_seconds=210):
    artifact = Path(args.artifact_dir).resolve()
    artifact.mkdir(parents=True, exist_ok=False)
    report = {"passed": False, "scope": "automatic checker and FSM via real HTTP; seeded implementation, no LLM",
              "binary_sha256": digest(args.binary), "checks": []}
    with tempfile.TemporaryDirectory(prefix="adk-check-canary-", dir="/target") as temporary:
        root = Path(temporary)
        root.chmod(0o755)
        for name in ("config", "policies", "data", "home", "artifacts", "workspace"):
            root.joinpath(name).mkdir()
        root.joinpath("home").chmod(0o700)
        root.joinpath("workspace").chmod(0o700)
        os.chown(root / "workspace", 65534, 65534)
        wrapper = root / "checker.py"
        wrapper.write_text(checker_source or "import json, pathlib, sys\nprint(json.dumps({'passed': pathlib.Path(sys.argv[1], 'answer.txt').read_text() == '42'}))\n")
        binary = Path("/usr/bin/python3").resolve()
        template = {"workspace": str(root / "workspace"), "artifact_root": str(root / "artifacts"),
                    "allowed_changes": ["answer.txt"], "acceptance": [{"id": "correct-answer", "checks": ["answer"]}],
                    "checks": {"answer": {"executable": str(binary), "wrapper": str(wrapper),
                               "executable_sha256": digest(binary), "wrapper_sha256": digest(wrapper),
                               "timeout_secs": 5, "kind": {"type": "assertion"}}},
                    "review": "none", "max_candidates": 3, "max_check_attempts": 2}
        registry = root / "checks.json"
        registry.write_text(json.dumps({"answer": template, "answer-human": dict(template, review="human")}))
        registry.chmod(0o600)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        token = secrets.token_urlsafe(32)
        config = {"server": {"host": "127.0.0.1", "port": port, "auth_token": token},
                  "database": {"enabled": True, "host": "127.0.0.1", "port": args.pg_port,
                               "dbname": args.database, "user": "postgres", "password": "postgres",
                               "pool_max": 8, "foreground_reserve": 2},
                  "data": {"dir": str(root / "data"), "db_name": "unused.db"},
                  "policies": {"dir": str(root / "policies"), "hot_reload": False},
                  "cluster": {"enabled": True, "role": "hub", "runtime_profile": "full",
                              "instance_id": "managed-check-canary", "session_bound_relay_enabled": False},
                  "providers": {}, "agents": [], "mcp_servers": {}, "mcp": {"watch_credentials": False},
                  "routines": {"enabled": False}, "github": {"sync_interval_minutes": 0}, "config_hot_reload": False}
        root.joinpath("config/agentdesk.yaml").write_text(json.dumps(config))
        root.joinpath("config/agentdesk.yaml").chmod(0o600)
        service = Service(args.binary, root, port, token, check_registry=registry)
        database = None
        started = time.monotonic()

        def expired(_signal, _frame):
            raise TimeoutError(f"managed-check canary exceeded {timeout_seconds} seconds")

        signal.signal(signal.SIGALRM, expired)
        signal.alarm(timeout_seconds)
        try:
            service.start()
            database = FixtureDb(args.database, args.pg_port)
            report["checks"], report["detail"], report["receipt"] = exercise_fn(service, database, template, artifact)
            report["passed"] = True
        except Exception as error:
            report["error"] = str(error).replace(token, "[redacted]")
        finally:
            signal.alarm(0)
            service.stop()
            if database:
                database.close()
            for log in [*root.glob("service-*.log"), *root.glob("logs/*.log*")]:
                artifact.joinpath(log.name).write_text(log.read_text(errors="replace").replace(token, "[redacted]"))
            report.update(boots=service.boots, duration_seconds=round(time.monotonic()-started, 3))
    artifact.joinpath("result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k not in ("detail", "receipt")}))
    return 0 if report["passed"] else 1


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
    return worker(args) if args.worker else host(args, worker_script=Path(__file__).name)


if __name__ == "__main__":
    raise SystemExit(main())
