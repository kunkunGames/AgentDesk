#!/usr/bin/env python3
"""Exercise the real phase-run HTTP API in disposable Linux Docker fixtures.

The host owns one fresh PostgreSQL database; the worker owns its service process,
HOME, configuration, and loopback listener. No provider or policy is configured.
This probes storage/API lifetime, not a trusted check driver or task acceptance.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


class Service:
    def __init__(self, binary, root, port, token, *, check_registry=None):
        self.binary, self.root, self.port, self.token = binary, root, port, token
        self.check_registry = check_registry
        self.campaign_registry = None
        self.process = None
        self.log = None
        self.boots = 0

    def request(self, path, body=None, authenticated=True, method=None):
        headers = {"Content-Type": "application/json"}
        if authenticated:
            headers["Authorization"] = "Bearer " + self.token
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/api/{path}",
            data=None if body is None else json.dumps(body).encode(), headers=headers, method=method,
        )
        # Ignore ambient proxy configuration, including for loopback requests.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            response = opener.open(request, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            payload = response.read().decode()
            return response.status, json.loads(payload) if payload else None

    def start(self):
        require(self.process is None, "service already owned")
        self.boots += 1
        self.log = (self.root / f"service-{self.boots}.log").open("wb")
        environment = {
            "PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8",
            "HOME": str(self.root / "home"),
            "XDG_CONFIG_HOME": str(self.root / "home/config"),
            "XDG_CACHE_HOME": str(self.root / "home/cache"),
            "XDG_DATA_HOME": str(self.root / "home/data"),
            "AGENTDESK_ROOT_DIR": str(self.root),
            "AGENTDESK_CONFIG": str(self.root / "config/agentdesk.yaml"),
        }
        if self.check_registry is not None:
            environment["AGENTDESK_CHECK_REGISTRY"] = str(self.check_registry)
        if self.campaign_registry is not None:
            environment["AGENTDESK_CAMPAIGN_REGISTRY"] = str(self.campaign_registry)
        self.process = subprocess.Popen(
            [self.binary, "dcserver"], cwd=self.root, env=environment,
            stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True,
        )
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            require(self.process.poll() is None, "canary service exited during startup")
            try:
                status, _ = self.request("cluster/test-phase-runs")
                if status == 200:
                    return
            except (OSError, ValueError):
                pass
            time.sleep(0.25)
        raise RuntimeError("canary HTTP readiness timed out")

    def stop(self):
        if self.process is not None:
            if self.process.poll() is None:
                os.killpg(self.process.pid, signal.SIGTERM)
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait(timeout=10)
            self.process = None
        if self.log is not None:
            self.log.close()
            self.log = None


def exercise(service):
    checks = []
    base = "cluster/test-phase-runs"

    def call(path, body, expected):
        status, payload = service.request(path, body)
        require(status == expected, f"{path}: expected {expected}, got {status}: {payload}")
        return payload

    request = {
        "phase_key": "canary-acceptance", "head_sha": "synthetic-candidate-a",
        "idempotency_key": "canary-check-a", "resource_lock_key": "canary-resource",
        "holder_instance_id": "canary-node", "holder_job_id": "canary-job-a",
        "required_capabilities": {"check": "synthetic-http-probe"}, "ttl_secs": 300,
    }
    status, _ = service.request(base, authenticated=False)
    require(status == 401, "protected phase-run API accepted an unauthenticated caller")
    checks.append("unauthenticated_request_rejected")
    first = call(base + "/start", request, 200)
    require(first["started"], "initial execution not acquired")
    duplicate = call(base + "/start", request, 409)
    require(not duplicate["started"] and duplicate["run"]["id"] == first["run"]["id"],
            "duplicate delivery created another execution")
    checks.append("duplicate_start_returns_existing_execution")
    result = {**request, "status": "passed", "evidence": {"synthetic": True, "exit_code": 0},
              "release_lock": True}
    call(base + "/complete", {**result, "holder_job_id": "wrong-job"}, 400)
    call(base + "/upsert", result, 400)
    checks.append("wrong_owner_and_raw_upsert_rejected")
    completed = call(base + "/complete", result, 200)
    require(completed["lock_released"], "successful completion did not release its lease")
    replay = call(base + "/complete", result, 200)
    require(replay["run"] == completed["run"] and not replay["lock_released"],
            "identical receipt replay changed evidence or released another lease")
    call(base + "/complete", {**result, "evidence": {"changed": True}}, 400)
    checks.append("terminal_evidence_immutable_and_replay_idempotent")

    stale_request = {**request, "idempotency_key": "canary-check-b", "holder_job_id": "canary-job-b"}
    call(base + "/start", stale_request, 200)
    released = call("cluster/resource-locks/release", {
        "lock_key": request["resource_lock_key"], "holder_instance_id": "canary-node",
        "holder_job_id": "canary-job-b",
    }, 200)
    require(released["released"], "fixture lease revocation failed")
    successor = {**request, "idempotency_key": "canary-check-c", "holder_job_id": "canary-job-c"}
    call(base + "/start", successor, 200)
    call(base + "/complete", {**result, **stale_request}, 400)
    require(not call(base + "/complete", result, 200)["lock_released"],
            "old terminal replay released successor lease")
    checks.append("late_collector_and_old_release_cannot_replace_successor")
    call(base + "/complete", {**result, **successor}, 200)

    service.stop()
    service.start()
    after_restart = call(base + "/start", request, 409)
    require(after_restart["run"] == completed["run"], "restart changed the terminal receipt")
    status, evidence = service.request(base + "/evidence?phase_key=canary-acceptance&head_sha=synthetic-candidate-a")
    require(status == 200 and evidence["run"]["idempotency_key"] == "canary-check-c",
            "persisted passing evidence was not queryable after restart")
    status, rows = service.request(base)
    require(status == 200 and len(rows["runs"]) == 3, "unexpected durable execution count")
    stale = next(row for row in rows["runs"] if row["idempotency_key"] == "canary-check-b")
    require(stale["status"] == "running" and stale["evidence"] == {},
            "revoked execution gained passing evidence")
    checks.append("restart_preserves_receipts_and_unresolved_execution")
    return checks, rows["runs"]


def worker(args):
    artifact = Path(args.artifact_dir).resolve()
    artifact.mkdir(parents=True, exist_ok=False)
    report = {"passed": False, "scope": "real HTTP storage lifecycle with synthetic evidence",
              "binary_sha256": digest(args.binary), "checks": []}
    with tempfile.TemporaryDirectory(prefix="adk-phase-canary-", dir="/target") as temporary:
        root = Path(temporary)
        token = secrets.token_urlsafe(32)
        for name in ["config", "policies", "data", "home/config", "home/cache", "home/data"]:
            (root / name).mkdir(parents=True, exist_ok=True)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        config = {
            "server": {"host": "127.0.0.1", "port": port, "auth_token": token},
            "database": {"enabled": True, "host": "127.0.0.1", "port": args.pg_port,
                         "dbname": args.database, "user": "postgres", "password": "postgres",
                         "pool_max": 8, "foreground_reserve": 2},
            "data": {"dir": str(root / "data"), "db_name": "unused.db"},
            "policies": {"dir": str(root / "policies"), "hot_reload": False},
            # Fresh databases are seeded only by the canonical Hub bootstrap.
            # This Hub has no agents, policies, channels, routines, or providers.
            "cluster": {"enabled": True, "role": "hub", "runtime_profile": "full",
                        "instance_id": "phase-lifecycle-canary", "session_bound_relay_enabled": False},
            "providers": {}, "agents": [], "mcp_servers": {},
            "mcp": {"watch_credentials": False}, "routines": {"enabled": False},
            "github": {"sync_interval_minutes": 0}, "config_hot_reload": False,
        }
        config_path = root / "config/agentdesk.yaml"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        config_path.chmod(0o600)
        service = Service(args.binary, root, port, token)
        started = time.monotonic()

        def deadline_expired(_signal, _frame):
            raise TimeoutError("canary lifetime exceeded 210 seconds")

        signal.signal(signal.SIGALRM, deadline_expired)
        signal.alarm(210)
        try:
            service.start()
            report["checks"], report["runs"] = exercise(service)
            report["passed"] = True
        except Exception as error:
            report["error"] = str(error).replace(token, "[redacted]")
        finally:
            signal.alarm(0)
            service.stop()
            report.update(boots=service.boots, duration_seconds=round(time.monotonic() - started, 3))
            for log in root.glob("service-*.log"):
                (artifact / log.name).write_text(log.read_text(errors="replace").replace(token, "[redacted]"),
                                               encoding="utf-8")
    (artifact / "result.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key != "runs"}))
    return 0 if report["passed"] else 1


def host(args, *, worker_script="probe_test_phase_lifecycle.py", worker_args=(), timeout_seconds=240):
    def docker(*arguments):
        return subprocess.run(["docker", *arguments], check=True, capture_output=True,
                              text=True, timeout=30).stdout

    # Refuse a production port/container; this probe has a fixed fixture contract.
    require(args.pg_port == 44620, "only the documented disposable fixture port 44620 is supported")
    require(args.postgres_container.startswith("adk24x7-native-test-"), "unexpected fixture container")
    ports = json.loads(docker("inspect", "--format", "{{json .NetworkSettings.Ports}}", args.postgres_container))
    require(any(binding["HostIp"] == "127.0.0.1" and binding["HostPort"] == str(args.pg_port)
                for binding in ports.get("5432/tcp", [])), "fixture port is not isolated to loopback")
    output = Path(args.artifact_dir).resolve()
    output.mkdir(parents=True, exist_ok=False)
    database = "phase_canary_" + uuid.uuid4().hex
    remote_artifact = "/target/" + database

    def sql(statement):
        return docker("exec", args.postgres_container, "psql", "-U", "postgres", "-d", "postgres",
                      "-v", "ON_ERROR_STOP=1", "-c", statement)

    sql(f'CREATE DATABASE "{database}"')
    try:
        result = subprocess.run([
            "docker", "exec", args.build_container, "python3", "/workspace/scripts/" + worker_script,
            "--worker", "--binary", args.binary, "--database", database,
            "--pg-port", str(args.pg_port), "--artifact-dir", remote_artifact,
            *worker_args,
        ], capture_output=True, text=True, timeout=timeout_seconds)
        docker("cp", args.build_container + ":" + remote_artifact + "/.", str(output))
        print(result.stdout.strip())
        if result.stderr:
            print(result.stderr.strip())
        return result.returncode
    finally:
        # Only the database whose CREATE succeeded above is eligible for removal.
        sql(f'DROP DATABASE "{database}" WITH (FORCE)')
        (output / "cleanup.json").write_text(json.dumps({"fixture_database_removed": True}) + "\n")


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
    if args.worker:
        require(args.database and args.database.startswith("phase_canary_"), "fresh fixture database required")
        return worker(args)
    return host(args)


if __name__ == "__main__":
    raise SystemExit(main())
