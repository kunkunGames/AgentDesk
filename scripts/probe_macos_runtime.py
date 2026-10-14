#!/usr/bin/env python3
"""Native macOS Hub boot/restart canary using an owned, disposable PostgreSQL DB.

Does not configure providers, channels, routines or managed execution. A pass
proves native boot/API/storage only, not Linux containment or 24-hour operation.
"""
import argparse
import getpass
import json
from pathlib import Path
import platform
import secrets
import socket
import subprocess
import tempfile
import time
import uuid

from probe_test_phase_lifecycle import Service, digest, require


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--psql", default="/opt/homebrew/bin/psql")
    parser.add_argument("--database-owner", default="agentdesk")
    parser.add_argument("--native-operations", action="store_true",
                        help="also exercise Campaign admission with a local script fixture; no LLM")
    parser.add_argument("--goal-revisions", action="store_true")
    parser.add_argument("--native-graphs", action="store_true",
                        help="also verify graph goal opt-in and restart persistence; no LLM")
    args = parser.parse_args()
    require(platform.system() == "Darwin", "this canary requires native macOS")
    require(args.database_owner.isidentifier(), "invalid database owner")
    artifact = Path(args.artifact_dir).resolve()
    artifact.mkdir(parents=True, exist_ok=False)
    artifact.chmod(0o700)
    binary = str(Path(args.binary).resolve(strict=True))
    name = "adk24x7_canary_" + uuid.uuid4().hex

    def sql(statement, database="postgres", owner=None):
        command = [args.psql, "-XAt", "--no-password", "-v", "ON_ERROR_STOP=1",
                   "-U", owner or getpass.getuser(), "-d", database, "-c", statement]
        return subprocess.run(command, check=True, capture_output=True,
                              text=True, timeout=30).stdout.strip()

    created = False
    report = {"passed": False, "scope": "native macOS Hub API and PostgreSQL restart; no LLM or managed execution",
              "binary_sha256": digest(binary), "checks": [], "database": name}
    started = time.monotonic()
    try:
        sql(f'CREATE DATABASE "{name}" OWNER "{args.database_owner}"')
        created = True
        with tempfile.TemporaryDirectory(prefix="adk24x7-native-") as temporary:
            root = Path(temporary)
            token = secrets.token_urlsafe(32)
            for directory in ["config", "policies", "data", "home/config", "home/cache", "home/data"]:
                (root / directory).mkdir(parents=True, exist_ok=True)
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            config = {
                "server": {"host": "127.0.0.1", "port": port, "auth_token": token},
                "database": {"enabled": True, "host": "127.0.0.1", "port": 5432,
                             "dbname": name, "user": args.database_owner, "password": "",
                             "pool_max": 8, "foreground_reserve": 2},
                "data": {"dir": str(root / "data"), "db_name": "unused.db"},
                "policies": {"dir": str(root / "policies"), "hot_reload": False},
                "cluster": {"enabled": True, "role": "hub", "runtime_profile": "full",
                            "instance_id": "mac-native-canary", "session_bound_relay_enabled": False},
                "providers": {}, "agents": [], "mcp_servers": {},
                "mcp": {"watch_credentials": False}, "routines": {"enabled": False},
                "github": {"sync_interval_minutes": 0}, "config_hot_reload": False,
            }
            config_path = root / "config/agentdesk.yaml"
            if args.native_operations:
                (root / "routines").mkdir()
                (root / "routines/probe.js").write_text(
                    'agentdesk.routines.register({name:"Campaign canary",tick(){'
                    'return {action:"complete",result:{fixture_receipt:true}};}});')
                config["routines"] = {"enabled": True, "dir": str(root / "routines"),
                                      "tick_interval_secs": 1, "hot_reload": True}
            config_path.write_text(json.dumps(config))
            config_path.chmod(0o600)
            service = Service(binary, root, port, token)
            try:
                service.start()
                status, _ = service.request("cluster/test-phase-runs", authenticated=False)
                require(status == 401, "canary API accepted unauthenticated access")
                report["checks"].append("native_boot_and_admin_authentication")
                payload = dict(id="native-ledger-canary", title="Native restart canary",
                               description="No execution configured", status="planned", round=1, nodes=[])
                status, _ = service.request("campaigns", payload)
                require(status == 201, f"campaign create returned {status}")
                report["checks"].append("native_http_ledger_write")
                service.stop()
                service.start()
                status, _ = service.request("campaigns/native-ledger-canary")
                require(status == 200, "ledger did not survive native restart")
                require(sql("SELECT count(*) FROM _sqlx_migrations WHERE NOT success", name,
                            args.database_owner) == "0", "failed migration")
                report["latest_migration"] = int(sql("SELECT max(version) FROM _sqlx_migrations", name,
                                                       args.database_owner))
                report["checks"].append("native_restart_preserves_ledger_and_migrations")
                if args.native_operations:
                    from probe_campaign_native_operations import check
                    check(service, lambda statement: sql(statement, name, args.database_owner),
                          root, report)
                if args.native_graphs:
                    from probe_campaign_graph_runtime import check
                    check(service, lambda statement: sql(statement, name, args.database_owner), report,
                          revise=args.goal_revisions)
                    from probe_campaign_api_docs import check as check_api_docs
                    check_api_docs(service, report)
                report["passed"] = True
            finally:
                service.stop()
                report["boots"] = service.boots
                for log in [*root.glob("service-*.log"), *root.glob("logs/*.log*")]:
                    artifact.joinpath(log.name).write_text(log.read_text(errors="replace").replace(token, "[redacted]"))
    except Exception as error:
        report["error"] = str(error)
    finally:
        if created:
            # Only the random DB created above is eligible for cleanup.
            require(name.startswith("adk24x7_canary_") and name.isidentifier(), "unsafe cleanup target")
            sql(f'DROP DATABASE "{name}" WITH (FORCE)')
        report["database_dropped"] = created
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        artifact.joinpath("result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
