#!/usr/bin/env python3
"""Observe native permission-before-effect and terminal contracts, without a real model.

This probe does not implement AgentDesk's durable budget or prove process/OS
isolation. It records the native protocol that the existing adapter must bind.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import socket
import subprocess
import sys
import tempfile
import threading
import time
from urllib.request import urlopen

from probe_opencode_native import ModelFixture, isolated_environment, request


def serve_model_fixture(case: str):
    """Owned by a Rust test; the actual AgentDesk adapter launches OpenCode."""
    with tempfile.TemporaryDirectory(prefix="agentdesk-adapter-native-") as temp:
        root = Path(temp)
        workspace = root / "workspace"
        workspace.mkdir()
        target = workspace / "allowed.txt"
        target.write_text("BEFORE_NATIVE_EDIT\n")
        if os.name != "posix" or os.geteuid() != 0:
            raise RuntimeError("managed native fixture requires disposable Linux root launcher")
        root.chmod(0o755)
        workspace.chmod(0o700)
        os.chown(workspace, 65534, 65534)
        os.chown(target, 65534, 65534)
        outside = root / "outside.txt"
        outside.write_text("OUTSIDE_SYNTHETIC_MARKER\n")
        outside.chmod(0o666)
        linked = workspace / "alias.txt"
        linked.symlink_to(outside)
        (workspace / "environment.txt").symlink_to("/proc/self/environ")
        model = ModelFixture(workspace)
        model.tool_script = (
            [("read", {"filePath": str(target)}),
             ("edit", {"filePath": str(target), "oldString": "BEFORE_NATIVE_EDIT", "newString": "AFTER_NATIVE_EDIT"})]
            if case == "read_edit" else [("read", {"filePath": str(target)})] * 4
        )
        if case == "aliases":
            model.tool_script = [
                ("read", {"filePath": str(linked)}),
                ("edit", {"filePath": str(linked), "oldString": "OUTSIDE_SYNTHETIC_MARKER", "newString": "ESCAPED"}),
                ("read", {"filePath": str(workspace / "environment.txt")}),
            ]
        if case == "quiet":
            model.stall_gate = threading.Event()
        threading.Thread(target=model.serve_forever, daemon=True).start()
        env = isolated_environment(root, model.server_port)
        config = json.loads(env["OPENCODE_CONFIG_CONTENT"])
        config.update(lsp=False, formatter=False, permission={"*": "deny", "read": "allow", "edit": "allow"})
        profile_root = root / "profiles"
        profile_home = profile_root / "opencode" / "native-contract-fixture"
        config_path = profile_home / "xdg-config" / "opencode" / "opencode.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(json.dumps(config))
        config_path.chmod(0o600)
        try:
            print(json.dumps({"workspace": str(workspace), "profile_home": str(profile_home), "profile_root": str(profile_root)}), flush=True)
            sys.stdin.readline()
            tool_results = [message for body in model.requests for message in body.get("messages", []) if message.get("role") == "tool"]
            print(json.dumps({"model_requests": len(model.requests), "case": case,
                "outside_unchanged": outside.read_text() == "OUTSIDE_SYNTHETIC_MARKER\n",
                "outside_marker_leaked": "OUTSIDE_SYNTHETIC_MARKER" in json.dumps(tool_results),
                "credential_marker_leaked": "synthetic-native-provider-token" in json.dumps(tool_results)}), flush=True)
        finally:
            if model.stall_gate is not None:
                model.stall_gate.set()
            model.shutdown()
            model.server_close()


def events(origin: str, output: queue.Queue):
    try:
        with urlopen(origin + "/global/event", timeout=30) as stream:
            for line in stream:
                if line.startswith(b"data:"):
                    envelope = json.loads(line[5:])
                    output.put(envelope.get("payload", envelope))
    except Exception as error:
        output.put(error)


def run_case(origin: str, model: ModelFixture, case: str, tool_script: list, budget: int) -> dict:
    model.requests.clear()
    model.tool_script = tool_script
    title = "AgentDesk managed contract " + case
    rules = [{"permission": "*", "pattern": "*", "action": "deny"}]
    rules += [{"permission": category, "pattern": "*", "action": "ask"} for category in ("read", "edit")]
    _, session = request(origin, "/session", {"title": title, "permission": rules})
    assert session["title"] == title and session["permission"] == rules
    sid = session["id"]
    observed = queue.Queue()
    reader = threading.Thread(target=events, args=(origin, observed), daemon=True)
    reader.start()
    # Confirm the SSE connection before starting the native execution.
    while True:
        event = observed.get(timeout=10)
        if isinstance(event, Exception):
            raise event
        if event.get("type") == "server.connected":
            break
    request(origin, f"/session/{sid}/prompt_async", {
        "agent": "build", "model": {"providerID": "contract-fixture", "modelID": "fixture"},
        "parts": [{"type": "text", "text": "Execute the fixture sequence, then finish."}],
    })
    grants = []
    seen = set()
    deadline = time.monotonic() + 30
    terminal_seen = False
    while time.monotonic() < deadline:
        event = observed.get(timeout=max(.1, deadline - time.monotonic()))
        if isinstance(event, Exception):
            raise event
        props = event.get("properties", {})
        if props.get("sessionID") != sid:
            continue
        if event.get("type") == "permission.asked":
            permission_id = props["id"]
            assert props["permission"] in ("read", "edit")
            assert props.get("tool", {}).get("callID"), "permission must identify the tool call"
            if permission_id in seen:
                continue
            seen.add(permission_id)
            if props["permission"] == "edit":
                assert (model.workspace / "allowed.txt").read_text() == "BEFORE_NATIVE_EDIT\n", "write occurred before permission"
            reply = "once" if len(grants) < budget else "reject"
            status, _ = request(origin, f"/permission/{permission_id}/reply", {"reply": reply})
            assert status == 200
            grants.append({"permission": props["permission"], "reply": reply, "call_id": props["tool"]["callID"]})
        elif event.get("type") == "session.idle" and grants:
            terminal_seen = True
            break
        elif event.get("type") == "session.error":
            raise AssertionError("native session error: " + json.dumps(props))
    assert terminal_seen, "missing matching terminal event"
    _, status = request(origin, "/session/status")
    _, history = request(origin, f"/session/{sid}/message")
    parts = [part for message in history for part in message.get("parts", []) if part.get("type") == "tool"]
    states = [part["state"]["status"] for part in parts]
    assert all(state in ("completed", "error") for state in states), states
    assert sid not in status or status[sid].get("type") == "idle", status
    completed = sum(state == "completed" for state in states)
    expected = min(len(tool_script), budget)
    assert completed == expected, {"completed": completed, "expected": expected, "grants": grants, "states": states}
    if case == "read_then_edit":
        assert (model.workspace / "allowed.txt").read_text() == "AFTER_NATIVE_EDIT\n"
        assert [grant["permission"] for grant in grants] == ["read", "edit"]
    else:
        assert [grant["reply"] for grant in grants] == ["once"] * budget + ["reject"]
    return {"case": case, "budget": budget, "grants": grants, "completed_tools": completed,
            "tool_states": states, "matching_idle_seen": terminal_seen,
            "native_status_idle": True, "model_requests": len(model.requests),
            "agentdesk_budget_connected": False, "physical_quiescence_proven": False}


def probe(binary: Path) -> dict:
    before = hashlib.sha256(binary.read_bytes()).hexdigest()
    with tempfile.TemporaryDirectory(prefix="agentdesk-managed-native-") as temp:
        root = Path(temp)
        workspace = root / "workspace"
        workspace.mkdir()
        target = workspace / "allowed.txt"
        target.write_text("BEFORE_NATIVE_EDIT\n")
        model = ModelFixture(workspace)
        threading.Thread(target=model.serve_forever, daemon=True).start()
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        origin = f"http://127.0.0.1:{port}"
        env = isolated_environment(root, model.server_port)
        config = json.loads(env["OPENCODE_CONFIG_CONTENT"])
        config.update(lsp=False, formatter=False, permission={"*": "deny", "read": "allow", "edit": "allow"})
        env["OPENCODE_CONFIG_CONTENT"] = json.dumps(config)
        child = None
        try:
            with (root / "server.log").open("wb") as log:
                child = subprocess.Popen([str(binary), "serve", "--pure", "--hostname", "127.0.0.1", "--port", str(port)],
                    cwd=workspace, env=env, stdout=log, stderr=log,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
                until = time.monotonic() + 30
                while True:
                    try:
                        _, health = request(origin, "/global/health")
                        break
                    except Exception:
                        if child.poll() is not None or time.monotonic() >= until:
                            raise RuntimeError("isolated OpenCode failed to start")
                        time.sleep(.1)
                assert health.get("version") == "1.14.41", health
                cases = [run_case(origin, model, "budget_three", [("read", {"filePath": str(target)})] * 4, 3),
                         run_case(origin, model, "read_then_edit", [
                             ("read", {"filePath": str(target)}),
                             ("edit", {"filePath": str(target), "oldString": "BEFORE_NATIVE_EDIT", "newString": "AFTER_NATIVE_EDIT"}),
                         ], 2)]
                assert hashlib.sha256(binary.read_bytes()).hexdigest() == before
                return {"version": health["version"], "binary_sha256": before, "cases": cases,
                        "real_model_used": False, "production_config_used": False, "passed": True}
        finally:
            if child is not None:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
            model.shutdown()
            model.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--serve-model-fixture", choices=("read_edit", "exhausted", "quiet", "aliases"))
    args = parser.parse_args()
    if args.serve_model_fixture:
        serve_model_fixture(args.serve_model_fixture)
        sys.exit(0)
    if args.binary is None or args.output is None:
        parser.error("--binary and --output are required for a standalone native probe")
    result = probe(args.binary.resolve(strict=True))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
