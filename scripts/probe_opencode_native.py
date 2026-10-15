#!/usr/bin/env python3
"""Probe one installed OpenCode contract with a loopback model and disposable home.

No provider credentials, production config, or real model are used. This is
native-tool contract evidence, not model quality or 24-hour operating evidence.
The supplied binary must support the V1 session permission contract.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class ModelFixture(ThreadingHTTPServer):
    def __init__(self, workspace: Path):
        super().__init__(("127.0.0.1", 0), ModelHandler)
        self.workspace = workspace
        self.requests: list[dict] = []
        self.tool = "read"
        self.tool_script: list[tuple[str, dict]] | None = None
        self.stall_gate: threading.Event | None = None


class ModelHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append(body)
        if self.server.stall_gate is not None:
            self.server.stall_gate.wait(timeout=30)
        messages = body.get("messages", [])
        result_count = sum(message.get("role") == "tool" for message in messages)
        script = self.server.tool_script
        if result_count >= (len(script) if script is not None else 1):
            delta = {"role": "assistant", "content": "fixture finished"}
            finish = "stop"
        else:
            if script is not None:
                tool, arguments = script[result_count]
            else:
                tool = self.server.tool
                arguments = (
                    {"filePath": str(self.server.workspace / "allowed.txt")}
                    if tool == "read"
                    else {"command": "echo forbidden > forbidden.txt", "description": "native denial probe"}
                )
            delta = {"role": "assistant", "tool_calls": [{
                "index": 0, "id": f"fixture_call_{result_count}", "type": "function",
                "function": {"name": tool, "arguments": json.dumps(arguments)},
            }]}
            finish = "tool_calls"
        chunks = []
        for payload, reason in [(delta, None), ({}, finish)]:
            chunks.append("data: " + json.dumps({
                "id": "fixture_completion", "object": "chat.completion.chunk",
                "created": 1, "model": "fixture",
                "choices": [{"index": 0, "delta": payload, "finish_reason": reason}],
            }) + "\n\n")
        output = ("".join(chunks) + "data: [DONE]\n\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(output)))
        self.end_headers()
        self.wfile.write(output)


def request(origin: str, path: str, body=None, method=None):
    data = None if body is None else json.dumps(body).encode()
    req = Request(origin + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=30) as response:
        payload = response.read()
        return response.status, json.loads(payload) if payload else None


def isolated_environment(root: Path, model_port: int) -> dict[str, str]:
    env = {key: os.environ[key] for key in ("SYSTEMROOT", "WINDIR", "COMSPEC", "PATH") if key in os.environ}
    for key in ("USERPROFILE", "HOME", "APPDATA", "LOCALAPPDATA", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME", "TEMP", "TMP"):
        directory = root / key.lower()
        directory.mkdir()
        env[key] = str(directory)
    config = {
        "enabled_providers": ["contract-fixture"],
        "model": "contract-fixture/fixture", "small_model": "contract-fixture/fixture",
        "permission": {"*": "deny", "read": "allow", "bash": "allow"}, "plugin": [], "autoupdate": False,
        "compaction": {"auto": False},
        "agent": {
            "build": {"steps": 3},
            "denied-shell": {"mode": "primary", "steps": 3, "permission": {"bash": "deny"}},
        },
        "provider": {"contract-fixture": {
            "npm": "@ai-sdk/openai-compatible", "name": "Loopback contract fixture",
            "options": {"baseURL": f"http://127.0.0.1:{model_port}/v1", "apiKey": "fixture-only"},
            "models": {"fixture": {"name": "fixture", "limit": {"context": 8192, "output": 1024}}},
        }},
    }
    env.update(
        OPENCODE_CONFIG_CONTENT=json.dumps(config),
        OPENCODE_DISABLE_MODELS_FETCH="true", OPENCODE_DISABLE_AUTOUPDATE="true",
        OPENCODE_DISABLE_DEFAULT_PLUGINS="true",
    )
    return env


def probe(binary: Path, expected_version: str) -> dict:
    with binary.open("rb") as source:
        binary_sha256 = hashlib.file_digest(source, "sha256").hexdigest()
    with tempfile.TemporaryDirectory(prefix="agentdesk-native-contract-") as temp:
        root = Path(temp)
        workspace = root / "workspace"
        workspace.mkdir()
        (workspace / "allowed.txt").write_text("NATIVE_READ_RECEIPT", encoding="utf-8")
        model = ModelFixture(workspace)
        model_thread = threading.Thread(target=model.serve_forever, daemon=True)
        model_thread.start()
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        origin = f"http://127.0.0.1:{port}"
        env = isolated_environment(root, model.server_port)
        child = None
        try:
            with (root / "server.log").open("wb") as log:
                child = subprocess.Popen(
                    [str(binary), "serve", "--pure", "--hostname", "127.0.0.1", "--port", str(port)],
                    cwd=workspace, env=env, stdout=log, stderr=log,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
                deadline = time.monotonic() + 30
                while True:
                    try:
                        _, health = request(origin, "/global/health")
                        break
                    except Exception:
                        if child.poll() is not None or time.monotonic() >= deadline:
                            raise RuntimeError("isolated OpenCode did not become ready")
                        time.sleep(.1)
                if health.get("version") != expected_version:
                    raise RuntimeError(f"unsupported binary version: {health.get('version')!r}; expected {expected_version}")
                _, agents = request(origin, "/agent")
                effective = {agent["name"]: agent["permission"] for agent in agents}
                observations = {}
                cases = [
                    ("read", "read", "build", ["read"], False),
                    ("bash_denied", "bash", "build", ["read"], False),
                    ("profile_deny_preserved", "bash", "denied-shell", ["read", "bash"], False),
                    ("bash_allowed", "bash", "build", ["read", "bash"], True),
                ]
                for case, tool, agent, categories, allow_bash in cases:
                    model.tool = tool
                    model.requests.clear()
                    session_rules = [{"permission": "*", "pattern": "*", "action": "deny"}]
                    for rule in effective[agent]:
                        for category in sorted(categories):
                            if rule["permission"] in ("*", category):
                                session_rules.append({**rule, "permission": category})
                    _, session = request(origin, "/session", {"permission": session_rules})
                    if session.get("permission") != session_rules:
                        raise RuntimeError("session permission rules were ignored or changed")
                    sid = session["id"]
                    try:
                        _, response = request(origin, f"/session/{sid}/message", {
                            "agent": agent,
                            "model": {"providerID": "contract-fixture", "modelID": "fixture"},
                            "parts": [{"type": "text", "text": "Run the fixture tool once, then finish."}],
                        })
                        _, history = request(origin, f"/session/{sid}/message")
                        parts = [part for message in history for part in message.get("parts", [])]
                        tools = [part for part in parts if part.get("type") == "tool"]
                        if not model.requests:
                            raise RuntimeError("no model fixture request; tool probe did not execute")
                        if tool == "read":
                            passed = any(part.get("tool") == "read" and part.get("state", {}).get("status") == "completed" and "NATIVE_READ_RECEIPT" in part.get("state", {}).get("output", "") for part in tools)
                        elif allow_bash:
                            passed = any(part.get("tool") == "bash" and part.get("state", {}).get("status") == "completed" for part in tools)
                            passed = passed and (workspace / "forbidden.txt").is_file()
                        else:
                            passed = any(part.get("tool") in ("bash", "invalid") and part.get("state", {}).get("status") in ("error", "completed") and any(word in json.dumps(part).lower() for word in ("denied", "reject", "not available", "not found", "invalid")) for part in tools)
                            passed = passed and not (workspace / "forbidden.txt").exists()
                        observations[case] = {"passed": passed, "model_requests": len(model.requests), "tools": [{"tool": p.get("tool"), "state": p.get("state", {}).get("status"), "error": p.get("state", {}).get("error")} for p in tools]}
                        if not passed:
                            raise RuntimeError(f"{case} native probe failed: {json.dumps({'observation': observations[case], 'response': response})}")
                    finally:
                        request(origin, f"/session/{sid}/abort", {})
                        request(origin, f"/session/{sid}", method="DELETE")
                try:
                    _, invalid = request(origin, "/session", {"permission": [{"permission": "read", "pattern": "*", "action": "invalid-action"}]})
                except HTTPError as error:
                    if error.code != 400:
                        raise
                else:
                    request(origin, f"/session/{invalid['id']}", method="DELETE")
                    raise RuntimeError("invalid permission action was accepted")
                with binary.open("rb") as source:
                    if hashlib.file_digest(source, "sha256").hexdigest() != binary_sha256:
                        raise RuntimeError("OpenCode binary changed during the native probe")
                return {"binary": str(binary), "binary_sha256": binary_sha256, "version": health["version"], "contract": "session.permission/1", "invalid_action_rejected": True, "observations": observations, "real_model_used": False, "production_config_used": False}
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
            model_thread.join(timeout=5)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = probe(args.binary.resolve(strict=True), args.expected_version)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
