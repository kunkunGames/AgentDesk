#!/usr/bin/env python3
"""Measure whether native steps forcibly bounds a deliberately repeating model.

The model runs on loopback and can only request a read in a disposable workspace.
An independent request ceiling and wall-clock ceiling bound the experiment.
Exit 2 means the configured native steps did not provide a hard execution cap;
it must not be reported as a successful 24/7 acceptance test.
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

from probe_opencode_native import isolated_environment, request


class RepeatingModel(ThreadingHTTPServer):
    def __init__(self, workspace: Path, maximum: int):
        super().__init__(("127.0.0.1", 0), RepeatingHandler)
        self.workspace = workspace
        self.maximum = maximum
        self.main_requests: list[dict] = []
        self.auxiliary_requests = 0
        self.lock = threading.Lock()
        self.ceiling_hit = threading.Event()


class RepeatingHandler(BaseHTTPRequestHandler):
    server: RepeatingModel

    def log_message(self, *_args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        auxiliary = body.get("model") == "auxiliary"
        with self.server.lock:
            if auxiliary:
                self.server.auxiliary_requests += 1
            else:
                # Store only observations needed for the claim, not request text.
                self.server.main_requests.append({
                    "advertised_tools": len(body.get("tools", [])),
                    "tool_choice": body.get("tool_choice"),
                })
            sequence = len(self.server.main_requests)
        if not auxiliary and sequence > self.server.maximum:
            self.server.ceiling_hit.set()
            self.send_error(503, "fixture request safety bound")
            return
        if auxiliary:
            delta = {"role": "assistant", "content": "Fixture title"}
            finish = "stop"
        else:
            delta = {"role": "assistant", "tool_calls": [{
                "index": 0, "id": f"read_{sequence}", "type": "function",
                "function": {"name": "read", "arguments": json.dumps({
                    "filePath": str(self.server.workspace / "allowed.txt"),
                })},
            }]}
            finish = "tool_calls"
        chunks = []
        for payload, reason in [(delta, None), ({}, finish)]:
            chunks.append("data: " + json.dumps({
                "id": f"fixture_{sequence}", "object": "chat.completion.chunk",
                "created": 1, "model": body.get("model"),
                "choices": [{"index": 0, "delta": payload, "finish_reason": reason}],
            }) + "\n\n")
        output = ("".join(chunks) + "data: [DONE]\n\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(output)))
        self.end_headers()
        self.wfile.write(output)


def probe(binary: Path, version: str, steps: int, maximum: int) -> dict:
    with binary.open("rb") as source:
        binary_sha256 = hashlib.file_digest(source, "sha256").hexdigest()
    with tempfile.TemporaryDirectory(prefix="adk-opencode-steps-") as directory:
        root = Path(directory)
        workspace = root / "workspace"
        workspace.mkdir()
        (workspace / "allowed.txt").write_text("BOUNDED_READ_FIXTURE\n", encoding="utf-8")
        model = RepeatingModel(workspace, maximum)
        thread = threading.Thread(target=model.serve_forever, daemon=True)
        thread.start()
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        origin = f"http://127.0.0.1:{port}"
        env = isolated_environment(root, model.server_port)
        config = json.loads(env["OPENCODE_CONFIG_CONTENT"])
        config["permission"] = {"*": "deny", "read": "allow"}
        config["agent"] = {"build": {"steps": steps}}
        config["small_model"] = "contract-fixture/auxiliary"
        config["provider"]["contract-fixture"]["models"]["auxiliary"] = {
            "name": "auxiliary", "limit": {"context": 8192, "output": 256},
        }
        env["OPENCODE_CONFIG_CONTENT"] = json.dumps(config)
        child = None
        try:
            with (root / "server.log").open("wb") as log:
                child = subprocess.Popen(
                    [str(binary), "serve", "--pure", "--hostname", "127.0.0.1", "--port", str(port)],
                    cwd=workspace, env=env, stdout=log, stderr=log,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
                ready_by = time.monotonic() + 30
                while True:
                    try:
                        _, health = request(origin, "/global/health")
                        break
                    except Exception:
                        if child.poll() is not None or time.monotonic() >= ready_by:
                            raise RuntimeError("isolated OpenCode did not become ready")
                        time.sleep(.1)
                if health.get("version") != version:
                    raise RuntimeError("the installed version does not match the approved contract")
                _, agents = request(origin, "/agent")
                build = next(agent for agent in agents if agent["name"] == "build")
                if build.get("steps") != steps:
                    raise RuntimeError("native agent did not bind configured steps")
                _, session = request(origin, "/session", {"permission": [
                    {"permission": "*", "pattern": "*", "action": "deny"},
                    {"permission": "read", "pattern": "*", "action": "allow"},
                ]})
                sid = session["id"]
                prompt_status, _ = request(origin, f"/session/{sid}/prompt_async", {
                    "agent": "build",
                    "model": {"providerID": "contract-fixture", "modelID": "fixture"},
                    "parts": [{"type": "text", "text": "Exercise the read fixture."}],
                })
                observation_by = time.monotonic() + 30
                inactive = False
                while time.monotonic() < observation_by:
                    _, statuses = request(origin, "/session/status")
                    with model.lock:
                        has_request = bool(model.main_requests)
                    inactive = has_request and sid not in statuses
                    if inactive or model.ceiling_hit.is_set():
                        break
                    time.sleep(.1)
                _, history = request(origin, f"/session/{sid}/message")
                tools = [part for message in history for part in message.get("parts", []) if part.get("type") == "tool"]
                _, status_before_abort = request(origin, "/session/status")
                abort_status, _ = request(origin, f"/session/{sid}/abort", {})
                with model.lock:
                    main_requests = list(model.main_requests)
                    auxiliary_requests = model.auxiliary_requests
                with binary.open("rb") as source:
                    if hashlib.file_digest(source, "sha256").hexdigest() != binary_sha256:
                        raise RuntimeError("OpenCode binary changed during the probe")
                bounded = bool(main_requests) and len(main_requests) <= steps and inactive
                return {
                    "binary": str(binary), "binary_sha256": binary_sha256, "version": version,
                    "configured_steps": steps, "effective_steps": build["steps"],
                    "max_successful_fixture_responses": maximum,
                    "main_model_requests": len(main_requests), "auxiliary_model_requests": auxiliary_requests,
                    "model_request_observations": main_requests, "prompt_status": prompt_status,
                    "observed_tool_states": [{"tool": part.get("tool"), "status": part.get("state", {}).get("status")} for part in tools],
                    "session_status_before_abort": status_before_abort.get(sid),
                    "steps_enforced_against_repeating_fixture": bounded,
                    "abort_acknowledged": 200 <= abort_status < 300,
                    "real_model_used": False, "production_config_used": False,
                    "quiescence_proven_by_abort": False,
                }
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
            thread.join(timeout=5)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--steps", type=int, default=3)
    parser.add_argument("--max-fixture-responses", type=int, default=8)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.steps <= 16 or not args.steps + 2 <= args.max_fixture_responses <= 32:
        parser.error("steps must be 1..16 and fixture responses must be steps+2..32")
    result = probe(args.binary.resolve(strict=True), args.expected_version, args.steps, args.max_fixture_responses)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result["steps_enforced_against_repeating_fixture"] else 2)


if __name__ == "__main__":
    main()
