#!/usr/bin/env python3
"""Stage a bounded native CLI profile for existing AgentDesk routines.

Reuses installed provider CLIs and their existing authentication. Contains no
scheduler, model HTTP client, credential extraction or service activation.
The Linux managed-execution contract remains a separate, unsupported-on-Mac
capability; OpenCode steps are behavioral guidance, not a hard tool budget.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import shlex
import subprocess


def profile_files(home, output, qwen_config, opencode_config):
    entries = qwen_config.get("modelProviders", {}).get("openai", [])
    existing = next(item for item in entries if item.get("id") == "z-ai/glm-5.3")
    # Copy only provider identity and the NAME of its existing environment
    # binding. Never extract an API key or change the authentication store.
    model = {key: existing[key] for key in ("baseUrl", "envKey") if key in existing}
    model.update(id="z-ai/glm-5.3-flash", name="GLM 5.3 Flash routine executor",
                 generationConfig={"timeout": 180000, "maxRetries": 1,
                                   "reasoning": True, "samplingParams": {"max_tokens": 4096}})
    # Qwen 0.22.3 treats eight different shell commands as action stagnation.
    # Disable that heuristic for maintenance; identical-call/shell-repeat guards
    # and the explicit 60-call cap remain always on in this native version.
    qwen = {"modelProviders": {"openai": [model]},
            "model": {"maxWallTimeSeconds": 1800, "maxToolCalls": 60,
                      "maxSessionTurns": 30, "sessionTokenLimit": 180000,
                      "maxSubagentDepth": 1, "skipLoopDetection": True}}
    agent = opencode_config.get("default_agent") or "agentdesk-primary"
    opencode = {"model": "deepseek/z-ai/glm-5.3-flash",
                "small_model": "deepseek/z-ai/glm-5.3-flash", "default_agent": agent,
                "agent": {agent: {"model": "deepseek/z-ai/glm-5.3-flash", "steps": 24}},
                "provider": {"deepseek": {"models": {"z-ai/glm-5.3-flash": {
                    "name": "GLM 5.3 Flash", "limit": {"context": 131072, "output": 8192}}}}},
                "subagent_depth": 1, "permission": {"task": "deny"}, "plugin": [],
                "mcp": {name: {"enabled": False} for name in opencode_config.get("mcp", {})}}
    runtime_bin = home / ".adk/runtime-bin"
    cache = home / ".adk/runtime-data/cache"
    variables = {
        "PATH": f"{runtime_bin}:{home}/.adk/rustup/toolchains/1.94.1-aarch64-apple-darwin/bin:/Library/Developer/CommandLineTools/usr/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin",
        "AGENTDESK_PR_BATCH_SIZE": "1", "AGENTDESK_MONOLITH_PR_BATCH_SIZE": "1",
        "XDG_CACHE_HOME": str(cache), "RUSTUP_HOME": str(home / ".adk/rustup"),
        "CARGO_HOME": str(home / ".adk/cargo"),
        "LIBOPUS_LIB_DIR": "/opt/homebrew/Cellar/opus/1.6.1", "LIBOPUS_NO_PKG": "1",
        "AGENTDESK_SYNC_REPO_DIR": str(home / ".adk/repositories/agentdesk"),
        "AGENTDESK_SYNC_WORK_ROOT": str(home / ".adk/runtime-data/upstream-worktrees"),
        "GIT_CONFIG_COUNT": "2",
        "GIT_CONFIG_KEY_0": "credential.https://github.com.helper", "GIT_CONFIG_VALUE_0": "",
        "GIT_CONFIG_KEY_1": "credential.https://github.com.helper",
        "GIT_CONFIG_VALUE_1": f"!{runtime_bin}/gh auth git-credential",
    }
    common = "#!/bin/sh\nset -eu\numask 077\n" + "".join(
        f"export {key}={shlex.quote(value)}\n" for key, value in variables.items())
    qwen_wrapper = common + 'export QWEN_CODE_SYSTEM_SETTINGS_PATH="${QWEN_CODE_SYSTEM_SETTINGS_PATH:-' + \
        str(output / 'qwen.json') + '}"\n' + \
        'exec /opt/homebrew/bin/qwen --exclude-tools agent "$@"\n'
    opencode_wrapper = common + "export OPENCODE_CONFIG_CONTENT=" + shlex.quote(json.dumps(opencode)) + "\n" + \
        'exec /opt/homebrew/bin/opencode "$@"\n'
    variables.update(AGENTDESK_QWEN_PATH=str(output / "bin/qwen"),
                     AGENTDESK_OPENCODE_PATH=str(output / "bin/opencode"),
                     QWEN_CODE_SYSTEM_SETTINGS_PATH=str(output / "qwen.json"))
    return {"qwen.json": json.dumps(qwen, indent=2) + "\n",
            "opencode.json": json.dumps(opencode, indent=2) + "\n",
            "bin/qwen": qwen_wrapper, "bin/opencode": opencode_wrapper,
            "launchd-environment.json": json.dumps(variables, indent=2) + "\n",
            "scope.json": json.dumps({
                "scheduler": "existing AgentDesk routines", "model": "z-ai/glm-5.3-flash",
                "qwen_headless_limits": {"seconds": 1800, "tool_calls": 60, "turns": 30},
                "qwen_heuristic_loop_checks": False, "qwen_always_on_guards": True,
                "opencode_steps_advisory": 24, "routine_timeout_required": True,
                "managed_execution_v1": False, "historical_external_state_copied": False,
                "routines_to_reuse": ["qwen-skill-sync-daily-backup", "qwen-agentdesk-upstream-sync-daily",
                                     "qwen-agentdesk-daily-pr-review", "monitoring-agentdesk-pr-merge-ready-daily",
                                     "monitoring-monolith-daily-pr-review", "monitoring-working-watchdog"],
                "task_rules": ["fresh attempt; no external-state session resume", "one PR per native maintenance run; preserve remaining queue",
                               "no admin merge, plain force push or branch deletion", "no model fallback until accepted",
                               "provider error is not task completion", "preserve business schedules and checkpoints"],
            }, indent=2) + "\n"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if platform.system() != "Darwin":
        raise RuntimeError("native macOS profile staging required")
    home = Path.home().resolve()
    output = Path(args.output).resolve()
    if not output.is_relative_to(home / ".adk") or output.exists():
        raise RuntimeError("output must be a new directory below the internal ~/.adk root")
    for path in [home / ".adk/runtime-bin/gh", home / ".qwen/projects",
                 home / ".local/share/opencode/snapshot", home / ".local/share/opencode/tool-output"]:
        if not path.exists() or not path.resolve().is_relative_to(home / ".adk"):
            raise RuntimeError(f"runtime prerequisite is not on the internal profile path: {path}")
    qwen = json.loads((home / ".qwen/settings.json").read_text())
    opencode = json.loads((home / ".config/opencode/opencode.json").read_text())
    files = profile_files(home, output, qwen, opencode)
    output.mkdir(parents=True, mode=0o700)
    (home / ".adk/runtime-data/cache").mkdir(parents=True, exist_ok=True, mode=0o700)
    for name, contents in files.items():
        target = output / name
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with target.open("x") as handle:
            handle.write(contents)
        target.chmod(0o700 if name.startswith("bin/") else 0o600)
    for name in ["qwen", "opencode"]:
        subprocess.run(["/bin/sh", "-n", str(output / "bin" / name)], check=True)
        result = subprocess.run([str(output / "bin" / name), "--version"], check=True,
                                capture_output=True, text=True, timeout=15)
        print(json.dumps({"provider": name, "version": result.stdout.strip()}))
    print(json.dumps({"staged": str(output), "activated": False, "files": sorted(files)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
