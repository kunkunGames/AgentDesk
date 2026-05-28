#!/usr/bin/env python3
"""Run the AgentDesk Discord relay E2E matrix.

This is the operator/orchestrator entry point. It drives worker channels from
outside those workers, so a worker never runs a driver against its own mailbox.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

sys.path.insert(0, str(Path(__file__).resolve().parent))

import run_tui_relay as cell_driver  # noqa: E402


DEFAULT_CONFIG = Path.home() / ".adk" / "release" / "config" / "agentdesk.yaml"
DEFAULT_CELLS = cell_driver.SUPPORTED_CELLS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8791")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--cells", default=",".join(DEFAULT_CELLS))
    parser.add_argument("--filter", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--twice", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-destructive", action="store_true")
    parser.add_argument(
        "--hard-reset-session-each",
        action="store_true",
        help="Pass through to cell driver; only use for dedicated E2E cells.",
    )
    parser.add_argument(
        "--turn-start-timeout-s",
        type=float,
        default=float(os.environ.get("AGENTDESK_E2E_TURN_START_TIMEOUT_S", "180")),
    )
    return parser.parse_args()


def load_channel_ids(config_path: Path) -> dict[str, str]:
    with config_path.open("r", encoding="utf-8") as fp:
        config = yaml.safe_load(fp)
    agents = config.get("agents") if isinstance(config, dict) else None
    if not isinstance(agents, list):
        raise ValueError(f"{config_path} has no agents list")

    resolved: dict[str, str] = {}
    for cell in DEFAULT_CELLS:
        agent_id = cell_driver.cell_default_agent(cell)
        provider = cell_driver.cell_provider(cell)
        agent = next(
            (
                item
                for item in agents
                if isinstance(item, dict) and item.get("id") == agent_id
            ),
            None,
        )
        if not isinstance(agent, dict):
            raise ValueError(f"agent {agent_id!r} not found in {config_path}")
        channels = agent.get("channels")
        channel = channels.get(provider) if isinstance(channels, dict) else None
        channel_id = channel.get("id") if isinstance(channel, dict) else None
        if not channel_id:
            raise ValueError(f"agent {agent_id!r} has no {provider} channel id")
        resolved[cell] = str(channel_id)
    return resolved


def parse_cells(raw: str) -> list[str]:
    cells = [cell.strip() for cell in raw.split(",") if cell.strip()]
    unknown = [cell for cell in cells if cell not in DEFAULT_CELLS]
    if unknown:
        raise ValueError(f"unsupported cell(s): {', '.join(unknown)}")
    return cells


def resolve_output_dir(arg: str | None) -> Path:
    if arg:
        out = Path(arg)
    else:
        run_id = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        out = Path("out/e2e/tui_relay") / f"matrix-{run_id}"
    out.mkdir(parents=True, exist_ok=True)
    return out


def run_cell(
    *,
    cell: str,
    channel_id: str,
    args: argparse.Namespace,
    output_dir: Path,
    pass_index: int,
) -> dict[str, Any]:
    cell_output = output_dir / f"pass-{pass_index}" / cell
    cmd = [
        sys.executable,
        "scripts/e2e/run_tui_relay.py",
        "--base-url",
        args.base_url,
        "--cell",
        cell,
        "--channel-id",
        channel_id,
        "--output",
        str(cell_output),
        "--turn-start-timeout-s",
        str(args.turn_start_timeout_s),
    ]
    if args.filter:
        cmd.extend(["--filter", args.filter])
    if args.dry_run:
        cmd.append("--dry-run")
    if args.allow_destructive:
        cmd.append("--allow-destructive")
    if args.hard_reset_session_each:
        cmd.append("--hard-reset-session-each")

    print(f"[matrix] pass={pass_index} cell={cell} channel={channel_id}")
    print("[matrix] command=" + " ".join(cmd))
    proc = subprocess.run(cmd, check=False, text=True)
    report_path = cell_output / f"report.{cell}.json"
    report: dict[str, Any] | None = None
    if report_path.exists():
        report = json.loads(report_path.read_text(encoding="utf-8"))
    return {
        "pass_index": pass_index,
        "cell": cell,
        "channel_id": channel_id,
        "returncode": proc.returncode,
        "report": str(report_path),
        "totals": (report or {}).get("totals"),
        "ok": proc.returncode == 0 and bool(report),
    }


def main() -> int:
    args = parse_args()
    cells = parse_cells(args.cells)
    channel_ids = load_channel_ids(Path(args.config).expanduser())
    output_dir = resolve_output_dir(args.output)
    pass_count = 2 if args.twice else 1

    results: list[dict[str, Any]] = []
    for pass_index in range(1, pass_count + 1):
        for cell in cells:
            result = run_cell(
                cell=cell,
                channel_id=channel_ids[cell],
                args=args,
                output_dir=output_dir,
                pass_index=pass_index,
            )
            results.append(result)
            totals = result.get("totals") or {}
            print(
                "[matrix] result "
                f"pass={pass_index} cell={cell} ok={result['ok']} "
                f"pass_count={totals.get('pass')} fail={totals.get('fail')} "
                f"skip={totals.get('skipped')} report={result['report']}"
            )

    summary = {
        "output": str(output_dir),
        "cells": cells,
        "passes": pass_count,
        "results": results,
        "ok": all(result["ok"] for result in results),
    }
    summary_path = output_dir / "matrix.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[matrix] report -> {summary_path}")
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
