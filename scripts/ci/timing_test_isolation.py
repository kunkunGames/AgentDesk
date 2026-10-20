#!/usr/bin/env python3
"""Run OS-deadline tests after the concurrent sweep, with unchanged assertions.

Selection comes from the same source inventory and non-PG filter as the broad
sweep. The isolated process uses the existing transcript adjudicator with an
empty failure ledger: missing, extra, ignored, or failed tests are fatal.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
import check_test_lane_coverage as coverage
import run_test_lane as runner
import timing_test_isolation_contract as contract

ISOLATED_FILTERS = contract.TIMING_FILTERS


def partition(ids):
    selected = frozenset(ids)
    isolated = frozenset(name for name in selected if contract.is_timing_id(name))
    broad = selected - isolated
    if broad & isolated or broad | isolated != selected:
        raise ValueError("timing test partition lost or duplicated an id")
    return broad, isolated


def full_partition(root: Path):
    skips = coverage.load_non_pg_skip_args(root)
    manifest = root / "scripts/lib_test_inventory_manifest.txt"
    # Partition source IDs from every platform. In particular, Windows has no
    # reviewed static/runtime difference ledger; inventing one would hide tests.
    # The broad library adjudicator retains its reviewed host-specific set, and
    # the isolated adjudicator must observe every selected timing ID on the host.
    inventory = runner.load_lib_inventory_manifest(manifest)
    selected = frozenset(name for name in inventory
                         if not any(value in name for value in skips[1::2]))
    broad, isolated = partition(selected)
    if partition(inventory)[1] != isolated:
        raise ValueError("the canonical non-PG skips swallow a timing test")
    if (not broad or not any(ISOLATED_FILTERS[0] in name for name in isolated)
            or ISOLATED_FILTERS[1] not in isolated):
        raise ValueError("the broad and timing selections must both be nonempty")
    timing_skips = [item for value in ISOLATED_FILTERS for item in ("--skip", value)]
    actual_broad = frozenset(name for name in inventory
                            if not any(value in name for value in
                                       (*skips[1::2], *ISOLATED_FILTERS)))
    if actual_broad != broad:
        raise ValueError("the broad command does not select the partition complement")
    replay = set(coverage.load_non_pg_filter_replay(root))
    if replay & selected:
        raise ValueError("a replay id must not also execute in the broad or timing process")
    return skips, timing_skips, broad, isolated


def cargo_command(*, prepared: bool):
    prefix = [sys.executable, "scripts/ci/lib-test-artifact.py", "run", "--"] if prepared else []
    return [*prefix, "cargo", "test", "--lib"]


def adjudicate(root: Path, ids, command, skips=(), *, env=None) -> int:
    ids = sorted(set(ids))
    if not ids:
        return 0  # A selected PR may name no timing test; full_partition forbids this for a full run.
    if partition(ids)[1] != frozenset(ids):
        raise ValueError("a non-timing id was routed to the isolated process")
    if env is None:
        env = os.environ.copy()
    # One library target and at most one nested harness.
    # The primary summary must still equal the exact id set, including all zeros.
    with tempfile.TemporaryDirectory(prefix="adk-timing-tests-") as name:
        temporary = Path(name).resolve()
        if not temporary.is_relative_to(Path(tempfile.gettempdir()).resolve()):
            raise ValueError("timing expectation directory escaped the temporary root")
        expected = temporary / "expected.ids"
        ledger = temporary / "strict-ledger.txt"
        expected.write_text("\n".join(ids) + "\n", encoding="utf-8")
        ledger.write_text("# Timing tests admit no failure or ignored exception.\n", encoding="utf-8")
        return subprocess.run([
            sys.executable, "scripts/run_test_lane.py", "--lane", "timing-isolated",
            "--max-summaries", "2", "--expected-ids", str(expected), "--ledger", str(ledger),
            *skips, "--", *command], cwd=root, env=env, check=False).returncode


def run_isolated(root: Path, ids, skips=(), *, prepared=True, env=None) -> int:
    if env is None:
        env = os.environ.copy()
        env.pop("AGENTDESK_ROOT_DIR", None)
    command = [*cargo_command(prepared=prepared), "--", "--exact",
               *sorted(set(ids)), "--test-threads=1", *skips]
    return adjudicate(root, ids, command, skips, env=env)


def check_partition(root: Path) -> None:
    _, _, broad, isolated = full_partition(root)
    replay = set(coverage.load_non_pg_filter_replay(root))
    print("timing-test-isolation: " + json.dumps({
        "inventory": "static-all-platforms", "broad": len(broad),
        "isolated": len(isolated), "replay": len(replay), "disjoint": True,
        "union_ids_sha256": hashlib.sha256("\n".join(sorted(broad | isolated | replay)).encode()).hexdigest(),
        "isolated_ids": sorted(isolated), "automatic_retry": False,
    }, sort_keys=True), flush=True)


def run_serial(root: Path, command) -> int:
    if not contract.serial_argv_allowed(command):
        raise ValueError("serial command does not match the closed timing Cargo contract")
    isolated = full_partition(root)[3]
    return adjudicate(root, isolated, command)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("check", "serial"))
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    try:
        if args.operation == "check":
            if args.command:
                raise ValueError("check accepts no command or filter override")
            check_partition(ROOT)
            return 0
        command = args.command[1:] if args.command[:1] == ["--"] else args.command
        return run_serial(ROOT, command)
    except (OSError, ValueError) as error:
        print(f"ERROR timing-test-isolation: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
