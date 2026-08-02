#!/usr/bin/env python3
"""Reject operator-specific home paths in deployable portable surfaces."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


SPECIFIC_USER_HOME = re.compile(r"/Users/(?!(?:REPLACE_ME|user|me|example)(?:/|$))[A-Za-z0-9._-]+")
EXCLUDED_RUST_DIRS = {".git", ".claude", "target"}

DEFAULT_PATTERNS = (
    "scripts/_defaults.sh",
    "scripts/auto-queue-monitor.sh",
    "scripts/auto_queue_monitor_state.py",
    "scripts/build-release.sh",
    "scripts/deploy.sh",
    "scripts/deploy-dashboard.sh",
    "scripts/deploy-release.sh",
    "scripts/ensure-agentdesk-cli.sh",
    "scripts/install.sh",
    "scripts/queue-stability-batch.sh",
    "scripts/setup-hooks.sh",
    "scripts/resolve-python-runner.sh",
    "scripts/pg_tunnel.sh",
    "scripts/launchd-migrated/*.sh",
    "scripts/launchd-migrated/*.py",
    "scripts/check-portable-paths.py",
    "scripts/relay_watchdog.py",
    "scripts/operator-init-portable.py",
    "scripts/portable-operator-migration-dry-run.py",
    "policies/**/*",
    "routines/**/*.js",
    "agentdesk.example.yaml",
)


def iter_default_paths(root: Path) -> list[Path]:
    paths: list[Path] = []
    for pattern in DEFAULT_PATTERNS:
        paths.extend(
            path
            for path in root.glob(pattern)
            if path.is_file()
            and not any(part.startswith(".") for part in path.relative_to(root).parts)
        )
    return sorted(set(paths))


def rel_display(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def scan_file(path: Path) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    text = path.read_text(encoding="utf-8")
    for line_no, line in enumerate(text.splitlines(), start=1):
        if SPECIFIC_USER_HOME.search(line):
            hits.append((line_no, line.strip()))
    return hits


def iter_rust_paths(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*.rs")
        if path.is_file()
        and not any(part in EXCLUDED_RUST_DIRS for part in path.relative_to(root).parts)
    )


def _skip_block_comment(text: str, start: int) -> int:
    depth = 1
    index = start + 2
    while index < len(text) and depth:
        if text.startswith("/*", index):
            depth += 1
            index += 2
        elif text.startswith("*/", index):
            depth -= 1
            index += 2
        else:
            index += 1
    return index


def _skip_trivia(text: str, start: int) -> int:
    index = start
    while index < len(text):
        if text[index].isspace():
            index += 1
        elif text.startswith("//", index):
            newline = text.find("\n", index + 2)
            index = len(text) if newline == -1 else newline + 1
        elif text.startswith("/*", index):
            index = _skip_block_comment(text, index)
        else:
            break
    return index


def _quoted_string_end(text: str, start: int) -> int:
    index = start + 1
    while index < len(text):
        if text[index] == "\\":
            index += 2
        elif text[index] == '"':
            return index + 1
        else:
            index += 1
    return len(text)


def _raw_string_span(text: str, start: int) -> tuple[int, str] | None:
    if text[start] != "r":
        return None
    index = start + 1
    while index < len(text) and text[index] == "#":
        index += 1
    if index >= len(text) or text[index] != '"':
        return None
    hashes = text[start + 1:index]
    content_start = index + 1
    closer = '"' + hashes
    content_end = text.find(closer, content_start)
    if content_end == -1:
        return len(text), text[content_start:]
    return content_end + len(closer), text[content_start:content_end]


def _home_literal_end(text: str, start: int) -> int | None:
    if start < len(text) and text[start] == '"':
        end = _quoted_string_end(text, start)
        return end if text[start + 1:end - 1] == "HOME" else None
    raw = _raw_string_span(text, start) if start < len(text) else None
    if raw is None:
        return None
    end, content = raw
    return end if content == "HOME" else None


def _matches_compile_time_home(text: str, token_end: int) -> bool:
    index = _skip_trivia(text, token_end)
    if index >= len(text) or text[index] != "!":
        return False
    index = _skip_trivia(text, index + 1)
    if index >= len(text) or text[index] != "(":
        return False
    index = _skip_trivia(text, index + 1)
    return _home_literal_end(text, index) is not None


def find_compile_time_home_macros(text: str) -> list[int]:
    """Return line numbers for real Rust env!(...HOME...) macro invocations."""

    hits: list[int] = []
    index = 0
    while index < len(text):
        if text.startswith("//", index):
            newline = text.find("\n", index + 2)
            index = len(text) if newline == -1 else newline + 1
            continue
        if text.startswith("/*", index):
            index = _skip_block_comment(text, index)
            continue
        if text[index] == '"':
            index = _quoted_string_end(text, index)
            continue
        raw = _raw_string_span(text, index)
        if raw is not None:
            index = raw[0]
            continue
        if text[index].isalpha() or text[index] == "_":
            token_start = index
            index += 1
            while index < len(text) and (text[index].isalnum() or text[index] == "_"):
                index += 1
            if text[token_start:index] == "env" and _matches_compile_time_home(text, index):
                hits.append(text.count("\n", 0, token_start) + 1)
            continue
        index += 1
    return hits


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--rust-env-home-only",
        action="store_true",
        help='Only reject real Rust env!("HOME") macro invocations.',
    )
    parser.add_argument("paths", nargs="*", type=Path, help="Explicit files to scan instead of the default deployable set")
    args = parser.parse_args(argv)

    root = args.root.resolve()
    explicit_paths = [path.resolve() for path in args.paths]
    paths = explicit_paths if explicit_paths else iter_default_paths(root)

    failures: list[str] = []
    if not args.rust_env_home_only:
        for path in paths:
            if not path.is_file():
                continue
            for line_no, line in scan_file(path):
                failures.append(f"{rel_display(path, root)}:{line_no}: {line}")

    rust_paths = (
        [path for path in explicit_paths if path.suffix == ".rs"]
        if explicit_paths
        else iter_rust_paths(root)
    )
    rust_failures = [
        f"{rel_display(path, root)}:{line_no}"
        for path in rust_paths
        for line_no in find_compile_time_home_macros(path.read_text(encoding="utf-8"))
    ] if args.rust_env_home_only else []

    if rust_failures:
        print(
            'ERROR: compile-time env!("HOME") found; use robust runtime path expansion instead:',
            file=sys.stderr,
        )
        for failure in rust_failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    if failures:
        print("ERROR: operator-specific /Users/<name> paths found in portable deployable surfaces:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    if args.rust_env_home_only:
        print(f'OK: no compile-time env!("HOME") in {len(rust_paths)} Rust file(s)')
    else:
        print(f"OK: scanned {len(paths)} portable deployable file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
