#!/usr/bin/env python3
"""Verify the exact, ordered SQL used to converge the two immutable histories."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path


def verify(root: Path) -> None:
    manifest = json.loads((root / "migrations/convergence.json").read_text())
    if manifest["schema"] != "agentdesk.postgres-convergence.v1" or manifest["version"] != 146:
        raise ValueError("unknown convergence contract")
    if set(manifest["profiles"]) != {"upstream", "campaign_v5"}:
        raise ValueError("both convergence histories are required")
    for profile, contract in manifest["profiles"].items():
        versions = [int(Path(item["path"]).name[:4]) for item in contract["sources"]]
        expected = list(range(131, 146)) if profile == "upstream" else [131, *range(133, 142)]
        if versions != expected:
            raise ValueError(f"{profile}: convergence source order or coverage changed")
        parts = [
            b"-- Forward convergence of immutable PostgreSQL histories.\n"
            b"-- Generated and verified by scripts/check_migration_convergence.py.\n"
            b"-- Historical receipts are preserved; only this version is recorded.\n\n"
        ]
        for item in contract["sources"]:
            path = root / item["path"]
            folder = "postgres_campaign_v5" if profile == "upstream" else "postgres"
            if path.parent != root / "migrations" / folder:
                raise ValueError("source outside immutable catalog")
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != item["sha256"]:
                raise ValueError(f"immutable source changed: {item['path']}")
            parts += [
                f"-- BEGIN {item['path']} sha256={item['sha256']}\n".encode(),
                data, b"\n", f"-- END {item['path']}\n\n".encode(),
            ]
        expected_bytes = b"".join(parts)
        output = root / contract["output"]
        folder = "postgres" if profile == "upstream" else "postgres_campaign_v5"
        if output != root / "migrations" / folder / "0146_campaign_upstream_convergence.sql":
            raise ValueError("unexpected convergence output")
        if output.read_bytes() != expected_bytes or hashlib.sha256(expected_bytes).hexdigest() != contract["sha256"]:
            raise ValueError(f"{profile}: convergence bytes changed")


if __name__ == "__main__":
    verify(Path(__file__).resolve().parents[1])
    print("Both immutable migration convergence payloads verified")
