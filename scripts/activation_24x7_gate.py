#!/usr/bin/env python3
"""Audit the captured v5 evidence without treating fixtures as 24/7 acceptance.

Exit 1: invalid evidence. Exit 2: consistent evidence, activation requirements
unmet. --audit-only returns 0 for consistent partial evidence. This offline
command neither enables nor disables a running service.
"""
import argparse
import json
from pathlib import Path
import sys

from operating_readiness_evidence import EvidenceError, audit


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--receipt", default="target/24x7-native/OPERATING-READINESS-RESULT.json")
    parser.add_argument("--audit-only", action="store_true", help="allow a valid partial audit to exit 0")
    parser.add_argument("--output", type=Path, help="write a new report; existing evidence is never overwritten")
    args = parser.parse_args(argv)
    try:
        report = audit(args.repo_root, args.receipt)
        encoded = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("x", encoding="utf-8") as handle:
                handle.write(encoded)
        print(encoded, end="")
        return 0 if args.audit_only else 2
    except (EvidenceError, OSError, ValueError, KeyError, TypeError) as error:
        print(json.dumps({"evidence_valid": False, "activated": False, "error": str(error)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
