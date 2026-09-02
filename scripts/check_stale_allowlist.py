#!/usr/bin/env python3
"""Check that all keys in scripts/audit_allowlist.toml match current violations."""

import sys
import toml
import importlib
from pathlib import Path

def main() -> int:
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root / "scripts"))

    allowlist_path = root / "scripts" / "audit_allowlist.toml"
    with open(allowlist_path, "r", encoding="utf-8") as f:
        allowlist = toml.load(f)

    stale_found = False
    for check_name, keys in allowlist.items():
        if not keys:
            continue

        try:
            mod = importlib.import_module(f"audit_maintainability.checks.{check_name}")
            # Run check without any allowlist to get all current violations
            findings = mod._run(set())

            # Extract allowlist_keys from the findings
            current_keys = {
                f.extra["allowlist_key"]
                for f in findings
                if f.extra and "allowlist_key" in f.extra
            }

            for k in keys:
                if k not in current_keys:
                    print(
                        f"::error file=scripts/audit_allowlist.toml::stale allowlist entry found "
                        f"in {check_name}: {k}. The underlying violation has been removed or "
                        f"shifted. Remove this entry from the allowlist."
                    )
                    stale_found = True
        except Exception as e:
            print(f"::error::failed to run check {check_name} for stale allowlist validation: {e}")
            stale_found = True

    if stale_found:
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
