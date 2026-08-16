"""Reject repository-root scratch files using the PR analyzer's canonical policy."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure the repository root is in sys.path so we can import from scripts.*
# regardless of how this script is executed.
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from scripts.analyze_prs import is_scratch_file_path


def find_root_scratch_files(root: Path) -> list[Path]:
    """Return root-level regular files that match the canonical scratch policy."""
    return sorted(
        (
            path
            for path in root.iterdir()
            if path.is_file() and is_scratch_file_path(path.name)
        ),
        key=lambda path: path.name,
    )


def main(root: Path | None = None) -> int:
    repository_root = root or Path.cwd()
    scratch_files = find_root_scratch_files(repository_root)
    for path in scratch_files:
        print(f"ERROR: Scratch file detected in repository root: {path.name}")
    return 1 if scratch_files else 0


if __name__ == "__main__":
    raise SystemExit(main())
