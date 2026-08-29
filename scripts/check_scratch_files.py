"""Reject repository scratch files using the PR analyzer's canonical policy."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# Direct `python3 scripts/check_scratch_files.py` does not put the repo
# root on sys.path, so the scripts.* import would fail without this.
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from scripts.analyze_prs import is_scratch_file_path


def find_scratch_files(root: Path) -> list[str]:
    """Return regular files in the repository that match the canonical scratch policy."""
    output = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=root,
    )
    return sorted(
        path_str
        for path_str in output.decode("utf-8").split("\0")
        if path_str and is_scratch_file_path(path_str)
    )


def main(root: Path | None = None) -> int:
    repository_root = root or Path.cwd()
    scratch_files = find_scratch_files(repository_root)
    for path in scratch_files:
        print(f"ERROR: Scratch file detected in repository: {path}")
    return 1 if scratch_files else 0


if __name__ == "__main__":
    raise SystemExit(main())
