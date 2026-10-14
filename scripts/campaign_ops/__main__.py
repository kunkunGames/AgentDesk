"""One-shot goal enrollment. The Rust Hub owns scheduling and execution."""
import argparse
import json
from pathlib import Path
from .client import Api
from .enrollment import enroll
from .workflows import SOURCES


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--jobs", nargs="+", choices=list(SOURCES), default=list(SOURCES))
    parser.add_argument("--apply", action="store_true", help="persist goals; otherwise show validated payloads")
    args = parser.parse_args()
    for result in enroll(Api(args.root), args.root, args.jobs, apply=args.apply):
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
