"""One bounded read-only macOS/Linux observation, suitable for launchd/cron."""
import argparse
import fcntl
import json
import os
from pathlib import Path
from campaign_ops.client import Api
from campaign_ops.observation import MAX_SAMPLES, assess, collect


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / "observation.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = args.output / "window.json"
        if path.exists() and path.stat().st_size > 16*1024*1024:
            raise ValueError("Observation window exceeds its bounded size")
        previous = json.loads(path.read_text()) if path.exists() else {}
        if previous and previous.get("campaign_id") != args.campaign:
            raise ValueError("Cannot mix different campaigns in one observation window")
        samples = previous.get("samples", [])[-(MAX_SAMPLES-1):]
        samples.append(collect(Api(args.root), args.root, args.campaign))
        report = {"schema_version": 1, "campaign_id": args.campaign,
                  "assessment": assess(samples), "samples": samples}
        pending = path.with_suffix(".pending.json")
        pending.write_text(json.dumps(report, indent=2) + "\n")
        os.replace(pending, path)
        first = args.output / "first-24h-window.json"
        if report["assessment"]["elapsed_24_hours"] and not first.exists():
            pending_first = args.output / "first-24h-window.pending.json"
            pending_first.write_text(json.dumps(report, indent=2) + "\n")
            os.replace(pending_first, first)
        print(json.dumps(report["assessment"]))


if __name__ == "__main__":
    main()
