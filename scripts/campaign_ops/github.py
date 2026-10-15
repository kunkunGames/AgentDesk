"""Repository-scoped GitHub commands using the owned internal runtime tool."""
import os
import json
from pathlib import Path
import re
import subprocess
import sys
from .github_feedback import feedback

REPOS = {"kunkunGames/AgentDesk", "kunkunGames/monolith"}
ACTIONS = {"list", "view", "checks", "diff", "comment", "edit", "review", "merge", "close"}
READ_ACTIONS = {"review-threads", "check-details", "feedback"}


def runtime_gh(home=None):
    """Avoid login-shell PATH resolving Homebrew tools on an offloaded volume."""
    home = Path.home() if home is None else Path(home)
    directory = home / ".adk/runtime-bin"
    candidate = directory / "gh"
    if not candidate.is_file() or not os.access(candidate, os.X_OK):
        raise ValueError("internal AgentDesk GitHub CLI is missing or not executable")
    resolved = candidate.resolve()
    if not resolved.is_relative_to(directory.resolve()) or resolved.stat().st_dev != home.stat().st_dev:
        raise ValueError("AgentDesk GitHub CLI must remain in the internal runtime directory")
    return str(candidate)


def command(argv, executable):
    if len(argv) < 2 or argv[0] not in REPOS or argv[1] not in ACTIONS:
        raise ValueError("usage: routine-gh.py <allowed-repository> <pr-action> [number] [options]")
    repo, action, *args = argv
    if action != "list" and (not args or not args[0].isdigit()):
        raise ValueError("PR number must be numeric; cross-repository URLs are not accepted")
    for argument in args:
        if argument.startswith("-R") or argument in {"--repo", "--admin", "--delete-branch"} or argument.startswith(("--repo=", "--admin=", "--delete-branch=")):
            raise ValueError("repository override, admin bypass, and branch deletion are forbidden")
    return [executable, "pr", action, "--repo", repo, *args]


def read_request(argv):
    """Fixed PR reads grant no arbitrary API, repository or authentication access."""
    if len(argv) not in {3, 4} or argv[0] not in REPOS or argv[1] not in READ_ACTIONS or not argv[2].isdigit() or int(argv[2]) <= 0:
        raise ValueError("usage: routine-gh.py <allowed-repository> <review-threads|check-details|feedback> <number> [representative-check-id]")
    if len(argv) == 4 and (argv[1] != "check-details" or not argv[3].isdigit() or int(argv[3]) <= 0):
        raise ValueError("only check-details accepts one numeric representative check ID")
    return argv[0], argv[1], int(argv[2])


def read_json(executable, args):
    result = subprocess.run([executable, *args], check=True, capture_output=True, text=True, timeout=60)
    if len(result.stdout.encode()) > 4 * 1024 * 1024:
        raise ValueError("GitHub read exceeds the bounded report limit")
    return json.loads(result.stdout)


def review_threads(executable, repo, number):
    owner, name = repo.split("/")
    query = """query($owner:String!,$name:String!,$number:Int!,$after:String){
      repository(owner:$owner,name:$name){pullRequest(number:$number){headRefOid
        reviewThreads(first:100,after:$after){pageInfo{hasNextPage endCursor}
          nodes{id isResolved isOutdated path line comments(first:100){totalCount
            nodes{author{login} body url createdAt}}}}}}}"""
    threads, after, head = [], None, None
    for _ in range(10):
        args = ["api", "graphql", "-f", "query=" + query, "-f", "owner=" + owner,
                "-f", "name=" + name, "-F", "number=" + str(number)]
        if after is not None:
            args += ["-f", "after=" + after]
        response = read_json(executable, args)
        if response.get("errors"):
            raise ValueError("GitHub review-thread query did not complete")
        pr = response["data"]["repository"]["pullRequest"]
        if head is not None and pr["headRefOid"] != head:
            raise ValueError("PR head changed during review-thread inspection")
        head = pr["headRefOid"]
        page = pr["reviewThreads"]
        threads.extend(page["nodes"])
        if not page["pageInfo"]["hasNextPage"]:
            return {"repository": repo, "number": number, "head": head, "threads": threads,
                    "complete": all(t["comments"]["totalCount"] == len(t["comments"]["nodes"]) for t in threads)}
        after = page["pageInfo"]["endCursor"]
    raise ValueError("PR review-thread pagination exceeds the bounded inspection limit")


def check_details(executable, repo, number, representative_id=None):
    """Report every check status and inspect at most one pinned failure in detail."""
    head = read_json(executable, ["pr", "view", str(number), "--repo", repo, "--json", "headRefOid"])["headRefOid"]
    if not re.fullmatch(r"[0-9a-f]{40}", head):
        raise ValueError("GitHub returned an invalid head revision")
    pages = read_json(executable, ["api", f"repos/{repo}/commits/{head}/check-runs?per_page=100", "--paginate", "--slurp"])
    checks = [check for page in pages for check in page["check_runs"]]
    complete = bool(pages) and len(checks) == pages[0]["total_count"]
    for check in checks:
        if check["head_sha"] != head:
            raise ValueError("check-run head differs from the inspected PR")
    failures = sorted((c for c in checks if c["conclusion"] not in {None, "success", "skipped", "neutral"}), key=lambda c: int(c["id"]))
    representative = next((c for c in failures if int(c["id"]) == representative_id), None) if representative_id is not None else next(iter(failures), None)
    if representative_id is not None and representative is None:
        raise ValueError("representative check must be a failing check on this PR head")
    if representative is not None:
        check_id = int(representative["id"])
        representative["annotations"] = read_json(executable, ["api", f"repos/{repo}/check-runs/{check_id}/annotations?per_page=100"])
        complete = complete and len(representative["annotations"]) == representative["output"]["annotations_count"]
        match = re.fullmatch(r"https://github\.com/" + re.escape(repo) + r"/actions/runs/[0-9]+/job/([0-9]+)(?:\?[^#]*)?", representative.get("details_url") or "")
        if match:
            representative["job"] = read_json(executable, ["api", f"repos/{repo}/actions/jobs/{match[1]}"])
    current = read_json(executable, ["pr", "view", str(number), "--repo", repo, "--json", "headRefOid"])["headRefOid"]
    if current != head:
        raise ValueError("PR head changed during check inspection")
    # App permissions and unrelated account metadata are not business evidence.
    evidence = []
    for check in checks:
        item = {key: check[key] for key in ["id", "name", "head_sha", "status", "conclusion",
                "details_url", "started_at", "completed_at", "output", "annotations"] if key in check}
        if "job" in check:
            job = check["job"]
            item["job"] = {key: job[key] for key in ["id", "run_id", "head_sha", "status", "conclusion",
                "name", "workflow_name", "steps", "created_at", "started_at", "completed_at", "runner_id"] if key in job}
        evidence.append(item)
    return {"repository": repo, "number": number, "head": head, "checks": evidence, "complete": complete,
            "details_scope": "at_most_one_representative_failure",
            "representative_check_id": int(representative["id"]) if representative is not None else None,
            "uninspected_failure_details": [int(c["id"]) for c in failures if c is not representative]}


def main():
    try:
        # Validate scope before dependency probing or subprocess execution.
        if len(sys.argv) >= 3 and sys.argv[2] in READ_ACTIONS:
            repo, action, number = read_request(sys.argv[1:])
            executable = runtime_gh()
            if action == "review-threads":
                report = review_threads(executable, repo, number)
            elif action == "feedback":
                report = feedback(executable, repo, number, read_json)
            else:
                report = check_details(executable, repo, number, int(sys.argv[4]) if len(sys.argv) == 5 else None)
            print(json.dumps(report))
            return
        args = command(sys.argv[1:], str(Path.home() / ".adk/runtime-bin/gh"))
        args[0] = runtime_gh()
        completed = subprocess.run(args, check=False, timeout=60)
    except (ValueError, KeyError, subprocess.SubprocessError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(completed.returncode)
