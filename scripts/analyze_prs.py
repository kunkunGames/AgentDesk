import subprocess
import json
from datetime import datetime, timedelta, timezone

def run(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
    return result.stdout.strip(), result.returncode

def _detect_repo():
    # Repo-agnostic: derive the owner/name from the checked-out repo so this runs
    # against whatever fork/origin it lives in, not a hardcoded one.
    out, code = run("gh repo view --json nameWithOwner --jq .nameWithOwner")
    return out if code == 0 and out else "itismyfield/AgentDesk"

REPO = _detect_repo()

def parse_github_timestamp(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def head_commit_timestamp(pr):
    head_oid = pr.get("headRefOid")
    if not head_oid:
        return None
    timestamp, code = run(
        f"gh api repos/{REPO}/commits/{head_oid} --jq .commit.committer.date"
    )
    if code != 0:
        return None
    return parse_github_timestamp(timestamp)

print("Fetching PRs...")
prs_json, gh_code = run(f"gh pr list --repo {REPO} --state open --limit 50 --json number,title,headRefName,createdAt,headRefOid,body")

if gh_code != 0 or not prs_json:
    print("Warning: `gh` CLI not available or failed. Skipping PR analysis.")
    exit(0)

try:
    prs = json.loads(prs_json)
except Exception as e:
    print(f"Error parsing JSON: {e}")
    print(prs_json)
    exit(1)

inventory_refresh_count = 0
has_errors = False
now = datetime.now(timezone.utc)

for pr in prs:
    num = pr['number']
    title = pr['title']
    body = str(pr.get('body') or '').lower()
    head_commit_at = head_commit_timestamp(pr)
    print(f"\n# {num} - {title}")

    # Check PR hygiene requirements
    if "workfingerprint" not in body:
        print("  [!] MISSING FINGERPRINT: PR body lacks the required 'WorkFingerprint' section.")
        has_errors = True
    if "duplicate" not in body and "overlap" not in body:
        print("  [!] MISSING OVERLAP CHECK: PR body fails to explicitly mention a 'duplicate' or 'overlap' check.")
        has_errors = True
    if "verification" not in body:
        print("  [!] MISSING VERIFICATION: PR body lacks the required 'verification' commands and results.")
        has_errors = True
    if "skipped checks" not in body:
        print("  [!] MISSING SKIPPED CHECKS: PR body fails to mention 'skipped checks' with reasons.")
        has_errors = True
    if "risk" not in body:
        print("  [!] MISSING RISK: PR body fails to mention 'risk' assessment.")
        has_errors = True
    if "rollback notes" not in body:
        print("  [!] MISSING ROLLBACK NOTES: PR body fails to mention 'rollback notes'.")
        has_errors = True

    # 2026-05-13 lesson: treat low-signal or stale broad branches as queue debt
    is_stale = head_commit_at is not None and (now - head_commit_at) > timedelta(days=14)

    # Get diff stat
    stat, _ = run(f"gh pr diff {num} --repo {REPO} --stat")
    print(f"Stat:\n{stat}")

    if is_stale:
        print(f"  [!] STALE BRANCH: Head commit is > 14 days old. Treat as queue debt. Close or recommend closing instead of salvaging in place.")
        has_errors = True


    if stat:
        scratch_found = []
        for line in stat.split("\n"):
            if "|" not in line:
                continue
            filename = line.split("|")[0].strip()
            # Catch common ad-hoc files dumped in the root directory
            is_root_test = filename.startswith("test_") and "/" not in filename and filename.endswith((".rs", ".py", ".sh", ".txt"))
            is_plan = filename in ("plan.md", "pr-body.md", "scratch.txt", "scratch.md", "output.log")
            if is_root_test or is_plan:
                scratch_found.append(filename)
        if scratch_found:
            print(f"  [!] SCRATCH FILE GUARD: PR contains likely ad-hoc scratch files ({', '.join(scratch_found)}). Run a changed-file audit and clean up repository pollution.")
            has_errors = True

    # PR #214/#215 lesson: no-change PRs must have 0 changed files
    if "no-change" in title.lower():
        if "#" not in body and "branch" not in body and "pr " not in body:
            print("  [!] MISSING OVERLAPPING PR INFO: No-change PR body fails to explicitly list exact overlapping PR numbers and branches.")
            has_errors = True
        files_json, _ = run(f"gh pr view {num} --repo {REPO} --json files")
        try:
            files_data = json.loads(files_json)
            if files_data.get("files") is not None:
                if len(files_data["files"]) > 0:
                    print(f"  [!] UNSAFE NO-CHANGE PR: Title claims no-change but modifies {len(files_data['files'])} files.")
                    has_errors = True
                else:
                    print(f"  [i] EMPTY NO-CHANGE PR: No changed files. If no durable queue-hygiene artifact is changed, it is a close candidate (report only).")
        except Exception:
            pass

    # PR #199/#200/#201 lesson: check for multiple inventory refreshes
    if "inventory" in title.lower() and "refresh" in title.lower():
        inventory_refresh_count += 1
        if "duplicate-pr guard" not in body and "duplicate pr guard" not in body:
            print("  [!] MISSING DUPLICATE PR GUARD: Inventory refresh PR body fails to mention 'duplicate-pr guard'.")
            has_errors = True

if inventory_refresh_count > 1:
    print("\n[!] WARNING: Multiple open inventory refresh PRs detected. Ensure strict duplicate-PR guard is followed.")
    has_errors = True

if has_errors:
    print("\n[!] PR hygiene checks failed. Please fix the issues above.")
    exit(1)
