import subprocess
import json
import re
import sys
from datetime import datetime, timedelta, timezone

def run(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
    return result.stdout.strip(), result.returncode

def _detect_repo():
    # Repo-agnostic: derive the owner/name from the checked-out repo so this runs
    # against whatever fork/origin it lives in, not a hardcoded one.
    out, code = run("gh repo view --json nameWithOwner --jq .nameWithOwner")
    return out if code == 0 and out else "itismyfield/AgentDesk"

def parse_github_timestamp(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def head_commit_timestamp(pr, repo):
    head_oid = pr.get("headRefOid")
    if not head_oid:
        return None
    timestamp, code = run(
        f"gh api repos/{repo}/commits/{head_oid} --jq .commit.committer.date"
    )
    if code != 0:
        return None
    return parse_github_timestamp(timestamp)

def _strip_html_comments(value):
    return re.sub(r"<!--.*?-->", "", value, flags=re.S)

def _meaningful_field_value(value, *, allow_none=False):
    normalized = _strip_html_comments(value).strip().strip("-–—").strip()
    if re.match(
        r"(?i)^(?:\*\*)?[a-z0-9 /_-]+(?:\*\*)?\s*:(?:\*\*)?\s*$",
        normalized,
    ):
        return False
    placeholders = {"n/a", "todo", "tbd", "na"}
    if not allow_none:
        placeholders.add("none")
    return bool(normalized) and normalized.casefold() not in placeholders

def _is_markdown_heading(line):
    return bool(re.match(r"^[ \t]{0,3}#{1,6}[ \t]+\S", line))

def _is_top_level_field_label(line):
    if line[:1] in {" ", "\t"}:
        return False
    return re.match(
        r"^(?:[-*]\s*)?(?:\[[ xX]\]\s*)?(?:\*\*)?[a-z0-9 /_-]+(?:\*\*)?\s*:(?:\*\*)?(?:\s.*)?$",
        line.strip(),
        re.I,
    )

def _meaningful_branch_ref(value):
    normalized = value.strip().strip("`").strip(".,;:)]}")
    return _meaningful_field_value(normalized)

def has_non_empty_body_field(body, labels, *, allow_none=False, stop_at_field_labels=True):
    for label in labels:
        pattern = re.compile(
            rf"(?im)^[ \t]*(?:[-*][ \t]*)?(?:\[[xX]\][ \t]*)?(?:#{{1,6}}[ \t]*)?(?:\*\*)?{re.escape(label)}(?:\*\*)?(?:[ \t]*:(?:\*\*)?[ \t]*(.*)|[ \t]*)$"
        )
        for match in pattern.finditer(body):
            if _meaningful_field_value(match.group(1) or "", allow_none=allow_none):
                return True

            for next_line in body[match.end():].splitlines():
                stripped = next_line.strip()
                if not stripped:
                    continue
                commentless = _strip_html_comments(stripped).strip()
                if not commentless:
                    continue
                if _is_markdown_heading(next_line):
                    break
                # Next field label is a boundary whether or not it carries a
                # value: an empty `- Risk:` must not borrow the value of a
                # following populated field (e.g. `- Rollback notes: revert`).
                is_field_label = _is_top_level_field_label(next_line)
                if stop_at_field_labels and is_field_label:
                    break
                if _meaningful_field_value(commentless, allow_none=allow_none):
                    return True
                if is_field_label:
                    continue
                break
    return False

def _has_checked_template_ack(body, labels):
    label_pattern = "|".join(re.escape(label) for label in labels)
    return bool(
        re.search(
            rf"(?im)^[ \t]*[-*][ \t]*\[[xX]\][ \t]*\*\*(?:{label_pattern})(?::\*\*|\*\*[ \t]*:)",
            body,
        )
    )

def has_duplicate_guard_ack(body):
    labels = [
        "duplicate pr guard",
        "duplicate-pr guard",
        "duplicate/overlap check",
        "overlap check",
    ]
    if _has_checked_template_ack(body, labels):
        return True
    return has_non_empty_body_field(body, labels)

def has_no_change_verification_ack(body):
    labels = [
        "no-change verification",
        "no change verification",
    ]
    if _has_checked_template_ack(body, labels):
        return True
    return has_non_empty_body_field(body, labels)

def has_stale_branch_cleanup_ack(body):
    labels = [
        "stale branch cleanup",
        "stale-branch cleanup",
    ]
    if _has_checked_template_ack(body, labels):
        return True
    return has_non_empty_body_field(body, labels)

def has_scratch_file_cleanup_ack(body):
    labels = [
        "scratch file cleanup",
        "scratch-file cleanup",
    ]
    if _has_checked_template_ack(body, labels):
        return True
    return has_non_empty_body_field(body, labels)

def has_mergeability_status_ack(body):
    if re.search(r"(?im)^[ \t]*[-*][ \t]*\[[xX]\][ \t]*\*\*mergeability status:\*\*", body):
        return True
    return has_non_empty_body_field(
        body,
        [
            "mergeability status",
            "mergeability-status",
        ],
    )

def has_verification_grounding_ack(body):
    labels = [
        "verification grounding",
        "verification-grounding",
    ]
    if _has_checked_template_ack(body, labels):
        return True
    return has_non_empty_body_field(body, labels)

def has_overlap_reference(body):
    pr_ref = re.compile(r"(?i)(?:#[0-9]+|github\.com/[^/\s]+/[^/\s]+/pull/[0-9]+)")
    overlap_context = re.compile(r"(?i)\b(?:overlaps?|overlapping|duplicates?|supersed(?:e|ed|es|ing)?|replaces?|same scope)\b")
    negated_overlap_context = re.compile(r"(?i)\b(?:non[- ]?overlapp?ing|non[- ]?overlap|not overlapping|not overlap|does not overlap|no overlapping|no overlap)\b")
    branch_ref = re.compile(r"(?i)\b(?:branch(?:es)?|head(?:\s+ref)?|ref)\b\s*[:=-]?\s*`?([A-Za-z0-9][A-Za-z0-9._/-]*)`?")
    overlap_detail_field = re.compile(
        r"(?i)^(?:[-*]\s*)?(?:\*\*)?(?:pr|pull request|branch(?:es)?|head(?: ref)?|ref)(?:\*\*)?\s*:(?:\*\*)?"
    )
    in_overlap_block = False
    block_has_pr = False
    block_has_branch = False

    for line in body.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        is_boundary = _is_markdown_heading(line) or _is_top_level_field_label(line)
        is_overlap_detail = bool(overlap_detail_field.search(stripped))
        is_negated_overlap = bool(negated_overlap_context.search(stripped))
        if is_boundary and not (in_overlap_block and is_overlap_detail):
            if in_overlap_block and block_has_pr and block_has_branch:
                return True
            in_overlap_block = bool(overlap_context.search(stripped)) and not is_negated_overlap
            block_has_pr = False
            block_has_branch = False
        if is_negated_overlap:
            continue

        line_has_context = in_overlap_block or bool(overlap_context.search(stripped))
        if not line_has_context:
            continue

        block_has_pr = block_has_pr or bool(pr_ref.search(stripped))
        block_has_branch = block_has_branch or any(
            _meaningful_branch_ref(match.group(1))
            for match in branch_ref.finditer(stripped.replace("**", ""))
        )
        if block_has_pr and block_has_branch:
            return True

    return in_overlap_block and block_has_pr and block_has_branch

def has_template_summary(body):
    return has_non_empty_body_field(body, ["summary"], stop_at_field_labels=False)

def is_scratch_file_path(path):
    if not path or "/" in path:
        return False
    if path.endswith(".diff") or path.endswith(".patch"):
        return True
    root_scratch_files = {
        "pr-body.md",
        "plan.md",
        "plan.txt",
        "test.sh",
        "test.sql",
        "test.py",
        "test.js",
        "verify.sh",
        "sql_test.rs",
        "prs.json",
        "scratch.json",
        "scratchpad.json",
    }
    if path in root_scratch_files:
        return True
    return bool(
        re.match(r"^(?:scratch|scratchpad|test_scratch)(?:[._-].+)?\.(?:md|txt|sh|sql|rs|py|js|json)$", path)
        or re.match(r"^test_[A-Za-z0-9._-]+\.(?:rs|py|js|json)$", path)
    )

def is_generated_inventory_path(path):
    # Regenerated wholesale by scripts/generate_inventory_docs.py. A PR whose
    # entire diff is these files carries no reviewable intent, so the analyzer
    # asks for a no-change report instead. (They ARE tracked in git — two are
    # also listed in .gitignore, which has no effect once a file is tracked.)
    generated_inventory_files = {
        "docs/generated/module-inventory.md",
        "docs/generated/giant-file-registry.md",
        "docs/generated/route-inventory.md",
        "docs/generated/worker-inventory.md",
    }
    return path in generated_inventory_files

def main():
    repo = _detect_repo()
    print("Fetching PRs...")
    prs_json, gh_code = run(f"gh pr list --repo {repo} --state open --limit 50 --json number,title,headRefName,createdAt,headRefOid,body")

    if gh_code != 0 or not prs_json:
        print("Warning: `gh` CLI not available or failed. Skipping PR analysis.")
        return 0

    try:
        prs = json.loads(prs_json)
    except Exception as e:
        print(f"Error parsing JSON: {e}")
        print(prs_json)
        return 1

    inventory_refresh_count = 0
    now = datetime.now(timezone.utc)

    for pr in prs:
        num = pr['number']
        title = pr['title']
        body = str(pr.get('body') or '')
        normalized_body = body.lower()
        head_commit_at = head_commit_timestamp(pr, repo)
        print(f"\n# {num} - {title}")

        # Check PR hygiene requirements
        summary_satisfies_change_context = has_template_summary(body)
        if not has_non_empty_body_field(body, ["what changed"]) and not summary_satisfies_change_context:
            print("  [!] MISSING WHAT CHANGED: PR body lacks a required 'What changed' description.")
        if not has_non_empty_body_field(body, ["why"]) and not summary_satisfies_change_context:
            print("  [!] MISSING WHY: PR body lacks a required 'Why' description.")
        if "workfingerprint" not in normalized_body:
            print("  [!] MISSING FINGERPRINT: PR body lacks the required 'WorkFingerprint' section.")
        if not has_non_empty_body_field(body, ["agent"]):
            print("  [!] MISSING AGENT: PR body lacks the required 'Agent' field.")
        if not has_non_empty_body_field(body, ["boundary"]):
            print("  [!] MISSING BOUNDARY: PR body lacks the required 'Boundary' field.")
        if not has_non_empty_body_field(body, ["primary files"]):
            print("  [!] MISSING PRIMARY FILES: PR body lacks the required 'Primary files' field.")
        if not has_non_empty_body_field(body, ["public api impact"]):
            print("  [!] MISSING PUBLIC API IMPACT: PR body lacks the required 'Public API impact' field.")
        if not has_non_empty_body_field(body, ["docs impact"]):
            print("  [!] MISSING DOCS IMPACT: PR body lacks the required 'Docs impact' field.")
        if not has_non_empty_body_field(body, ["queue hygiene invariant"]):
            print("  [!] MISSING QUEUE HYGIENE INVARIANT: PR body lacks the required 'Queue hygiene invariant' field.")
        if not has_non_empty_body_field(body, ["related prs/issues checked", "related prs/issues", "related prs"]):
            print("  [!] MISSING RELATED PRS: PR body lacks the required 'Related PRs/issues checked' field.")
        if not has_non_empty_body_field(body, ["why this is non-overlapping", "non-overlapping reason"]):
            print("  [!] MISSING NON-OVERLAPPING REASON: PR body lacks the required 'Why this is non-overlapping' field.")
        if not has_duplicate_guard_ack(body):
            print("  [!] MISSING OVERLAP CHECK: PR body lacks a completed duplicate/overlap guard acknowledgement.")
        if not has_scratch_file_cleanup_ack(body):
            print("  [!] MISSING SCRATCH FILE CLEANUP CHECK: PR body lacks a completed scratch file cleanup acknowledgement.")
        if not has_mergeability_status_ack(body):
            print("  [!] MISSING MERGEABILITY STATUS CHECK: PR body lacks a completed mergeability status acknowledgement.")
        if not has_verification_grounding_ack(body):
            print("  [!] MISSING VERIFICATION GROUNDING CHECK: PR body lacks a completed verification grounding acknowledgement.")
        if not has_non_empty_body_field(body, ["verification commands and results", "verification"]):
            print("  [!] MISSING VERIFICATION: PR body lacks the required 'verification' commands and results.")
        if not has_non_empty_body_field(
            body,
            ["skipped checks and reasons", "skipped checks with reasons", "skipped checks"],
            allow_none=True,
        ):
            print("  [!] MISSING SKIPPED CHECKS: PR body fails to mention 'skipped checks' with reasons.")
        if not has_non_empty_body_field(body, ["risk and rollback notes", "risk", "risk assessment"]):
            print("  [!] MISSING RISK: PR body fails to mention 'risk' assessment.")
        if not has_non_empty_body_field(body, ["risk and rollback notes", "rollback notes", "rollback"]):
            print("  [!] MISSING ROLLBACK NOTES: PR body fails to mention 'rollback notes'.")

        # In AgentDesk, PR hygiene and merge-readiness are strictly validated by scripts/analyze_prs.py.
        # It enforces that PR titles matching jules/<agent_name>/* branches explicitly start with
        # the corresponding AgentName: prefix.
        head_ref_name = pr.get('headRefName', '')
        if head_ref_name.startswith('jules/'):
            parts = head_ref_name.split('/')
            if len(parts) >= 3:
                agent_slug = parts[1].lower()
                overrides = {
                    "cartographer-lite": "Cartographer-Lite:",
                    "onboardingsmith": "OnboardingSmith:",
                    "domainkeeper": "DomainKeeper:",
                    "api-routemaster": "ApiRoutemaster:",
                }
                if agent_slug in overrides:
                    expected_prefix = overrides[agent_slug]
                else:
                    expected_prefix = "".join(p.capitalize() for p in agent_slug.split("-")) + ":"

                if not title.startswith(expected_prefix):
                    print(f"  [!] INVALID TITLE PREFIX: Branch '{head_ref_name}' requires PR title to start with '{expected_prefix}'. Found: '{title}'.")

        # 2026-05-13 lesson: treat low-signal or stale broad branches as queue debt
        is_stale = head_commit_at is not None and (now - head_commit_at) > timedelta(days=14)

        # Get diff stat
        stat, _ = run(f"gh pr diff {num} --repo {repo} --stat")
        print(f"Stat:\n{stat}")

        # PR files
        files_json, _ = run(f"gh pr view {num} --repo {repo} --json files")
        files_data = {}
        try:
            if files_json:
                files_data = json.loads(files_json)
        except Exception:
            pass

        # Scratch file detection
        if files_data.get("files") is not None:
            scratch_files = []
            generated_inventory_files = []
            other_files = []
            for f in files_data["files"]:
                path = f.get("path", "")
                if is_scratch_file_path(path):
                    scratch_files.append(path)
                if is_generated_inventory_path(path):
                    generated_inventory_files.append(path)
                else:
                    other_files.append(path)

            if scratch_files:
                print(f"  [!] SCRATCH FILE DETECTED: PR includes scratch files like pr-body.md, plan.md, or test scripts ({', '.join(scratch_files)}).")

            if generated_inventory_files and not other_files:
                print(f"  [!] GENERATED INVENTORY DRIFT ONLY: PR's entire diff is regenerated inventory docs ({', '.join(generated_inventory_files)}). Re-run scripts/generate_inventory_docs.py on main and file a no-change report instead of a PR.")

        if is_stale:
            print(f"  [!] STALE BRANCH: Head commit is > 14 days old. Treat as queue debt. Close or recommend closing instead of salvaging in place.")
            if not has_stale_branch_cleanup_ack(body):
                print("  [!] MISSING STALE BRANCH CLEANUP CHECK: PR body lacks a completed stale branch cleanup acknowledgement.")

        # PR #214/#215 lesson: no-change PRs must have 0 changed files
        if "no-change" in title.lower() or "no change" in title.lower():
            if not has_no_change_verification_ack(body):
                print("  [!] MISSING NO-CHANGE VERIFICATION CHECK: PR body lacks a completed no-change verification acknowledgement.")
            if files_data.get("files") is not None:
                if len(files_data["files"]) > 0:
                    print(f"  [!] UNSAFE NO-CHANGE PR: Title claims no-change but modifies {len(files_data['files'])} files.")
                else:
                    print(f"  [!] EMPTY NO-CHANGE PR: No changed files. A no-change result should not become a PR unless it explicitly changes a queue-hygiene artifact. Consider closing.")
                    if not has_overlap_reference(body):
                        print("  [!] MISSING OVERLAP REFERENCE: Empty no-change PR body must explicitly list the exact overlapping PR numbers and branches.")

        # PR #199/#200/#201 lesson: check for multiple inventory refreshes
        if "inventory" in title.lower() and "refresh" in title.lower():
            inventory_refresh_count += 1
            if not has_duplicate_guard_ack(body):
                print("  [!] MISSING DUPLICATE PR GUARD: Inventory refresh PR body lacks a completed duplicate-pr guard acknowledgement.")

    if inventory_refresh_count > 1:
        print("\n[!] WARNING: Multiple open inventory refresh PRs detected. Ensure strict duplicate-PR guard is followed.")
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
