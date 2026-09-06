#!/usr/bin/env python3

import argparse
import datetime as dt
import json
import re
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ID = "itismyfield/AgentDesk"
REPO_OWNER = "itismyfield"
REPO_NAME = "AgentDesk"
DB_PATH = Path.home() / ".adk/release/data/agentdesk.sqlite"
COMMENT_LIMIT = 20
QUERY_CHUNK = 10


CATEGORY_A = {
    "rework": [30, 35, 37, 73, 85, 86, 89, 97, 98, 105],
    "improve": [64, 79, 90, 92, 96, 109, 110, 114],
    "reject": [81, 108, 119],
}
CATEGORY_B = [
    1,
    3,
    4,
    5,
    6,
    7,
    8,
    55,
    63,
    64,
    70,
    71,
    73,
    75,
    76,
    77,
    78,
    79,
    80,
    81,
    83,
    84,
    85,
    86,
    89,
    90,
    91,
    92,
    93,
    95,
    96,
    97,
    98,
    99,
    100,
    101,
    102,
    103,
    104,
    105,
    106,
    107,
    108,
    109,
    110,
    111,
    112,
    113,
    114,
    115,
    116,
    117,
    118,
    119,
    120,
    122,
    123,
    124,
    125,
    126,
    127,
    128,
    129,
    137,
    139,
    142,
]
CATEGORY_C = [
    2,
    9,
    10,
    11,
    18,
    19,
    25,
    28,
    40,
    54,
    58,
    59,
    63,
    76,
    87,
    88,
    102,
    106,
    107,
    111,
    113,
    115,
    116,
    117,
    120,
    122,
    123,
    124,
    125,
    126,
    127,
    128,
    129,
    137,
    139,
]
CATEGORY_D = [
    1,
    12,
    13,
    14,
    15,
    16,
    17,
    20,
    21,
    22,
    23,
    24,
    26,
    27,
    29,
    31,
    32,
    33,
    34,
    36,
    38,
    39,
    41,
    42,
    43,
    44,
    45,
    46,
    47,
    48,
    49,
    50,
    51,
    52,
    55,
    56,
    57,
    60,
    61,
    62,
    65,
    66,
    67,
    68,
    69,
    72,
    74,
    75,
    78,
    82,
    83,
    84,
    91,
    93,
    95,
    99,
    100,
    101,
    103,
    104,
    112,
]

STATUS_ONLY_MARKERS = (
    "칸반 상태:",
    "칸반 done 상태",
)
POSITIVE_REVIEW_MARKERS = (
    "No new findings",
    "모두 해소",
    "blocker 수정 완료",
    "리뷰 피드백 반영 완료",
    "improve 반영",
    "코드 리뷰 결과 `pass`",
    "코드 리뷰 결과 pass",
    "이전 R1-R4 지적사항 모두 반영 확인",
    "검토 완료",
)
NEGATIVE_REVIEW_MARKERS = (
    "VERDICT: rework",
    "VERDICT: improve",
    "VERDICT: reject",
    "Review finding:",
    "리뷰 결과",
    "blocking 이슈",
    "blocker",
    "**High**",
)
FIX_FOLLOWUP_MARKERS = (
    "반영 완료",
    "수정 완료",
    "blocker 수정 완료",
    "카운터 리뷰 재요청",
    "모두 pass",
    "모두 해소",
    "모두 반영",
    "반영 확인",
    "반영 후 완료",
    "No new findings",
    "close:",
)


@dataclass
class CommentInfo:
    created_at: str
    body: str
    url: str | None


def run_command(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or f"command failed: {cmd}")
    return proc.stdout


def parse_json_maybe_nested(raw: str | None) -> Any:
    if not raw:
        return None
    data: Any = raw
    for _ in range(2):
        if isinstance(data, str):
            stripped = data.strip()
            if not stripped:
                return None
            try:
                data = json.loads(stripped)
            except json.JSONDecodeError:
                return stripped
        else:
            break
    return data


def issue_numbers() -> list[int]:
    nums = set(CATEGORY_B) | set(CATEGORY_C) | set(CATEGORY_D)
    for values in CATEGORY_A.values():
        nums.update(values)
    return sorted(nums)


def build_category_map() -> dict[int, list[str]]:
    result: dict[int, list[str]] = {}
    for bucket, values in CATEGORY_A.items():
        for issue in values:
            result.setdefault(issue, []).append(f"A:{bucket}")
    for label, values in [("B:missing-dod", CATEGORY_B), ("C:no-review", CATEGORY_C), ("D:no-verdict", CATEGORY_D)]:
        for issue in values:
            result.setdefault(issue, []).append(label)
    return result


def fetch_github_issues(numbers: list[int]) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    for start in range(0, len(numbers), QUERY_CHUNK):
        chunk = numbers[start : start + QUERY_CHUNK]
        issue_blocks = []
        for number in chunk:
            alias = f"i{number}"
            issue_blocks.append(
                f"""
                {alias}: issue(number:{number}) {{
                  number
                  title
                  state
                  url
                  body
                  closedAt
                  comments(last:{COMMENT_LIMIT}) {{
                    nodes {{
                      author {{ login }}
                      body
                      createdAt
                      url
                    }}
                  }}
                }}
                """
            )
        query = f"""
        query {{
          repository(owner:"{REPO_OWNER}", name:"{REPO_NAME}") {{
            {" ".join(issue_blocks)}
          }}
        }}
        """
        raw = run_command(["gh", "api", "graphql", "-f", f"query={query}"])
        payload = json.loads(raw)
        repo_data = payload["data"]["repository"]
        for number in chunk:
            node = repo_data.get(f"i{number}")
            if node:
                result[number] = node
    return result


def parse_dod(body: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    in_dod = False
    for line in body.splitlines():
        if line.startswith("## "):
            header = line[3:].strip().lower()
            if header in {"dod", "definition of done"}:
                in_dod = True
                continue
            if in_dod:
                break
        if not in_dod:
            continue
        match = re.match(r"^\s*[-*]\s*\[([ xX])\]\s+(.*)$", line)
        if match:
            items.append(
                {
                    "checked": match.group(1).lower() == "x",
                    "text": match.group(2).strip(),
                }
            )
    return items


def is_status_only_comment(body: str) -> bool:
    lowered = body.lower()
    return any(marker.lower() in lowered for marker in STATUS_ONLY_MARKERS)


def comment_excerpt(body: str, limit: int = 220) -> str:
    one_line = " ".join(body.split())
    if len(one_line) <= limit:
        return one_line
    return one_line[: limit - 3] + "..."


def extract_comment_verdict(body: str) -> str | None:
    match = re.search(r"VERDICT:\s*(pass|approved|improve|reject|rework)", body, re.IGNORECASE)
    if match:
        return match.group(1).lower()
    if re.search(r"코드 리뷰 결과\s*`?pass`?", body, re.IGNORECASE):
        return "pass-inferred"
    if "No new findings" in body:
        return "pass-inferred"
    if "모두 해소" in body:
        return "pass-inferred"
    if "지적사항 모두 반영 확인" in body:
        return "pass-inferred"
    if "리뷰 결과" in body or "Review finding:" in body:
        if "blocker" in body.lower() or "**High**" in body or "blocking 이슈" in body:
            return "negative-inferred"
    return None


def is_fix_followup(body: str) -> bool:
    lowered = body.lower()
    return any(marker.lower() in lowered for marker in FIX_FOLLOWUP_MARKERS)


def has_any_marker(body: str, markers: tuple[str, ...]) -> bool:
    lowered = body.lower()
    return any(marker.lower() in lowered for marker in markers)


def analyze_comments(comments: list[dict[str, Any]]) -> dict[str, Any]:
    substantive: list[CommentInfo] = []
    for node in comments:
        body = (node.get("body") or "").strip()
        if not body or is_status_only_comment(body):
            continue
        substantive.append(CommentInfo(node.get("createdAt") or "", body, node.get("url")))

    last_substantive = substantive[-1] if substantive else None
    last_review = None
    last_negative = None
    last_positive = None
    for comment in substantive:
        verdict = extract_comment_verdict(comment.body)
        if verdict is not None:
            last_review = comment
            if verdict in {"rework", "reject", "improve", "negative-inferred"}:
                last_negative = comment
            if verdict in {"pass", "approved", "pass-inferred"}:
                last_positive = comment
        elif has_any_marker(comment.body, POSITIVE_REVIEW_MARKERS):
            last_review = comment
            last_positive = comment
        elif has_any_marker(comment.body, NEGATIVE_REVIEW_MARKERS):
            last_review = comment
            last_negative = comment

    followup_after_negative = None
    if last_negative is not None:
        for comment in substantive:
            if comment.created_at <= last_negative.created_at:
                continue
            if is_fix_followup(comment.body) or extract_comment_verdict(comment.body) in {
                "pass",
                "approved",
                "pass-inferred",
            }:
                followup_after_negative = comment
    return {
        "last_substantive": last_substantive,
        "last_review": last_review,
        "last_negative": last_negative,
        "last_positive": last_positive,
        "followup_after_negative": followup_after_negative,
    }


def fetch_db_state(numbers: list[int]) -> dict[int, dict[str, Any]]:
    placeholders = ",".join("?" for _ in numbers)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cards: dict[int, dict[str, Any]] = {}
    card_ids: list[str] = []
    card_rows = conn.execute(
        f"""
        SELECT kc.id,
               kc.repo_id,
               kc.github_issue_number,
               kc.title,
               kc.status,
               kc.review_round,
               kc.review_status,
               kc.github_issue_url,
               kc.deferred_dod_json,
               kc.metadata,
               kc.latest_dispatch_id,
               kc.updated_at,
               crs.state AS canonical_state,
               crs.last_verdict AS canonical_verdict,
               crs.review_round AS canonical_review_round
          FROM kanban_cards kc
          LEFT JOIN card_review_state crs ON crs.card_id = kc.id
         WHERE kc.repo_id = ?
           AND kc.github_issue_number IN ({placeholders})
        """,
        [REPO_ID, *numbers],
    ).fetchall()
    for row in card_rows:
        issue = row["github_issue_number"]
        cards[issue] = dict(row)
        card_ids.append(row["id"])

    dispatch_by_card: dict[str, list[dict[str, Any]]] = {}
    if card_ids:
        placeholders = ",".join("?" for _ in card_ids)
        dispatch_rows = conn.execute(
            f"""
            SELECT kanban_card_id, id, dispatch_type, status, title, result, context, created_at, updated_at
              FROM task_dispatches
             WHERE kanban_card_id IN ({placeholders})
             ORDER BY created_at, rowid
            """,
            card_ids,
        ).fetchall()
        for row in dispatch_rows:
            dispatch_by_card.setdefault(row["kanban_card_id"], []).append(dict(row))

    for issue, info in cards.items():
        history = dispatch_by_card.get(info["id"], [])
        info["dispatches"] = history
        info["last_review_dispatch"] = None
        info["last_review_decision_dispatch"] = None
        info["last_work_dispatch"] = None
        for item in history:
            dtype = item["dispatch_type"]
            if dtype == "review":
                info["last_review_dispatch"] = item
            elif dtype == "review-decision":
                info["last_review_decision_dispatch"] = item
            elif dtype in {"implementation", "rework"}:
                info["last_work_dispatch"] = item
    conn.close()
    return cards


def dispatch_verdict(dispatch: dict[str, Any] | None) -> str | None:
    if not dispatch:
        return None
    parsed = parse_json_maybe_nested(dispatch.get("result"))
    if isinstance(parsed, dict):
        verdict = parsed.get("verdict") or parsed.get("decision")
        if isinstance(verdict, str):
            return verdict
    return None


def dispatch_summary(dispatch: dict[str, Any] | None) -> str:
    if not dispatch:
        return "none"
    dtype = dispatch.get("dispatch_type") or "unknown"
    status = dispatch.get("status") or "unknown"
    verdict = dispatch_verdict(dispatch)
    parsed = parse_json_maybe_nested(dispatch.get("result"))
    bits = [dtype, status]
    if verdict:
        bits.append(f"verdict={verdict}")
    if isinstance(parsed, dict) and parsed.get("commit"):
        bits.append(f"commit={parsed['commit']}")
    return ", ".join(bits)


def determine_validity(issue: int, categories: list[str], github: dict[str, Any], db: dict[str, Any] | None, comment_analysis: dict[str, Any]) -> tuple[str, str]:
    last_negative = comment_analysis["last_negative"]
    followup_after_negative = comment_analysis["followup_after_negative"]
    last_positive = comment_analysis["last_positive"]
    if any(cat.startswith("A:") for cat in categories):
        if last_negative and followup_after_negative:
            return (
                "likely-resolved-by-followup",
                "negative review exists, but later follow-up claims fix/no-new-findings; avoid duplicate implementation and validate current code/tests first",
            )
        if last_negative:
            return (
                "still-actionable",
                "negative review exists without later fix evidence; candidate for reopen or direct rework",
            )
        if last_positive:
            return (
                "likely-resolved",
                "review trail ends in positive/no-new-findings signal",
            )
        return (
            "needs-manual-check",
            "no clear negative review trail found in latest comments",
        )

    if github and github.get("state") == "CLOSED" and last_positive:
        return ("likely-resolved", "closed issue with positive review signal")
    if db and db.get("status") == "done" and not last_negative:
        return ("likely-resolved", "done in runtime DB and no outstanding negative review signal")
    return ("needs-manual-check", "no high-confidence obsolescence signal")


def determine_dod(github: dict[str, Any]) -> tuple[str, int, int, list[str]]:
    if not github:
        return ("missing-issue", 0, 0, [])
    items = parse_dod(github.get("body") or "")
    if not items:
        return ("no-dod-section", 0, 0, [])
    unchecked = [item["text"] for item in items if not item["checked"]]
    if unchecked:
        return ("unchecked", len(items), len(unchecked), unchecked)
    return ("all-checked", len(items), 0, [])


def determine_review(github: dict[str, Any], db: dict[str, Any] | None, comment_analysis: dict[str, Any]) -> tuple[str, str]:
    last_review = comment_analysis["last_review"]
    if last_review is not None:
        verdict = extract_comment_verdict(last_review.body)
        if verdict is None and "Review finding:" in last_review.body:
            verdict = "negative-inferred"
        if verdict is None and "리뷰 결과" in last_review.body:
            verdict = "negative-inferred"
        if verdict is not None:
            return ("review-found", verdict)
        return ("review-found", "unknown")

    if db:
        db_round = db.get("canonical_review_round")
        if db_round:
            verdict = db.get("canonical_verdict") or dispatch_verdict(db.get("last_review_dispatch"))
            return ("db-only", verdict or "unknown")
    return ("missing", "none")


def recommend_action(
    categories: list[str],
    validity: str,
    dod_state: str,
    review_state: str,
    review_verdict: str,
) -> str:
    if any(cat.startswith("A:") for cat in categories):
        if validity == "still-actionable":
            return "reopen-or-rework"
        if validity == "likely-resolved-by-followup":
            return "validate-current-code-before-any-new-fix"
        return "manual-review-check"

    if "B:missing-dod" in categories:
        if dod_state == "unchecked":
            return "run-real-validation-and-sync-dod"
        if dod_state == "no-dod-section":
            return "confirm-no-dod-or-backfill-checklist"

    if "C:no-review" in categories and review_state == "missing":
        return "fresh-review-needed"

    if "D:no-verdict" in categories:
        if review_state == "review-found" and review_verdict in {"pass-inferred", "pass", "approved"}:
            return "record-pass-verdict-or-sync-state"
        if review_state == "review-found" and review_verdict in {
            "rework",
            "reject",
            "improve",
            "negative-inferred",
        }:
            return "record-negative-verdict-and-handle-followup"
        return "manual-verdict-reconstruction"

    return "manual-check"


def make_issue_section(
    issue: int,
    categories: list[str],
    github: dict[str, Any] | None,
    db: dict[str, Any] | None,
    comment_analysis: dict[str, Any],
) -> str:
    validity, validity_note = determine_validity(issue, categories, github or {}, db, comment_analysis)
    dod_state, dod_total, dod_unchecked, unchecked_items = determine_dod(github or {})
    review_state, review_verdict = determine_review(github or {}, db, comment_analysis)
    recommendation = recommend_action(categories, validity, dod_state, review_state, review_verdict)

    title = github.get("title") if github else (db.get("title") if db else "(missing)")
    gh_state = github.get("state") if github else "MISSING"
    db_status = db.get("status") if db else "missing"
    review_dispatch = dispatch_summary(db.get("last_review_dispatch") if db else None)
    work_dispatch = dispatch_summary(db.get("last_work_dispatch") if db else None)

    lines = [
        f"### #{issue} {title}",
        f"- Categories: {', '.join(categories) if categories else 'none'}",
        f"- GitHub state: {gh_state}",
        f"- Runtime DB status: {db_status}",
        f"- Validity: {validity}",
        f"- Validity note: {validity_note}",
        f"- DoD: {dod_state} ({dod_total} items, {dod_unchecked} unchecked)",
        f"- Review: {review_state} ({review_verdict})",
        f"- Last review dispatch: {review_dispatch}",
        f"- Last implementation/rework dispatch: {work_dispatch}",
        f"- Recommended action: {recommendation}",
    ]

    if unchecked_items:
        lines.append("- Unchecked DoD:")
        for item in unchecked_items[:4]:
            lines.append(f"  - {item}")
        if len(unchecked_items) > 4:
            lines.append(f"  - ... {len(unchecked_items) - 4} more")

    last_negative = comment_analysis["last_negative"]
    if last_negative is not None:
        lines.append(f"- Last negative review: {comment_excerpt(last_negative.body)}")
    followup_after_negative = comment_analysis["followup_after_negative"]
    if followup_after_negative is not None:
        lines.append(f"- Follow-up after negative review: {comment_excerpt(followup_after_negative.body)}")
    last_substantive = comment_analysis["last_substantive"]
    if last_substantive is not None and last_substantive is not followup_after_negative:
        lines.append(f"- Last substantive comment: {comment_excerpt(last_substantive.body)}")
    lines.append("")
    return "\n".join(lines)


def build_report(github_data: dict[int, dict[str, Any]], db_data: dict[int, dict[str, Any]]) -> str:
    generated_at = dt.datetime.now(dt.timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    numbers = issue_numbers()
    category_map = build_category_map()

    lines = [
        "# Pipeline Bypass Audit",
        "",
        f"- Generated at: {generated_at}",
        f"- Repo: `{REPO_ID}`",
        f"- Runtime DB: `{DB_PATH}`",
        f"- Scope: {len(numbers)} issue numbers from categories A/B/C/D",
        "",
        "## Summary",
        "",
    ]

    matched_db = sum(1 for num in numbers if num in db_data)
    matched_gh = sum(1 for num in numbers if num in github_data)
    lines.append(f"- GitHub issues fetched: {matched_gh}/{len(numbers)}")
    lines.append(f"- Runtime DB cards found: {matched_db}/{len(numbers)}")

    a_resolved = 0
    a_actionable = 0
    needs_dod = 0
    missing_review = 0
    missing_verdict = 0

    per_issue_blocks: dict[str, list[str]] = {
        "A": [],
        "B": [],
        "C": [],
        "D": [],
    }

    for number in numbers:
        categories = category_map.get(number, [])
        github = github_data.get(number)
        db = db_data.get(number)
        comment_analysis = analyze_comments((github or {}).get("comments", {}).get("nodes", []))
        validity, _ = determine_validity(number, categories, github or {}, db, comment_analysis)
        dod_state, _, _, _ = determine_dod(github or {})
        review_state, review_verdict = determine_review(github or {}, db, comment_analysis)

        if any(cat.startswith("A:") for cat in categories):
            if validity == "likely-resolved-by-followup":
                a_resolved += 1
            elif validity == "still-actionable":
                a_actionable += 1
            per_issue_blocks["A"].append(make_issue_section(number, categories, github, db, comment_analysis))
        if "B:missing-dod" in categories:
            if dod_state in {"unchecked", "no-dod-section"}:
                needs_dod += 1
            per_issue_blocks["B"].append(make_issue_section(number, categories, github, db, comment_analysis))
        if "C:no-review" in categories:
            if review_state == "missing":
                missing_review += 1
            per_issue_blocks["C"].append(make_issue_section(number, categories, github, db, comment_analysis))
        if "D:no-verdict" in categories:
            if review_state == "missing" or review_verdict in {"unknown", "none"}:
                missing_verdict += 1
            per_issue_blocks["D"].append(make_issue_section(number, categories, github, db, comment_analysis))

    lines.extend(
        [
            f"- Category A issues likely already resolved by later follow-up: {a_resolved}",
            f"- Category A issues still actionable from latest negative review trail: {a_actionable}",
            f"- Category B issues with unchecked/no DoD section still visible on GitHub: {needs_dod}",
            f"- Category C issues with no review signal found: {missing_review}",
            f"- Category D issues still missing verdict reconstruction: {missing_verdict}",
            "",
            "## Category A",
            "",
            "Priority: rework/improve/reject then done. Main question is whether later work already addressed the negative review.",
            "",
        ]
    )
    lines.extend(per_issue_blocks["A"])
    lines.extend(
        [
            "## Category B",
            "",
            "Main question: unchecked DoD items that still require real validation or checklist sync.",
            "",
        ]
    )
    lines.extend(per_issue_blocks["B"])
    lines.extend(
        [
            "## Category C",
            "",
            "Main question: issues reported as review_round=0 / no review trail.",
            "",
        ]
    )
    lines.extend(per_issue_blocks["C"])
    lines.extend(
        [
            "## Category D",
            "",
            "Main question: review happened but verdict was never recorded or cannot be reconstructed confidently.",
            "",
        ]
    )
    lines.extend(per_issue_blocks["D"])
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit bypassed pipeline issues from runtime DB + GitHub.")
    parser.add_argument("--output", required=True, help="Markdown report path")
    args = parser.parse_args()

    numbers = issue_numbers()
    github_data = fetch_github_issues(numbers)
    db_data = fetch_db_state(numbers)
    report = build_report(github_data, db_data)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    print(str(output_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
