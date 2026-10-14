"""Protect complete feedback evidence used by independent external-effect gates."""
from copy import deepcopy
import unittest
from unittest.mock import Mock, patch

from campaign_ops.github import main, read_request
from campaign_ops.github_feedback import feedback


REPO = "kunkunGames/AgentDesk"
NUMBER = 2239
IDENTITY = {"number": NUMBER, "url": f"https://github.com/{REPO}/pull/{NUMBER}",
            "headRefOid": "a" * 40, "baseRefOid": "b" * 40, "state": "OPEN",
            "updatedAt": "2026-10-01T00:00:00Z"}


def response(connection=None, rows=(), total=0, cursor=None):
    pr = deepcopy(IDENTITY)
    if connection:
        pr[connection] = {"totalCount": total, "nodes": list(rows),
                          "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor}}
    else:
        pr.update(comments={"totalCount": total}, reviews={"totalCount": 1})
    return {"data": {"repository": {"pullRequest": pr}}}


def row(source_id, body):
    return {"id": source_id, "databaseId": 7, "author": None, "body": body,
            "url": f"https://github.com/{REPO}/pull/{NUMBER}#issuecomment-7",
            "createdAt": "2026-10-01T00:00:00Z", "updatedAt": "2026-10-01T00:00:00Z"}


def successful_reads():
    return [response("comments", [row("C1", "original")], 2, "next"),
            response("comments", [row("C2", "new objection")], 2),
            response("reviews", [row("R1", "requested changes")], 1), response(total=2)]


class FeedbackContractTests(unittest.TestCase):
    def test_raw_pages_and_final_recheck_preserve_original_provider_payloads(self):
        pages = successful_reads()
        originals = deepcopy(pages)
        report = feedback("/internal/gh", REPO, NUMBER, Mock(side_effect=pages))
        self.assertEqual([e["response"] for e in report["read_evidence"]], originals)
        first = report["read_evidence"][0]["response"]["data"]["repository"]["pullRequest"]
        terminal = report["read_evidence"][1]["response"]["data"]["repository"]["pullRequest"]
        final = report["read_evidence"][-1]["response"]["data"]["repository"]["pullRequest"]
        self.assertEqual(first["comments"]["pageInfo"], {"hasNextPage": True, "endCursor": "next"})
        self.assertEqual(terminal["comments"]["pageInfo"], {"hasNextPage": False, "endCursor": None})
        self.assertEqual(final["comments"]["totalCount"], 2)
        self.assertEqual(final["reviews"]["totalCount"], 1)
        self.assertIn("after=next", report["read_evidence"][1]["argv"])
        self.assertEqual(final["headRefOid"], report["head"])

    def test_all_pages_preserve_actual_feedback_and_report_counts(self):
        read = Mock(side_effect=successful_reads())
        report = feedback("/internal/gh", REPO, NUMBER, read)
        self.assertTrue(report["complete"])
        self.assertEqual([r["body"] for r in report["comments"]], ["original", "new objection"])
        self.assertEqual(report["reviews"][0]["body"], "requested changes")
        self.assertIsNone(report["comments"][0]["author"])
        self.assertEqual(report["pagination"]["comments"],
                         {"total_count": 2, "fetched_count": 2, "pages": 2, "has_next_page": False})
        self.assertEqual(report["scope"], "issue_comments_and_pull_request_reviews")
        self.assertIn("after=next", read.call_args_list[1].args[1])
        for call in read.call_args_list:
            self.assertEqual(call.args[0], "/internal/gh")
            self.assertIn("owner=kunkunGames", call.args[1])
            self.assertIn("name=AgentDesk", call.args[1])
            self.assertIn("number=2239", call.args[1])

    def test_identity_drift_between_pages_blocks_the_entire_read(self):
        for key, changed in [("headRefOid", "c" * 40), ("baseRefOid", "d" * 40),
                             ("state", "CLOSED"), ("updatedAt", "2026-10-01T00:00:01Z")]:
            pages = successful_reads()
            pages[1]["data"]["repository"]["pullRequest"][key] = changed
            read = Mock(side_effect=pages)
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "changed during"):
                feedback("/internal/gh", REPO, NUMBER, read)
            self.assertEqual(read.call_count, 2)

    def test_final_recheck_rejects_new_comments_or_review_changes(self):
        for connection in ["comments", "reviews"]:
            pages = successful_reads()
            pages[-1]["data"]["repository"]["pullRequest"][connection]["totalCount"] += 1
            with self.subTest(connection=connection), self.assertRaisesRegex(ValueError, "changed during"):
                feedback("/internal/gh", REPO, NUMBER, Mock(side_effect=pages))

    def test_incomplete_terminal_page_is_never_labeled_complete(self):
        pages = successful_reads()
        pages[1]["data"]["repository"]["pullRequest"]["comments"]["nodes"] = []
        report = feedback("/internal/gh", REPO, NUMBER, Mock(side_effect=pages))
        self.assertFalse(report["complete"])
        self.assertEqual(report["pagination"]["comments"]["total_count"], 2)
        self.assertEqual(report["pagination"]["comments"]["fetched_count"], 1)

    def test_repeated_ids_and_nonadvancing_cursors_fail_closed(self):
        pages = successful_reads()
        pages[1] = response("comments", [row("C1", "duplicate")], 2)
        with self.assertRaisesRegex(ValueError, "duplicate source IDs"):
            feedback("/internal/gh", REPO, NUMBER, Mock(side_effect=pages))
        pages[1] = response("comments", [row("C2", "second")], 2, "next")
        with self.assertRaisesRegex(ValueError, "cursor did not advance"):
            feedback("/internal/gh", REPO, NUMBER, Mock(side_effect=pages))

    def test_api_errors_never_return_partial_success_or_retry(self):
        read = Mock(return_value={"errors": [{"message": "permission denied"}]})
        with self.assertRaisesRegex(ValueError, "did not complete"):
            feedback("/internal/gh", REPO, NUMBER, read)
        self.assertEqual(read.call_count, 1)

    def test_page_and_size_limits_preserve_unavailable_evidence(self):
        pages = [response("comments", [row("C" + str(n), "body")], 20, "cursor" + str(n)) for n in range(3)]
        read = Mock(side_effect=pages)
        with patch("campaign_ops.github_feedback.MAX_PAGES", 2), self.assertRaisesRegex(ValueError, "inspection limit"):
            feedback("/internal/gh", REPO, NUMBER, read)
        self.assertEqual(read.call_count, 2)
        read = Mock(side_effect=successful_reads())
        with patch("campaign_ops.github_feedback.MAX_BYTES", 16), self.assertRaisesRegex(ValueError, "report limit"):
            feedback("/internal/gh", REPO, NUMBER, read)
        self.assertEqual(read.call_count, 1)

    @patch("campaign_ops.github.feedback", return_value={"complete": True})
    @patch("campaign_ops.github.runtime_gh", return_value="/internal/gh")
    @patch("sys.argv", ["routine-gh.py", REPO, "feedback", str(NUMBER)])
    def test_cli_uses_only_the_validated_fixed_read(self, runtime, read):
        main()
        self.assertEqual(read.call_args.args[:3], ("/internal/gh", REPO, NUMBER))
        self.assertEqual(read_request([REPO, "feedback", str(NUMBER)]), (REPO, "feedback", NUMBER))
        for args in [[REPO, "feedback", "0"], ["other/repo", "feedback", "1"],
                     [REPO, "feedback", "1", "7"], [REPO, "feedback", "1", "--repo=other/repo"]]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                read_request(args)
