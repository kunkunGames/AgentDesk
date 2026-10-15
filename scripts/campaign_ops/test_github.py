"""Protect the original allowlists and the internal execution dependency."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from campaign_ops.github import command, runtime_gh, read_request, review_threads, check_details, main


class GitHubContractTests(unittest.TestCase):
    @patch("campaign_ops.github.subprocess.run")
    @patch("campaign_ops.github.runtime_gh", return_value="/internal/gh")
    @patch("sys.argv", ["routine-gh.py", "kunkunGames/monolith", "checks", "2929", "--required"])
    def test_regular_pr_commands_have_a_deadline_and_do_not_retry(self, runtime, run):
        import subprocess
        run.side_effect = subprocess.TimeoutExpired(["/internal/gh", "pr", "checks"], 60)
        with self.assertRaises(SystemExit) as exit_status:
            main()
        self.assertEqual(exit_status.exception.code, 2)
        run.assert_called_once_with(["/internal/gh", "pr", "checks", "--repo", "kunkunGames/monolith", "2929", "--required"], check=False, timeout=60)

    def test_fixed_api_reads_reject_cross_repo_flags_urls_and_extra_fields(self):
        self.assertEqual(read_request(["kunkunGames/monolith", "review-threads", "2929"]),
                         ("kunkunGames/monolith", "review-threads", 2929))
        self.assertEqual(read_request(["kunkunGames/monolith", "check-details", "2929", "7"]),
                         ("kunkunGames/monolith", "check-details", 2929))
        for args in [["other/repo", "check-details", "1"], ["kunkunGames/monolith", "api", "1"],
                     ["kunkunGames/AgentDesk", "review-threads", "https://github.com/other/repo/pull/1"],
                     ["kunkunGames/monolith", "check-details", "1", "--admin"],
                     ["kunkunGames/monolith", "review-threads", "1", "7"],
                     ["kunkunGames/monolith", "check-details", "1", "0"],
                     ["kunkunGames/monolith", "check-details", "1", "7", "8"]]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                read_request(args)

    @patch("campaign_ops.github.read_json")
    def test_review_threads_preserves_resolution_and_fails_closed_on_head_drift(self, read):
        page = {"data": {"repository": {"pullRequest": {"headRefOid": "a" * 40,
                "reviewThreads": {"pageInfo": {"hasNextPage": False}, "nodes": [
                    {"isResolved": False, "comments": {"totalCount": 1, "nodes": [{"body": "needs correction"}]}}]}}}}}
        read.return_value = page
        report = review_threads("/internal/gh", "kunkunGames/monolith", 2929)
        self.assertTrue(report["complete"])
        self.assertFalse(report["threads"][0]["isResolved"])
        page["data"]["repository"]["pullRequest"]["reviewThreads"]["pageInfo"] = {"hasNextPage": True, "endCursor": "next"}
        changed = {"data": {"repository": {"pullRequest": {"headRefOid": "b" * 40}}}}
        read.side_effect = [page, changed]
        with self.assertRaisesRegex(ValueError, "head changed"):
            review_threads("/internal/gh", "kunkunGames/monolith", 2929)

    @patch("campaign_ops.github.read_json")
    def test_check_details_keeps_failures_annotations_and_only_repo_scoped_jobs(self, read):
        head = "a" * 40
        check = {"id": 7, "head_sha": head, "conclusion": "failure", "output": {"annotations_count": 1},
                 "details_url": "https://github.com/kunkunGames/monolith/actions/runs/3/job/4"}
        read.side_effect = [{"headRefOid": head}, [{"total_count": 1, "check_runs": [check]}],
                            [{"message": "billing gate"}], {"steps": []}, {"headRefOid": head}]
        report = check_details("/internal/gh", "kunkunGames/monolith", 2929)
        self.assertTrue(report["complete"])
        self.assertEqual(report["checks"][0]["conclusion"], "failure")
        self.assertEqual(report["checks"][0]["job"]["steps"], [])
        self.assertIn("repos/kunkunGames/monolith/actions/jobs/4", read.call_args_list[3].args[1])

    @patch("campaign_ops.github.read_json")
    def test_multiple_failures_inspect_only_one_and_preserve_all_statuses(self, read):
        head = "a" * 40
        checks = [{"id": n, "head_sha": head, "conclusion": "failure", "output": {"annotations_count": 1},
                   "details_url": f"https://github.com/kunkunGames/monolith/actions/runs/3/job/{n}"} for n in [8, 7]]
        read.side_effect = [{"headRefOid": head}, [{"total_count": 2, "check_runs": checks}],
                            [{"message": "primary failure"}], {"id": 7, "steps": []}, {"headRefOid": head}]
        report = check_details("/internal/gh", "kunkunGames/monolith", 2929)
        self.assertTrue(report["complete"])
        self.assertEqual(report["representative_check_id"], 7)
        self.assertEqual(report["uninspected_failure_details"], [8])
        self.assertEqual(len(report["checks"]), 2)
        self.assertNotIn("annotations", report["checks"][0])
        self.assertNotIn("job", report["checks"][0])
        endpoints = [args.args[1][-1] for args in read.call_args_list]
        self.assertEqual(sum("/annotations?" in endpoint for endpoint in endpoints), 1)
        self.assertEqual(sum("/actions/jobs/" in endpoint for endpoint in endpoints), 1)

    @patch("campaign_ops.github.read_json")
    def test_representative_selector_is_pinned_and_rejects_unknown_check(self, read):
        head = "a" * 40
        checks = [{"id": n, "head_sha": head, "conclusion": "failure", "output": {"annotations_count": 0},
                   "details_url": "https://github.com/other/repo/actions/runs/3/job/9"} for n in [7, 8]]
        read.side_effect = [{"headRefOid": head}, [{"total_count": 2, "check_runs": checks}], [], {"headRefOid": head}]
        report = check_details("/internal/gh", "kunkunGames/monolith", 2929, 8)
        self.assertEqual(report["representative_check_id"], 8)
        self.assertEqual(report["uninspected_failure_details"], [7])
        self.assertFalse(any("/actions/jobs/" in args.args[1][-1] for args in read.call_args_list))
        read.reset_mock()
        read.side_effect = [{"headRefOid": head}, [{"total_count": 2, "check_runs": checks}]]
        with self.assertRaisesRegex(ValueError, "on this PR head"):
            check_details("/internal/gh", "kunkunGames/monolith", 2929, 999)
        self.assertEqual(read.call_count, 2)

    @patch("campaign_ops.github.read_json")
    def test_check_details_fails_closed_on_head_drift_and_incomplete_annotations(self, read):
        head = "a" * 40
        check = {"id": 7, "head_sha": head, "conclusion": "failure", "output": {"annotations_count": 2}}
        read.side_effect = [{"headRefOid": head}, [{"total_count": 1, "check_runs": [check]}], [], {"headRefOid": head}]
        self.assertFalse(check_details("/internal/gh", "kunkunGames/monolith", 2929)["complete"])
        read.side_effect = [{"headRefOid": head}, [{"total_count": 1, "check_runs": [check]}], [], {"headRefOid": "b" * 40}]
        with self.assertRaisesRegex(ValueError, "head changed"):
            check_details("/internal/gh", "kunkunGames/monolith", 2929)

    @patch("campaign_ops.github.check_details", return_value={"complete": True})
    @patch("campaign_ops.github.runtime_gh", return_value="/internal/gh")
    @patch("sys.argv", ["routine-gh.py", "kunkunGames/monolith", "check-details", "2929", "8"])
    def test_cli_passes_the_validated_representative_selector(self, runtime, details):
        main()
        details.assert_called_once_with("/internal/gh", "kunkunGames/monolith", 2929, 8)

    def test_list_uses_pinned_executable_and_preserves_flags(self):
        self.assertEqual(command(["kunkunGames/AgentDesk", "list", "--state", "open"], "/internal/gh"),
                         ["/internal/gh", "pr", "list", "--repo", "kunkunGames/AgentDesk", "--state", "open"])

    def test_disallowed_repository_and_action_are_rejected(self):
        for args in [[], ["other/repo", "list"], ["kunkunGames/AgentDesk", "api"], ["kunkunGames/AgentDesk", "auth"]]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                command(args, "/internal/gh")

    def test_cross_repository_numbers_and_override_flags_are_rejected(self):
        with self.assertRaises(ValueError):
            command(["kunkunGames/AgentDesk", "view", "https://github.com/other/repo/pull/1"], "/internal/gh")
        for flag in ["-Rother/repo", "--repo", "--repo=other/repo", "--admin", "--admin=true", "--delete-branch", "--delete-branch=true"]:
            with self.subTest(flag=flag), self.assertRaises(ValueError):
                command(["kunkunGames/monolith", "merge", "7", flag], "/internal/gh")

    def test_missing_internal_tool_never_falls_back_to_path(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"PATH": "/offloaded/bin"}):
            with self.assertRaisesRegex(ValueError, "missing"):
                runtime_gh(directory)

    def test_internal_tool_wins_over_login_shell_path(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"PATH": "/offloaded/bin"}):
            tool = Path(directory) / ".adk/runtime-bin/gh"
            tool.parent.mkdir(parents=True)
            tool.write_text("#!/bin/sh\nexit 0\n")
            tool.chmod(0o755)
            self.assertEqual(runtime_gh(directory), str(tool))

    @unittest.skipIf(os.name == "nt", "native Mac/Linux symlink check")
    def test_runtime_symlink_cannot_escape_the_owned_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory) / "home"
            tool = home / ".adk/runtime-bin/gh"
            tool.parent.mkdir(parents=True)
            external = Path(directory) / "external-gh"
            external.write_text("#!/bin/sh\nexit 0\n")
            external.chmod(0o755)
            tool.symlink_to(external)
            with self.assertRaisesRegex(ValueError, "internal runtime"):
                runtime_gh(home)
