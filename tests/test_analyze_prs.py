import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.analyze_prs import (
    has_duplicate_guard_ack,
    has_non_empty_body_field,
    has_no_change_verification_ack,
    has_stale_branch_cleanup_ack,
    has_scratch_file_cleanup_ack,
    has_mergeability_status_ack,
    has_verification_grounding_ack,
    has_overlap_reference,
    has_template_summary,
    is_generated_inventory_path,
    is_scratch_file_path,
    _is_top_level_field_label,
)
from scripts.check_root_scratch_files import (
    find_root_scratch_files,
    main as check_root_scratch_files,
)


class PrAnalyzerBodyFieldTests(unittest.TestCase):
    def test_blank_risk_does_not_borrow_populated_rollback(self):
        body = "- Risk:\n- Rollback notes: revert this PR"

        self.assertFalse(has_non_empty_body_field(body, ["risk", "risk assessment"]))
        self.assertTrue(has_non_empty_body_field(body, ["rollback notes", "rollback"]))

    def test_template_placeholders_are_not_meaningful_fields(self):
        body = """
## WorkFingerprint

- Risk:
- Rollback notes:
"""

        self.assertFalse(has_non_empty_body_field(body, ["risk", "risk assessment"]))
        self.assertFalse(has_non_empty_body_field(body, ["rollback notes", "rollback"]))

    def test_html_comment_placeholder_is_not_meaningful_field_content(self):
        body = """
## Summary

<!-- 1-3 bullets: what changed and why. -->
"""

        self.assertFalse(has_non_empty_body_field(body, ["summary"]))

    def test_html_comment_before_real_content_does_not_hide_content(self):
        body = """
## Summary

<!-- 1-3 bullets: what changed and why. -->
Update analyzer hygiene checks.
"""

        self.assertTrue(has_non_empty_body_field(body, ["summary"]))

    def test_allow_none_is_limited_to_explicit_callers(self):
        body = "- Skipped checks with reasons: none"
        labels = ["skipped checks with reasons", "skipped checks"]

        self.assertFalse(has_non_empty_body_field(body, labels))
        self.assertTrue(has_non_empty_body_field(body, labels, allow_none=True))
        self.assertFalse(
            has_non_empty_body_field(
                "- Skipped checks with reasons: n/a",
                labels,
                allow_none=True,
            )
        )

    def test_bolded_field_label_counts_as_populated(self):
        body = "- **Agent:** Codex"

        self.assertTrue(has_non_empty_body_field(body, ["agent"]))

    def test_empty_bold_sublabel_is_not_parent_field_content(self):
        body = "- **Risk**:\n  - **Impact**:"

        self.assertFalse(has_non_empty_body_field(body, ["risk", "risk assessment"]))

    def test_skipped_checks_and_reasons_label_allows_none(self):
        body = "- Skipped checks and reasons: none"

        self.assertTrue(
            has_non_empty_body_field(
                body,
                ["skipped checks and reasons", "skipped checks with reasons", "skipped checks"],
                allow_none=True,
            )
        )

    def test_combined_risk_and_rollback_notes_counts_for_both(self):
        body = "- Risk and rollback notes: low risk; revert this PR."

        self.assertTrue(has_non_empty_body_field(body, ["risk and rollback notes", "risk"]))
        self.assertTrue(
            has_non_empty_body_field(body, ["risk and rollback notes", "rollback notes"])
        )

    def test_multiline_field_value_stops_at_next_field_label(self):
        body = """
- Risk:
  Limited to analyzer reporting.
- Rollback notes:
  Revert the analyzer change.
"""

        self.assertTrue(has_non_empty_body_field(body, ["risk", "risk assessment"]))
        self.assertTrue(has_non_empty_body_field(body, ["rollback notes", "rollback"]))

    def test_indented_key_value_lines_count_as_field_content(self):
        body = """
- Risk:
  - Impact: low
- Rollback notes:
  Command: git revert HEAD
"""

        self.assertTrue(has_non_empty_body_field(body, ["risk", "risk assessment"]))
        self.assertTrue(has_non_empty_body_field(body, ["rollback notes", "rollback"]))

    def test_related_prs_issues_label_alias_is_populated(self):
        body = "- Related PRs/issues: checked #123"

        self.assertTrue(
            has_non_empty_body_field(
                body,
                ["related prs/issues checked", "related prs/issues", "related prs"],
            )
        )

    def test_related_prs_issues_allows_bare_issue_ref_value(self):
        body = """
- Related PRs/issues checked:
  #1234
"""

        self.assertTrue(
            has_non_empty_body_field(
                body,
                ["related prs/issues checked", "related prs/issues", "related prs"],
            )
        )


class PrAnalyzerDuplicateGuardTests(unittest.TestCase):
    def test_unchecked_template_duplicate_guard_is_not_acknowledgement(self):
        body = "- [ ] **Duplicate PR guard:** I have checked for overlapping open PRs."

        self.assertFalse(has_duplicate_guard_ack(body))

    def test_checked_template_duplicate_guard_is_acknowledgement(self):
        body = "- [x] **Duplicate PR guard:** I have checked for overlapping open PRs."

        self.assertTrue(has_duplicate_guard_ack(body))

    def test_checked_template_duplicate_guard_accepts_colon_outside_bold(self):
        body = "- [x] **Duplicate PR guard**: I have checked for overlapping open PRs."

        self.assertTrue(has_duplicate_guard_ack(body))

    def test_bare_checked_template_duplicate_guard_accepts_colon_outside_bold(self):
        body = "- [x] **Duplicate PR guard**:"

        self.assertTrue(has_duplicate_guard_ack(body))

    def test_unchecked_template_duplicate_guard_with_colon_outside_is_not_ack(self):
        body = "- [ ] **Duplicate PR guard**: I have checked for overlapping open PRs."

        self.assertFalse(has_duplicate_guard_ack(body))

    def test_filled_duplicate_overlap_field_is_acknowledgement(self):
        body = (
            "- duplicate/overlap check: compared against sibling upstream-pr "
            "branches; no overlapping scope found."
        )

        self.assertTrue(has_duplicate_guard_ack(body))

    def test_blank_duplicate_field_does_not_borrow_next_value(self):
        body = "- Duplicate PR guard:\n- Risk: limited to analyzer reporting"

        self.assertFalse(has_duplicate_guard_ack(body))


class PrAnalyzerNoChangeVerificationGuardTests(unittest.TestCase):
    def test_unchecked_template_no_change_guard_is_not_acknowledgement(self):
        body = "- [ ] **No-change verification:** If this PR claims no change..."

        self.assertFalse(has_no_change_verification_ack(body))

    def test_checked_template_no_change_guard_is_acknowledgement(self):
        body = "- [x] **No-change verification:** If this PR claims no change..."

        self.assertTrue(has_no_change_verification_ack(body))

    def test_bare_checked_template_no_change_guard_accepts_colon_outside_bold(self):
        body = "- [x] **No-change verification**:"

        self.assertTrue(has_no_change_verification_ack(body))

    def test_filled_no_change_field_is_acknowledgement(self):
        body = "- no-change verification: checked using gh pr view --json files"

        self.assertTrue(has_no_change_verification_ack(body))


class PrAnalyzerStaleBranchCleanupGuardTests(unittest.TestCase):
    def test_unchecked_template_stale_branch_guard_is_not_acknowledgement(self):
        body = "- [ ] **Stale branch cleanup:** I am not salvaging a stale broad branch in-place."

        self.assertFalse(has_stale_branch_cleanup_ack(body))

    def test_checked_template_stale_branch_guard_is_acknowledgement(self):
        body = "- [x] **Stale branch cleanup:** I am not salvaging a stale broad branch in-place."

        self.assertTrue(has_stale_branch_cleanup_ack(body))

    def test_bare_checked_template_stale_branch_guard_accepts_colon_outside_bold(self):
        body = "- [x] **Stale branch cleanup**:"

        self.assertTrue(has_stale_branch_cleanup_ack(body))

    def test_filled_stale_branch_field_is_acknowledgement(self):
        body = "- stale branch cleanup: closed stale branch and recreated."

        self.assertTrue(has_stale_branch_cleanup_ack(body))


class PrAnalyzerScratchFileCleanupGuardTests(unittest.TestCase):
    def test_unchecked_template_scratch_file_guard_is_not_acknowledgement(self):
        body = "- [ ] **Scratch file cleanup:** I have run `git status`..."

        self.assertFalse(has_scratch_file_cleanup_ack(body))

    def test_checked_template_scratch_file_guard_is_acknowledgement(self):
        body = "- [X] **Scratch file cleanup:** I have run `git status`..."

        self.assertTrue(has_scratch_file_cleanup_ack(body))

    def test_bare_checked_template_scratch_file_guard_accepts_colon_outside_bold(self):
        body = "- [x] **Scratch file cleanup**:"

        self.assertTrue(has_scratch_file_cleanup_ack(body))

    def test_filled_scratch_file_field_is_acknowledgement(self):
        body = "- scratch file cleanup: ran git diff --check and git status."

        self.assertTrue(has_scratch_file_cleanup_ack(body))


class PrAnalyzerMergeabilityStatusGuardTests(unittest.TestCase):
    def test_unchecked_template_mergeability_status_guard_is_not_acknowledgement(self):
        body = "- [ ] **Mergeability status:** I am not claiming merge-ready from partial check status."

        self.assertFalse(has_mergeability_status_ack(body))

    def test_checked_template_mergeability_status_guard_is_acknowledgement(self):
        body = "- [X] **Mergeability status:** I am not claiming merge-ready from partial check status."

        self.assertTrue(has_mergeability_status_ack(body))

    def test_filled_mergeability_status_field_is_acknowledgement(self):
        body = "- mergeability status: tests pass."

        self.assertTrue(has_mergeability_status_ack(body))


class PrAnalyzerVerificationGroundingGuardTests(unittest.TestCase):
    def test_unchecked_template_verification_grounding_guard_is_not_acknowledgement(self):
        body = "- [ ] **Verification Grounding:** I am not claiming PostgreSQL, Discord, tmux, provider runtime, browser, or CI verification unless it was actually executed."

        self.assertFalse(has_verification_grounding_ack(body))

    def test_checked_template_verification_grounding_guard_is_acknowledgement(self):
        body = "- [X] **Verification Grounding:** I am not claiming PostgreSQL, Discord, tmux, provider runtime, browser, or CI verification unless it was actually executed."

        self.assertTrue(has_verification_grounding_ack(body))

    def test_filled_verification_grounding_field_is_acknowledgement(self):
        body = "- verification grounding: verified locally."

        self.assertTrue(has_verification_grounding_ack(body))


class PrAnalyzerOverlapReferenceTests(unittest.TestCase):
    def test_overlap_reference_with_hash_and_branch(self):
        body = "This is a no-change PR overlapping with #1234 on branch codex/same-scope."
        self.assertTrue(has_overlap_reference(body))

    def test_overlap_reference_with_url_and_branch(self):
        body = "Overlap with https://github.com/owner/repo/pull/5678 on branch feature/replacement."
        self.assertTrue(has_overlap_reference(body))

    def test_missing_overlap_reference(self):
        body = "This is a no-change PR but lacks exact PR numbers."
        self.assertFalse(has_overlap_reference(body))

    def test_pr_number_without_branch_is_not_overlap_reference(self):
        body = "This is a no-change PR overlapping with #1234."
        self.assertFalse(has_overlap_reference(body))

    def test_ref_prefix_inside_word_is_not_branch_reference(self):
        body = "Overlap with #1234; refreshed docs only."

        self.assertFalse(has_overlap_reference(body))

    def test_plural_overlap_wording_with_branch_is_reference(self):
        body = "This no-change PR overlaps #1234 on branch inventory-refresh."
        self.assertTrue(has_overlap_reference(body))

    def test_duplicate_plural_wording_with_branch_is_reference(self):
        body = "This no-change PR duplicates #1234 on branch feature/foo."
        self.assertTrue(has_overlap_reference(body))

    def test_pull_url_without_branch_is_not_overlap_reference(self):
        body = "Overlap with https://github.com/owner/repo/pull/5678."
        self.assertFalse(has_overlap_reference(body))

    def test_generic_template_issue_references_are_not_overlap_references(self):
        body = """
- Dashboard checklist issue references: #1254 / #1251
- Related PRs/issues checked: #9999
"""

        self.assertFalse(has_overlap_reference(body))

    def test_overlap_reference_inside_overlap_field(self):
        body = """
- Duplicate/overlap check:
  - #1234 on branch codex/same-scope covers this no-change PR.
"""

        self.assertTrue(has_overlap_reference(body))

    def test_overlap_reference_preserves_bold_detail_fields(self):
        body = """
- Duplicate/overlap check:
- **PR**: #1234
- **Branch**: feature/foo
"""

        self.assertTrue(has_overlap_reference(body))

    def test_overlap_reference_accepts_bare_issue_ref_line_inside_overlap_block(self):
        body = """
- Duplicate/overlap check:
  #1234 on branch codex/same-scope
"""

        self.assertTrue(has_overlap_reference(body))

    def test_overlap_reference_accepts_split_pr_and_branch_lines(self):
        body = """
- Duplicate/overlap check:
  - PR: #1234
  - Branch: codex/same-scope
"""

        self.assertTrue(has_overlap_reference(body))

    def test_non_overlapping_reason_is_not_overlap_evidence(self):
        body = """
- Why this is non-overlapping: not overlapping with #1234 on branch feature/foo
"""

        self.assertFalse(has_overlap_reference(body))

    def test_no_overlap_wording_is_not_overlap_evidence(self):
        body = """
- Duplicate/overlap check: checked #123 on branch feature/foo; no overlap found
"""

        self.assertFalse(has_overlap_reference(body))

    def test_negated_boundary_resets_incomplete_overlap_block(self):
        body = """
- Duplicate/overlap check:
  - PR: #123
- Why this is non-overlapping:
  #123 on branch feature/foo
"""

        self.assertFalse(has_overlap_reference(body))

    def test_placeholder_branch_value_is_not_overlap_reference(self):
        body = """
- Duplicate/overlap check:
  - PR: #1234
  - Branch: none
"""

        self.assertFalse(has_overlap_reference(body))


class PrAnalyzerTemplateSummaryTests(unittest.TestCase):
    def test_populated_template_summary_counts_as_change_context(self):
        body = """
## Summary

Update analyzer hygiene checks to match the current template.
"""

        self.assertTrue(has_template_summary(body))

    def test_empty_template_summary_is_not_change_context(self):
        body = """
## Summary

## Test plan
"""

        self.assertFalse(has_template_summary(body))

    def test_html_comment_template_summary_is_not_change_context(self):
        body = """
## Summary

<!-- 1-3 bullets: what changed and why. -->
"""

        self.assertFalse(has_template_summary(body))

    def test_colon_prefixed_summary_bullet_counts_as_change_context(self):
        body = """
## Summary

- Tests: add analyzer coverage.
"""

        self.assertTrue(has_template_summary(body))

    def test_multiline_labelled_summary_bullet_counts_as_change_context(self):
        body = """
## Summary

- Tests:
  Add analyzer coverage.
"""

        self.assertTrue(has_template_summary(body))

    def test_empty_summary_field_labels_are_not_change_context(self):
        body = """
## Summary

- What changed:
- Why:
"""

        self.assertFalse(has_template_summary(body))


class PrAnalyzerScratchPathTests(unittest.TestCase):
    def test_root_scratch_files_are_flagged(self):
        self.assertTrue(is_scratch_file_path("pr-body.md"))
        self.assertTrue(is_scratch_file_path("test.sh"))
        self.assertTrue(is_scratch_file_path("scratch.sh"))
        self.assertTrue(is_scratch_file_path("scratchpad.sh"))
        self.assertTrue(is_scratch_file_path("scratch-check.sql"))
        self.assertTrue(is_scratch_file_path("test_cli.rs"))
        self.assertTrue(is_scratch_file_path("verify.sh"))
        self.assertTrue(is_scratch_file_path("test.py"))
        self.assertTrue(is_scratch_file_path("scratch.js"))
        self.assertTrue(is_scratch_file_path("test_script.py"))
        self.assertTrue(is_scratch_file_path("scratch.sql"))
        self.assertTrue(is_scratch_file_path("scratchpad.sql"))
        self.assertTrue(is_scratch_file_path("scratch.rs"))
        self.assertTrue(is_scratch_file_path("test_scratch.rs"))
        self.assertTrue(is_scratch_file_path("sql_test.rs"))
        self.assertTrue(is_scratch_file_path("patch.diff"))
        self.assertTrue(is_scratch_file_path("changes.patch"))

    def test_checked_in_scripts_and_migrations_are_not_scratch(self):
        self.assertFalse(is_scratch_file_path("scripts/deploy-release.sh"))
        self.assertFalse(is_scratch_file_path("migrations/postgres/001_init.sql"))

    def test_nested_scratch_files_are_flagged(self):
        self.assertTrue(is_scratch_file_path(".github/pr-body.md"))
        self.assertTrue(is_scratch_file_path("src/patch.diff"))
        self.assertTrue(is_scratch_file_path("docs/plan.md"))
        self.assertTrue(is_scratch_file_path("src/scratch.js"))
        self.assertTrue(is_scratch_file_path("policies/scratchpad.py"))
        self.assertTrue(is_scratch_file_path("scripts/test_scratch.sh"))

    def test_nested_test_files_are_not_scratch(self):
        self.assertFalse(is_scratch_file_path("scripts/test.sh"))
        self.assertFalse(is_scratch_file_path("tests/test.py"))
        self.assertFalse(is_scratch_file_path("src/test_cli.rs"))


class PrAnalyzerGeneratedInventoryPathTests(unittest.TestCase):
    def test_generated_inventory_docs_are_flagged(self):
        self.assertTrue(is_generated_inventory_path("docs/generated/module-inventory.md"))
        self.assertTrue(is_generated_inventory_path("docs/generated/giant-file-registry.md"))
        self.assertTrue(is_generated_inventory_path("docs/generated/route-inventory.md"))
        self.assertTrue(is_generated_inventory_path("docs/generated/worker-inventory.md"))

    def test_curated_docs_and_source_are_not_generated_inventory(self):
        self.assertFalse(is_generated_inventory_path("docs/generated/README.md"))
        self.assertFalse(is_generated_inventory_path("docs/generated/pg-audit-checklist.md"))
        self.assertFalse(is_generated_inventory_path("src/dispatch/dispatch_status.rs"))
        self.assertFalse(is_generated_inventory_path(""))


class CiScriptScratchGuardTests(unittest.TestCase):
    def test_ci_guard_uses_canonical_analyzer_policy(self):
        script = Path("scripts/ci-script-checks.sh").read_text()

        self.assertIn('"$PYTHON" -m scripts.check_root_scratch_files', script)
        self.assertNotIn("for scratch_file in", script)

    def test_root_guard_reuses_all_canonical_scratch_types(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            scratch_names = {
                "patch.diff",
                "changes.patch",
                "test.py",
                "scratch-tool.js",
                "test_payload.json",
                "scratch.sql",
                "test_cli.rs",
            }
            for name in scratch_names | {"README.md"}:
                (root / name).write_text("fixture\n", encoding="utf-8")

            self.assertEqual(
                {path.name for path in find_root_scratch_files(root)}, scratch_names
            )
            self.assertEqual(check_root_scratch_files(root), 1)

    def test_root_guard_accepts_a_clean_root(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "README.md").write_text("fixture\n", encoding="utf-8")

            self.assertEqual(find_root_scratch_files(root), [])
            self.assertEqual(check_root_scratch_files(root), 0)

class PrAnalyzerRegexTests(unittest.TestCase):
    def test_bold_label_before_colon(self):
        body = "- **Agent**: Steward"
        self.assertTrue(has_non_empty_body_field(body, ["agent"]))

    def test_bold_label_after_colon(self):
        body = "- **Agent:** Steward"
        self.assertTrue(has_non_empty_body_field(body, ["agent"]))

    def test_bold_label_in_field_check(self):
        body = "- **Agent**: Steward"
        self.assertTrue(_is_top_level_field_label(body))

    def test_bold_label_in_field_check_after_colon(self):
        body = "- **Agent:** Steward"
        self.assertTrue(_is_top_level_field_label(body))



if __name__ == "__main__":
    unittest.main()
