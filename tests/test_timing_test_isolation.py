"""A timing lane must run the same IDs, with no retry or failure exemption."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "timing_test_isolation", ROOT / "scripts/ci/timing_test_isolation.py")
TIMING = sys.modules.setdefault(SPEC.name, importlib.util.module_from_spec(SPEC))
SPEC.loader.exec_module(TIMING)
WATCHDOG = TIMING.ISOLATED_FILTERS[0] + "tests::fast_exit_waits_for_dump_only_within_remaining_blackout_budget"
CODEX = TIMING.ISOLATED_FILTERS[1]


def transcript(ids, *, ignored=(), failed=(), summary=True):
    lines = [f"test {name} ... {'ignored' if name in ignored else 'FAILED' if name in failed else 'ok'}"
             for name in ids]
    if failed:
        lines += ["", "failures:", *(f"    {name}" for name in failed), ""]
    if summary:
        lines += [f"test result: {'FAILED' if failed else 'ok'}. "
                  f"{len(ids) - len(ignored) - len(failed)} passed; {len(failed)} failed; "
                  f"{len(ignored)} ignored; 0 measured; 0 filtered out; finished in 0.01s"]
    return "\n".join(lines) + "\n"


class TimingIsolationTests(unittest.TestCase):
    def test_every_source_timing_id_moves_once_and_filter_replay_stays_disjoint(self):
        skips, timing_skips, broad, isolated = TIMING.full_partition(ROOT)
        inventory = TIMING.runner.load_lib_inventory_manifest(
            ROOT / "scripts/lib_test_inventory_manifest.txt")
        selected = {name for name in inventory
                    if not any(skip in name for skip in skips[1::2])}
        replay = set(TIMING.coverage.load_non_pg_filter_replay(ROOT))
        self.assertEqual(broad | isolated, selected)
        self.assertFalse(broad & isolated or replay & selected)
        self.assertEqual(isolated, {name for name in inventory
                                    if any(value in name for value in TIMING.ISOLATED_FILTERS)})
        self.assertIn(WATCHDOG, isolated)
        self.assertIn(CODEX, isolated)
        self.assertEqual(timing_skips[1::2], list(TIMING.ISOLATED_FILTERS))

    def test_partition_keeps_selected_prs_small_and_rejects_wrong_routing(self):
        self.assertEqual(TIMING.partition(["ordinary::test", WATCHDOG, CODEX, WATCHDOG]),
                         (frozenset({"ordinary::test"}), frozenset({WATCHDOG, CODEX})))
        self.assertEqual(TIMING.run_isolated(ROOT, []), 0)
        with self.assertRaisesRegex(ValueError, "non-timing"):
            TIMING.run_isolated(ROOT, ["ordinary::test"])
        self.assertEqual(TIMING.partition([CODEX + "_lookalike", "prefix::" + WATCHDOG])[1],
                         frozenset())

    def test_a_missing_timing_family_or_an_overlapping_replay_cannot_run(self):
        for inventory in ({"ordinary::test", WATCHDOG}, {"ordinary::test", CODEX}):
            with self.subTest(inventory=inventory), mock.patch.object(
                    TIMING.runner, "load_lib_inventory_manifest", return_value=inventory):
                with self.assertRaisesRegex(ValueError, "nonempty"):
                    TIMING.full_partition(ROOT)
        with mock.patch.object(TIMING.coverage, "load_non_pg_filter_replay",
                               return_value=(WATCHDOG,)):
            with self.assertRaisesRegex(ValueError, "replay id"):
                TIMING.full_partition(ROOT)
        skips = TIMING.coverage.load_non_pg_skip_args(ROOT)
        with mock.patch.object(TIMING.coverage, "load_non_pg_skip_args",
                               return_value=(*skips, "--skip", WATCHDOG)):
            with self.assertRaisesRegex(ValueError, "swallow"):
                TIMING.full_partition(ROOT)
        inventory = TIMING.runner.load_lib_inventory_manifest(
            ROOT / "scripts/lib_test_inventory_manifest.txt")
        with mock.patch.object(TIMING.runner, "load_lib_inventory_manifest",
                               return_value=inventory | {CODEX + "_lookalike"}):
            with self.assertRaisesRegex(ValueError, "partition complement"):
                TIMING.full_partition(ROOT)

    def adjudicate(self, reported, *, ignored=(), failed=(), summary=True, exit_code=0):
        output = transcript(reported, ignored=ignored, failed=failed, summary=summary)
        fake_binary = [sys.executable, "-c",
                       f"import sys; sys.stdout.write({output!r}); sys.exit({exit_code})"]
        real_run, results, expectations = subprocess.run, [], []

        def capture(command, **kwargs):
            expected = Path(command[command.index("--expected-ids") + 1])
            ledger = Path(command[command.index("--ledger") + 1])
            expectations.append((expected, ledger, expected.read_text().split(), ledger.read_text()))
            self.assertEqual(command[command.index("--exact") + 1:],
                             [CODEX, WATCHDOG, "--test-threads=1", "--skip", "_pg"])
            self.assertNotIn("AGENTDESK_ROOT_DIR", kwargs["env"])
            result = real_run(command, capture_output=True, text=True, **kwargs)
            results.append(result)
            return result

        with mock.patch.object(TIMING, "cargo_command", return_value=fake_binary), \
                mock.patch.object(TIMING.subprocess, "run", side_effect=capture), \
                mock.patch.dict(os.environ, {"AGENTDESK_ROOT_DIR": "foreign-root"}):
            code = TIMING.run_isolated(ROOT, [WATCHDOG, CODEX], ["--skip", "_pg"])
        self.assertEqual(len(results), 1, "a failing execution must never be retried")
        expected, ledger, ids, text = expectations[0]
        self.assertEqual(ids, [CODEX, WATCHDOG])
        self.assertEqual([line for line in text.splitlines() if line and not line.startswith("#")], [])
        self.assertFalse(expected.exists() or ledger.exists() or expected.parent.exists())
        return code, results[0].stdout + results[0].stderr

    def test_exact_actual_ids_pass_and_missing_extra_ignored_failed_or_zero_are_red(self):
        self.assertEqual(self.adjudicate([CODEX, WATCHDOG])[0], 0)
        cases = (
            ({"reported": [CODEX]}, "lane-missing"),
            ({"reported": [CODEX, WATCHDOG, "foreign::test"]}, "lane-extra"),
            ({"reported": [CODEX, WATCHDOG], "ignored": [WATCHDOG]}, "undeclared-ignored"),
            ({"reported": [CODEX, WATCHDOG], "failed": [WATCHDOG]}, "unexpected"),
            ({"reported": []}, "no planned test executed"),
            ({"reported": [CODEX, WATCHDOG], "summary": False}, "no libtest"),
            ({"reported": [CODEX, WATCHDOG], "exit_code": 7}, "rc=7"),
        )
        for arguments, diagnostic in cases:
            with self.subTest(arguments=arguments):
                code, output = self.adjudicate(**arguments)
                self.assertNotEqual(code, 0, output)
                self.assertIn(diagnostic, output)

    def test_direct_shell_runs_each_phase_once_and_stops_on_any_failed_phase(self):
        windows_bash = Path("C:/Program Files/Git/bin/bash.exe")
        bash = str(windows_bash) if os.name == "nt" and windows_bash.is_file() else shutil.which("bash")
        self.assertIsNotNone(bash, "the CI contract requires Bash")
        setup = '''
phase() { echo "$1" >> "$TRACE"; [ "$1" != "$FAILED_PHASE" ]; }
python3() {
  case "$1:$2" in
    scripts/ci/timing_test_isolation.py:check) phase check ;;
    scripts/ci/timing_test_isolation.py:serial) phase serial ;;
    scripts/run_test_lane.py:--lane) phase broad ;;
    *) return 99 ;;
  esac
}
cargo() { phase broad; }
'''
        for all_targets, prepared in ((True, False), (False, False), (False, True)):
            for failed in ("check", "broad", "replay", "serial", "none"):
                with self.subTest(all_targets=all_targets, prepared=prepared, failed=failed), \
                        tempfile.TemporaryDirectory(prefix="adk-timing-shell-") as directory:
                    root = Path(directory)
                    filter_path = root / "scripts/ci/non-pg-test-filter.sh"
                    filter_path.parent.mkdir(parents=True)
                    filter_path.write_bytes(b'NON_PG_SKIP_ARGS=(--skip _pg)\n'
                                            b'run_non_pg_filter_replay() { phase replay; }\n')
                    trace = root / "trace"
                    result = subprocess.run([bash, "-eo", "pipefail", "-c", setup +
                        TIMING.contract.sweep_script(all_targets=all_targets, prepared=prepared)],
                        cwd=root, env={**os.environ, "BASH_ENV": "/dev/null",
                                       "TRACE": trace.as_posix(), "FAILED_PHASE": failed},
                        capture_output=True, text=True)
                    phases = ["check", "broad", "replay", "serial"]
                    expected = phases if failed == "none" else phases[:phases.index(failed) + 1]
                    self.assertEqual(trace.read_text().splitlines(), expected, result.stderr)
                    self.assertEqual(result.returncode == 0, failed == "none", result.stderr)

    def test_serial_argv_rejects_selection_changes_before_executing(self):
        command = TIMING.contract.serial_command()
        mutations = [command + ["--list"], command + ["--ignored"],
                     command + ["--skip", WATCHDOG], command[:-1] + ["--test-threads=2"],
                     command[:4] + [CODEX] + command[5:], ["env", "-u", "OTHER_ROOT", *command]]
        with mock.patch.object(TIMING, "adjudicate") as run:
            for mutation in mutations:
                with self.subTest(command=mutation):
                    with self.assertRaisesRegex(ValueError, "closed timing"):
                        TIMING.run_serial(ROOT, mutation)
        run.assert_not_called()

    def test_static_extractors_read_the_real_serial_cargo_argv_and_refuse_changed_suffixes(self):
        from scripts import check_pg_test_lane_membership as membership
        from scripts import check_test_target_integrity as integrity
        import shlex

        for prepared, unset_root in ((False, False), (False, True), (True, True)):
            command = TIMING.contract.serial_command(prepared=prepared, unset_root=unset_root)
            line = shlex.join(TIMING.contract.SERIAL_PREFIX + command)
            expected = command[command.index("cargo"):]
            with self.subTest(prepared=prepared, unset_root=unset_root):
                self.assertEqual(membership._proven_cargo_argv(line), expected)
                self.assertEqual(integrity._parse_command_line(line), expected)
                for suffix in (" --list", " --ignored", " --skip foreign"):
                    with self.assertRaisesRegex(ValueError, "cargo-command-unparsed"):
                        membership._proven_cargo_argv(line + suffix)
                    self.assertFalse(integrity._parse_command_line(line + suffix))

    def test_current_main_and_nightly_keep_the_complete_fatal_shell_contract(self):
        for filename, jobs, all_targets in (("ci-main.yml", ("full_non_pg",), False),
                                            ("ci-nightly.yml", ("full_macos", "full_windows"), True)):
            workflow = yaml.safe_load((ROOT / ".github/workflows" / filename).read_text())
            for name in jobs:
                job = workflow["jobs"][name]
                step_name = "cargo test (non-PG)" if all_targets else "Library sweep (selection-set gated)"
                steps = [step for step in job["steps"] if step.get("name") == step_name]
                with self.subTest(workflow=filename, job=name):
                    self.assertEqual(len(steps), 1)
                    self.assertEqual(steps[0]["run"], TIMING.contract.sweep_script(all_targets=all_targets))
                    self.assertFalse(job.get("continue-on-error") or steps[0].get("continue-on-error"))

    def test_timing_skip_exception_requires_the_entire_exact_run_scalar(self):
        contract = TIMING.contract
        script = contract.sweep_script(all_targets=True)

        def yaml_block(body):
            return ("jobs:\n  example:\n    steps:\n      - name: Sweep\n        run: |\n" +
                    "".join("          " + line + "\n" for line in body.splitlines()))

        good = yaml_block(script)
        self.assertEqual(len(contract.verified_timing_skip_lines(good)), 1)
        for before, after in (("run_non_pg_filter_replay\n", ""),
                              ("run_non_pg_filter_replay\n", "run_non_pg_filter_replay\n" * 2),
                              (" serial -- ", " serial -- --list "),
                              ("python3 scripts/ci/timing_test_isolation.py serial", "true || python3 scripts/ci/timing_test_isolation.py serial"),
                              ("--test-threads=1", "--test-threads=2"),
                              ("--all-targets --", "--all-targets -- --skip foreign")):
            with self.subTest(mutation=(before, after)):
                changed = script.replace(before, after)
                self.assertNotEqual(changed, script)
                self.assertFalse(contract.verified_timing_skip_lines(yaml_block(changed)))
        self.assertEqual(contract.verified_timing_skip_lines(good +
            "        run: |\n          cargo test --lib -- --skip foreign\n"),
            contract.verified_timing_skip_lines(good))
        nested = "run: |\n" + "".join("  " + line + "\n" for line in script.splitlines())
        env_data = "jobs:\n  example:\n    env:\n      NOTE: |\n" + "".join(
            "        " + line + "\n" for line in nested.splitlines())
        heredoc = yaml_block("cat <<'PAYLOAD'\n" + nested + "PAYLOAD\n")
        self.assertFalse(contract.verified_timing_skip_lines(env_data))
        self.assertFalse(contract.verified_timing_skip_lines(heredoc))


if __name__ == "__main__":
    unittest.main()
