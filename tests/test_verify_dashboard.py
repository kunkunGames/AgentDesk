"""Behavior contracts for the dashboard verification security gate."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_VERIFY_SCRIPT = REPO_ROOT / "scripts/verify-dashboard.sh"


class DashboardAuditWaiverTests(unittest.TestCase):
    def test_stale_waiver_guard_runs_before_dashboard_build(self) -> None:
        script = DASHBOARD_VERIFY_SCRIPT.read_text(encoding="utf-8")
        audit_failure = script.index('if [ "$audit_status" -ne 0 ]; then')
        stale_waiver = script.index(
            'elif [ -n "${DASHBOARD_AUDIT_WAIVER:-}" ]; then'
        )
        dashboard_build = script.index('echo "==> Dashboard build"')

        self.assertLess(audit_failure, stale_waiver)
        self.assertLess(stale_waiver, dashboard_build)
        self.assertIn(
            "The waiver is stale and must be removed",
            script[stale_waiver:dashboard_build],
        )
        self.assertIn("exit 1", script[stale_waiver:dashboard_build])

    @unittest.skipUnless(os.name == "posix", "requires a POSIX shell fixture")
    def test_dashboard_audit_waiver_policy_matrix(self) -> None:
        cases = (
            {
                "name": "clean audit without waiver",
                "audit_status": 0,
                "waiver": None,
                "returncode": 0,
                "continues": True,
                "stderr": None,
            },
            {
                "name": "clean audit with stale waiver",
                "audit_status": 0,
                "waiver": "fixed upstream",
                "returncode": 1,
                "continues": False,
                "stderr": "The waiver is stale and must be removed",
            },
            {
                "name": "failed audit with documented waiver",
                "audit_status": 7,
                "waiver": "no fix available",
                "returncode": 0,
                "continues": True,
                "stderr": "WAIVED — reason: no fix available",
            },
            {
                "name": "failed audit without waiver",
                "audit_status": 7,
                "waiver": None,
                "returncode": 7,
                "continues": False,
                "stderr": "dashboard npm audit found high/critical advisories",
            },
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            fixture_root = Path(temp_dir)
            scripts_dir = fixture_root / "scripts"
            dashboard_dir = fixture_root / "dashboard"
            fake_bin = fixture_root / "bin"
            scripts_dir.mkdir()
            dashboard_dir.mkdir()
            fake_bin.mkdir()

            verifier = scripts_dir / "verify-dashboard.sh"
            verifier.write_text(
                DASHBOARD_VERIFY_SCRIPT.read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            verifier.chmod(0o755)
            (dashboard_dir / "package.json").write_text("{}\n", encoding="utf-8")
            (dashboard_dir / "package-lock.json").write_text(
                "{}\n", encoding="utf-8"
            )

            fake_node = fake_bin / "node"
            fake_node.write_text(
                "#!/usr/bin/env bash\n"
                "if [ \"${1:-}\" = \"-v\" ]; then echo v22.22.0; fi\n"
                "exit 0\n",
                encoding="utf-8",
            )
            fake_node.chmod(0o755)

            fake_npm = fake_bin / "npm"
            fake_npm.write_text(
                "#!/usr/bin/env bash\n"
                "printf '%s\\n' \"$*\" >> \"$NPM_LOG\"\n"
                "if [ \"${1:-}\" = \"audit\" ]; then\n"
                "  exit \"${FAKE_AUDIT_STATUS:-0}\"\n"
                "fi\n"
                "exit 0\n",
                encoding="utf-8",
            )
            fake_npm.chmod(0o755)

            npm_log = fixture_root / "npm.log"
            for case in cases:
                with self.subTest(case["name"]):
                    npm_log.unlink(missing_ok=True)
                    env = os.environ.copy()
                    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
                    env["NPM_LOG"] = str(npm_log)
                    env["FAKE_AUDIT_STATUS"] = str(case["audit_status"])
                    if case["waiver"] is None:
                        env.pop("DASHBOARD_AUDIT_WAIVER", None)
                    else:
                        env["DASHBOARD_AUDIT_WAIVER"] = str(case["waiver"])

                    result = subprocess.run(
                        ["bash", str(verifier)],
                        cwd=fixture_root,
                        env=env,
                        text=True,
                        capture_output=True,
                        check=False,
                    )
                    calls = npm_log.read_text(encoding="utf-8").splitlines()

                    self.assertEqual(result.returncode, case["returncode"])
                    self.assertEqual(
                        calls[:2],
                        ["ci --no-audit --no-fund", "audit --audit-level=high"],
                    )
                    self.assertEqual("run build" in calls, case["continues"])
                    self.assertEqual("test" in calls, case["continues"])
                    if case["stderr"] is not None:
                        self.assertIn(case["stderr"], result.stderr)


if __name__ == "__main__":
    unittest.main()
