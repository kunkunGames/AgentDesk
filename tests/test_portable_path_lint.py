from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check-portable-paths.py"


class PortablePathLintTests(unittest.TestCase):
    def test_default_deployable_surfaces_have_no_specific_user_home_literals(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--root", str(REPO_ROOT)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_specific_user_home_literal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.sh"
            path.write_text("echo /Users/itismyfield/.adk/release\n", encoding="utf-8")

            result = subprocess.run(
                [sys.executable, str(SCRIPT), "--root", tmp, str(path)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("/Users/itismyfield", result.stderr)

    def test_default_scan_includes_release_scripts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            scripts_dir = Path(tmp) / "scripts"
            scripts_dir.mkdir()
            deploy_release = scripts_dir / "deploy-release.sh"
            deploy_release.write_text("echo portable\n", encoding="utf-8")
            deploy_dashboard = scripts_dir / "deploy-dashboard.sh"
            deploy_dashboard.write_text("echo /Users/itismyfield/.adk/release\n", encoding="utf-8")

            result = subprocess.run(
                [sys.executable, str(SCRIPT), "--root", tmp],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("scripts/deploy-dashboard.sh", result.stderr)

    def test_default_scan_includes_release_helpers_and_policies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            scripts_dir = Path(tmp) / "scripts"
            scripts_dir.mkdir()
            bad_script_paths = [
                scripts_dir / "_defaults.sh",
                scripts_dir / "deploy.sh",
                scripts_dir / "ensure-agentdesk-cli.sh",
                scripts_dir / "queue-stability-batch.sh",
            ]
            for path in bad_script_paths:
                path.write_text("echo /Users/itismyfield/.adk/release\n", encoding="utf-8")

            policies_dir = Path(tmp) / "policies" / "lib"
            policies_dir.mkdir(parents=True)
            policy_path = policies_dir / "portable.js"
            policy_path.write_text("const path = '/Users/itismyfield/.adk/release';\n", encoding="utf-8")

            result = subprocess.run(
                [sys.executable, str(SCRIPT), "--root", tmp],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("scripts/_defaults.sh", result.stderr)
        self.assertIn("scripts/deploy.sh", result.stderr)
        self.assertIn("scripts/ensure-agentdesk-cli.sh", result.stderr)
        self.assertIn("scripts/queue-stability-batch.sh", result.stderr)
        self.assertIn("policies/lib/portable.js", result.stderr)

    def test_placeholder_home_literals_are_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "example.md"
            path.write_text(
                "\n".join(
                    [
                        "/Users/REPLACE_ME/.adk/release",
                        "/Users/user/.adk/release",
                        "/Users/me/.adk/release",
                        "/Users/example/.adk/release",
                    ]
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [sys.executable, str(SCRIPT), "--root", tmp, str(path)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_placeholder_prefix_with_punctuation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.md"
            path.write_text(
                "\n".join(
                    [
                        "/Users/user-name/.adk/release",
                        "/Users/me.dev/.adk/release",
                        "/Users/example-prod/.adk/release",
                    ]
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [sys.executable, str(SCRIPT), "--root", tmp, str(path)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("/Users/user-name", result.stderr)
        self.assertIn("/Users/me.dev", result.stderr)
        self.assertIn("/Users/example-prod", result.stderr)

    def test_rust_env_home_gate_detects_macro_whitespace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.rs"
            path.write_text(
                'const A: &str = env!( "HOME" );\nconst B: &str = env! /* gap */ ("HOME");\n',
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--root",
                    tmp,
                    "--rust-env-home-only",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("bad.rs:1", result.stderr)
        self.assertIn("bad.rs:2", result.stderr)

    def test_rust_env_home_gate_ignores_comments_and_string_fixtures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "safe.rs"
            path.write_text(
                "\n".join(
                    [
                        '// env!("HOME")',
                        '/* env!( "HOME" ) */',
                        'const EXAMPLE: &str = r#"env!("HOME")"#;',
                        'const OTHER: &str = env!("CARGO_PKG_VERSION");',
                    ]
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--root",
                    tmp,
                    "--rust-env-home-only",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_rust_env_home_gate_rejects_raw_home_literal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad_raw.rs"
            path.write_text('const HOME: &str = env!(r#"HOME"#);\n', encoding="utf-8")

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--root",
                    tmp,
                    "--rust-env-home-only",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("bad_raw.rs:1", result.stderr)


if __name__ == "__main__":
    unittest.main()
