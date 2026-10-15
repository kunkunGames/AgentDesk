"""Counterexamples to accepting stub, stale or inflated operational evidence."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import activation_24x7_gate as gate
import operating_readiness_evidence as evidence


class ActivationEvidenceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="adk-readiness-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.artifacts = self.root / "target/24x7-native"
        self.artifacts.mkdir(parents=True)
        self.receipt_name = "target/24x7-native/OPERATING-READINESS-RESULT.json"
        self.receipt = {"receipt_ready": True, "operational_ready": False, "base_commit": "a" * 40,
                        "evidence_sha256": {}, "blocking_requirements": ["W3/E1 runtime supply and file consumption"],
                        "operating_probes": {}, "scope": {key: False for key in
                        ("real_model_calls", "production_deployment", "os_service_or_reboot_acceptance", "observed_24_hours")}}
        names = sorted(evidence.ROOT_INPUTS | {"src/lib.rs", "policies/check.js", "migrations/1.sql"})
        files = []
        for name in names:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture " + name)
            files.append({"path": name, "sha256": evidence.sha(path)})
        fingerprint = hashlib.sha256("".join(f"{x['path']}\0{x['sha256']}\n" for x in files).encode()).hexdigest()
        self.receipt.update(source_capture="source.json", source_fingerprint=fingerprint, input_files=len(files))
        self.pin("source.json", {"base_commit": "a" * 40, "files": files, "source_fingerprint": fingerprint})
        binary = self.root / "fixture-executable"
        binary.write_bytes(b"a captured executable fixture")
        self.binary_hash = evidence.sha(binary)
        build = {"exit_code": 0}
        smoke = {"version_exit": 0, "help_exit": 0}
        self.receipt["binary"] = {"path": "fixture-executable", "sha256": self.binary_hash,
                                  "build": build, "cli_smoke": smoke}
        self.pin("operations-current-build-exit.json", build)
        self.pin("operations-current-regression-exit.json", build)
        self.pin("operations-final-cli-smoke.json", smoke)
        log = "test fixture::case ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 5 filtered out; finished in 0.10s\n"
        self.pin("regression.log", log)
        self.receipt["rust"] = {"passed": 1, "failed": 0, "ignored": 0, "filtered_out": 5,
                                "test_seconds": 0.1, "log": "regression.log", "build_and_test": build,
                                "log_sha256": self.receipt["evidence_sha256"]["regression.log"]}
        for scenario, checks in evidence.SCENARIO_CHECKS.items():
            name = f"{scenario}/result.json"
            self.receipt["operating_probes"][scenario] = {"artifact": name}
            self.probe(name, checks)
        self.probe(evidence.HTTP_ARTIFACT, evidence.HTTP_CHECKS)
        self.save()

    def pin(self, name, value):
        path = self.artifacts / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value if isinstance(value, str) else json.dumps(value), encoding="utf-8")
        self.receipt["evidence_sha256"][name] = evidence.sha(path)

    def probe(self, name, checks):
        self.pin(name, {"passed": True, "binary_sha256": self.binary_hash, "checks": sorted(checks),
                        "boots": 2, "duration_seconds": 1.0})
        self.pin(str(Path(name).with_name("cleanup.json")).replace("\\", "/"), {"fixture_database_removed": True})

    def save(self):
        (self.root / self.receipt_name).write_text(json.dumps(self.receipt), encoding="utf-8")

    def mutate_probe(self, **changes):
        name = evidence.HTTP_ARTIFACT
        data = evidence.read_json(self.artifacts / name)
        data.update(changes)
        self.pin(name, data)
        self.save()

    def audit(self):
        return evidence.audit(self.root, self.receipt_name)

    def test_consistent_fixture_is_partial_and_not_e1(self):
        report = self.audit()
        self.assertTrue(report["evidence_valid"])
        self.assertFalse(report["operational_ready"])
        self.assertEqual(report["rust"]["passed"], 1)  # No magic 373 threshold.
        self.assertNotIn("fixture_a_to_b_handoff", report["confirmed"])
        self.assertTrue(report["journeys"]["E1"].startswith("not accepted"))

    def test_gate_exit_is_closed_and_audit_only_is_explicit(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(gate.main(["--repo-root", str(self.root)]), 2)
            self.assertEqual(gate.main(["--repo-root", str(self.root), "--audit-only"]), 0)

    def test_campaign_fixture_requires_restart_and_remains_partial(self):
        name = "campaign/result.json"
        self.receipt["campaign_probe"] = name
        self.probe(name, evidence.CAMPAIGN_CHECKS)
        self.save()
        with self.assertRaisesRegex(evidence.EvidenceError, "restart not observed"):
            self.audit()
        data = evidence.read_json(self.artifacts / name)
        data["boots"] = 3
        self.pin(name, data)
        self.save()
        report = self.audit()
        self.assertIn("protected_campaign_file_handoff_and_bounded_rework", report["confirmed"])
        self.assertTrue(report["journeys"]["E1"].startswith("partial:"))
        self.assertFalse(report["activated"])
        self.assertFalse(report["scope"]["real_model_calls"])

    def test_campaign_claim_cannot_reuse_unrelated_http_assertions(self):
        self.receipt["campaign_probe"] = evidence.HTTP_ARTIFACT
        self.mutate_probe(boots=3)
        with self.assertRaisesRegex(evidence.EvidenceError, "missing probe assertions"):
            self.audit()

    def test_missing_receipt_rejects_old_stub_counterexample(self):
        (self.root / self.receipt_name).unlink()
        with self.assertRaisesRegex(evidence.EvidenceError, "missing evidence file"):
            self.audit()

    def test_binary_replacement_is_rejected(self):
        (self.root / "fixture-executable").write_bytes(b"another build")
        with self.assertRaisesRegex(evidence.EvidenceError, "executable differs"):
            self.audit()

    def test_changed_source_is_rejected(self):
        (self.root / "src/lib.rs").write_text("new source")
        with self.assertRaisesRegex(evidence.EvidenceError, "source changed"):
            self.audit()

    def test_new_uncaptured_source_is_rejected(self):
        (self.root / "src/new.rs").write_text("uncaptured source")
        with self.assertRaisesRegex(evidence.EvidenceError, "omits current src"):
            self.audit()

    def test_tampered_log_is_rejected(self):
        (self.artifacts / "regression.log").write_text("373 passed; 0 failed")
        with self.assertRaisesRegex(evidence.EvidenceError, "evidence changed"):
            self.audit()

    def test_stub_pass_substring_is_not_a_test_result(self):
        self.pin("regression.log", "373 passed; 0 failed")
        self.receipt["rust"]["log_sha256"] = self.receipt["evidence_sha256"]["regression.log"]
        self.save()
        with self.assertRaisesRegex(evidence.EvidenceError, "successful test result"):
            self.audit()

    def test_inflated_test_count_is_rejected(self):
        self.receipt["rust"]["passed"] = 373
        self.save()
        with self.assertRaisesRegex(evidence.EvidenceError, "summary differs"):
            self.audit()

    def test_nonhex_digest_is_rejected(self):
        self.receipt["binary"]["sha256"] = "z" * 64
        self.save()
        with self.assertRaisesRegex(evidence.EvidenceError, "invalid SHA-256"):
            self.audit()

    def test_probe_from_different_binary_is_rejected(self):
        self.mutate_probe(binary_sha256="0" * 64)
        with self.assertRaisesRegex(evidence.EvidenceError, "another executable"):
            self.audit()

    def test_missing_assertions_is_not_passed(self):
        self.mutate_probe(checks=[])
        with self.assertRaisesRegex(evidence.EvidenceError, "missing probe assertions"):
            self.audit()

    def test_string_passed_is_not_boolean_true(self):
        self.mutate_probe(passed="true")
        with self.assertRaisesRegex(evidence.EvidenceError, "probe failed"):
            self.audit()

    def test_restart_evidence_is_required(self):
        self.mutate_probe(boots=1)
        with self.assertRaisesRegex(evidence.EvidenceError, "restart not observed"):
            self.audit()

    def test_unremoved_fixture_is_rejected(self):
        self.pin("managed-checks-operating-final/cleanup.json", {"fixture_database_removed": False})
        self.save()
        with self.assertRaisesRegex(evidence.EvidenceError, "cleanup unproven"):
            self.audit()

    def test_required_unpinned_file_is_rejected(self):
        del self.receipt["evidence_sha256"]["source.json"]
        self.save()
        with self.assertRaisesRegex(evidence.EvidenceError, "not pinned"):
            self.audit()

    def test_build_failure_is_rejected(self):
        self.pin("operations-current-build-exit.json", {"exit_code": 1})
        self.receipt["binary"]["build"] = {"exit_code": 1}
        self.save()
        with self.assertRaisesRegex(evidence.EvidenceError, "unsuccessful"):
            self.audit()

    def test_relative_path_escape_is_rejected(self):
        self.receipt["evidence_sha256"]["../outside"] = "0" * 64
        self.save()
        with self.assertRaisesRegex(evidence.EvidenceError, "non-relative"):
            self.audit()

    def test_source_fingerprint_mismatch_is_rejected(self):
        self.receipt["source_fingerprint"] = "0" * 64
        self.save()
        with self.assertRaisesRegex(evidence.EvidenceError, "fingerprint mismatch"):
            self.audit()

    def test_toggling_operational_claim_cannot_promote_fixture(self):
        self.receipt["scope"]["observed_24_hours"] = True
        self.save()
        with self.assertRaisesRegex(evidence.EvidenceError, "unsupported operational claim"):
            self.audit()

    def test_existing_report_is_preserved(self):
        output = self.root / "result.json"
        output.write_text("historical result")
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(gate.main(["--repo-root", str(self.root), "--output", str(output)]), 1)
        self.assertEqual(output.read_text(), "historical result")

    def test_python_optimized_mode_still_validates(self):
        (self.root / "fixture-executable").write_bytes(b"replacement")
        run = subprocess.run([sys.executable, "-O", gate.__file__, "--repo-root", str(self.root), "--audit-only"],
                             capture_output=True, text=True)
        self.assertEqual(run.returncode, 1)
        self.assertIn("executable differs", run.stderr)


if __name__ == "__main__":
    unittest.main()
