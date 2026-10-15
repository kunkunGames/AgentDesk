"""Clock, gaps, identity and real business holds remain distinct evidence."""
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from campaign_ops.observation import assess, collect


class ObservationTests(unittest.TestCase):
    def samples(self):
        start = datetime(2026, 10, 1, tzinfo=timezone.utc)
        return [{"observed_at": (start+timedelta(minutes=5*i)).isoformat(),
                 "available": True, "binary_sha256": "installed",
                 "health": {"status":"healthy", "fully_recovered":True,
                            "doctor_failed":0, "declared_binary_sha256":"installed"},
                 "campaign": {"hold_reason":None}, "occurrences":[]}
                for i in range(289)]

    def test_full_day_is_elapsed_and_healthy_without_claiming_business_acceptance(self):
        report = assess(self.samples())
        self.assertTrue(report["elapsed_24_hours"])
        self.assertTrue(report["continuous_healthy"])
        self.assertFalse(report["full_v5_acceptance"])

    def test_gap_outage_and_doctor_failure_each_prevent_continuity(self):
        for mutation in ("gap", "outage", "doctor"):
            samples = self.samples()
            if mutation == "gap": del samples[10:15]
            elif mutation == "outage": samples[12] = {"observed_at":samples[12]["observed_at"],"available":False}
            else: samples[12]["health"]["doctor_failed"] = 1
            self.assertFalse(assess(samples)["continuous_healthy"],mutation)

    def test_binary_change_or_fabricated_provenance_prevents_continuity(self):
        for field in ("binary_sha256", "declared_binary_sha256"):
            samples = self.samples()
            target = samples[10] if field == "binary_sha256" else samples[10]["health"]
            target[field] = "different"
            self.assertFalse(assess(samples)["deployment_identity_preserved"])

    def test_clock_regression_or_unknown_timezone_cannot_claim_continuity(self):
        samples = self.samples()
        samples[10]["observed_at"] = samples[9]["observed_at"]
        self.assertFalse(assess(samples)["continuous_healthy"])
        samples[10]["observed_at"] = "2026-10-01T00:50:00"
        with self.assertRaises(ValueError): assess(samples)

    def test_business_hold_is_reported_separately_from_server_health(self):
        samples = self.samples()
        samples[20]["occurrences"] = [{"hold_reason":"Unreconciled outcome"}]
        report = assess(samples)
        self.assertTrue(report["continuous_healthy"])
        self.assertEqual(report["held_observation_count"],1)
        self.assertFalse(report["full_v5_acceptance"])

    def test_collection_uses_only_gets_and_drops_provider_and_authentication_content(self):
        class Api:
            def __init__(self): self.paths = []
            def get(self, path):
                self.paths.append(path)
                if path == "/api/health":
                    return {"status":"healthy","fully_recovered":True,
                            "latest_startup_doctor":{"failed_count":0},
                            "authentication":"SECRET_SENTINEL"}
                if path.endswith("/execution"):
                    return {"enrolled":True,"goal":{"budget":{}},
                            "dag_learning":{"sample_count":3,"min_samples":10},
                            "tasks":[{"provider_text":"SECRET_SENTINEL"}]}
                return {"campaign":{"status":"active","revision":1,"description":"SECRET_SENTINEL"}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/"bin").mkdir()
            (root/"bin/agentdesk").write_bytes(b"fixture-binary")
            api = Api()
            result = collect(api,root,"campaign")
            self.assertTrue(result["available"])
            self.assertEqual(api.paths,["/api/health","/api/campaigns/campaign","/api/campaigns/campaign/execution"])
            self.assertEqual(result["binary_sha256"],hashlib.sha256(b"fixture-binary").hexdigest())
            self.assertNotIn("SECRET_SENTINEL",str(result))

    def test_transport_error_preserves_unavailability_without_storing_cli_text(self):
        class Api:
            def get(self, path): raise RuntimeError("SECRET_SENTINEL")
        result = collect(Api(),Path("unused"),"campaign")
        self.assertFalse(result["available"])
        self.assertNotIn("SECRET_SENTINEL",str(result))


if __name__ == "__main__": unittest.main()
