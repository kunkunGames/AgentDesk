import copy
import datetime as dt
import json
import unittest

from .health import MAX_DISPATCHES, assess, collect, digest


class FakeApi:
    def __init__(self):
        self.paths = []
        self.row = {"channel_id": 123, "session_active_dispatch_id": "own", "session_agent_id": "coder",
                    "relay_stall_state": "active_foreground_stream", "watcher_attached": False,
                    "stall_shadow_verdict": "control_plane_desync", "session_key": "SECRET_SENTINEL"}
        self.detail = {"status": "healthy", "fully_recovered": True, "db": True, "disk_low": False,
                       "latest_startup_doctor": {"failed_count": 0, "warned_count": 2},
                       "mailboxes": [self.row], "active_session_audit": {"truncated": True},
                       "release_source": {"deployed_repo_head": "a" * 40,
                                          "declared_build_provenance": {"binary_sha256": "b" * 64, "source_manifest_sha256": "c" * 64}}}
        self.full = False

    def get(self, path, **query):
        self.paths.append((path, query))
        if path.endswith("/execution"):
            return {"enrolled": True, "tasks": [{"task_key": "current", "card_id": "c", "role": "work"}],
                    "failures": [{"raw": "SECRET_SENTINEL"}], "hold_reason": "retained old hold"}
        if path == "/api/dispatches":
            rows = [{"id": "own", "kanban_card_id": "c", "status": "dispatched", "context": {"token": "SECRET_SENTINEL"}}]
            return {"dispatches": rows * MAX_DISPATCHES if self.full else rows if query["status"] == "dispatched" else []}
        if path == "/api/health/detail":
            return self.detail
        raise AssertionError(path)


class NativeHealthEvidenceTests(unittest.TestCase):
    def snapshot(self, api=None, minute=0):
        return collect(api or FakeApi(), "campaign", "current", lambda: dt.datetime(2026, 10, 3, 0, minute, tzinfo=dt.timezone.utc))

    def test_self_owned_foreground_progress_and_shadow_disagreement_are_not_incidents(self):
        api = FakeApi()
        value = self.snapshot(api)
        self.assertTrue(value["mailboxes"][0]["observer_owned"])
        self.assertEqual(assess(value)["conclusion"], "no_current_fault_in_declared_native_scope")
        self.assertEqual(value["campaign"]["hold_reason"], "retained old hold")
        self.assertFalse(assess(value)["business_accepted"])
        self.assertNotIn("SECRET_SENTINEL", json.dumps(value))
        self.assertEqual(len(api.paths), 5)
        self.assertTrue(all(p.startswith("/api/") for p, _ in api.paths))

    def test_resolved_target_is_history_and_cannot_be_proposed_as_current_fault(self):
        api = FakeApi(); api.row["relay_stall_state"] = "tmux_alive_relay_dead"
        before = self.snapshot(api)
        self.assertEqual(assess(before)["conclusion"], "requires_recheck")
        api.row["relay_stall_state"] = "healthy"
        after = self.snapshot(api, 1)
        result = assess(after, before)
        self.assertEqual(result["conclusion"], "no_current_fault_in_declared_native_scope")
        self.assertEqual(len(result["resolved_observations"]), 1)
        self.assertEqual(result["confirmed_faults"], [])

    def test_real_self_owned_stall_is_not_exempted_and_requires_same_owner_confirmation(self):
        api = FakeApi(); api.row["relay_stall_state"] = "tmux_alive_relay_dead"
        before, after = self.snapshot(api), self.snapshot(api, 1)
        self.assertEqual(assess(after, before)["conclusion"], "observed_fault")
        api.row["session_active_dispatch_id"] = "successor"
        successor = self.snapshot(api, 2)
        self.assertEqual(assess(successor, before)["conclusion"], "requires_recheck")

    def test_actual_nonterminal_inventory_truncation_blocks_complete_coverage(self):
        api = FakeApi(); api.full = True
        self.assertIn("nonterminal_dispatch_inventory_incomplete", assess(self.snapshot(api))["coverage_gates"])

    def test_scope_time_identity_or_artifact_changes_cannot_confirm_a_fault(self):
        api = FakeApi(); api.detail["db"] = False
        before = self.snapshot(api)
        self.assertEqual(assess(before, before)["conclusion"], "blocked")
        after = self.snapshot(api, 1); after["release"]["binary_sha256"] = "changed"
        after["evidence_digest"] = digest({k:v for k,v in after.items() if k != "evidence_digest"})
        self.assertEqual(assess(after, before)["conclusion"], "blocked")
        tampered = copy.deepcopy(before); tampered["core"]["db"] = True
        with self.assertRaises(ValueError): assess(tampered)

    def test_unknown_primary_classifier_or_missing_core_evidence_stays_blocked(self):
        api = FakeApi(); api.row["relay_stall_state"] = None
        self.assertEqual(assess(self.snapshot(api))["conclusion"], "blocked")
        api.row["relay_stall_state"] = "healthy"; api.detail.pop("db")
        self.assertEqual(assess(self.snapshot(api))["conclusion"], "blocked")
        api = FakeApi(); api.detail.pop("release_source")
        self.assertIn("required_deployment_identity_missing", assess(self.snapshot(api))["coverage_gates"])
