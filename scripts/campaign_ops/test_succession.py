import copy
import unittest
from .succession import successor_goal


class SuccessionTests(unittest.TestCase):
    def setUp(self):
        self.c = {"id": "old", "status": "paused"}
        self.goal = {"repo": "owner/repo", "agents": ["coder", "qa"], "constraints": [],
                     "budget": {"max_dispatches": 20, "max_elapsed_secs": 10800,
                                "max_evolutions": 2, "max_observed_tool_calls": 300,
                                "node_timeout_secs": 1800}}
        self.state = {"enrolled": True, "goal": self.goal, "observed_tool_calls": 0,
                      "elapsed_dispatch_seconds": 1150.8,
                      "tasks": [{"card_id": "card", "role": "work", "version": 2,
                                 "task_key": "inspect", "observation": {"captured": True}}],
                      "versions": [{"version": 2, "failure_id": "failure", "graph": {
                          "nodes": [{"task_key": "inspect", "effect": "artifact_only"}]}}]}
        self.dispatches = [{"id": "dispatch", "card": "card", "status": "completed"}]

    def prepare(self):
        return successor_goal(self.c, self.state, self.dispatches, self.goal,
                              successor_id="new", verified_tool_calls=42)

    def test_remaining_budget_and_history_are_preserved_without_mutation(self):
        original = copy.deepcopy(self.state)
        result = self.prepare()
        self.assertEqual(result["budget"], dict(self.goal["budget"], max_dispatches=19,
                         max_elapsed_secs=9649, max_evolutions=1, max_observed_tool_calls=258))
        self.assertIn("/api/campaigns/old/execution", result["constraints"][0])
        self.assertEqual(self.state, original)

    def test_external_effect_cannot_be_replayed_by_scope_revision(self):
        self.state["versions"][0]["graph"]["nodes"][0]["effect"] = "external"
        with self.assertRaisesRegex(ValueError, "remain pinned"):
            self.prepare()

    def test_inflight_missing_observation_and_unaccounted_task_are_rejected(self):
        self.dispatches[0]["status"] = "running"
        with self.assertRaises(ValueError): self.prepare()
        self.dispatches[0]["status"] = "completed"
        self.state["tasks"][0]["observation"] = None
        with self.assertRaises(ValueError): self.prepare()
        self.state["tasks"][0]["observation"] = {"captured": True}
        self.dispatches.clear()
        with self.assertRaises(ValueError): self.prepare()

    def test_exhausted_budget_and_periodic_parent_are_not_reset(self):
        self.state["elapsed_dispatch_seconds"] = 10800
        with self.assertRaises(ValueError): self.prepare()
        self.state["elapsed_dispatch_seconds"] = 0
        self.goal["schedule"] = {"daily_minutes": [180]}
        with self.assertRaises(ValueError): self.prepare()


if __name__ == "__main__":
    unittest.main()
