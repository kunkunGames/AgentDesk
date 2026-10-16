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
                      "budget_enforcement": {"scope": "campaign_lifetime", "published_tasks": 1},
                      "metrics": {"schema_version": 1, "time_basis": "dispatch_lifetime",
                                  "roles": [{"role": "work", "status": "completed", "dispatches": 1}]},
                      "elapsed_dispatch_seconds": 1150.8,
                      "tasks": [{"card_id": "card", "role": "work", "version": 2,
                                 "task_key": "inspect", "observation": {"dispatch_id": "dispatch"}}],
                      "versions": [{"version": 2, "failure_id": "failure", "graph": {
                          "nodes": [{"task_key": "inspect", "effect": "artifact_only"}]}}]}
        self.dispatches = [{"id": "dispatch", "card": "card", "status": "completed",
                            "dispatch_type": "implementation"}]

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
        self.state["tasks"][0]["observation"] = {"dispatch_id": "dispatch"}
        self.dispatches.clear()
        with self.assertRaises(ValueError): self.prepare()

    def test_exhausted_budget_and_periodic_parent_are_not_reset(self):
        self.state["elapsed_dispatch_seconds"] = 10800
        with self.assertRaises(ValueError): self.prepare()
        self.state["elapsed_dispatch_seconds"] = 0
        self.goal["schedule"] = {"daily_minutes": [180]}
        with self.assertRaises(ValueError): self.prepare()

    def test_omitted_attempts_are_rejected_even_when_every_card_is_present(self):
        for status in ("completed", "failed", "cancelled", "running"):
            with self.subTest(status=status):
                self.state["metrics"]["roles"] = [
                    {"role": "work", "status": "completed", "dispatches": 2}
                ] if status == "completed" else [
                    {"role": "work", "status": "completed", "dispatches": 1},
                    {"role": "work", "status": status, "dispatches": 1},
                ]
                with self.assertRaisesRegex(ValueError, "dispatch-attempt accounting"):
                    self.prepare()

    def test_latest_observation_cannot_attest_a_previous_completed_attempt(self):
        self.state["metrics"]["roles"][0]["dispatches"] = 2
        self.dispatches.append({"id": "previous", "card": "card", "status": "completed",
                                "dispatch_type": "rework"})
        with self.assertRaisesRegex(ValueError, "own canonical captured observation"):
            self.prepare()

    def test_missing_or_inconsistent_publication_accounting_is_rejected(self):
        for published in (None, True, -1, 0, 2, 1.0):
            with self.subTest(published=published):
                self.state["budget_enforcement"]["published_tasks"] = published
                with self.assertRaisesRegex(ValueError, "published-task accounting"):
                    self.prepare()
        self.state["budget_enforcement"]["published_tasks"] = 2
        self.state["tasks"].append(copy.deepcopy(self.state["tasks"][0]))
        with self.assertRaisesRegex(ValueError, "published-task accounting"):
            self.prepare()

    def test_missing_malformed_or_duplicate_attempt_metrics_are_rejected(self):
        original = copy.deepcopy(self.state["metrics"])
        for replacement in ({}, {**original, "schema_version": True},
                            {**original, "time_basis": "task_lifetime"}):
            self.state["metrics"] = replacement
            with self.assertRaisesRegex(ValueError, "dispatch-lifetime metrics"):
                self.prepare()
        for count in (0, -1, True, 1.0, 2**63):
            self.state["metrics"] = copy.deepcopy(original)
            self.state["metrics"]["roles"][0]["dispatches"] = count
            with self.assertRaisesRegex(ValueError, "dispatch group"):
                self.prepare()
        self.state["metrics"] = copy.deepcopy(original)
        self.state["metrics"]["roles"] *= 2
        with self.assertRaisesRegex(ValueError, "dispatch group"):
            self.prepare()

    def test_distinct_published_tasks_use_the_native_publication_budget(self):
        task = copy.deepcopy(self.state["tasks"][0])
        task.update(card_id="other-card", observation={"dispatch_id": "other-dispatch"})
        self.state["tasks"].append(task)
        self.state["budget_enforcement"]["published_tasks"] = 2
        self.state["metrics"]["roles"][0]["dispatches"] = 2
        self.dispatches.append({"id": "other-dispatch", "card": "other-card", "status": "completed",
                                "dispatch_type": "implementation"})
        self.assertEqual(self.prepare()["budget"]["max_dispatches"], 18)

    def test_other_dispatch_scopes_and_boolean_tool_usage_are_rejected(self):
        for dispatch_type in (None, "review", ""):
            self.dispatches[0]["dispatch_type"] = dispatch_type
            with self.assertRaisesRegex(ValueError, "Implementation/rework"):
                self.prepare()
        with self.assertRaisesRegex(ValueError, "Verified provider tool usage"):
            successor_goal(self.c, self.state, self.dispatches, self.goal,
                           successor_id="new", verified_tool_calls=True)


if __name__ == "__main__":
    unittest.main()
