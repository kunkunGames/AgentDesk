import unittest
from .client import audited_goal


class AuditedGoalTests(unittest.TestCase):
    def test_complete_revision_chain_resolves_current_goal(self):
        old, middle, current = ({"objective": text} for text in ["old", "middle", "current"])
        state = {"goal": current, "goal_revisions": [
            {"previous_goal": middle, "revised_goal": current},
            {"previous_goal": old, "revised_goal": middle}]}
        self.assertEqual(audited_goal(state, old), current)
        state["goal_revisions"].pop()
        with self.assertRaises(ValueError): audited_goal(state, old)

    def test_unchanged_goal_and_unaudited_drift(self):
        self.assertEqual(audited_goal({"goal": {"a": 1}}, {"a": 1}), {"a": 1})
        with self.assertRaises(ValueError): audited_goal({"goal": {"a": 2}}, {"a": 1})
