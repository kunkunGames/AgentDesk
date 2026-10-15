import copy
from pathlib import Path
import unittest
from .enrollment import enroll, goal
from .workflows import SOURCES


class ApiFixture:
    def __init__(self):
        self.rows = [{"id": value, "status": "paused", "in_flight_run_id": None} for value in SOURCES.values()]
        self.rows.append({"id": "historically-paused", "status": "paused"})
        self.docs = {}
        self.states = {}
        self.writes = []

    def campaigns(self):
        return list(self.docs.values())

    def get(self, path):
        if path == "/api/routines":
            return {"routines": copy.deepcopy(self.rows)}
        if path == "/api/settings/runtime-config":
            return {"current": {"internalTasksEnabled": True}}
        return copy.deepcopy(self.states.get(path, {"enrolled": False}))

    def call(self, method, path, body):
        self.writes.append((method, path, copy.deepcopy(body)))
        if path == "/api/campaigns":
            self.docs[body["id"]] = dict(body, revision=1)
            return {"campaign": self.docs[body["id"]]}
        self.states[path] = {"enrolled": True, "goal": copy.deepcopy(body["goal"])}
        return {"enrolled": True}


class EnrollmentTests(unittest.TestCase):
    root = Path("/Users/kunkun/.adk/release")

    def test_skills_keeps_configured_mirrors_without_pr_helper_restriction(self):
        skills = goal("skills", self.root)
        self.assertIn("mirror_targets.json", skills["objective"])
        self.assertNotIn("routine-gh.py", skills["objective"])
        self.assertIn("do not add repositories", skills["objective"])
        self.assertTrue(any("Every configured skill mirror" in s for s in skills["success"]))
        self.assertTrue(any("all configured mirror targets" in s for s in skills["constraints"]))
        self.assertTrue(any("source routine's --workspace argument" in s for s in skills["constraints"]))
        self.assertTrue(any("without changing cwd or adding destinations" in s for s in skills["constraints"]))
        for job in ("agentdesk-review", "agentdesk-merge", "monolith"):
            self.assertIn("routine-gh.py", goal(job, self.root)["objective"])
        self.assertEqual(skills["schedule"]["daily_minutes"], [180])

    def test_six_eligible_goals_are_idempotent_and_do_not_create_execution_cards(self):
        api = ApiFixture()
        before = copy.deepcopy(api.rows)
        result = enroll(api, self.root, SOURCES, apply=True)
        self.assertEqual(len(result), 6)
        self.assertTrue(all(state["goal"]["budget"]["max_evolutions"] == 8 for state in api.states.values()))
        self.assertEqual(len(api.writes), 12)
        self.assertTrue(all(path.startswith("/api/campaigns") for _, path, _ in api.writes))
        self.assertTrue(all(not doc["nodes"] for doc in api.docs.values()))
        self.assertEqual(api.rows, before)
        self.assertTrue(all(r["status"] == "already_enrolled" for r in enroll(api, self.root, SOURCES, apply=True)))
        self.assertEqual(len(api.writes), 12)

    def test_unknown_historical_or_running_source_cannot_be_admitted(self):
        api = ApiFixture()
        with self.assertRaises(ValueError):
            enroll(api, self.root, ["historically-paused"], apply=True)
        api.rows[0]["in_flight_run_id"] = "owned-running-work"
        with self.assertRaises(ValueError):
            enroll(api, self.root, SOURCES, apply=True)
        self.assertEqual(api.writes, [])

    def test_contract_drift_is_not_overwritten(self):
        api = ApiFixture()
        enroll(api, self.root, ["skills"], apply=True)
        api.states["/api/campaigns/ops-goal-skills/execution"]["goal"]["objective"] = "Another operator goal"
        with self.assertRaises(ValueError):
            enroll(api, self.root, ["skills"], apply=True)
        self.assertEqual(len(api.writes), 2)


if __name__ == "__main__":
    unittest.main()
