"""Authenticated CLI transport; credentials remain owned by AgentDesk."""
import json
import subprocess
from urllib.parse import urlencode


class Api:
    def __init__(self, root):
        self.root = root

    def call(self, method, path, body=None):
        command = [str(self.root / "bin/agentdesk"), "--json", "api", method, path]
        if body is not None:
            command.append(json.dumps(body, ensure_ascii=False))
        result = subprocess.run(command, cwd=self.root, capture_output=True,
                                text=True, timeout=60)
        if result.returncode:
            raise RuntimeError(f"{method} {path}: {result.stderr[-1500:]}")
        data = json.loads(result.stdout)
        if data.get("error"):
            raise RuntimeError(f"{method} {path}: {data['error']}")
        return data

    def get(self, path, **query):
        return self.call("GET", path + ("?" + urlencode(query) if query else ""))

    def campaigns(self):
        result = []
        while True:
            page = self.get("/api/campaigns", limit=500, offset=len(result))["campaigns"]
            result.extend(page)
            if len(page) < 500:
                return result

    def save(self, campaign):
        body = {k: campaign[k] for k in ("title", "description", "status", "round", "nodes")}
        body["expected_revision"] = campaign["revision"]
        return self.call("PUT", "/api/campaigns/" + campaign["id"], body)["campaign"]


def object_value(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


def audited_goal(state, enrolled_goal):
    """Follow explicit operator revisions, refusing unaudited contract drift."""
    current = enrolled_goal
    for revision in reversed(state.get("goal_revisions", [])):
        if revision["previous_goal"] != current:
            raise ValueError("Goal revision history is incomplete or diverges from enrollment")
        current = revision["revised_goal"]
    if current != state["goal"]:
        raise ValueError("Live goal differs without a complete operator revision history")
    return current
