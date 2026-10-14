"""The six historically eligible routine goals and their preserved local schedules."""
SOURCES = {
    "skills": "a66c61eb-4654-43d0-99b8-b38e4c9e0b44",
    "upstream": "bd01d1c1-27bf-4965-a96f-0eb9e08276f6",
    "agentdesk-review": "8519cc7e-d0a1-46fb-877d-875e36c4fd08",
    "agentdesk-merge": "3e9d98b0-8851-4433-90b7-f9229505038a",
    "monolith": "95b23dc5-b045-4adc-9c58-e20b59304fbb",
    "health": "ce9860ff-86d8-4afa-8294-7d4b3ea6c8b2",
}
SLOTS = {"skills": [(3, 0)], "upstream": [(5, 0)],
         "agentdesk-review": [(8, 0)], "agentdesk-merge": [(13, 0)],
         "monolith": [(1, 30), (10, 30), (15, 30), (20, 30)],
         "health": [(h, m) for h in range(24) for m in (0, 30)]}


SCRIPT_REFS = {
    "skills": "skill-sync/daily.js", "upstream": "codex/agentdesk-upstream-sync.js",
    "agentdesk-review": "codex/agentdesk-pr-review.js",
    "agentdesk-merge": "codex/agentdesk-pr-merge-ready.js",
    "monolith": "gamer/monolith-pr-review.js", "health": "monitoring/working-watchdog.js",
}
OBJECTIVES = {
    "skills": "Keep installed skills synchronized and backed up; independently verify real mirror/regression/backup reports. Respect the existing daily duplicate guard and do not write memory candidates.",
    "upstream": "Integrate upstream changes without losing fork changes. Inspect and continue any preserved merge worktree, split conflict resolution and exact-head validation into bounded tasks, and independently verify the resulting artifact. No force-sync, migration rewrite, deployment or runtime DB changes.",
    "agentdesk-review": "Review at most one current AgentDesk PR, split actual review, focused remediation and independent exact-head verification into concrete tasks. No merge, branch deletion or unrequested comments. Keep infrastructure blockers distinct from test results.",
    "agentdesk-merge": "Continue AgentDesk PR work through independent current-head validation and merge only when all required checks/reviews are satisfied. Reuse an existing Campaign's review evidence only after verifying the same live head. No admin bypass or branch deletion.",
    "monolith": "Review and remediate at most one current monolith PR, independently verify actual current-head checks and merge eligible valuable changes without admin bypass or branch deletion.",
    "health": "Detect actual operational faults and stalled dispatches. Produce actionable incident tasks only for observed faults, with independent verification. A healthy system needs no heartbeat execution cards. No service restart or cancellation of unknown work.",
}
