import re

ops_file = "src/server/routes/domains/ops.rs"
integrations_file = "src/server/routes/domains/integrations.rs"

with open(integrations_file, "r") as f:
    int_content = f.read()

# Remove hooks from imports in integrations
int_content = re.sub(r'github_dashboard, hooks,\n\s*meetings', 'github_dashboard,\n    meetings', int_content)

# Remove the routes from integrations
routes_to_remove = r"""\s*\.route\("/hook/reset-status", post\(hooks::reset_status\)\)
\s*\.route\("/hook/skill-usage", post\(hooks::skill_usage\)\)
\s*\.route\(
\s*"/hook/session/\{sessionKey\}",
\s*delete\(hooks::disconnect_session\),
\s*\)"""

int_content = re.sub(routes_to_remove, "", int_content)

with open(integrations_file, "w") as f:
    f.write(int_content)

with open(ops_file, "r") as f:
    ops_content = f.read()

# Add hooks to imports in ops
ops_content = re.sub(r'health_api, idle_recap,', 'health_api, hooks, idle_recap,', ops_content)

# Add the routes to ops
routes_to_add = """            .route("/hook/reset-status", post(hooks::reset_status))
            .route("/hook/skill-usage", post(hooks::skill_usage))
            .route(
                "/hook/session/{sessionKey}",
                delete(hooks::disconnect_session),
            )
"""

# Insert before claude-session-id
ops_content = ops_content.replace(
    '            .route(\n                "/dispatched-sessions/claude-session-id",',
    routes_to_add + '            .route(\n                "/dispatched-sessions/claude-session-id",'
)

with open(ops_file, "w") as f:
    f.write(ops_content)
