//! Narrow resolved native permissions for an explicit per-session allowlist.
//!
//! AgentDesk does not reimplement OpenCode's path/command pattern evaluator.
//! Native deny/ask rules and their order are preserved for allowed categories.

use serde_json::{Value, json};
use std::collections::BTreeSet;

pub(crate) const DEFAULT_ALLOWED_TOOLS: &[&str] = &[
    "bash",
    "read",
    "edit",
    "glob",
    "grep",
    "list",
    "task",
    "webfetch",
    "websearch",
    "skill",
    "todowrite",
    "todoread",
    "question",
    "lsp",
];
const PROBED_VERSION: &str = "1.14.41";
// These native controls gate an allowed tool; they are not tools themselves.
// Preserve explicit operator rules instead of replacing them with deny-all.
const NATIVE_CONTROL_PERMISSIONS: &[&str] = &["external_directory", "doom_loop"];

pub(super) struct PreparedPermissions {
    pub(super) agent: String,
    pub(super) rules: Value,
    pub(super) config: Value,
}

pub(super) fn prepare(
    base_url: &str,
    auth: &str,
    tools: Option<&[String]>,
    managed: bool,
) -> Result<Option<PreparedPermissions>, String> {
    let Some(categories) = allowed_categories(tools)? else {
        return Ok(None);
    };
    let get = |path: &str| -> Result<Value, String> {
        super::session_request_agent()
            .get(&format!("{base_url}{path}"))
            .set("Authorization", auth)
            .call()
            .map_err(|error| {
                format!("OpenCode permission capability probe {path} failed: {error}")
            })?
            .into_json()
            .map_err(|_| format!("OpenCode permission capability response {path} is invalid"))
    };
    validate_runtime(&get("/global/health")?, managed)?;
    let config = get("/config")?;
    let agent = match config.get("default_agent") {
        None => "build",
        Some(Value::String(name)) if !name.is_empty() => name,
        _ => return Err("OpenCode default agent capability is invalid".into()),
    };
    let agents = get("/agent")?;
    let effective_rules = agents
        .as_array()
        .and_then(|agents| agents.iter().find(|candidate| candidate["name"] == agent))
        .and_then(|agent| agent.get("permission"))
        .ok_or("OpenCode effective agent permissions are unavailable")?;
    Ok(Some(PreparedPermissions {
        agent: agent.to_string(),
        rules: intersect_rules(&categories, effective_rules, managed)?,
        config,
    }))
}

fn allowed_categories(tools: Option<&[String]>) -> Result<Option<BTreeSet<&str>>, String> {
    let Some(tools) = tools else { return Ok(None) };
    let mut categories = BTreeSet::new();
    for tool in tools {
        let category = match tool.trim() {
            "Bash" => "bash",
            "Read" => "read",
            "Glob" => "glob",
            "Grep" => "grep",
            "Task" => "task",
            "WebFetch" => "webfetch",
            "WebSearch" => "websearch",
            "Skill" => "skill",
            native if DEFAULT_ALLOWED_TOOLS.contains(&native) => native,
            other => {
                return Err(format!(
                    "Unsupported OpenCode allowed_tools entry {other:?}; configure native permission categories (for example read or edit), not provider-specific tools or command patterns"
                ));
            }
        };
        categories.insert(category);
    }
    Ok(Some(categories))
}

fn intersect_rules(
    categories: &BTreeSet<&str>,
    effective: &Value,
    managed: bool,
) -> Result<Value, String> {
    let effective = effective
        .as_array()
        .ok_or("OpenCode effective permission rules are invalid")?;
    let mut rules = vec![json!({"permission": "*", "pattern": "*", "action": "deny"})];
    for rule in effective {
        let permission = rule
            .get("permission")
            .and_then(Value::as_str)
            .ok_or("OpenCode permission name is missing")?;
        if rule.get("pattern").and_then(Value::as_str).is_none()
            || !matches!(
                rule.get("action").and_then(Value::as_str),
                Some("allow" | "ask" | "deny")
            )
        {
            return Err("OpenCode permission pattern/action is unsupported".into());
        }
        if NATIVE_CONTROL_PERMISSIONS.contains(&permission) {
            // The managed v1 contract admits only pre-authorized read/edit.
            // Its external access and loop controls remain denied by the mask.
            if !managed {
                rules.push(rule.clone());
            }
            continue;
        }
        if !managed && permission == "*" && rule["action"] != "allow" {
            // Later wildcard denies/asks also constrain explicit auxiliary
            // grants. A wildcard allow must never create such a grant.
            for control in NATIVE_CONTROL_PERMISSIONS {
                let mut projected = rule.clone();
                projected["permission"] = json!(control);
                rules.push(projected);
            }
        }
        // Keep path/command patterns native. Unknown category glob semantics
        // must not accidentally bypass a profile deny by being ignored.
        if permission != "*" && permission.contains(['*', '?', '[', ']']) {
            // Disabled MCPs produce native category globs such as `context7*`.
            // A literal prefix disjoint from every allowed built-in category
            // cannot grant or deny that category. Keep the leading deny-all
            // for those tools; never interpret an overlapping glob ourselves.
            let prefix = permission.split(['*', '?', '[', ']']).next().unwrap_or("");
            let literal_prefix = !prefix.is_empty()
                && prefix
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
            if literal_prefix
                && categories
                    .iter()
                    .copied()
                    .chain(NATIVE_CONTROL_PERMISSIONS.iter().copied())
                    .all(|category| !category.starts_with(prefix))
            {
                continue;
            }
            return Err(
                "OpenCode permission category glob overlaps the allowlist or is unsupported".into(),
            );
        }
        for category in categories {
            if permission == "*" || permission == *category {
                let mut projected = rule.clone();
                projected["permission"] = json!(category);
                rules.push(projected);
            }
        }
    }
    Ok(Value::Array(rules))
}

fn validate_runtime(health: &Value, managed: bool) -> Result<(), String> {
    let version = health.get("version").and_then(Value::as_str);
    if health.get("healthy").and_then(Value::as_bool) != Some(true)
        || !version.is_some_and(|version| !version.trim().is_empty())
    {
        return Err("OpenCode permission capability requires a healthy, identified runtime".into());
    }
    // The managed budget/termination protocol is certified for this exact
    // runtime. Ordinary native permissions are checked by their API schema and
    // session binding below; they must not inherit the managed binary pin.
    if managed && version != Some(PROBED_VERSION) {
        return Err(format!(
            "Managed OpenCode execution requires the probed version {PROBED_VERSION}; runtime capability is unsupported"
        ));
    }
    Ok(())
}

pub(super) fn validate_session(response: &Value, expected: Option<&Value>) -> Result<(), String> {
    if let Some(expected) = expected
        && response.get("permission") != Some(expected)
    {
        return Err(
            "OpenCode did not bind the requested native session permissions; prompt was not sent"
                .into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_permission_preparation_binds_the_actual_default_agent() {
        let rules = json!([
            {"permission": "*", "pattern": "*", "action": "allow"},
            {"permission": "read", "pattern": "*.env", "action": "deny"}
        ]);
        let (origin, server) = super::super::completion_tests::fixture(vec![
            (
                "GET /global/health ",
                200,
                json!({"healthy": true, "version": "1.14.41"}).to_string(),
            ),
            (
                "GET /config ",
                200,
                json!({"default_agent": "restricted"}).to_string(),
            ),
            (
                "GET /agent ",
                200,
                json!([{"name": "restricted", "permission": rules}]).to_string(),
            ),
        ]);
        let tools = ["read".into()];
        let prepared = prepare(&origin, "", Some(&tools), false).unwrap().unwrap();
        assert_eq!(prepared.agent, "restricted");
        assert_eq!(
            prepared.rules.as_array().unwrap().last().unwrap()["action"],
            "deny"
        );
        server.join().unwrap();
    }

    #[test]
    fn ordinary_permissions_accept_another_runtime_with_the_same_native_contract() {
        let (origin, server) = super::super::completion_tests::fixture(vec![
            (
                "GET /global/health ",
                200,
                json!({"healthy": true, "version": "1.14.42"}).to_string(),
            ),
            ("GET /config ", 200, "{}".into()),
            (
                "GET /agent ",
                200,
                json!([{"name":"build","permission":[
                    {"permission":"*","pattern":"*","action":"allow"},
                    {"permission":"read","pattern":"*.secret","action":"deny"}
                ]}])
                .to_string(),
            ),
        ]);
        let prepared = prepare(&origin, "", Some(&["read".into()]), false)
            .unwrap()
            .unwrap();
        assert_eq!(prepared.rules[0]["action"], "deny");
        assert_eq!(prepared.rules[1]["permission"], "read");
        assert_eq!(prepared.rules[2]["pattern"], "*.secret");
        assert_eq!(prepared.rules[2]["action"], "deny");
        server.join().unwrap();
    }

    #[test]
    fn managed_permissions_reject_an_unprobed_version_before_config_or_session() {
        let (origin, server) = super::super::completion_tests::fixture(vec![(
            "GET /global/health ",
            200,
            json!({"healthy": true, "version": "1.14.42"}).to_string(),
        )]);
        let error = prepare(&origin, "", Some(&["read".into()]), true)
            .err()
            .expect("managed binary pin must reject before any additional request");
        assert!(error.contains(PROBED_VERSION));
        server.join().unwrap();
    }

    #[test]
    fn ignored_session_permissions_abort_before_prompt_or_init() {
        let health = json!({"healthy": true, "version": "1.14.41"}).to_string();
        let rules = json!([{"permission": "*", "pattern": "*", "action": "allow"}]);
        let (origin, server) = super::super::completion_tests::fixture(vec![
            ("GET /global/health ", 200, health.clone()),
            ("GET /global/health ", 200, health),
            ("GET /config ", 200, "{}".into()),
            (
                "GET /agent ",
                200,
                json!([{"name": "build", "permission": rules}]).to_string(),
            ),
            ("POST /session ", 200, json!({"id": "active"}).to_string()),
            ("POST /session/active/abort ", 200, "true".into()),
        ]);
        let (sender, receiver) = std::sync::mpsc::channel();
        let error = super::super::run_session(
            "must not send",
            None,
            Some(&["read".into()]),
            None,
            &origin,
            "",
            &sender,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(error.contains("did not bind"));
        assert!(receiver.try_iter().next().is_none());
        server.join().unwrap();
    }

    #[test]
    fn readonly_rules_deny_shell_subagents_mcp_and_edits_by_default() {
        let tools = ["Read".into(), "read".into()];
        let categories = allowed_categories(Some(&tools)).unwrap().unwrap();
        let rules = intersect_rules(
            &categories,
            &json!([{"permission": "*", "pattern": "*", "action": "allow"}]),
            false,
        )
        .unwrap();
        assert_eq!(
            rules,
            json!([
                {"permission": "*", "pattern": "*", "action": "deny"},
                {"permission": "read", "pattern": "*", "action": "allow"}
            ])
        );
        assert_eq!(
            intersect_rules(&BTreeSet::new(), &json!([]), false)
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(allowed_categories(None).unwrap().is_none());
    }

    #[test]
    fn unsupported_or_ambiguous_grants_never_fall_back_to_advisory() {
        for tool in [
            "Write",
            "Edit",
            "Bash(git *)",
            "mcp__unknown",
            "shell",
            "subagent",
            "*",
            "external_directory",
            "doom_loop",
        ] {
            assert!(allowed_categories(Some(&[tool.into()])).is_err(), "{tool}");
        }
    }

    #[test]
    fn wrong_version_or_ignored_schema_rejects_prompt_admission() {
        assert!(validate_runtime(&json!({"healthy": true, "version": "1.14.41"}), true).is_ok());
        for health in [
            json!({"healthy": true}),
            json!({"healthy": false, "version": "1.14.41"}),
            json!({"healthy": true, "version": "2.0.0"}),
        ] {
            assert!(validate_runtime(&health, true).is_err());
        }
        for health in [
            json!({"healthy": true}),
            json!({"healthy": false, "version": "1.14.42"}),
            json!({"healthy": true, "version": " "}),
        ] {
            assert!(validate_runtime(&health, false).is_err());
        }
        let rules = json!([{"permission": "*", "pattern": "*", "action": "deny"}]);
        assert!(validate_session(&json!({"permission": rules}), Some(&rules)).is_ok());
        for response in [
            json!({}),
            json!({"permissions": rules}),
            json!({"permission": [{"permission": "*", "pattern": "*", "action": "allow"}]}),
        ] {
            assert!(validate_session(&response, Some(&rules)).is_err());
        }
    }

    #[test]
    fn unrelated_mcp_globs_preserve_deny_all_and_native_categories() {
        let categories = BTreeSet::from(["read", "edit"]);
        let rules = json!([
            {"permission": "context7*", "pattern": "*", "action": "deny"},
            {"permission": "toolbox_*", "pattern": "*", "action": "allow"},
            {"permission": "read", "pattern": "*", "action": "allow"},
            {"permission": "read", "pattern": "*.env", "action": "deny"}
        ]);
        assert_eq!(
            intersect_rules(&categories, &rules, false).unwrap(),
            json!([
                {"permission": "*", "pattern": "*", "action": "deny"},
                {"permission": "read", "pattern": "*", "action": "allow"},
                {"permission": "read", "pattern": "*.env", "action": "deny"}
            ])
        );
        for category in [
            "r*",
            "*read",
            "[r]ead",
            "read|toolbox*",
            "external_*",
            "doom_*",
        ] {
            assert!(
                intersect_rules(
                    &categories,
                    &json!([
                        {"permission": category, "pattern": "*", "action": "deny"}
                    ]),
                    false,
                )
                .is_err(),
                "{category}"
            );
        }
    }

    #[test]
    fn tool_filter_preserves_native_directory_and_loop_controls_without_new_grants() {
        let effective = json!([
            {"permission": "*", "pattern": "*", "action": "allow"},
            {"permission": "external_directory", "pattern": "*", "action": "ask"},
            {"permission": "external_directory", "pattern": "/trusted/worktrees/*", "action": "allow"},
            {"permission": "external_directory", "pattern": "/trusted/worktrees/private/*", "action": "deny"},
            {"permission": "doom_loop", "pattern": "*", "action": "ask"},
            {"permission": "read", "pattern": "*.env", "action": "deny"},
            {"permission": "edit", "pattern": "*", "action": "allow"}
        ]);
        assert_eq!(
            intersect_rules(&BTreeSet::from(["read"]), &effective, false).unwrap(),
            json!([
                {"permission": "*", "pattern": "*", "action": "deny"},
                {"permission": "read", "pattern": "*", "action": "allow"},
                {"permission": "external_directory", "pattern": "*", "action": "ask"},
                {"permission": "external_directory", "pattern": "/trusted/worktrees/*", "action": "allow"},
                {"permission": "external_directory", "pattern": "/trusted/worktrees/private/*", "action": "deny"},
                {"permission": "doom_loop", "pattern": "*", "action": "ask"},
                {"permission": "read", "pattern": "*.env", "action": "deny"}
            ])
        );
        let later_restrictions = json!([
            {"permission": "external_directory", "pattern": "/trusted/*", "action": "allow"},
            {"permission": "doom_loop", "pattern": "*", "action": "allow"},
            {"permission": "*", "pattern": "private/*", "action": "deny"},
            {"permission": "*", "pattern": "review/*", "action": "ask"},
            {"permission": "read", "pattern": "*", "action": "allow"},
            {"permission": "*", "pattern": "*", "action": "allow"}
        ]);
        let narrowed =
            intersect_rules(&BTreeSet::from(["read"]), &later_restrictions, false).unwrap();
        for control in NATIVE_CONTROL_PERMISSIONS {
            let control_rules: Vec<_> = narrowed
                .as_array()
                .unwrap()
                .iter()
                .filter(|rule| rule["permission"] == *control)
                .map(|rule| {
                    (
                        rule["pattern"].as_str().unwrap(),
                        rule["action"].as_str().unwrap(),
                    )
                })
                .collect();
            assert_eq!(
                &control_rules[1..],
                &[("private/*", "deny"), ("review/*", "ask")],
                "preserve later native restrictions without a wildcard allow for {control}",
            );
        }
    }

    #[test]
    fn managed_read_edit_contract_retains_the_denied_native_control_boundary() {
        assert_eq!(
            intersect_rules(
                &BTreeSet::from(["read", "edit"]),
                &json!([
                    {"permission": "*", "pattern": "*", "action": "allow"},
                    {"permission": "external_directory", "pattern": "*", "action": "allow"},
                    {"permission": "doom_loop", "pattern": "*", "action": "ask"}
                ]),
                true,
            )
            .unwrap(),
            json!([
                {"permission": "*", "pattern": "*", "action": "deny"},
                {"permission": "edit", "pattern": "*", "action": "allow"},
                {"permission": "read", "pattern": "*", "action": "allow"}
            ])
        );
    }

    #[test]
    fn native_control_rules_cannot_enable_any_tool_in_an_empty_allowlist() {
        assert_eq!(
            intersect_rules(
                &BTreeSet::new(),
                &json!([
                    {"permission": "*", "pattern": "*", "action": "allow"},
                    {"permission": "external_directory", "pattern": "/trusted/*", "action": "allow"}
                ]),
                false,
            )
            .unwrap(),
            json!([
                {"permission": "*", "pattern": "*", "action": "deny"},
                {"permission": "external_directory", "pattern": "/trusted/*", "action": "allow"}
            ])
        );
    }

    #[test]
    fn allowlist_preserves_profile_denies_asks_and_pattern_precedence() {
        let tools = ["read".into(), "bash".into()];
        let categories = allowed_categories(Some(&tools)).unwrap().unwrap();
        let effective = json!([
            {"permission": "*", "pattern": "*", "action": "allow"},
            {"permission": "read", "pattern": "*.env", "action": "deny"},
            {"permission": "bash", "pattern": "git *", "action": "ask"},
            {"permission": "bash", "pattern": "*", "action": "deny"},
            {"permission": "edit", "pattern": "*", "action": "allow"}
        ]);
        assert_eq!(
            intersect_rules(&categories, &effective, false).unwrap(),
            json!([
                {"permission": "*", "pattern": "*", "action": "deny"},
                {"permission": "bash", "pattern": "*", "action": "allow"},
                {"permission": "read", "pattern": "*", "action": "allow"},
                {"permission": "read", "pattern": "*.env", "action": "deny"},
                {"permission": "bash", "pattern": "git *", "action": "ask"},
                {"permission": "bash", "pattern": "*", "action": "deny"}
            ])
        );
        assert!(
            intersect_rules(
                &categories,
                &json!([{"permission": "*sh", "pattern": "*", "action": "allow"}]),
                false,
            )
            .is_err()
        );
    }
}
