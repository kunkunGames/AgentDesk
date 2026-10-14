//! Sidecar classification and immutable work-dispatch baseline context.

use serde_json::json;

pub(crate) fn dispatch_context_requests_sidecar(context: &serde_json::Value) -> bool {
    context
        .get("sidecar_dispatch")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
        || context
            .get("phase_gate")
            .and_then(|value| value.as_object())
            .is_some()
}

pub(super) fn inject_work_dispatch_baseline_commit(
    dispatch_type: &str,
    context: &mut serde_json::Value,
) {
    if !matches!(dispatch_type, "implementation" | "rework") {
        return;
    }
    let Some(obj) = context.as_object_mut() else {
        return;
    };
    if obj.contains_key("baseline_commit") {
        return;
    }

    let target_repo = obj
        .get("target_repo")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty());
    let baseline_commit =
        crate::services::platform::shell::resolve_repo_dir_for_target(target_repo)
            .ok()
            .flatten()
            .and_then(|repo_dir| {
                crate::services::platform::shell::git_dispatch_baseline_commit(&repo_dir)
            });

    if let Some(baseline_commit) = baseline_commit {
        obj.insert("baseline_commit".to_string(), json!(baseline_commit));
    }
}
