import re
with open("src/dispatch/dispatch_create.rs", "r") as f:
    c = f.read()

# Replace the first instance of 'let mut base' with 'let base'
# Wait, actually we don't need 'base' at all.
# Let's just do an exact string replace.

old_str = """    } else {
        let mut base = serde_json::to_string(&context_with_session_strategy)?;
        let phase_gate_sidecar = context_with_session_strategy
            .get("phase_gate")
            .and_then(|value| value.as_object())
            .is_some();
        let worktree_target = if let Some((wt_path, wt_branch)) ="""

new_str = """    } else {
        let phase_gate_sidecar = context_with_session_strategy
            .get("phase_gate")
            .and_then(|value| value.as_object())
            .is_some();
        let worktree_target = if let Some((wt_path, wt_branch)) ="""

c = c.replace(old_str, new_str)

old_str2 = """        if let Some((wt_path, wt_branch, managed_created)) = worktree_target
            && let Ok(mut obj) =
                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&base)
        {"""

new_str2 = """        if let Some((wt_path, wt_branch, managed_created)) = worktree_target
            && let Some(obj) = context_with_session_strategy.as_object_mut()
        {"""

c = c.replace(old_str2, new_str2)

old_str3 = """            tracing::info!(
                "[dispatch] {} dispatch for card {}: injecting worktree_path={}",
                dispatch_type,
                kanban_card_id,
                wt_path
            );
            base = serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or(base);
        }
        if dispatch_type == "review-decision"
            && let Ok(mut obj) =
                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&base)
        {
            inject_review_dispatch_identifiers(pg_pool, kanban_card_id, dispatch_type, &mut obj)
                .await;
            base = serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or(base);
        }
        base
    };"""

new_str3 = """            tracing::info!(
                "[dispatch] {} dispatch for card {}: injecting worktree_path={}",
                dispatch_type,
                kanban_card_id,
                wt_path
            );
        }
        if dispatch_type == "review-decision"
            && let Some(obj) = context_with_session_strategy.as_object_mut()
        {
            inject_review_dispatch_identifiers(pg_pool, kanban_card_id, dispatch_type, obj)
                .await;
        }
        serde_json::to_string(&context_with_session_strategy)?
    };"""

c = c.replace(old_str3, new_str3)

with open("src/dispatch/dispatch_create.rs", "w") as f:
    f.write(c)
