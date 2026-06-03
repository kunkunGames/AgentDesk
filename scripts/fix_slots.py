import re

with open("src/db/auto_queue/slots.rs", "r", encoding="utf-8") as f:
    text = f.read()

# First conflict in slots.rs
slots_conflict_1 = r'''<<<<<<< HEAD
             CROSS JOIN LATERAL \(SELECT COALESCE\(NULLIF\(d\.context, ''\), '\{\}'\)::jsonb AS ctx\) c
             WHERE d\.to_agent_id = \$1
               AND d\.status IN \('completed', 'failed', 'cancelled'\)
               AND COALESCE\(NULLIF\(c\.ctx->>'slot_index', ''\)::BIGINT, -1\) = \$2
               AND COALESCE\(\(c\.ctx->>'auto_queue'\)::BOOLEAN, FALSE\) = TRUE
               AND COALESCE\(\(c\.ctx->>'sidecar_dispatch'\)::BOOLEAN, FALSE\) = FALSE
               AND c\.ctx->'phase_gate' IS NULL
=======
               AND \{slot_index_expr\} = \$2
               AND COALESCE\(\(\(COALESCE\(NULLIF\(d\.context, ''\), '\{\{\}\}'\)::jsonb\)->>'auto_queue'\)::BOOLEAN, FALSE\) = TRUE
               AND COALESCE\(\(\(COALESCE\(NULLIF\(d\.context, ''\), '\{\{\}\}'\)::jsonb\)->>'sidecar_dispatch'\)::BOOLEAN, FALSE\) = FALSE
               AND \(COALESCE\(NULLIF\(d\.context, ''\), '\{\{\}\}'\)::jsonb\)->'phase_gate' IS NULL
>>>>>>> upstream/main'''

slots_replacement_1 = '''             CROSS JOIN LATERAL (SELECT COALESCE(NULLIF(d.context, ''), '{{}}')::jsonb AS ctx) c
             WHERE d.to_agent_id = $1
               AND d.status IN ('completed', 'failed', 'cancelled')
               AND {slot_index_expr} = $2
               AND COALESCE((c.ctx->>'auto_queue')::BOOLEAN, FALSE) = TRUE
               AND COALESCE((c.ctx->>'sidecar_dispatch')::BOOLEAN, FALSE) = FALSE
               AND c.ctx->'phase_gate' IS NULL'''

text = re.sub(slots_conflict_1, slots_replacement_1, text, flags=re.MULTILINE)

# Second conflict in slots.rs (the active_dispatch_on_slot_predicate substitution)
# We just want to accept upstream/main for the second conflict
slots_conflict_2 = r'''<<<<<<< HEAD
    sqlx::query_scalar::<_, bool>\(
        "SELECT COUNT\(\*\) > 0
         FROM task_dispatches
         CROSS JOIN LATERAL \(SELECT COALESCE\(NULLIF\(context, ''\), '\{\}'\)::jsonb AS ctx\) c
         WHERE to_agent_id = \$1
           AND status IN \('pending', 'dispatched'\)
           AND COALESCE\(NULLIF\(c\.ctx->>'slot_index', ''\)::BIGINT, -1\) = \$2
           AND COALESCE\(\(c\.ctx->>'sidecar_dispatch'\)::BOOLEAN, FALSE\) = FALSE
           AND c\.ctx->'phase_gate' IS NULL
           AND id != \$3
           AND \(
               COALESCE\(dispatch_type, 'implementation'\) NOT IN \('review', 'review-decision', 'create-pr'\)
               OR status = 'pending'
               OR EXISTS \(
                   SELECT 1
                   FROM sessions s
                   WHERE s\.active_dispatch_id = task_dispatches\.id
                     AND COALESCE\(s\.status, ''\) NOT IN \('disconnected', 'completed', 'failed', 'cancelled'\)
               \)
           \)",
    \)
    \.bind\(agent_id\)
    \.bind\(slot_index\)
    \.bind\(exclude_id\)
    \.fetch_one\(pool\)
    \.await
=======
    let active_dispatch_exists = active_dispatch_on_slot_predicate\(
        "\$1",
        "\$2",
        DispatchSlotPolarity::Exists,
        Some\("d\.id != \$3"\),
    \);
    let query = format!\("SELECT \{active_dispatch_exists\}"\);
    sqlx::query_scalar::<_, bool>\(&query\)
        \.bind\(agent_id\)
        \.bind\(slot_index\)
        \.bind\(exclude_id\)
        \.fetch_one\(pool\)
        \.await
>>>>>>> upstream/main'''

slots_replacement_2 = '''    let active_dispatch_exists = active_dispatch_on_slot_predicate(
        "$1",
        "$2",
        DispatchSlotPolarity::Exists,
        Some("d.id != $3"),
    );
    let query = format!("SELECT {active_dispatch_exists}");
    sqlx::query_scalar::<_, bool>(&query)
        .bind(agent_id)
        .bind(slot_index)
        .bind(exclude_id)
        .fetch_one(pool)
        .await'''

text = re.sub(slots_conflict_2, slots_replacement_2, text, flags=re.MULTILINE)

with open("src/db/auto_queue/slots.rs", "w", encoding="utf-8") as f:
    f.write(text)

print("Fixed slots.rs")

# Now we must update slot_predicate.rs to use LATERAL JOIN so that it is performant
with open("src/db/auto_queue/slot_predicate.rs", "r", encoding="utf-8") as f:
    pred_text = f.read()

pred_old = '''    let slot_index = dispatch_slot_index_expr("d.context");
    let sidecar = dispatch_bool_flag_expr("d.context", "sidecar_dispatch");
    let phase_gate = dispatch_json_member_expr("d.context", "phase_gate");
    let extra = match extra_clause {
        Some(clause) => format!("\\n               AND ({clause})"),
        None => String::new(),
    };
    format!(
        "{keyword} (
             SELECT 1
             FROM task_dispatches d
             WHERE d.to_agent_id = {agent_expr}
               AND d.status IN ('pending', 'dispatched')
               AND {slot_index} = {slot_expr}
               AND {sidecar} = FALSE
               AND {phase_gate} IS NULL{extra}'''

pred_new = '''    let slot_index = dispatch_slot_index_expr("c.ctx");
    let sidecar = dispatch_bool_flag_expr("c.ctx", "sidecar_dispatch");
    let phase_gate = dispatch_json_member_expr("c.ctx", "phase_gate");
    let extra = match extra_clause {
        Some(clause) => format!("\\n               AND ({clause})"),
        None => String::new(),
    };
    format!(
        "{keyword} (
             SELECT 1
             FROM task_dispatches d
             CROSS JOIN LATERAL (SELECT COALESCE(NULLIF(d.context, ''), '{{}}')::jsonb AS ctx) c
             WHERE d.to_agent_id = {agent_expr}
               AND d.status IN ('pending', 'dispatched')
               AND {slot_index} = {slot_expr}
               AND {sidecar} = FALSE
               AND {phase_gate} IS NULL{extra}'''

pred_text = pred_text.replace(pred_old, pred_new)

# We also need to fix the tests inside slot_predicate.rs that have the hardcoded expectation!
pred_test_old = '''    fn legacy_claim_predicate(keyword: &str, agent_expr: &str, slot_expr: &str) -> String {
        format!(
            "{keyword} (
             SELECT 1
             FROM task_dispatches d
             WHERE d.to_agent_id = {agent_expr}
               AND d.status IN ('pending', 'dispatched')
               AND COALESCE(NULLIF((COALESCE(NULLIF(d.context, ''), '{{}}')::jsonb)->>'slot_index', '')::BIGINT, -1) = {slot_expr}
               AND COALESCE(((COALESCE(NULLIF(d.context, ''), '{{}}')::jsonb)->>'sidecar_dispatch')::BOOLEAN, FALSE) = FALSE
               AND (COALESCE(NULLIF(d.context, ''), '{{}}')::jsonb)->'phase_gate' IS NULL'''

pred_test_new = '''    fn legacy_claim_predicate(keyword: &str, agent_expr: &str, slot_expr: &str) -> String {
        format!(
            "{keyword} (
             SELECT 1
             FROM task_dispatches d
             CROSS JOIN LATERAL (SELECT COALESCE(NULLIF(d.context, ''), '{{}}')::jsonb AS ctx) c
             WHERE d.to_agent_id = {agent_expr}
               AND d.status IN ('pending', 'dispatched')
               AND COALESCE(NULLIF((COALESCE(NULLIF(c.ctx, ''), '{{}}')::jsonb)->>'slot_index', '')::BIGINT, -1) = {slot_expr}
               AND COALESCE(((COALESCE(NULLIF(c.ctx, ''), '{{}}')::jsonb)->>'sidecar_dispatch')::BOOLEAN, FALSE) = FALSE
               AND (COALESCE(NULLIF(c.ctx, ''), '{{}}')::jsonb)->'phase_gate' IS NULL'''

pred_text = pred_text.replace(pred_test_old, pred_test_new)

# Wait, `dispatch_slot_index_expr` uses `COALESCE(NULLIF(c.ctx, '')`. Wait, `c.ctx` is ALREADY jsonb!
# The macro `dispatch_slot_index_expr` does: `(COALESCE(NULLIF({column}, ''), '{{}}')::jsonb)`
# If `{column}` is `"c.ctx"`, then it evaluates to `(COALESCE(NULLIF(c.ctx, ''), '{{}}')::jsonb)`. Postgres will complain if `c.ctx` is jsonb and we try to NULLIF it with a string!
# Oh! `LATERAL (SELECT COALESCE(NULLIF(d.context, ''), '{}')::jsonb AS ctx) c` returns `ctx` of type `jsonb`.
# Let me change `dispatch_slot_index_expr` to NOT do that if it's already jsonb, or just use the old way in the LATERAL.
# Or better, just rewrite the LATERAL to return text instead of jsonb? No, we WANT to parse jsonb ONCE.
# Let's fix `dispatch_slot_index_expr` and the others:

pred_funcs_old = '''pub(crate) fn dispatch_slot_index_expr(column: &str) -> String {
    format!(
        "COALESCE(NULLIF((COALESCE(NULLIF({column}, ''), '{{}}')::jsonb)->>'slot_index', '')::BIGINT, -1)"
    )
}

/// SQL boolean expression that extracts `context.<key>` as a BOOLEAN, defaulting
/// to `FALSE`.
fn dispatch_bool_flag_expr(column: &str, key: &str) -> String {
    format!("COALESCE(((COALESCE(NULLIF({column}, ''), '{{}}')::jsonb)->>'{key}')::BOOLEAN, FALSE)")
}

/// SQL expression accessing `context.<key>` as raw JSONB (for `IS NULL` tests).
fn dispatch_json_member_expr(column: &str, key: &str) -> String {
    format!("(COALESCE(NULLIF({column}, ''), '{{}}')::jsonb)->'{key}'")
}'''

pred_funcs_new = '''pub(crate) fn dispatch_slot_index_expr(column: &str) -> String {
    if column == "c.ctx" {
        "COALESCE(NULLIF(c.ctx->>'slot_index', '')::BIGINT, -1)".to_string()
    } else {
        format!("COALESCE(NULLIF((COALESCE(NULLIF({column}, ''), '{{}}')::jsonb)->>'slot_index', '')::BIGINT, -1)")
    }
}

fn dispatch_bool_flag_expr(column: &str, key: &str) -> String {
    if column == "c.ctx" {
        format!("COALESCE((c.ctx->>'{key}')::BOOLEAN, FALSE)")
    } else {
        format!("COALESCE(((COALESCE(NULLIF({column}, ''), '{{}}')::jsonb)->>'{key}')::BOOLEAN, FALSE)")
    }
}

fn dispatch_json_member_expr(column: &str, key: &str) -> String {
    if column == "c.ctx" {
        format!("c.ctx->'{key}'")
    } else {
        format!("(COALESCE(NULLIF({column}, ''), '{{}}')::jsonb)->'{key}'")
    }
}'''

pred_text = pred_text.replace(pred_funcs_old, pred_funcs_new)

# And fix the test's legacy predicate comparison
legacy_replace_old = '''               AND COALESCE(NULLIF((COALESCE(NULLIF(c.ctx, ''), '{{}}')::jsonb)->>'slot_index', '')::BIGINT, -1) = {slot_expr}
               AND COALESCE(((COALESCE(NULLIF(c.ctx, ''), '{{}}')::jsonb)->>'sidecar_dispatch')::BOOLEAN, FALSE) = FALSE
               AND (COALESCE(NULLIF(c.ctx, ''), '{{}}')::jsonb)->'phase_gate' IS NULL'''

legacy_replace_new = '''               AND COALESCE(NULLIF(c.ctx->>'slot_index', '')::BIGINT, -1) = {slot_expr}
               AND COALESCE((c.ctx->>'sidecar_dispatch')::BOOLEAN, FALSE) = FALSE
               AND c.ctx->'phase_gate' IS NULL'''
pred_text = pred_text.replace(legacy_replace_old, legacy_replace_new)

# And fix the replace string in exists_form_with_bind_params_matches_slots_and_runtime_inline
test_replace_old = '''        let expected = legacy_claim_predicate("EXISTS", "$1", "$2")
            .replace(
                "AND (COALESCE(NULLIF(d.context, ''), '{}')::jsonb)->'phase_gate' IS NULL\\n",
                "AND (COALESCE(NULLIF(d.context, ''), '{}')::jsonb)->'phase_gate' IS NULL\\n               AND (d.id != $3)\\n",
            );'''

test_replace_new = '''        let expected = legacy_claim_predicate("EXISTS", "$1", "$2")
            .replace(
                "AND c.ctx->'phase_gate' IS NULL\\n",
                "AND c.ctx->'phase_gate' IS NULL\\n               AND (d.id != $3)\\n",
            );'''
pred_text = pred_text.replace(test_replace_old, test_replace_new)


with open("src/db/auto_queue/slot_predicate.rs", "w", encoding="utf-8") as f:
    f.write(pred_text)

print("Fixed slot_predicate.rs")

# slots.rs uses {slot_index_expr} in the first query. Let's make sure it gets it right.
# In slots.rs, we need to pass "c.ctx" instead of "d.context" to `dispatch_slot_index_expr` if it's there.
with open("src/db/auto_queue/slots.rs", "r", encoding="utf-8") as f:
    slots_text = f.read()

slots_text = slots_text.replace(
    'let slot_index_expr = dispatch_slot_index_expr("d.context");',
    'let slot_index_expr = dispatch_slot_index_expr("c.ctx");'
)

with open("src/db/auto_queue/slots.rs", "w", encoding="utf-8") as f:
    f.write(slots_text)

