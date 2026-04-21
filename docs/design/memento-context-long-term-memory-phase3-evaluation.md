# Memento Context Long-Term Memory Phase 3 Evaluation

Date: 2026-04-18

## Scope

This report closes `REQ-009` / `TEST-006` from:

- `/Users/kunkun/PRD/memento-context-long-term-memory/memento-context-long-term-memory-injection-hardening-prd.md`
- `/Users/kunkun/PRD/memento-context-long-term-memory/memento-context-long-term-memory-injection-hardening-spec.md`

The question for phase 3 is whether `context()` itself still needs a query-aware schema redesign after phase 1 and phase 2 landed.

## What Was Evaluated

Phase 1 correctness in `memento-mcp`:

- `learning_extraction` is injected into both `allFragments` and `rankedInjection`
- anchor selection is workspace-safe
- structured ranking recognizes anchors via `is_anchor = TRUE`

Phase 2 integration in `AgentDesk`:

- standard Discord turns use bootstrap on first load, then switch to query recall only when `memory.query_recall_after_bootstrap` is enabled
- meeting orchestration builds targeted recall queries from `agenda + transcript` when the same query-recall gate is enabled
- memento query recall uses `text`, `contextText`, `sessionId`, `excludeSeen=true`
- external recall formatting deduplicates across `rankedInjection`, `core`, `working`, and `anchors`
- query path remains bounded by explicit `pageSize` and `tokenBudget`

## Evidence

Code-path and regression evidence now exists in these files:

- `memento-mcp`
  - `lib/memory/ContextBuilder.js`
  - `tests/unit/context-builder.test.js`
  - `tests/unit/context-structured.test.js`
- `AgentDesk`
  - `src/services/memory/mod.rs`
  - `src/services/memory/memento.rs`
  - `src/services/discord/router/message_handler.rs`
  - `src/services/discord/meeting_orchestrator.rs`
  - `src/runtime_layout/mod.rs`
  - `src/services/discord/settings.rs`
  - `src/services/discord/settings/memory.rs`

Targeted regression checks used for this phase:

- `test_memento_recall_calls_context_tool_over_mcp`
- `test_memento_query_recall_calls_recall_tool_over_mcp`
- `test_format_context_payload_for_external_recall_dedups_across_sections`
- `test_format_context_payload_for_external_recall_caps_ranked_lines`
- `test_build_participant_recall_request_uses_query_mode_and_combines_agenda_and_transcript`
- `recall_mode_defaults_to_bootstrap_until_query_recall_is_enabled_for_memento`
- `recall_mode_bootstraps_only_on_first_memento_turn_when_query_recall_enabled`
- `test_resolve_memory_settings_query_recall_uses_runtime_default_and_override`
- `test_resolve_memory_settings_keeps_query_recall_defaults`

Operational rollout evaluated for this phase used these enabled settings:

- `AgentDesk`: runtime memory config with `memory.query_recall_after_bootstrap = true`
- `memento-mcp`: runtime memory config with `contextInjection.hardening.enabled = true`

Legacy compatibility remains available via explicit opt-out:

- `AgentDesk`: set `memory.query_recall_after_bootstrap: false`
- `memento-mcp`: set `contextInjection.hardening.enabled: false`

## Findings

1. `context()` still serves the right job as a bootstrap loader.
2. Turn-by-turn targeted retrieval is now handled by the existing `recall()` tool without widening the `context` schema.
3. Standard Discord turns and meeting turns both have a concrete query path when query recall is enabled.
4. Prompt growth is bounded by `pageSize=8`, `tokenBudget=1200`, and formatter dedup/caps.
5. The remaining gap was rollout policy and verification artifacts, not a missing `context()` capability.

## Decision

Do **not** open a new PRD for a query-aware `context()` redesign at this time.

The current architecture is sufficient because:

- bootstrap and query responsibilities are now separated
- targeted recall is already available where it matters
- the public `context` schema stays stable
- rollout can still fall back to legacy mode with explicit config

## Reopen Criteria

Open a separate PRD only if production evidence shows one of these:

- targeted `recall()` misses essential memories that are present in bootstrap data
- `recall()` latency or token bounds are insufficient for real meeting/Discord flows
- a repeated class of tasks requires query semantics before bootstrap completes
- the caller cannot express needed retrieval intent through current `recall()` inputs
