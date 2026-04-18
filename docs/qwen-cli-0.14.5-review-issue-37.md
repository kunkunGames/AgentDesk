# Qwen CLI 0.14.4 -> 0.14.5 Review for AgentDesk

Issue: `#37`  
Date: `2026-04-18`

## Executive summary

Recommendation:

- Apply now: none
- Needs design: only if AgentDesk later wants a dedicated non-turn telemetry/context surface for Qwen
- Hold: additional Qwen telemetry surfaces beyond the current stream parsing path

The `0.14.4 -> 0.14.5` update does not require an immediate AgentDesk patch. AgentDesk already parses usage-bearing stream events from Qwen's turn output, and local top-level CLI comparison did not show a command-surface delta that maps directly to a current AgentDesk feature gap.

## What changed in Qwen CLI 0.14.5

Verified on `2026-04-18` with local CLI comparisons between:

- `qwen 0.14.5`
- `npx -y @qwen-code/qwen-code@0.14.4`

Observed CLI delta:

- No top-level `qwen --help` diff was observed from the local comparison.

Evidence:

- local `qwen --version`
- local `npx -y @qwen-code/qwen-code@0.14.4 --version`
- local `qwen --help`
- local `npx -y @qwen-code/qwen-code@0.14.4 --help`

## Current AgentDesk integration shape

AgentDesk already consumes the most relevant live signal from Qwen's streaming path:

- `src/services/qwen.rs:238` through `src/services/qwen.rs:337` run Qwen in streaming mode with retry-aware session handling.
- `src/services/qwen.rs:536` through `src/services/qwen.rs:589` inspect Qwen stream events and update status from usage-bearing payloads.
- `src/services/qwen_tmux_wrapper.rs:328` through `src/services/qwen_tmux_wrapper.rs:371` retry stalled tmux-backed Qwen sessions instead of assuming the stream is healthy forever.
- `src/services/discord/model_picker_interaction.rs:211` through `src/services/discord/model_picker_interaction.rs:243` remain model-only and do not expose separate Qwen telemetry controls.

## Classification

### Apply now

None.

Why:

- AgentDesk already parses Qwen usage from live stream events.
- No top-level CLI surface delta was observed that requires a transport or UX change.

### Needs design

### 1. Dedicated non-turn Qwen telemetry/context collection

Value:

- Medium if the product later wants provider-side context or usage inspection outside an active turn.

Effort:

- Medium.

Risk:

- Medium. A new polling or side-channel telemetry path would need provider-specific semantics and should not be inferred from turn-stream behavior alone.

Reason this is not an apply-now patch:

- The current AgentDesk product surface does not yet use a separate Qwen context/telemetry command path.

### Hold

### 1. Extra Qwen telemetry surfaces beyond stream-event parsing

Value:

- Unknown until there is a concrete product requirement.

Effort:

- Medium.

Risk:

- Medium, because it can create duplicated or inconsistent telemetry semantics across providers.

## Conclusion

The `0.14.4 -> 0.14.5` upgrade is safe to keep, but it does not justify an immediate AgentDesk code patch. AgentDesk already captures the main live usage signal it currently knows how to consume, and any additional Qwen-specific telemetry or context surface should be designed deliberately rather than attached opportunistically to this patch upgrade.
