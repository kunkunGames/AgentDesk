# Codex CLI 0.120.0 -> 0.121.0 Review for AgentDesk

Issue: `#35`  
Date: `2026-04-18`

## Executive summary

Recommendation:

- Apply now: none
- Needs design: plugin marketplace adoption, and any future `app-server` / WebSocket path
- Hold: native Codex memory controls and Codex anonymous analytics rollout

The main reason is that AgentDesk currently integrates Codex through `codex exec` plus `codex mcp add/remove`, not through `codex app-server` or a plugin marketplace lifecycle. The 0.121.0 release adds useful new surfaces, but they do not map directly onto AgentDesk's current execution path.

## What changed in Codex 0.121.0

Verified on `2026-04-18` with local CLI comparisons between:

- `codex-cli 0.121.0`
- `npx -y @openai/codex@0.120.0`

Observed CLI deltas:

- `codex --help` in 0.121.0 adds `marketplace` and changes `exec-server` wording from "binary" to "service".
- `codex app-server --help` in 0.121.0 adds `--ws-token-sha256`.
- `codex features list` in 0.121.0 shows:
  - `general_analytics` moved from `under development false` to `stable true`
  - `image_detail_original` moved from `experimental false` to `removed false`
  - `telepathy` was added as `under development false`
  - `use_agent_identity` was added as `under development false`
  - `workspace_dependencies` is present as `stable true`

Official release notes confirm the important product-level additions:

- 0.121.0 adds `codex marketplace add` and app-server support for installing plugin marketplaces.
- 0.121.0 adds app-server memory controls and expanded MCP/plugin support.

Source:

- OpenAI changelog: <https://developers.openai.com/codex/changelog>
- OpenAI plugins docs: <https://developers.openai.com/codex/plugins>
- OpenAI advanced config docs: <https://developers.openai.com/codex/config-advanced>

## Current AgentDesk integration shape

Current behavior in AgentDesk is still centered on `exec`:

- `src/services/discord/router/message_handler.rs:1500` calls `codex::execute_command_streaming(...)` per Discord turn.
- `src/services/codex.rs:925` hardcodes `exec`, and `src/services/codex.rs:930` adds `--skip-git-repo-check --json`.
- `src/services/codex.rs:934` uses `--dangerously-bypass-approvals-and-sandbox` for non-readonly execution.
- `src/services/codex_tmux_wrapper.rs:241` also hardcodes `exec`.

Remote `app-server`-style adoption is not present:

- `src/services/codex.rs:418` returns `Remote SSH execution is not available in AgentDesk`.
- `src/services/codex.rs:433` returns `Remote SSH tmux execution is not available in AgentDesk`.

AgentDesk does have adjacent integration points, but they are different from plugin marketplaces:

- `src/services/mcp_config.rs:85` and `src/services/mcp_config.rs:105` manage Codex MCP servers through `codex mcp add/remove`.
- `src/services/discord/mod.rs:1473` and `src/services/discord/mod.rs:1487` load Codex directory skills from `~/.codex/skills` and `<project>/.codex/skills`.
- `src/services/discord/model_picker_interaction.rs:211` only validates and stores model overrides; it does not manage plugin/app-server configuration.

## Classification

### Apply now

None.

Why:

- The 0.121.0 release does not require a compatibility fix for AgentDesk's existing `exec` path.
- AgentDesk already uses stable surfaces that still exist in 0.121.0: `exec`, model override flags, and `mcp add/remove`.
- There is no user-facing AgentDesk control today for plugin marketplace install, app-server lifecycle, or memory mode switching.

## Needs design

### 1. Plugin marketplace support

Reason:

- 0.121.0 introduces `codex marketplace add` and plugin marketplace installation support.
- Plugin bundles can include skills, apps, and MCP servers, not just prompt text.
- AgentDesk currently supports directory skills and MCP sync separately, but not a marketplace ownership model.

Why it needs design first:

- We need to decide whether plugin installation is host-global, project-local, or runtime-managed.
- We need a trust and approval model for plugin contents that may include apps and MCP servers.
- We need to define whether AgentDesk owns plugin installation, only detects installed plugins, or stays out of that lifecycle entirely.
- We need to avoid overlapping ownership between:
  - AgentDesk-managed MCP sync in `src/services/mcp_config.rs`
  - Codex plugin-provided MCP/app surfaces
  - provider skill scanning in `src/services/discord/mod.rs`

Recommendation:

- Treat plugin marketplace adoption as a separate follow-up card, not as part of the 0.121.0 update itself.

### 2. `app-server` / WebSocket adoption

Reason:

- `--ws-token-sha256` only matters on the `codex app-server` path.
- The larger 0.121.0 additions around memory controls, raw turn injection, and output modality are app-server/TUI oriented.
- AgentDesk currently invokes `codex exec` from the Discord message path, not `app-server`.

Recommendation:

- Only consider this if AgentDesk wants a persistent Codex daemon or remote session model later.
- Until then, `--ws-token-sha256` is not actionable.

## Hold

### 1. Native Codex memory mode/reset controls

Reason:

- The 0.121.0 release adds TUI/app-server controls for memory mode, reset, deletion, and cleanup.
- AgentDesk's current memory direction is explicitly `memento`, not native Codex file-backed or product-managed memory.

Evidence:

- `config/agents/_shared.prompt.md:10` says all memory should use `memento MCP`.
- `src/services/mcp_config.rs:12` defines the managed `memento` server name.
- `src/services/mcp_config.rs:27` checks for `memento` on providers including Codex.

Recommendation:

- Do not expose Codex-native memory controls in AgentDesk while runtime memory remains pinned to `memento`.

### 2. Codex anonymous analytics as an AgentDesk feature decision

Reason:

- In 0.121.0, `general_analytics` is now marked stable.
- OpenAI's advanced config docs describe machine-level anonymous metrics under `[analytics] enabled = false`.
- AgentDesk already stores its own turn analytics from provider output.

Evidence:

- `src/services/discord/turn_bridge/mod.rs:156` persists turn analytics rows.
- `src/services/session_backend.rs:281` extracts token/session analytics from Codex output JSONL.

Recommendation:

- Hold this as an explicit privacy and operations decision.
- Do not add AgentDesk product behavior around Codex analytics until ownership, defaults, and disclosure are defined.

## Suggested follow-up cards

1. `Codex plugin marketplace adoption design for AgentDesk`
2. `Decide whether AgentDesk should manage Codex plugins or only detect them`
3. `Evaluate whether any future Codex app-server path is worth supporting`
4. `Document why AgentDesk memory remains memento-backed even after Codex 0.121.0 memory controls`

## Final recommendation

The 0.121.0 upgrade is safe to keep, but there is no immediate AgentDesk code change that should be merged just because the CLI moved from 0.120.0 to 0.121.0.

The highest-value new capability is plugin marketplace support, but that is a product and runtime ownership decision, not a small compatibility patch. Everything else that looks new in 0.121.0 is either app-server scoped or intentionally out of scope for AgentDesk's current memory and analytics model.
