# Post-Stabilization Maintainability Backlog

Last refreshed: 2026-04-30 (against #1461 post-stabilization backlog)

This page is the holding checklist for Phase 3 maintainability work that must
not interrupt the relay recovery and CookingHeart two-node readiness lane before
the 2026-05-05 stabilization deadline.

## Promotion Rule

- Default priority is P2 after runtime stabilization.
- Promote an item to P1 only when it directly blocks relay recovery,
  CookingHeart two-node readiness, or a hard-gated CI check.
- Do not mix P2 cleanup into P0/P1 relay recovery commits unless the cleanup is
  required to make a failing test or hard gate pass. When that exception is
  used, the commit must name the failing check and keep the cleanup to the
  minimum touched surface.
- Warning-only audit findings stay backlog items until promoted by the rule
  above.

## Deferred Areas

| Area | Priority | Dependencies | Checklist |
| --- | --- | --- | --- |
| GitClient / GitFactCache | P2, promote to P1 only if shell/git subprocess behavior blocks recovery automation | Inventory in `docs/agent-maintenance/change-surfaces.md`; existing git subprocess findings from `scripts/audit_maintainability.py --check`; `src/services/platform/shell.rs` split plan | [ ] Define the typed git operation surface; [ ] introduce cache invalidation rules for branch/commit facts; [ ] migrate one caller behind tests before broad replacement |
| Policy typed facade completion | P2, promote only if policy raw DB access causes review/queue recovery failure | `docs/policy-typed-facade.md`; current JS policy slices; typed facade enforcement tests | [ ] Add typed review outcome + pipeline stage facade; [ ] add typed preflight/card metadata patch facade; [ ] add typed timeout scan + kv sweep facade; [ ] block migrated slices from `agentdesk.db.*` regressions |
| Provider adapter duplication reduction | P2 after provider recovery behavior is stable | Provider CLI smoke checks; provider migration docs; shared execution/context helpers | [ ] Identify duplicated prompt/session/output parsing code across Claude/Codex/Gemini/Qwen/OpenCode adapters; [ ] extract one low-risk shared helper; [ ] keep provider-specific behavior covered by adapter tests |
| Route SRP cleanup | P2/P3 because `route_srp_violations` is warning-only | `docs/generated/route-inventory.md`; generated route docs; service/domain boundaries | [ ] Pick one route family with stable behavior; [ ] move SQL/service calls behind a domain helper; [ ] keep HTTP JSON shape tests unchanged |
| UX / operability improvements | P2 unless an operator cannot recover or observe a relay failure | Health API snapshots; Discord command UX; recovery runbooks | [ ] List high-friction operator actions; [ ] add command/status copy improvements after recovery semantics settle; [ ] update runbook/docs alongside any user-visible command change |

## Hard-Gate Stewardship

`scripts/audit_maintainability.py --check` must remain green for hard gates:

- `direct_discord_sends`
- `legacy_sqlite_refs`
- `source_of_truth_alias_writes`
- `giant_files`

When a runtime fix shifts line-based allowlist entries, refresh only the affected
line entries in `scripts/audit_allowlist.toml`. Do not use a broad allowlist to
hide new callsites.

## Review Checklist

- [ ] The change is not carrying route/provider/policy cleanup unless promoted
      by the Promotion Rule.
- [ ] Any promoted cleanup names the failing test, hard gate, or relay readiness
      blocker.
- [ ] Warning-only audit items remain in this backlog or a narrower follow-up
      checklist.
- [ ] Hard gates are validated with
      `python3 scripts/audit_maintainability.py --check`.
