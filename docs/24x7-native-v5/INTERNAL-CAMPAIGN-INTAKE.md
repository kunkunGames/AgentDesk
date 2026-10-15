# Issue-less Campaign intake: implementation and observed limits

Verified on 2026-09-29 KST. This document describes the native Mac pilot, not
acceptance of unattended 24-hour operation or the Linux protected execution profile.

## Admission setting

`internalTasksEnabled` is a registered runtime configuration key with product
default `false`. Ordinary Auto Queue generation excludes cards without a GitHub
issue when the setting is off, reporting `internal_tasks_disabled`. Native work
dispatch creation also checks the setting before creating an issue-less workspace.
The UI toggle is **Settings → Auto Queue → 내부 칸반 작업 허용**.

When enabled, an issue-less card uses branch `adk/auto/card-<card UUID>` in a
managed Git worktree. Existing edits in that card's worktree are preserved on
reuse; no invented issue number or branch reset is involved. A valid local
repository mapping is still required. The opt-in does not certify provider
containment, grant managed execution v1 on macOS, or accept a business result.

The user's Mac installation has `current.internalTasksEnabled=true` and
`defaults.internalTasksEnabled=false`. The explicit override survived an actual
launchd service restart. The installed dashboard index matched the built index.

Historical September 29 installed binary SHA-256:
`3eb1ff9bd7feba2f6f5311cfc85ff911c13cb74e7e1b4e0521d5a75614de24b1`.
The current installed binary, Codex acceptance and recovery boundaries are recorded in
[CODEX-CAMPAIGN-VERIFICATION.md](CODEX-CAMPAIGN-VERIFICATION.md). Six enrolled recurring goals
are currently paused pending successful native provider acceptance.

## Native goal execution

The prototype Python coordinator has been removed. `scripts/campaign_ops` is now
a one-shot enrollment command; it writes only natural-language goals and original
schedules. The Rust Hub owns graph generation, atomic card/queue publication,
artifact handoff, bounded evolution and monitoring. See
[HIVE-ADOPTION-REVIEW.md](HIVE-ADOPTION-REVIEW.md) for contracts and actual limits.

## Internal repository transition

The live repository mapping now uses `/Users/kunkun/.adk/repositories/agentdesk`
on the internal APFS data volume (`/dev/disk3s5`). Its main checkout is clean,
HEAD is `4a10f1882c6d86cd64b2416a644e76aa772e4225`, and connectivity-only Git fsck
passed. Three preexisting upstream worktrees remain registered and untouched.

The old `/Users/kunkun/kunkunGames/agentdesk` resolves through a parent symlink to
`/Volumes/990PRO/MacMiniMigration-20260920/home/kunkunGames/agentdesk`.
Reading it timed out or returned `Interrupted system call`; rsync could not open
the source. The external original is preserved. This is a verified execution-path
transition to an existing internal clone, not proof that unreadable external
uncommitted changes have been copied.

After the remap, the two existing planning runs produced actual managed worktrees
and dispatches. Qwen dispatch `c073d2df-7c04-49b6-a734-367e4760ecaf` later failed
and its legacy automatic retry also failed. OpenCode dispatch
`10c7da5c-1cac-460b-bfce-60e70f7fdd5b` produced observed tools but no accepted
planning receipt. The two owned runs were ended through the canonical API; the
remaining owned Qwen inflight entry was cleared through the canonical lifecycle
without retry. Neither pilot is counted as successful graph/business execution.

The six source routines remain paused. Historically paused routines stay outside
the migration allowlist. New source registration preserves this boundary and
will not overwrite an existing goal contract or reactivate a paused goal.

## Previous v14 installation evidence

- Linux focused regression: **94 passed, 0 failed, 0 ignored**; settings PostgreSQL
  opt-in/restart tests, Git workspace isolation/reuse and dispatch/queue regressions.
- Historical Python prototype (now removed): **11 passed**; includes no-work completion, invalid DAG
  rejection, unrelated queue preservation and generated-batch recovery.
- Dashboard: TypeScript build, Vite build and **28 settings tests** passed.
- PostgreSQL lane inventory: **909 tests / 134 files**, membership/debt check passed.
  This inventory count is not a claim that all 909 tests were executed here.
- Native disposable-DB canary: two boots, authenticated API, persisted Campaign
  ledger, migration 134, owned test database removed successfully.
- Actual installation: healthy DB/dashboard, launchd service identity, explicit
  setting persistence across restart, served index equality.

Local evidence directory: `target/24x7-native/campaign-kanban-v14/`.
Important receipts: `source-files.json`, `internal-task-regression-exit.json`,
`activation-v14.json`, `runtime-v14-result.json`. Remote evidence is under
`~/.adk/runtime-data/campaign-kanban/`.

No actual model-generated DAG, business artifact handoff, self-healing graph
redeployment or measured 24-hour acceptance is claimed by these results.
