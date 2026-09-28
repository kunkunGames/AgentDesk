# Recurring native Campaign operations

`native_operations_v1` makes Campaign the admission and pause authority for
existing recurring operations executed by Qwen CLI or OpenCode. Migration 0134
binds an existing execution definition and its future durable runs to one
Campaign node. The original Routine stays `paused` permanently. Historical
Routine runs remain available and are not relabeled as Campaign runs.

This profile uses the existing native script/CLI executor, timeout cleanup,
lease recovery, and `routine_runs` execution ledger. It is separate from the
Linux-only `managed_execution_v1` containment/check profile. A native completion
receipt does not prove the business result or a measured 24-hour run.

## Admission and lifecycle

The existing server scheduler calls Campaign reconciliation and admission on
each tick. Admission locks the source and Campaign and commits the new run and
Campaign checkpoint in one PostgreSQL transaction. A transaction advisory lock
serializes Campaign admissions. At most one native Campaign operation runs at
a time; sharing a native CLI identity with an existing Routine also blocks
admission. A delayed occurrence does not create a catch-up replay loop.

Successful occurrences preserve the Campaign as `active`, record the node's
last completed run, and advance the existing schedule. Script-only operations
may finish without starting a model. Model operations require a fresh context
and the current transcript's typed terminal outcome (`completed`) before
the Campaign records execution success. Text that claims success is insufficient.

Failure, interruption, and missing completion evidence block the node and pause
an active Campaign. Automatic Routine failure resume excludes adopted rows.
Recovery of an expired lease cannot silently replay the failed occurrence.
Other independently eligible Campaigns may continue after the held run closes.

Pausing a Campaign prevents new admission; an already admitted run retains its
timeout and normal completion lifecycle. Stopping an active provider session
uses the existing native session controls and must retain the interrupted run.

## API

Authenticated `GET /api/campaigns/{id}/operations` returns the bound definition
and the last 20 native run receipts. The full execution history remains in the
durable run table and existing Routine history API.

Authenticated `POST /api/campaigns/{id}/operations` adopts a definition:

```json
{
  "routine_id": "existing-routine-id",
  "node_id": "operation",
  "expected_revision": 1,
  "expected_routine_updated_at": "2026-09-28T00:00:00Z",
  "next_due_at": "2026-09-29T00:00:00Z"
}
```

Create a planned or paused Campaign with one pending node, no dependencies, and
an assignee/provider matching the existing executor. Adoption requires no
in-flight source run, `fresh` execution, a finite timeout of at most 1800 seconds,
zero retries, and no fallback agent. Concurrent source changes or Campaign
revision changes reject the transaction. The shared native runtime must be
enabled in server configuration; independent Routine schedules stay paused.

Use the existing Campaign PUT API with `expected_revision` to activate or pause
the Campaign. After inspecting a failed run and its possible external effects,
explicit rearming requires both `status: active` on the Campaign and
`status: pending` on its node, retaining that failed run's `session_id`. If failure
cleared the next due time, this explicit rearm admits the next attempt immediately.
Merely activating a blocked Campaign does not replay it.

Database guards prevent resuming the original Routine, changing its execution
binding, replacing adopted Campaign instructions or executor, changing the bound
agent's provider, and attaching a competing Kanban execution owner. New work or
a changed execution contract requires a new definition/Campaign.

## Migration scope and verification

Migration tooling must use an explicit audited source set. Historically paused
or detached routines are excluded. A temporary maintenance pause may be included
only when the exact recorded pause revision still matches. Bind the whole set
before activating it, verify that every original remains paused, and compare
excluded rows before and after cutover.

The PostgreSQL tests in `services::routines::store::campaign::tests` cover atomic
claims, old-scheduler exclusion, immutable instructions/provider, stale source
rejection, failure hold, recurrence, explicit rearm, and restart reconstruction.

On macOS, `scripts/probe_macos_runtime.py --native-operations` additionally boots
the real server against a disposable PostgreSQL database and exercises HTTP
adoption/authentication, real scheduler execution, recurring runs, pause/restart,
and failure/restart. It uses a local script fixture and does not execute a model.
Production Qwen/OpenCode runs, their business effects, installed service recovery,
and the measured observation window require separate operational receipts.
