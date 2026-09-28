# Test phase run lifecycle

The existing `test_phase_runs` table records checks against a phase and candidate.
The `/api/cluster/test-phase-runs/start` and `/complete` APIs coordinate execution
ownership with the existing `resource_locks` table. They do not start a check
process or authenticate the contents of its submitted report.

## Identity and ownership

A request supplies `phase_key`, `head_sha`, and optionally `idempotency_key`.
The default key remains `phase_key:head_sha`. A key cannot be reused for another
phase/candidate, or for another card, repository, issue, or capability contract
once execution starts. An omitted optional contract field preserves the saved
value during start and owned completion.

Start also requires `resource_lock_key`, `holder_instance_id`, and
`holder_job_id`. The job ID identifies one execution attempt. Reusing the same
resource/instance/job tuple for another run is rejected, including after lease
expiry. New attempts need a new idempotency key and job ID.

## Start

Start serializes requests for the same idempotency key and commits the running
row together with its resource lease in one PostgreSQL transaction. A database
failure rolls both back. Twelve simultaneous identical deliveries can therefore
produce only one `started: true` response.

A running or terminal run returns `started: false` and the existing `run`.
The caller must not launch another process or reset that receipt. The existing
HTTP handler returns 409 for `started: false`; inspect the body to distinguish
an existing run from resource contention. Contention returns `current_lock`.
A queued unowned row can be started without losing its saved capability contract.

The resource lock retains its existing TTL and heartbeat semantics. Expiration
is coordination evidence, not proof that a child process or writer stopped.
The runner must reconcile or isolate old execution effects before reusing the
resource for a new attempt.

## Complete

For an owned run, complete must send all three exact ownership fields along
with its phase, candidate, key, terminal status, and evidence. Completion checks
that the saved run owns a live resource lease whose metadata identifies the
same run. A stale owner, expired/reassigned lease, changed candidate, or changed
contract is rejected before evidence changes.

The terminal result and optional lock release commit together. If releasing the
lock fails, the result remains running; callers cannot observe a successful
receipt from that failed transaction. The generic upsert endpoint cannot edit
an owned run or manufacture ownership.

Replaying the same terminal status/evidence/error returns the saved receipt
without changing its timestamps. A replay may release its own retained lock,
but cannot release a later execution's lock. Different terminal evidence is
rejected. Use a new attempt for a new result.

## Legacy reporting and verification boundary

Unowned upsert/completion remain available for existing reporting clients.
They do not acquire a resource and are not trusted managed check receipts.
The existing latest-passing-evidence endpoint remains a reporting query. Its
result alone must not be used to claim that all mandatory task acceptance
conditions passed.

The 24/7 W2 path still needs a protected check registry, immutable candidate,
actual subprocess execution and collection, mandatory acceptance coverage,
crash reconciliation, and the existing FSM success CAS. The storage lifecycle
is a prerequisite for that path, not its completion.

## Regression coverage

`src/server/test_phase_runs/lifecycle_pg_tests.rs` uses the existing disposable
PostgreSQL fixture to cover concurrent start, late completion after lease
expiry/takeover, ownership requirements, immutable terminal replay, candidate
and scope collisions, owner-token reuse, atomic rollback on storage failure,
queued contract preservation, delayed release, and legacy unowned reporting.
No model or external service is required beyond the isolated fixture database.

`scripts/probe_test_phase_lifecycle.py` also starts the actual Linux application
against a fresh database on the documented loopback PostgreSQL fixture. It
checks HTTP authentication, duplicate delivery, stale ownership, terminal replay,
and receipt persistence across service restart. Each service has an empty policy
directory, no configured providers or agents, and a temporary HOME/configuration.
The synthetic evidence proves API persistence; it is not an executed project check.
The harness stops its own service, removes its temporary configuration, and drops
only the database it created. Sanitized service logs and JSON results are retained.
