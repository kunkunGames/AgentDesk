# Native Campaign recovery

## Provider envelope and persisted runtime metadata

Provider result files use the strict native envelope and typed schema. Capture
validates both before adding its runtime-owned `summary`, `work_outcome` and
`completion_source`. `graph/result.rs` validates persisted dispatches by checking
only those recognized capture fields separately, then validating the original
provider envelope/schema. It requires the fixed capture summary, artifact
outcome and missing or recognized native completion source. Unknown fields,
forged metadata and malformed graph validation remain invalid. Runtime metadata
does not approve a DAG, accept artifacts or complete the business goal.

The audited reconciliation endpoint additionally handles an exact planning
failure caused by applying the provider schema to that captured runtime wrapper.
The error must identify an unexpected capture field. It requires the original
completed, passed, typed planner result with native provenance; no business task,
occurrence or non-rejected graph; no unknown external effects; settled owners
and cleanup; and finite remaining lifetime capacity. Recovered roots must be
artifact-only inspections. Operator correction evidence produces a new
candidate and fresh independent QA without another planner or resetting usage.
The original failure/result, rejected versions and unstarted repair card remain
preserved. Other failure shapes and exhausted budgets refuse this path.

Generic admission retries exclude native Campaign cards. Native reconciliation
retains ownership of held/running cards; ordinary non-native card retries keep
their behavior. This prevents repeated generic admission attempts against a
native hold without introducing a second retry owner.

The R2 source passed 303 Rust regressions with no failures/ignored cases and
40 Node timeout-policy tests, including real PostgreSQL recovery and SQLite
retry selection. Its CI inventory/selection has 942 PostgreSQL tests in 144
files with zero rule debt; not all inventory tests were executed. The scoped
maintenance comparison introduces no new findings, while the full audit retains
an existing concurrent `service_server_backflow` failure. Five isolated boots
and ten contract checks passed, followed by two settled production boots at
migration 139. Activation completed at `2026-09-30T21:19:11Z`, retaining settings
and all 28 Campaigns' contracts/history/usage. Actual Monolith version-3 QA then
passed and was accepted; its following prerequisite inspection was blocked by
the original native OpenCode skill gate in the Codex environment. Candidate
validation is not business acceptance. See `CODEX-CAMPAIGN-VERIFICATION.md` for
exact source/binary hashes, raw attempts and the six-goal terminal state.

The later v18 binary retains all R2 production Rust inputs and adds a
completion-capture test. All 3,266 forward source hashes, 285 executed regression
receipts and a separate five-boot/ten-check canary of that exact signed binary
were verified. The actual runtime timeout policy remains the separately
installed, hashed R2 policy; the v18 compile-source archive's older policy files
are not its provenance. Terminal verification confirms one passed goal, five
held goals and no owned active dispatches/open queues/cleanup. No earlier QA,
blocked receipt or lifetime usage was replaced.

## Pinned completion transport and lost validator recovery

Native turn completion and generic streaming finalization read the same bounded
`.agentdesk-campaign-result.json` before invoking the existing dispatch
completion authority. The reader requires the canonical dispatch workspace, a
fresh regular result file, the original JSON envelope restrictions and committed
tracked changes. An invalid native result cannot fall through to prose-based
completion. A stored provider receipt remains distinct from independent graph
approval and artifact acceptance.

The existing `POST /api/campaigns/{id}/execution/reconcile` also handles the exact
captured loss of an independent candidate validator's structured result. It
requires a terminal validation dispatch whose immutable DB result reports
`watcher_streaming_final` with `agent_response_present:true` and no native
receipt, no active graph, business publication or scheduled occurrence, no
unknown external effects, settled ownership/cleanup and remaining finite budget.
Every candidate root must be an artifact-only inspection. Operator correction
evidence is appended to the roots, the old candidate is rejected, and a distinct
candidate requires a fresh independent validator. The original validator,
failure and generic result are retained unaccepted. A retained result file may
diagnose transport loss; it must never be copied into the old DB result or used
as approval. Other failure shapes refuse this recovery atomically.

## Captured repair with a new artifact identity

A passed, checked, bounded agent-authored repair can itself be rejected because
it changes a published task's immutable contract under the same key. The audited
reconciliation endpoint narrowly recovers that exact planning failure without
dispatching another planner or changing lifetime budgets. For each changed
published contract it requires a failed, observed, unaccepted, non-running
artifact-only work task in the active graph, the same node/kind/agent and no
published descendants. Its proposed contract receives a distinct
`artifact-revision:<sha256>` identity. The old key, contract, receipt, failures
and charged usage remain intact. Accepted, running and effectful contracts,
unknown effects or proposals without a safely renewable identity are refused.
Only a candidate is staged; independent QA must validate the changed contract
before it can execute.

The earlier R1 isolated recovery source passed 292 Rust regressions with no failures
or ignored tests, including real PostgreSQL retention/refusal checks and native
streaming transport tests. Production activation completed at
`2026-09-30T18:20:08Z` after five isolated canary boots and two settled production
boots. That release used migration 138 and all 28 enrolled Campaigns' contracts and usage
were preserved. Fresh Skills QA then independently approved and activated
version 3, followed by accepted inspection and independent artifact QA. Actual
business acceptance later returned blocked because four provider links and
eligible regression/effect provenance remain unresolved. See
`CODEX-CAMPAIGN-VERIFICATION.md` for exact source/binary identities, actual
dispatch receipts and the separate business/24-hour acceptance boundaries.

## Captured planning recovery and finite budget correction

The preceding recovery release added an initial-planning branch to the existing
`POST /api/campaigns/{id}/execution/reconcile` endpoint. It requires a completed
infrastructure-blocked planning dispatch, its exact captured DAG, no prior
graph/business task/occurrence, no unknown external effects, settled owned
execution and queue cleanup, and remaining finite lifetime capacity. Every DAG
root must be an artifact-only inspection. Verified operator correction evidence
is appended to those roots for independent re-observation. The blocked planner
receipt and failure remain unchanged and unaccepted. Only a candidate is staged;
QA must independently validate it before any business work can begin. Invalid,
missing, oversized or effectful proposals are refused atomically.

`POST /api/campaigns/{id}/execution/budget` accepts `expected_revision`,
`request_id`, `evidence` and a complete `budget`. It requires pause and settled
ownership. Only explicit finite increases are allowed: node timeout and tool
limit mode remain pinned. Unknown outcomes or effect reconciliation holds cannot
be cleared. A budget hold clears only when the lifetime planning/version,
dispatch, time and tool limits leave capacity. The campaign remains paused.
Before/after goals are audited in migration 138's existing revision table; scope,
all graphs, published task keys, artifacts, old attempts, failures and usage
remain intact. New proposals still require independent validation. Full goal
changes retain the separate pre-business restrictions of `/execution/revise`.

The reviewed candidate also follows a verified `origin/HEAD`, then conventional
remote/local `main` or `master`, when creating worktrees. This fixes Monolith's
`master` checkout without inventing a `main` alias or moving existing work.
Source regressions passed 283 tests, including real PostgreSQL recovery and
budget refusal/retention checks and Git default-branch fixtures. Production
activation completed at `2026-09-30T16:12:09Z` after five isolated canary boots and
two settled production boots; all 28 enrolled Campaigns' contracts and usage were
retained. Installation and real business acceptance remain separate evidence in
the verification record.

## Bounded native handoff and evolution exhaustion

`GET /api/campaigns/{id}/execution` publishes `artifact_limits` even before
enrollment: `max_file_bytes=4194304`, `max_bundle_bytes=16777216`,
`max_outputs_per_task=16`, `max_inputs_per_task=32`. The domain contract in
`src/db/campaigns/graph.rs` owns these values. Graph validation, provider
instructions, capture and stored/pinned-file checks use that contract. Plan
declared outputs and mapped verifier inputs within both size and count limits;
do not silently omit evidence when splitting it. Capture rejects an oversized
file before freezing any bundle and reports its path and observed/allowed bytes.
These are bounded native handoff guarantees, not OS-level provider containment.

A terminal code/plan failure whose repair allowance is exhausted holds the
active campaign as `Plan evolution budget exhausted`. The original failure,
provider result, active version and charged usage remain. The canonical queue
termination/cleanup owner settles outstanding owned work. An independent batch
may observe that quiescent blocked result and continue other goals with no
dependency on it; it must still report the blocked goal and cannot report the
whole batch complete unless every selected goal passes.

The 2026-09-30 skills run exercised both the refusal and a real failure-driven
repair: a 6,245,678-byte history file exceeded capture's limit, then the repair
proposed too many output/input chunks. That release held the exhausted run
explicitly while preserving all 143 observed calls and 3,097.608443 seconds.
The contract release passed 277 regressions, an isolated four-boot Mac canary
and two settled production boots. See the verification record for receipts and
the remaining business/24-hour acceptance boundaries.

## Pre-business goal correction (migration 138 deployed)

`POST /api/campaigns/{id}/execution/revise` accepts `expected_revision`,
`request_id`, `evidence` and the full corrected `goal`. This operator endpoint
requires a paused campaign, no published business tasks or scheduled
occurrences, no active execution/queue ownership and no unreconciled unknown
failures. Repository, executors and schedule cannot change. Finite budget
adjustments, when explicitly supplied, are retained alongside prior values;
actual elapsed time, dispatches, events and graph versions are never reset.

Migration 138 retains before/after goals and evidence in
`campaign_goal_revisions`, exposed by the execution GET. A request ID is
idempotent only for identical content. Old candidate/validated/active graphs
are retired before the corrected goal can run. Successful terminal control
results not yet observed after pause are marked superseded without acceptance;
their raw dispatch receipts are preserved. Failed/unresolved control outcomes
cannot be silently discarded. The campaign remains paused. After explicit
resume, existing graph evolution must produce and independently validate a new
candidate. A correction is not permission to replay any business effect.

The migration-138 release was verified on the production Mac mini after forward
recovery on 2026-09-30. The original failed activation receipt is retained beside
the separate successful recovery receipt. The actual skills goal correction
preserved 47 tool observations and 573.7286 seconds of charged dispatch lifetime,
retired its narrowed candidate and required a fresh independent validation.
See [the live verification record](CODEX-CAMPAIGN-VERIFICATION.md) for the binary,
regression, restart and revision receipts. These checks do not prove completion
of all six business campaigns or measured 24-hour operation.

## Held artifact execution recovery

`POST /api/campaigns/{id}/execution/reconcile` records an operator's evidence
that a failed artifact-only inspection or execution can be observed again. It
reuses the active graph, replaces only the failed node's task key and appends
the evidence to that node's instructions. The result is a **candidate**, never
an approval or a completed task. The existing independent validation and
activation path must run before new work is admitted.

Request fields:

- `expected_revision`: the current campaign revision from `GET /api/campaigns/{id}`.
- `failure_id`: the captured failure ID from `GET /api/campaigns/{id}/execution`.
- `evidence`: nonempty operator observations, at most 8,000 UTF-8 bytes. Include
  the identified cause, the correction, exact verification commands and results,
  and any remaining uncertainty. Never include credentials.

Response: `{"candidate_version":2,"independent_validation_required":true}`.
Identical requests for the same failure return the same candidate. Different
evidence for an already reconciled failure is rejected. The audit record is
stored in `campaign_graph_reconciliations`, linked to the immutable original
failure and newly staged version. All changes commit atomically; a rejected
request leaves the hold intact.

The campaign must be active, internally enabled, held for an unknown or
infrastructure execution outcome, and quiescent after queue cleanup. The
selected task must be unaccepted, artifact-only, and part of the active graph.
No descendant may already be published. Other unknown failures, exhausted
budgets, in-flight work, existing candidates and stale revisions prevent
recovery. Completed effects and other nodes' contracts remain pinned. External
business effects are never retried through this endpoint.

The previous blocked card, provider result, failure category, observations and
graph version remain unchanged. Restoring a task's old dispatch to pending,
changing its receipt to passed, or cloning the campaign to reset budgets is not
part of recovery. Requirement redesign uses the existing failure-driven graph
planning path. The artifact environment branch preserves approved requirements;
the separate captured-proposal branch above can stage an already checked
agent-authored redesign under a distinct safe artifact identity and fresh QA.

# Archive deployment identity

`GET /api/health` exposes optional installer metadata under
`release_source.declared_build_provenance`: `binary_sha256`,
`source_manifest_sha256`, and `source_mode`. Digests must be lowercase SHA256.
These values are declarations from `runtime/release-source.json`; independently
hash the installed executable before relying on a binary identity. They are not
a claim that the current process image or source tree has been independently
verified by the health endpoint.

An archive build can truthfully lack `deployed_repo_head`. Its existing
`observation_status=partial` and `repo_head_missing` remain visible; a workspace
Git HEAD must not be substituted for the deployed binary. A diagnosis should
state the identity and evidence it actually needs and keep missing Git metadata
separate from confirmed service faults.
### Quiescent artifact reconciliation

Execution reconciliation accepts active or paused campaigns. A paused campaign
stays paused after the audit stages its candidate and records the original
failure's reconciliation. This permits a separate finite budget audit before
explicit resume publishes fresh independent QA. Reconciliation does not accept
the failed receipt, reuse a failed task key, reset spent usage or grant business
approval. The existing effect, descendant, cleanup, CAS and capacity guards
continue to apply; external-effect replay remains forbidden.
