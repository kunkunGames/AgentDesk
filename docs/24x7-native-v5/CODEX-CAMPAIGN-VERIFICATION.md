# Codex campaign verification — 2026-10-01 KST

The requested six-campaign Codex run is **1/6 completed** as of the actual native
ledger verification at `2026-09-30T21:45:20Z` (`2026-10-01 06:45:20 KST`). Health completed through its approved
DAG, inspection artifacts, independent QA and canonical card settlement. Skills
has independently approved version 3 and accepted inspection/independent QA,
but its actual acceptance assessment is blocked. Upstream's fresh version-2
validator ended when its tmux session died mid-turn; it has no terminal result
file and remains unaccepted. Monolith's fresh version-3 validator was accepted,
but the subsequent real prerequisite inspection returned blocked.
Review and merge retain unresolved ownership/effect holds. Periodic parents and
old routine schedules remain paused during this manual acceptance run. The
sequential observer finished at `2026-09-30T21:31:02Z` after all six goals had
been observed and their owned dispatches, sessions, inflight records, queue runs
and cleanup had settled. `complete:false` is the actual batch result.

| Goal | Actual state at this snapshot | Business acceptance |
| --- | --- | --- |
| Health | Completed; two accepted work nodes and three accepted controls | Passed |
| Skills | Inspection and independent QA accepted; four provider links and regression provenance remain unresolved | Blocked |
| Upstream | Fresh version-2 QA failed when its tmux session died mid-turn; underlying provider/signal cause is not established | Blocked |
| AgentDesk review | PR #2212 inspection blocked on unresolved Jules ownership | Blocked |
| AgentDesk merge | PR #2239 has an empty observed diff; ownership and required-check evidence unresolved | Blocked |
| Monolith | Version-3 candidate QA accepted; real prerequisite inspection stopped at the original native OpenCode `git-help` gate in the Codex catalog | Blocked |

## R2 release and real independent QA

The concurrent migration-139 schema change applied the strict provider JSON
schema to the persisted dispatch wrapper. That wrapper legitimately includes
runtime-owned `summary`, `work_outcome` and `completion_source`, causing a valid
captured planner result to fail with `unknown field completion_source`. The new
`src/db/campaigns/graph/result.rs` validates those three recognized runtime fields
separately, then applies the strict provider envelope and typed schema. Unknown
fields, forged metadata and malformed graph validation still fail. Provider
result files still reject runtime metadata; capture validates their typed schema
before adding the wrapper. Metadata grants no artifact or business acceptance.

The existing audited reconciliation endpoint now narrowly stages a candidate
from that exact captured passed planning failure before any business task or
occurrence. It requires native capture provenance, no live graph or candidate,
no unknown effects or unsettled owners, finite remaining lifetime capacity and
artifact-only inspection roots. It retains the original result, failure,
candidate history and usage and requires fresh independent QA. The generic
timeout policy also excludes native Campaign cards from its admission-retry
query, leaving reconciliation with their existing native owner. Ordinary card
retries retain their behavior.

The exact source passed **303 Rust regressions, zero failed/ignored**, including
real PostgreSQL capture/recovery, history/usage retention, refusal and fresh-QA
checks. **40 Node timeout-policy tests passed**, including the actual SQLite
selection of native-held/active cards versus ordinary retries. CI membership
and PR/nightly selection passed for **942 PostgreSQL tests across 144 files**,
with zero lane rule debt; this is inventory coverage, not execution of all 942
tests. Test-target integrity passed. The scoped maintainability comparison has
no new findings versus installed v17. The full audit still exits 1 for the
existing `service_server_backflow` in `intake_turn.rs` calling server-owned
learning preparation; this existing concurrent change remains visible and is
not waived or claimed fixed. Receipt: `maintainability-r2-comparison.json`.

The 13-file overlay SHA256 is
`fab4c6d82b34ad266bb22118f8c259b497b0aba7a325adb767c3af09a3551c71`.
All **3,265 compile inputs** are pinned by manifest SHA256
`2ace777b9ad3a3da3e432528c0b0f9970c369cbdcbf02c70aca20a6bc1ed67f1`.
The build preserves the exact installed v17 DAG-template/schema/learning changes;
it does not rebuild from a mutable concurrent checkout or roll back migration
139. The first owned Mac build hung because the default Cargo registry and
Rust toolchains resolve onto external 990PRO. Only that verified owned build was
stopped; its exit -15 and log remain. The successful build pins existing internal
`CARGO_HOME=~/.adk/cargo`, `RUSTUP_HOME=~/.adk/rustup` and
`RUSTUP_TOOLCHAIN=1.94.1-aarch64-apple-darwin`, and took 365.28 seconds.

The first canary retained a harness import failure after two boots. Its separate
retry packages two hashed Python probe modules without changing compile inputs.
The signed binary then passed **10 contract checks across five isolated boots**
at migration 139; the disposable database was dropped. Production activation
completed at `2026-09-30T21:19:11Z`, receipt
`activation-v15-native-result-contract-r2.json`. Installed SHA256 at that activation:
`cf34ead717a50a471f312564059e0f145f1e558b593a85bd755dd5c63a7b5822`.
Two settled production boots had **23 connected providers, zero doctor failures
and two existing warnings**. Settings and all 28 enrolled Campaigns' goals,
revisions, graphs, tasks, failures, occurrences and cumulative usage were
preserved. The exact generic timeout policy was installed as well. Archive
provenance still has no Git HEAD; measured 24-hour acceptance remains unproven.

Monolith's immutable planning failure
`e947005b9e4d44a518e6447cea07d2c3b5c2af844cd15956735ce04e52382e92`
was reconciled into version 3 without another planner or budget reset. Fresh QA
card `0e1576f4-4f70-2275-7d20-54a87229c4cb`, dispatch
`c5448f6f-14da-454e-bc22-17aa09d05af9`, completed with a captured typed result
and was accepted. This proves the wrapper fix in the actual Mac runtime. QA
explicitly approved only a prerequisite artifact DAG; it did not claim current
PR head/CI, review, mutation or business completion.

The following actual coder dispatch `80e85fb3-085a-4251-8f47-12a27cd39a99`
returned `blocked`. It observed the original mandatory native OpenCode
`git-help` load-or-stop rule and the absence of that loader in the active Codex
catalog, with `business_authorized:false` and no external effects. The native
ledger retained infrastructure failure
`2a3d605a4c02f7c16dcd8c78a5fad57c22c1ed791b077c705035633cdf1b588c`,
no accepted business artifact, **80 observed tools and 1,752.877876 seconds**.
The blocked node does not publish its downstream QA. The previous unstarted
planner card remains preserved; native ownership and finite evolution limits
prevent its generic retry. There was no PR review/mutation or task replay.

Upstream's fresh QA dispatch `1186404a-7486-4488-876a-9621c441e071`
failed at the provider/session boundary. Bounded event inspection found 21 tool
results, no terminal result event and no result file or exit-reason sidecar.
The session-ended event precedes cleanup by ten seconds, so cleanup is not
evidence of the cause. The underlying termination cause remains unestablished;
its failure, candidate and **71 tools/1,427.625439 seconds** stay unaccepted.

The first R2 observer retained an unexpected-EOF read error while the native QA
continued. A separate controller added bounded retries for read-only EOF/reset
errors, preserved the interrupted receipt and reattached to that same dispatch
without a new POST. Receipt: `codex-batch-r2-observer-retry1-20261001.json`.
It finished with one passed goal and five blocked goals, no owned active work
or queue cleanup. The terminal read-only native evidence is
`native-result-recovery-live-proof-r2-terminal-20261001.json`; all prior raw
attempts remain under their original names.

## Current forward release retains the R2 correction

A further independent deployment installed v18 at `2026-09-30T21:35:44Z`, after
the six-goal observer had finished. The initial R2-pinned terminal verifier
correctly refused the changed binary; no state was mutated by that check. The
separate forward compatibility verification at `2026-09-30T21:45:20Z` checked
the current binary's signature and SHA256, all **3,266 source hashes**, successful
source-bound Mac build and **285 executed regressions** including both new R2
PostgreSQL cases and metadata/schema checks. Its source differs from the R2
inputs only in documentation, two policy source files and an additional
`#[cfg(test)]` completion-capture test. All production Rust inputs and the
completion-capture production body are unchanged. Current installed SHA256:
`2b0bc5be373858ce486e9b77b9f6279736ed63f94aadd95103eb31960154058b`.
Current source manifest SHA256:
`2c160405c512412fd8f1e08f64afa0eacab6279e0cddf932189a4b672697c554`.

The actual installed timeout policy retains the R2 native-ownership exclusion
with SHA256 `29a17ec6b211ec565b3580848385a6fbdf0851374ac4184b3b9841bc53e36d71`.
That policy is verified separately against the R2 40-test source; v18's compile
source archive contains the older policy files and is not claimed as provenance
for this separately installed runtime policy. No foreign source or deployment
was overwritten. The exact signed v18 binary additionally passed a new,
independent **five-boot, ten-check** disposable native canary at migration 139;
the temporary database was dropped. This canary executes no LLM/business work.

The authenticated live service was healthy and fully recovered, with 23
connected providers, zero doctor failures and two existing warnings. Global
active/finalizing work, all six goals' active dispatches/open queue runs and
pending/dead-lettered cleanup were zero. Both owned executors remained Codex
CLI with `codex-alt-3`; all six periodic parents remained paused. The six goals'
revisions, holds and cumulative usage exactly matched the earlier R2 terminal
snapshot. Monolith's real QA ran under R2; the later binary compatibility check
does not relabel it as a new v18 execution. Terminal receipt:
`terminal-codex-forward-v18-20261001.json`. Batch completion remains **1/6** and
measured 24-hour operational acceptance remains unproven.

## Earlier R1 native result and captured proposal recovery

Two independent validators wrote bounded completion JSON in their assigned
workspaces during their own attempts, but their immutable DB results contained
only `watcher_streaming_final` and `agent_response_present`. The generic streaming
finalizer could settle the dispatch before the native completion owner read its
result file. Both paths now use the same pinned native reader before prose
completion. Missing, stale, foreign, unsafe or uncommitted results fail closed;
the existing canonical dispatch owner still performs settlement. Receipt capture
does not itself approve a graph or accept an artifact.

The existing audited reconciliation endpoint now permits fresh independent QA
of a never-executed candidate affected by that exact transport loss. The old
validator, result and failure remain unaccepted. Its retained file is diagnostic
evidence only, never substituted approval. The old candidate is rejected and a
distinct candidate includes the correction evidence in artifact-only inspection
roots. Business publication, scheduled occurrences, unknown external effects,
unsettled ownership and exhausted lifetime budgets refuse this recovery.

Skills also had a valid bounded agent-authored repair that reused its failed
published inspection key with a changed contract. Recovery now gives only that
failed, observed, unaccepted artifact contract a distinct identity, preserving
the original key, receipt and failure. Accepted/running/effectful contracts and
published descendants cannot be changed. No additional planner or budget reset
was used. The fresh QA card `69080492-6b2e-0480-b20b-f39a8b07f36c`, dispatch
`320f6f89-6a9d-4da1-93d0-57535cad628f`, completed with a structured receipt and
approval of the exact version-3 digest. The native ledger accepted that validator
and activated the graph. This approves the proposed work, not the original skill
synchronization goal. Its new inspection card
`602a4623-b07c-228c-ae1c-8bd21aded661` completed against the original seven
destinations and both mirrors and was accepted. An earlier live probe verified
12 actual calls/results with all persisted input fingerprints and output digests
matching. The independent inspection QA card
`a0077446-0c05-7d76-ee92-c928af2b26cd` passed: its receipt reports 31 checked
claims, 68 provided fixtures and additional independent guard/provider,
confinement, freshness and boundary fixtures. A separate file-level probe
verified all 12 producer and QA materialized file hashes against the accepted
manifest: total 3,170,174 bytes, largest file 1,048,733 bytes, bundle
`7fac0988895d2faee5f5788bcb8b70ea41521ebff0cf7fae9e519f834831c7e8`.
The post-QA receipt `native-handoff-skills-inspect-20260930T190736Z.json` verifies
that all hashes still match after the consumer was accepted. The earlier
`native-handoff-skills-inspect-20260930T185814Z.json` is also retained. This proves
actual artifact materialization and independent QA; business completion remains
separate.

The acceptance dispatch `c1b30df5-7703-4af8-8149-c8f4e15ec526` wrote an actual
blocked receipt. It accounts for all seven destinations and both mirrors, but
the Qwen workspace Claude destination lacks `gh-pr-summary`, `memory-merge`,
`memory-read` and `memory-write`. Twenty eligible regression claims lack executed
source/input hashes, exact command logs, installed-state relevance and safety
proof; five no-test skips remain explicit. The daily degraded guard, missing raw
report, maker/qwen scope mismatch and unknown historical effect ownership were
retained without replay. The ledger holds that unaccepted result and preserves
237 observed tools and 7,274.421165 seconds. Artifact-only fixture success was
not used as business acceptance.

The exact isolated source passed **292 Rust regressions, zero failed/ignored**,
including real PostgreSQL retention/refusal tests and native streaming transport
tests. API coverage and maintainability passed with no new hard-gate debt. The
same isolated source has **933 PostgreSQL tests in 142 files** with zero lane
rule debt and valid PR/nightly selection; this is inventory coverage, not
execution of all 933 tests. Other DAG-template changes in the shared checkout
were excluded from this build and are not covered by this regression claim.
The shared checkout's generated lane selection at that time was separately checked:
**939 PostgreSQL tests in 143 files**, zero rule debt, with its independent
DAG-template tests retained. The stale non-PG filter was regenerated from that
actual inventory; PR and nightly consumers now agree. This is a selection check
for the shared tree, not a compiled/runtime claim for its concurrent changes.

The final 75-file overlay SHA256 is
`fe30b593b474106672dcf55f53ab4e16d70bd2dd6b25f7a8d350941388ff4fe4`.
All 2,400 compile inputs are pinned by manifest SHA256
`97b2dbf894f9af5c43ac82b5ecdd8d0698a119218cd875cffa19ab59467d8388`.
The Mac build passed, and the signed candidate passed ten isolated contract
checks across five boots; its temporary database was dropped. Production
activation completed at `2026-09-30T18:20:08Z`, receipt
`activation-v15-native-result-contract-r1.json`. That release installed SHA256:
`0de60a16d3b8a46745922df733bf8dd02238f656301e41f227524cc084a6f264`.
Two distinct settled production boots had 23 connected providers, zero doctor
failures and two existing warnings. Settings and all 28 enrolled Campaigns'
goals, revisions, graphs, tasks, failures, occurrences and cumulative usage were
preserved exactly. That release used migration 138. Archive provenance still lacks a Git
HEAD, and measured 24-hour acceptance remains unproven.

The sequential observer started at `2026-09-30T18:21:21Z`, PID 94673, receipt
`codex-batch-result-recovery-observer-20261001.json`. It recovers each eligible
goal only at that goal's turn and advances only after owned dispatch, provider,
session, queue and cleanup settlement. Previous blocked attempts and lifetime
usage remain in the report. `native-result-recovery-live-proof-r1.json` captures
the earlier native state and immutable dispatch result hashes. The later
`native-result-recovery-live-proof-handoff.json` records this snapshot with
accepted artifact manifests; earlier evidence is retained under its original
name. It exited on a connection refusal during the concurrent forward
deployment below; its error and original code/receipt remain retained.

## Concurrent forward deployment and resumed validator recovery

A separate DAG-template deployment changed the installed binary and applied
migration 139 at `2026-09-30T19:24Z`. Its original attempt receipts retain
`activated:false` and a launchd bootstrap exit-code-5 failure. The separate final
`activation.json` subsequently recorded `activated:true` at
`2026-09-30T19:42:35Z`; the earlier failed attempts remain intact. The actual
service returned.
Independent read-only verification at `2026-09-30T19:43:34Z` checked the signed
installed/canary binary, all 3,262 source hashes, imported build/regression exit
records and the same-binary two-boot migration-139 canary. All 16 scoped recovery,
receipt and regression files were byte-identical to the 292-regression source.
Installed SHA256 at that verification:
`9ee3f5afc72ac7e6baf9bb774fbde9cd3870d677f9cce0011d951f0c764ed291`.
Source manifest SHA256 at that verification:
`f889c8d64784318c2a595c4b33c39c8f56746d9737563226f0e66d0f2c0bfdd3`.
The actual service was fully recovered, with 23 connected providers, zero doctor
failures, two warnings, no active/finalizing work or cleanup, internal admission
enabled and both owned executors still using `codex-alt-3`. The compatibility
receipt is `forward-template-release-compatibility.json`; this does not claim
ownership or complete acceptance of the separate template deployment. No binary
replacement or rollback to migration 138 was performed.

The same independent worker subsequently installed v17 at
`2026-09-30T20:30:37Z`, binary SHA256
`ceadf2948e086379093a1617ebd398e6cea62d6ea058bd2b96822354ede87df5`,
source manifest SHA256
`d79b0d2b97389e203303a5af4e6f153c55bbd8d9430a964cc9d3905fc7386234`.
Its 281-test build/regression receipts are separate evidence. The six v17 source
changes, including learning-sample and CI changes, were preserved in the R2
build above. Immutable source archives, rather than the subsequently changing
physical checkout, establish that baseline. These releases do not prove ten
real learning samples or measured 24-hour operational acceptance.

The first forward observer retained a PostgreSQL nullable-result parsing error
before either validator recovery request was submitted. The lookup now returns
explicit JSON `null`; its original module and log are preserved. The corrected
observer started at `2026-09-30T20:03:33Z`, PID 99536, receipt
`codex-batch-forward-observer-retry1-20261001.json`. It retains the earlier service
interruption and blocked business attempts, permits bounded retries only for
read-only observation connection errors and recovers only upstream/Monolith's
exact lost validators at their sequential turn. Skills, review and merge are
not replayed. Upstream's old candidate is now rejected and version 2 is staged;
the old validation result/failure and all 50 tool observations remain unchanged.
Only a fresh independent QA may approve that candidate.

## Earlier default-branch and planning recovery release

The same GitHub discovery failure affected upstream (22 observed tools), review
(11), and merge (18). Homebrew's `gh` resolves to the external 990PRO offload;
even `--version` did not return within 30 seconds. The owned internal `gh` 2.92.0
returns promptly and performs real authenticated PR discovery. The installed
repository-scoped helper now pins that internal executable with its original
repository/action/PR/admin/deletion allowlists unchanged. All six helper tests
passed on the Mac; the exact formerly hanging AgentDesk list returned 27 rows
in 5.27 seconds and Monolith list returned in 0.53 seconds. The helper receipt
is `github-helper-internal-binding.json`; no credentials were inspected.

Monolith's existing repository and current GitHub default both use `master`.
Its pending queue repeatedly failed checkout creation with `invalid reference:
main`; no provider dispatch or business effect occurred. The exact campaign and
queue run were paused, preserving the card/entry, and the previous observer stopped
in `quiescence-v15-recovery.json`. This is a pending admission, not a successful
execution. The source correction follows the repository's verified default and
keeps remote baseline semantics.

A new recovery candidate preserves the three raw blocked planner receipts while
staging only their captured DAGs for independent QA. It also permits an explicit,
finite budget-only increase without changing published Skills contracts or
resetting usage. **283 regressions passed, zero failures/ignored**, including real
PostgreSQL retention/refusal checks and Git default-branch fixtures. API coverage
and maintainability passed. CI membership passed for **928 PostgreSQL tests in
139 files with zero rule debt**; this is inventory coverage, not execution of all
928 tests. The exact 66-file overlay has SHA256
`8a477e61848db70ea55e93cb7844b2585c4b223482d320e774bc5a6d429df929`.
The Mac build passed. Its isolated canary passed ten contract checks across five
boots, including authenticated budget revision and restart retention, and dropped
the temporary database. Production activation completed at
`2026-09-30T16:12:09Z`, receipt `activation-v15-recovery-contract.json`. Installed
binary SHA256:
`c50d388ae3b35f2c45be9a9e0c1862751cd49e47a49e8416e39af6a9df6dd35c`.
Two distinct settled production boots had 23 connected providers, zero doctor
failures and two existing warnings. Settings were restored exactly. All 28
enrolled Campaigns retained their goals, revisions, versions, tasks, failures,
occurrences, holds and cumulative usage. Archive Git HEAD remains unavailable.

The new sequential observer started at `2026-09-30T16:14:33Z`, PID 45617, with
receipt `codex-batch-recovery-observer-20261001.json`. It applies an audited native
recovery only at each goal's own turn. Skills explicitly increased lifetime
dispatch allowance 20→30, evolutions 2→3 and observed tools 400→600, retaining
9,649 seconds, 1,800-second node timeout, all seven destinations/both mirrors,
existing graphs, failures and all previous 143 tools/3,097.608443 seconds. Its new
planner card `f8a83fc3-159c-e1af-7607-7001eb596680`, dispatch
`13fa6871-f6c3-4050-99ac-78a0303bd6a6`, is a real Codex execution. Candidate
staging, budget correction and this running snapshot are not business acceptance.

## Earlier artifact contract release and terminal evolution state

The fresh full-scope skills planner completed 14 actual tool calls/results and
independent candidate QA completed 21, with matching live hash observations.
The four-node version-2 DAG was activated. Its inspection then generated
`baseline.json` (2,868,818 bytes) and `history.json` (6,245,678 bytes). The latter
exceeded native capture's 4-MiB file limit and was refused. The provider's
`passed` receipt did not create an accepted manifest or business acceptance.
The captured failure drove a new real planner execution. That proposed 36
outputs and 36–38 consumer inputs, exceeding native counts of 16 and 32, and
was rejected. Original receipts, files, graph versions and failures remain.

The runtime now publishes shared native handoff limits in provider instructions
and `GET /api/campaigns/{id}/execution`: 4,194,304 bytes per file, 16,777,216
bytes per bundle, 16 outputs and 32 inputs per DAG task. Capture errors identify
the path, observed bytes and limit. All ten previously missing Campaign and
managed-check API method/path pairs are present in HTTP API discovery. An
active graph whose terminal repair exhausted its evolution budget now becomes
an explicit hold instead of silently waiting for another impossible repair.

Validation: **277 Rust regressions passed, zero failed/ignored**, including real
PostgreSQL evolution/ownership tests, artifact boundaries, provider framing and
API documentation. Maintainability and API coverage passed. PostgreSQL lane
membership passed for **924 tests across 137 files with zero rule debt**; that
inventory is not execution of all 924 tests. The isolated Mac canary passed
four boots, the new discovery/limits checks, revision authentication and
restart persistence; its temporary database was dropped.

Production activation completed at `2026-09-30T14:07:30Z`. Installed SHA256:
`08efc3616491f3835e2a436f55566b079338dc051de57902c4401682ee5553fd`.
Two distinct settled production boots had 23 connected providers, zero doctor
failures, two existing warnings and no cleanup backlog. Runtime settings were
restored exactly. Skills became `Plan evolution budget exhausted` with **143
observed calls and 3,097.608443 charged seconds unchanged**, and identical
goals, revisions, tasks, versions and captured failures. The initial install
preflight refused to stop an observer because Homebrew exposed its resolved
interpreter and `-u` option; it changed no binary or settings. Its failed receipt
is retained and hash-pinned by `activation-v15-api-docs-retry1.json`.

The earlier observer resumed as PID 9894 with both provider ownership and owned queue
run/cleanup settlement required before the next independent goal. It records
skills as blocked and proceeds without resetting usage or accepting the failed
output. Upstream planner card `ec626539-0512-7a07-3a42-cf59a7acdbf3`, dispatch
`b6c58f96-f3b9-4606-8bd5-b0869f9d9bf7`, was a real Codex execution. A live
snapshot verified 20 calls/results, no missing IDs and matching persisted input
fingerprints and output digests. This active snapshot is not terminal business
acceptance; the terminal result later retained its infrastructure block. The
observer was subsequently stopped for the recovery build above. Archive
provenance still lacks a Git HEAD, and measured 24-hour
acceptance remains unproven.

## Earlier tool identity and pre-business scope correction

Before the audited correction, the successor was **paused at revision 2**, before any
business task admission. Its planner submitted a 12-node DAG and QA completed
with `approved:true`, but both interpreted the assigned execution worktree as
excluding the original routine's configured workspace destinations. Direct
inspection of `provider_targets` and the filesystem confirmed three existing
targets under `~/.adk/release/workspaces/qwen`: `.claude/commands`,
`.gemini/skills`, and `.qwen/skills`. These are business destinations, not an
instruction to change the execution checkout. Four home targets alone do not
satisfy the original routine's scope. The QA receipt is retained and is not
treated as business acceptance. No business task was published before pause.

Both terminal control executions now have complete live telemetry evidence:
planner **21 calls/results** and QA **26 calls/results**, with all fingerprints
and output digests matching and no missing IDs. QA card is
`1e00669b-b8f8-9e6e-83d8-60b321fc8a83`, dispatch
`b3ce28b1-00c1-4d3e-8bfe-169403bc83b7`. Its result remains unobserved in the
paused graph ledger; retain the original result and do not activate it under a
corrected goal.

The source now contains an audited, pre-business goal revision API and migration
138. It retains the campaign, previous goal, usage and attempts, supersedes old
control acceptance without changing raw receipts, and requires fresh planning
and validation. Python enrollment/succession/history tests pass (10). The
CI inventory includes 923 PostgreSQL tests across 137 files with zero rule debt;
inventory membership is not execution of all 923 tests.

The first expanded Rust regression had 258 passes and one failure: the new
success-path fixture left a generated Auto Queue run open, and the API correctly
refused revision before cleanup. The corrected fixture asserts that refusal,
invokes the canonical queue termination owner, then verifies revision and
preserved usage. The fresh regression passed **259 tests, zero failed, zero
ignored** in 120.61 seconds. Its initial failure remains in the earlier log.

The macOS candidate built successfully and an incremental confirmation against
the final source completed. SHA256 is
`d0a1d4400a02548d7af1d846becc091b24e12bec0ba5e043bdcf4a35beeb6d4f`.
An isolated migration-138 canary passed four boots, unauthenticated/active-goal
revision refusal, paused revision, idempotency and restart persistence of both
old and corrected goals; its test database was dropped. The production install
committed migration 138, but its second boot stalled with one disconnected Codex
gateway. `activation-v15-goal-revision.json` retains `activated:false` and the
startup failure; rolling back to the migration-137 binary was correctly refused.

Forward recovery completed at `2026-09-30T12:27:48Z`. Two distinct production
boots finished provider reconciliation and startup doctor, with 23 connected
providers, no failed doctor checks, no degraded reasons, no cleanup backlog and
preserved settings. The installed executable matches the candidate SHA256 above,
and all source manifest hashes and migration 138 were independently checked.
`activation-v15-goal-revision-recovered.json` pins the original failed receipt's
hash instead of rewriting it. The provenance manifest now reports the actual
binary and migration 138; missing Git HEAD remains a partial source observation.
The revision API is **installed and exercised**. Two existing doctor warnings
(unregistered routine scripts and binding metadata) remain explicit. Restart
acceptance now waits for completed doctor and provider recovery rather than
accepting the earlier `doctor_pending` HTTP response.

The prepared skills correction explicitly includes the three original workspace
targets without changing execution cwd. Its finite limits are proposed to move
from 15 to 20 dispatches, one to two evolutions and 218 to 400 observed calls,
with both old/new limits audited and spent usage retained; elapsed limit remains
9,649 seconds. These are explicit extensions for the corrected full scope and
fresh plan/QA, not counter resets. The request was applied at
`2026-09-30T12:28:26Z`, campaign revision 3, with **47 tool observations and
573.7286 elapsed dispatch seconds preserved**. The prior candidate was rejected
and its unobserved QA result was superseded without business acceptance. The
batch observer and acceptance audit require a complete operator revision chain
when the live goal differs from the original enrollment receipt.

The same skills campaign resumed at revision 4 at `12:30:40Z`, under the original
`20260930-codex04` batch. The previous observer was proven absent before starting
its replacement. Native Hub admission dispatched fresh planner card
`1934b265-47bd-cf02-9f8c-65de32f7ee71` as
`37953f67-4ed9-4014-98e1-41f9e15f9664`. Actual tool observations are increasing;
fresh planning and independent validation remain in progress. This is execution
evidence, not a second completed campaign or 24-hour acceptance.

At `2026-09-30T02:44:14Z`, the validated tool-ID correction was activated with
SHA256 `ffe45cbe23395035ec36b61bb6b71a58917c548772a4625faa000e06b056d31c`.
Evidence: 257 regression tests passed, three isolated macOS canary boots passed
(test DB dropped), two production restarts passed and settings were preserved.
The subsequent runtime audit found healthy API/DB/dashboard, no degraded
provider, no pending/dead-letter cleanup, migration 137 and unchanged paused
periodic parents/routines. Doctor remains warned; archive source provenance
has a verified manifest but no Git HEAD. Neither limitation is labeled complete.

`ops-codex-skills-20260930-codex04-scope2` is now enrolled through canonical APIs.
Its predecessor remains paused with the original reconciliation hold and
unchanged immutable goal/failures. Remaining budgets are 15 dispatches, 9,649
seconds, one evolution and 218 observed tool calls. The sequential observer
tracks this explicit successor before admitting upstream/review/merge/Monolith.
Plan card `86b6e6b4-6377-a126-f374-baa49630bc16` is executing as dispatch
`004f505c-081d-41ca-9074-30752ea2bfe5`; actual rollout confirms `codex-alt-3`.

The live audit found **16 wrapper calls, 16 results, 16 stored calls, zero missing
IDs, 16 matching fingerprints and 16 matching output digests** while that card
was running. The active path is the stream bridge: session_backend pretty-JSON
input strings are hashed with the tool name, and output digests hash raw content
bytes. The watcher uses a different existing JSON-value representation; the
audit explicitly distinguishes both, rather than comparing the wrong encoding.
This proves live hash-only progress capture on a real admitted card. It does
not prove business completion, provider settlement, all-card coverage or a
24-hour run. Receipt: `live-toolids-86b6e6b4-6377-a126-f374-baa49630bc16.json`.

## Earlier skills succession and scope correction

At `2026-09-30T02:23:56Z`, completion remains **1/6**. Skills DAG version 2
was independently approved, but its first business inspection returned blocked
at `02:20:45Z`; the sequential observer exited after confirming no owned active
dispatches, provider sessions or inflight records. The infrastructure hold is
preserved. Both coder and qa were read back as Codex CLI / `codex-alt-3` /
`gpt-5.6-terra`; their recent provider executions completed without quota errors.

The migrated skills scope incorrectly mandated the PR-only `routine-gh.py`
helper. The original skill workflow requires all configured skill destinations
and both configured private mirrors. Enrollment now retains their allowlists,
existing authentication, daily duplicate guard and independent per-mirror
verification, without permitting new targets or memory/routing writes.
Existing immutable goals have not been overwritten.

`campaign_ops.succession` prepares a manual successor only from a paused,
fully accounted predecessor whose dispatched business nodes were artifact-only.
It refuses external effects, incomplete observations, active dispatches and
exhausted budgets. Eight enrollment/succession unit tests pass. The production
read-only preview found five completed provider executions and **42 raw provider
tool calls**, despite missing live graph telemetry. A second comparison with
actual normalized wrapper files found **82 tool-use records** (15/18/21/11/17
across the five executions). The successor conservatively deducts the larger
count for each execution; raw provider calls do not equal wrapper execution
units. Remaining limits are 15 dispatches, 9,649 seconds, one evolution and
**218 observed tool calls**, correcting the initial 258-call preview.
`skills-successor-preview.json` is a proposal, not enrolled execution; prior
failures, evidence and unverified outcomes remain preserved.

The tool-ID correction's Linux test build initially failed because four helper
functions were not imported in the new test module. Imports are corrected;
the Linux test build now passes, and the expanded native regression completed
with **257 passed, zero failed, zero ignored** in 118.18 seconds. This includes
the wrapper's modern/legacy tool identity pairs and graph observer ownership
tests. Receipts are `regression-toolids.log` and `regression-toolids-exit.json`.
The second macOS build passed against the exact corrected source manifest
(51 files). Deployment and real-card telemetry evidence are recorded above.

Canonical successor admission and the existing sequential observer/acceptance
audit now support the explicit `skills-successor-activation.json` mapping.
Admission requires the validated tool-ID deployment, quiescent runtime,
unchanged predecessor state, paused predecessor and exact immutable successor
goal. Admission and observer attachment have now executed against production.
`audit-live-toolids.py` compares actual wrapper IDs/input/output digests with
the graph progress table; it does not backfill missing historical telemetry or
treat provider tool counts as business success.

## Earlier evolution deployment and initial tool observations

At `2026-09-30T02:06:47Z`, the combined correction was installed with SHA256
`373537d5c6ddaaa50e65c5c3554767a05316545686a4150fe4f8f1f95411e61b`,
migration 137. The 224-test regression, three-boot isolated canary and two
production restarts passed. Runtime settings/routine states were preserved;
API/DB/dashboard are healthy with no degraded providers or cleanup backlog.
Doctor remains warned, and archive provenance remains partial (no Git HEAD).

The same skills campaign resumed without replaying any prior attempt. At
`02:07:15Z`, `audit-skill-evolution.py --require-progress` verified that candidate 1
became rejected, captured failure evidence remained present, and repair card
`76b8e79d-8098-035f-6cf2-0c63ee962159` includes the concrete QA rejection reason.
Its Codex dispatch is `4aaeb1b9-00f2-4c50-837b-340047a59662`. This proves automatic
repair admission from an actual plan rejection; the repaired DAG's independent
approval and business completion remain pending. The repair rollout completed on `codex-alt-3` at `02:11:17Z` after eight tool
calls. Candidate version 2 was staged and validator card
`9b2f6dd7-d2db-23f5-52fa-35c31f355b8b` was dispatched as
`651ef4f7-25a4-46e0-9961-621b0fbec26f`. The validator completed at `02:15:00Z` on `codex-alt-3` after seven tool calls,
returned `passed` with graph approval, and the ledger activated version 2.
Inspection card `b771113d-f1db-041e-3be3-8c3dcb021083` was admitted as dispatch
`ee375cc2-6475-4763-be54-877b27705816`. This is actual rejected-plan -> automatic
repair -> independent reapproval -> graph activation -> business dispatch evidence.
It is not completion of skill synchronization, regressions, mirror or backup.

Live telemetry verification failed: the wrapper output contains assistant
`tool_use` / user `tool_result` records but drops both `id` and `tool_use_id`.
The observer correctly refuses unidentifiable events; production counts remain
zero. The actual execution is `codex-tmux-wrapper`, not the direct native TUI
rollout path exercised by the initial parser test. The pending root correction
preserves provider IDs in both response-item and command-execution normalization.
`source-toolids.json` pins 51 files; Linux test and Mac release builds are running.
No historical counts are synthesized and no live tool enforcement claim is made.

Receipts: `activation-v15-evolution.json`, `runtime-audit-evolution.json`,
`maintenance-evolution.json`, `skills-evolution-before.json`, and
`skills-evolution-after.json`. Sequential observer reattached after the authorized
restart terminated its earlier HTTP connection.

A separate scope mismatch was confirmed: the generic enrollment objective tells
all jobs to use `routine-gh.py`, whose only repos are AgentDesk and monolith.
Skills requires its configured skill mirrors. Do not claim skills completion
while this restriction prevents required mirror work; reconcile the original
skill-sync contract and correct the job-specific enrollment scope without
silently rewriting an already-enrolled immutable goal or widening credentials.

## Prior settlement deployment and first completed campaign

Installed binary SHA256:
`3d072c9be568bb134553d3103cd340e0edb4b8b1fd42ebfa068ee418de242d65`;
migration 137. `regression-settlement.log` records **220 passed, 0 failed,
0 ignored** in 107.04 seconds. The three-boot isolated canary and two production
restarts passed. `runtime-audit-settlement.json` confirms healthy API/DB/dashboard,
no degraded providers, zero pending/dead-letter cleanup, internal tasks enabled
with product default false, six recurring parents paused and unchanged routine
counts (16 detached, 9 paused). Doctor has zero failures and two warnings;
archive build provenance remains partial because there is no deployed Git HEAD.

The health campaign was paused only at an idle maintenance boundary and resumed
without replaying any completed provider attempt. At `2026-09-30T01:33:07Z`, QA
card `830d4e9b-1778-0594-52ab-e33636b2ab5d` moved `in_progress -> done` with one
`OK` audit from `campaign_graph_completion`, without force or drift recovery.
Campaign `ops-codex-health-20260930-codex04` is completed with DAG version 2 and
five accepted tasks; the sixth historical blocked attempt remains unaccepted.

The actual QA rollout used `codex-alt-3`, ran ten tools and completed without
terminal error at `01:29:41Z`. Its independently supported result is `no_work`
within the current local Hub-visible operational scope, with no incident proposal
or external mutation. This does not establish full audio execution or source-SHA
coverage. The artifact audit verifies all three frozen output files and both
producer-to-QA input files by SHA256, plus independent validation and every active
work node accepted. `codex-batch-artifact-audit.json` remains `complete:false`
until all six business campaigns satisfy these checks.

The original latency fix waits for graph receipt/artifact validation, then uses
the canonical Kanban transition/audit/cleanup owner. It requires the exact latest
completed dispatch and no active dispatch on that card; metadata alone cannot
authorize completion. Existing configured direct transitions remain authoritative.
The earlier failed test and corrected fixture are retained in the history below.

## Skills candidate rejection and pending corrections

The skills planner completed on `codex-alt-3` and submitted six nodes:
inspect -> independent verification -> prepare -> independent verification ->
external apply -> independent final verification. Its candidate validator
`1452b061-6923-0fd2-4715-f158cf22f0e5` completed eight tool calls at
`2026-09-30T01:40:33Z` and rejected the plan before any business mutation.
The missing conditions are explicit transfer of archive/regression/baseline
proof to the final verifier, and a safe isolated test selection that cannot
write prohibited routing candidates or execute the old routine wrapper.

The live captured `plan` failure exposed a collector bug: a blocked validation
receipt is recorded before the candidate-rejection branch, leaving version 1
in `candidate` and preventing a repair plan. The pending correction reconciles
captured plan failures to rejected candidates, including pre-upgrade records,
and supplies the canonical validator reason to repair planning without changing
failure history. Regression coverage checks idempotent rejection, unchanged
failure records, fresh candidate staging and independent reapproval gating.

Also pending: hash-only tool observations from the native Codex watcher, which
bypasses the existing stream bridge telemetry caller. Events require the same
pinned/current turn and dispatch, deduplicate provider call IDs, and write a
bounded batch. Production still reports zero observed tools; no measured tool
budget enforcement is claimed until the new path is installed and observed.

`source-evolution.json` pins the combined 50-file source overlay. These pending
changes are not installed. The installed release remains the settlement binary
listed above. The telemetry test binary compiled successfully after correcting its explicit
module path; the first compile failure is retained in `telemetry-build-initial.jsonl`.
The combined correction is now building on Linux and Mac. The CI lane membership
gate passed: 921 inventoried PG tests across 136 files and zero rule debt. This
inventory is not a test execution claim. The telemetry regression slice passed **223 tests, 0 failed, 0 ignored** in
109.36 seconds (`regression-telemetry.log`). It includes native tool normalization,
pinned turn/dispatch ownership, hash-only payloads and PostgreSQL replay deduplication.
The combined regression (including candidate rejection repair) passed **224 tests,
0 failed, 0 ignored** in 183.27 seconds (`regression-evolution.log`). The Mac
release build, isolated canary and production deployment remain pending. `skills-evolution-before.json` records the real
pre-deployment failure state; `audit-skill-evolution.py --require-progress` will
check rejection, repair-card admission and concrete QA reason delivery afterward.

## Changes and verified prerequisites

- Native graph enrollment now accepts Codex in addition to Qwen and OpenCode.
- Migration `0136_campaign_codex_executor.sql` extends the database dispatch
  guard to Codex while retaining owner, active graph, internal-task opt-in,
  terminal replay, hold, and duplicate-attempt fences. Deployed migration 0135
  is unchanged (SHA256
  `a2bc772c0fc9c1747f395e14315198e67306449be28419452b83356e3bbad236`).
- The PostgreSQL regression exercises Codex enrollment, rejection of self
  verification, actual dispatch admission, wrong-executor rejection, duplicate
  rejection, and pause behavior. The first run exposed the old database provider
  restriction: 199 passed, 1 failed. After migration 0136, all **200 tests passed**,
  none failed or ignored, in 107.63 seconds.
- Mac agents `coder` and `qa` both resolve to Codex CLI with `gpt-5.6-terra`.
  After a verified default-account `usage_limit_exceeded` failure, `coder` was
  switched through the canonical agent API to existing profile `codex-alt-3`.
  `qa` already uses that profile. They remain separate agents and turns, but
  share the secondary account's quota. Model configuration was preserved.
- Installed Codex CLI reports `0.156.1`. A read-only minimal invocation returned
  `CODEX_READY`, `turn.completed`, and exit 0 in approximately five seconds.
  Earlier probe timeouts were caused by the diagnostic script inheriting open
  SSH stdin; using DEVNULL corrected the probe. This was not a model failure.
- The old Monolith occurrence had no dispatch. Its log explicitly reported a
  missing `github.repo_dirs.kunkunGames/monolith` mapping. The internal repository
  was clean and its origin matched. That mapping was added to the canonical
  configuration. The old occurrence was paused and its queue ended through
  canonical APIs. Its future dispatch path remains to be verified.

## Sequential execution contract

The prepared one-shot observer admits health, skills, upstream, AgentDesk review,
AgentDesk merge, then Monolith. It uses native Campaign/Auto Queue execution and
does not manufacture task results or call completion APIs. Each occurrence uses
`coder` for planning/work and `qa` for independent verification. Prior goals and
failed receipts remain preserved; original recurring parents remain paused.

The planner is explicitly limited to bounded read-only investigation and DAG
submission. Business execution and verification belong in DAG nodes. Existing
effects must be reconciled before any external mutation. Existing budgets and
success criteria are retained. A blocked campaign stops further admission for
diagnosis, rather than counting it as completed.

The first campaign, `ops-codex-health-20260930-codex01`, ended without a result.
Its actual Codex rollout contains `usage_limit_exceeded` and an out-of-credits
message; the earlier minimal successful CLI probe did not establish enough
quota for business execution.

The second campaign, `ops-codex-health-20260930-codex02`, used the secondary
profile. Codex generated a two-node inspection/independent-verification graph
and a planning receipt, then emitted `task_complete` without an error. Native
acceptance did not occur: a cancelled predecessor watcher failed the active
dispatch with `tmux_session_died` immediately after watcher replacement. The
logs identify `turn_bridge_tmux_ready` replacement at 16:16:39.971 UTC and the
stale watcher failure at 16:16:40.228 UTC. The proposal file is evidence of
successful secondary-account inference, not an approved or completed DAG.

The correction in `tmux_watcher/post_stream_exit.rs` prevents stale or cancelled
watchers from performing terminal dispatch mutations and requires a dead pane
before the remaining death cleanup. The native regression slice plus four
watcher ownership tests passed: **204 passed, 0 failed, 0 ignored**, in 100.54
seconds. The Mac build, isolated three-boot canary, and two production restarts
passed. Installed binary SHA256 is now
`7b77f324c806baa461594a38b2a8626ea177812d4bb8ef34e34ce931b1ecfd82`.
Settings and migration 136 were preserved. Activation and audit receipts are
`activation-v15-watcher.json` and `runtime-audit-watcher.json`. New batch
`20260930-codex03` admitted health under the secondary profile. Its planning
dispatch `04ae63d1-2122-4950-bea0-cf08ff9ee45a` completed through the native
receipt path, and its Auto Queue entry/run completed without watcher failure.
The card remains `in_progress`, and no candidate graph has been staged.

The initial observation of `in_progress` was incomplete: live audit records
subsequently show `review_drift_reconcile` moving the plan card to review at
16:47:00 UTC, followed by the existing review hook moving it to done. Candidate
version 1 was staged at 16:47:30 UTC. This was approximately seven minutes
after dispatch completion, not an indefinite deadlock. The graph publisher's
`review_mode=disabled` and canonical mainline restoration leave normal progress
dependent on delayed drift recovery; eliminating this delay remains follow-up
work, preserving the canonical FSM and review ownership.

The independent QA dispatch then failed native receipt ingestion because it
added `verification_scope` beside `campaign_receipt` and `graph_validation`.
Only those two keys and `campaign_graph` are permitted. No receipt or dispatch
was rewritten. The failed occurrence is held, with its candidate and evidence
preserved. The native prompt previously did not advertise the complete
top-level allowlist or observation length limits.

The next correction centralizes envelope keys and observation limits in
`db/campaigns/graph/receipt.rs`, reusing them in the prompt, completion reader,
and observation validator. Tests explicitly reject top-level authority fields
and extra scope while allowing explanatory evidence inside the receipt.
Linux compilation passed; the native regression slice passed **205 tests,
0 failures, 0 ignored**, in 105.06 seconds (`regression-contract.log`). The Mac
build, isolated three-boot canary and two production restarts passed. Installed
binary SHA256 is `2af5d3fdaf9935839fa42731bffacba552201bd53c401ab7b2d2e1b61bd2d0e0`;
migration 136 and settings were preserved. Batch `20260930-codex04` was paused
only for installation after verifying zero active dispatches/turns, then resumed
with the same IDs. The observation process terminated on connection refusal
during restart and was reattached after confirming that exit; no provider turn
was replayed. Receipts: `activation-v15-contract.json`,
`runtime-audit-contract.json`, `maintenance-contract.json`.

The plan was accepted, candidate version 1 was staged and independent QA on
`codex-alt-3` completed with a natively captured `approved=true` validation
receipt. Live state subsequently confirmed version 1 as **active**, with both
plan and validation accepted and the `inspect` business card dispatched to
`coder`. This is the first observed accepted-and-activated business DAG in the
Codex verification batches; it is not yet a completed business run.

`audit-codex-batch.py` independently reads live task/version state, checks frozen
bundle/file hashes and exact consumer input bytes, and verifies independent
agent coverage for the active graph. Its completion condition covers all six
original jobs and the active graph's accepted tasks, while preserving historical
failed/evolved tasks instead of requiring those failures to be rewritten.
The initial audit correctly reports incomplete while `inspect` is running.
All six business campaigns remain
incomplete. Zero native observed-tool counts remain an instrumentation gap.
Neither failed dispatch is rewritten as successful. Both failed batches remain
preserved. Native observed tool counts were zero despite real rollout tool
calls, so zero must not be interpreted as proof of no tool use.

### September 30 service verification and current hold

Batch `20260930-codex04` subsequently stopped at the first business node.
Dispatch `f938b83c-330f-4485-9e82-6182e89ab55a` returned a native `blocked`
receipt and wrote `inspection.json` and `incident-proposal.json`. The two
accepted planning/validation tasks and active version 1 remain preserved;
the inspection task is not accepted. The observation process exited on the
hold, so no later business campaign has been admitted. Completion is still
**0 of 6**, not one completed campaign merely because one DAG was activated.

The inspection found healthy core runtime and no verified stall, but could not
resolve the historical service-context voice failure or identify the deployed
binary through the health API. The old edge-tts executable ultimately resolved
through a tools-directory symlink onto external `990PRO`, and version probes
hung. The orchestrator preserved that environment, installed pinned
`edge-tts==7.2.8` under `/Users/kunkun/.adk/tools/edge-tts`, and changed only
`voice.tts.edge.command` to that environment's absolute executable path.

After verifying idle sessions, empty provider inflight records and no active
queue turns, a launchd restart loaded the new configuration. The verification
receipt at `2026-09-30T00:30:10Z` confirms `voice_edge_tts=pass`, healthy
database/dashboard, unchanged installed binary SHA256 and runtime settings,
and both `coder` and `qa` still bound to `codex-alt-3`. Startup status is now
`doctor_warned`, with remaining warnings for intentionally unregistered routine
scripts and duplicate provider bindings. Actual audio synthesis was not tested.
Receipts are `edge-tts-internal-install.json` and
`edge-tts-service-verification.json`; installed dependency versions are in
`edge-tts-internal-requirements.txt`.

The health API still reports partial source provenance (`repo_head_missing`).
The archive build's truthful manifest contains a binary SHA256 and overlay
manifest digest, but the current health serializer does not expose those
fields. A repository HEAD must not be fabricated to satisfy the inspection.
The held graph has no supported manual reconciliation endpoint yet: activation
and staging reject held campaigns. Subsequent recovery must preserve the
blocked receipt and validated graph, reconcile the captured failure, and
create an explicit new task attempt through native ownership rather than reset
database rows or claim the failed inspection passed.

A fresh provider usage observation after restart reports default profile
remaining quota of 82% for 5h and 53% for 7d, and `codex-alt-3` remaining quota
of 100% for 5h and 30% for 7d (`stale=false`). This supersedes the earlier
default-profile exhaustion snapshot; the earlier actual `usage_limit_exceeded`
failure remains valid historical evidence. Explicit secondary profile bindings
are retained. This change does not implement automatic profile rotation.

### Reconciliation correction installed; live recovery in progress

The missing operator recovery path is now implemented as
`POST /api/campaigns/{id}/execution/reconcile`. Migration 0137 appends a durable
failure-to-candidate audit record. The original failed task, failure category,
dispatch result and active graph remain intact. The transaction creates a new
candidate by preserving the graph's acceptance requirements and unaffected
nodes while replacing the failed artifact-only node's task key and attaching
bounded reconciliation evidence. Independent QA must approve the new candidate
before activation. External effects, published descendants, active ownership,
unfinished queue cleanup and exhausted budgets prevent this recovery.

The health serializer also exposes optional `declared_build_provenance` without
manufacturing a repository HEAD or removing the existing partial-provenance
status. Consumers must still independently compare the declared binary digest
with the installed executable. See [CAMPAIGN-RECOVERY.md](CAMPAIGN-RECOVERY.md).

The Linux library build passed. The established native regression slice plus
new recovery and release-source cases passed **219 tests, 0 failures, 0 ignored**
in 105.24 seconds (`regression-recovery.log`, `regression-recovery-exit.json`).
New PostgreSQL cases verify transactional rollback on budget failure, idempotent
reconciliation, unchanged failure evidence and unaffected graph nodes,
independent-validation gating, and rejection of external-effect replay. The
test inventory and CI lane selection were regenerated for the new test module.

The Mac build passed in 13m 15s. Its isolated canary passed three boots,
authentication, native admission/settings persistence and migration 137, then
dropped its disposable database. Two production restarts passed with settings
preserved. Installed binary SHA256 is now
`c1acd54ff1deb71b8f12ad9d1d01087754c5a4d4f4cba9917490967b12e70974`.
`runtime-audit-recovery.json` confirms healthy DB/dashboard, zero pending or
dead-letter cleanup tasks, the correct declared binary digest and migration 137.
Startup doctor remains warned rather than failed; the internal edge-tts command
passes its fresh doctor check. All six periodic parents remain paused, source
routines remain paused and excluded routines remain outside enrollment.

At `2026-09-30T00:56:20Z`, the authenticated recovery endpoint accepted current
revision 6 and captured reconciliation evidence for failure
`98087ac16e7d7b9326044d2f3dfdec0641dd273f8bec0e542757405fb8ac34fc`.
It created candidate version 2 in the same campaign and cleared the execution
hold; version 1 and its original blocked attempt remain unchanged. Independent
QA card `8dc1232c-421f-578b-9206-c3593ee90eea` was dispatched as
`a0a4bc0f-0d59-4d33-be5e-efb3ec299517`. Its real `codex-alt-3` rollout completed at `2026-09-30T01:00:08Z` after
seven tool calls, with no terminal error. The native completion result contains
`campaign_receipt.outcome=passed` and `graph_validation.approved=true`. The ledger subsequently accepted that validator and activated version 2; version
1 is superseded. At 01:10 UTC the generic drift path had settled the validator
almost ten minutes after its provider completion. The recovered inspection card
`c6e06042-08e8-532d-9a61-0d5eeee47f4b` then ran on `coder` using `codex-alt-3`.
Its provider rollout completed at `2026-09-30T01:17:48Z` after 14 tool calls,
with no terminal error. At 01:19 UTC its dispatch was completed with a `no_work`
receipt, but its card remained `in_progress` and the graph had not yet accepted
its artifacts. Independent downstream verification remains required; campaign
completion remains 0/6. Both `coder` and `qa` profile bindings were rechecked
through the live agent API. This is explicit secondary-profile selection, not
automatic rotation after quota exhaustion.

A receipt-authorized canonical card settlement change is being validated. It
requires an accepted graph observation for the exact latest completed dispatch,
a frozen artifact bundle for work nodes, and no active dispatch on that card.
It retains the canonical transition/audit/cleanup path and does not use forced
completion. The Linux test binary compiles and the CI membership gate passes
(920 inventoried PG tests, 136 files, zero rule debt). The first regression run passed 219 cases and failed the new settlement case:
its fixture inserted a completed dispatch while leaving its queue entry pending.
The canonical queue owner correctly did not count that unadmitted entry as done.
The fixture now models a dispatched queue entry; a fresh test build is running.
The failed evidence is preserved in `regression-settlement-initial.log`. The Mac
release build passed, and its isolated canary passed three boots, migration 137,
authentication, admission/settings persistence, and disposable database cleanup.
Candidate SHA256 is
`3d072c9be568bb134553d3103cd340e0edb4b8b1fd42ebfa068ee418de242d65`.
Only the cfg(test) fixture changed after this production build; all 47 source
manifest hashes were verified remotely. This change is not installed yet. The sequential
observer remains attached to the same batch and admits the next job only after
verified campaign completion and no active owned execution.

The CI lane membership check also passed: 919 inventoried PG tests across 136
files, generated selection/workflow paths synchronized, rule debt zero. This is
an inventory check; the executed regression slice is the 219 tests above.
Receipts: `activation-v15-recovery.json`, `runtime-audit-recovery.json`,
`health-reconciliation-recovery.json` and `regression-recovery-exit.json`.

## Evidence and recovery boundary

Local evidence lives under `target/24x7-native/campaign-graph-v15/`:

- `regression-codex-initial.log` and its exit receipt preserve the failed guard test.
- `regression-codex.log` and `regression-codex-exit.json` record the 200 passing tests.
- `source-codex.json` and `source-codex.tar.gz` pin the prepared build inputs.
- `codex-sequential.py` holds the bounded sequential admission/observation contract.
- `stage-codex.py` and `activate-codex.py` require the isolated restart canary,
  source hashes, successful tests, and a quiescent runtime before installation.

After the user restored Mac availability, the resumed release build completed in
13m 33s. The isolated native canary passed three boots and migration 136 and
dropped its disposable database. Production activation completed two healthy
restarts with settings preserved. That initial Codex binary SHA256 was
`9c2c2915e82d5f91392fb5442e2362c7791d99e7f77a78fe37c6e262a397644a`.
The production database and release provenance both report migration 136.
Receipts: `activation-v15-codex.json` and `runtime-audit-codex.json`.

Both the default Codex profile and qa's separate profile returned a real minimal
response and successful turn termination. Existing startup doctor failure for
missing `edge-tts` remains; healthy DB/dashboard is not whole-install acceptance.
The batch observer additionally checks owned inflight state and active sessions
before advancing to the next campaign.
