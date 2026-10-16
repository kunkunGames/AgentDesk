# Native Campaign planning quality

The existing Campaign tick, Kanban, Auto Queue and native completion owner remain
the execution path. Planning advice, admission reserves and a read-only health
collector reduce repeated exploration and avoid spending the last lifetime
allowance on another unfinishable repair.

## Control admission and remaining budget

`GET /api/campaigns/{id}/execution` exposes `control_budget`. It includes ordinary
lifetime usage, remaining publications/time/observed tools and role reserves.
An accepted dispatch-lifetime median from the same pinned DAG family estimates
each role; the estimate is capped by the node deadline. Without evidence, the
finite finish allowance is the smaller of 30 seconds and the node deadline.
These reserves are admission policy, not minimum runtimes or performance proof.

A new planner reserves its own turn, independent candidate validation and at
least one fresh business verification. A new validator reserves validation and
fresh verification. Actual work can require more time and must still be checked
against the current budget. Publication rechecks policy while holding canonical
campaign/task locks. Insufficient allowance records a specific hold before
creating a new card/batch; running owners, prior identities, accepted contracts,
unknown outcomes and consumed usage remain intact. Audited recovery is required.
Existing idempotent publications remain readable after allowance is exhausted.
Optional superior reviews retain their separate durable frozen execution policy.

The control prompt contains a time-stamped publication budget. Agents must read
the current `/execution` view before submitting; queued time can change the
remaining allowance. Observed tool counters remain post-event telemetry.

## Verified structure advice before promotion

New planner cards receive `.agentdesk-campaign-plan-input.json` through the
existing immutable artifact materialization path. Up to three same-scope
verified completed DAGs contribute only node kinds/effects, executor roles,
dependencies and artifact mappings. Old execution keys, incident descriptions,
business conclusions and acceptance receipts are excluded from those structures.
Current accepted contracts and bounded recent failure lessons accompany them.

Advice is not an active template or execution approval. The first-family
superior assignment, ordinary enrolled planner, default N=10 and superior
promotion/keep/defer decisions are unchanged. Every proposed DAG still requires
fresh independent validation and every new occurrence requires fresh work/QA.
The controller pins the advice manifest/digest in the task execution profile;
materialization occurs before provider execution and integrity is checked again
before accepting a plan. Legacy controls keep their original contracts.

## Scoped health evidence

```text
python3 ~/.adk/local-routines/bin/routine-health.py --campaign CAMPAIGN_ID --task-key TASK_KEY
python3 ~/.adk/local-routines/bin/routine-health.py --campaign CAMPAIGN_ID --task-key QA_TASK_KEY --previous input/health.json
python3 ~/.adk/local-routines/bin/routine-watchdog.py --campaign CAMPAIGN_ID --task-key TASK_KEY
```

The helper performs only authenticated native GETs through the existing CLI.
It projects current core health, primary relay classifiers and nonterminal
dispatch metadata. It records per-query coverage, timestamps, source hashes and
deployed identity. A full bounded nonterminal inventory, missing required core
facts or an unknown primary classifier prevents complete-scope conclusions.
Raw transcripts, session identifiers, authentication values and arbitrary
provider result/context text are excluded from output.

Healthy foreground/background work is not a fault because an `attached` field
or legacy shadow classifier differs. A genuine self-owned primary stall remains
eligible. Fault confirmation requires separate observations at least 30 seconds
apart, the same deployment/source/campaign scope and the same relay target and
dispatch identity. A recovered observation is history, not a current incident.

Private provider activity is unobserved. Historical saved links, old failed
attempts and incomplete optional audit projections alone do not create a new
universal approval requirement. Canonical unknown effects remain with their
existing owner. The helper does not reconcile, resume, dispatch or complete any
campaign. Independent QA must collect fresh evidence and verify the actual
business requirements; `business_accepted` is always false in helper output.

`routine-watchdog.py` additionally reads the four original monitoring watchdog
fields from the same canonical agent/card/dispatch/session relationships used
by native routine context loading. Its local PostgreSQL transaction is read-only
with a ten-second statement timeout. A saved agent task link is audited separately
from current runtime ownership; a busy signal is activity, not fault authority.
It never executes the old tick or writes a heartbeat/checkpoint. Native binary
and independently installed helper identities have separate delivery receipts.

## Operational evidence

Intentional managed-process and hard tmux resets retire the exact session's
watcher before process teardown, pane termination or capture-file cleanup.
Retirement removes the registry binding and sets cancellation inside the same
registry critical section. The outgoing watcher therefore cannot interpret a
normal dispatch reset as a crash and fail the next queued dispatch on that
thread. Other sessions and a channel rebound to a different session retain
their owners. Genuine unexpected pane death keeps the native failure path.

This ordering fixes an observed reset race in the Skills and Upstream campaign
attempts. Their original failures and charged lifetime usage remain canonical;
installing the fix does not turn those attempts into accepted business evidence
or authorize replay of an unknown external effect.

The first superior decision at N verified occurrences, template-instantiated
business acceptance and a measured 24-hour observation are distinct milestones.
Fixture tests and source/build receipts do not establish those milestones.
