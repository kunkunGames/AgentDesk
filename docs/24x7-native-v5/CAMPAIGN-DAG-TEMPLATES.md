# Campaign DAG contract and template lifecycle

The native Campaign owner uses a versioned JSON DAG, generated from the same Rust types that parse it. `GET /api/campaigns/graph-schema` publishes JSON Schema v1 for the graph and completion result. Schema validation is followed by enrolled-agent, dependency, cycle, independent-verifier, literal artifact-path and bounded artifact checks. A schema-valid plan is still subject to independent planning approval and actual execution verification.

Provider result files must satisfy the strict result schema before capture. The completion owner then adds a fixed artifact outcome, capture summary and recognized completion source. Reconciliation validates those server fields separately before validating the original envelope; they do not grant graph approval or business acceptance. Failed capture and reconciliation evidence remain intact.

## Operator settings

Dashboard: **Settings → Runtime → Campaign DAG**.

```yaml
runtime:
  campaign_superior_agent_id: codex
  campaign_dag_template_min_samples: 10
```

Runtime API keys are `campaignSuperiorAgentId` and `campaignDagTemplateMinSamples`. The default superior is the existing `codex` agent. The sample threshold defaults to 10 and accepts integers from 1 through 100. Explicit API/dashboard choices retain the existing runtime-config persistence and override YAML on restart. Invalid settings are rejected before writes.

The superior must exist and use a native Codex, Qwen or OpenCode provider. Its assignment is pinned when a control card is published; changing settings never rewrites already published cards. The superior is authorized only for its durable bootstrap-plan or template-review control assignment. Business execution remains restricted to the campaign's enrolled agents.

## Planning and accumulation

Each campaign family is scoped to its root campaign, objective, success conditions, constraints, repository and executor roles. Scheduled child occurrences share that family. Changing the scope creates a separate family; budget and schedule adjustments do not combine unrelated goals.

1. The family's first initial DAG is assigned to the superior.
2. Subsequent initial DAGs and bounded repairs use the enrolled planner. Planning approval uses another executor.
3. One completed occurrence contributes at most one sample, after every DAG node has a canonical accepted observation, completed dispatch, settled card and captured output manifest. Incomplete graphs, planning-only approvals and failed runs do not increase the count. Retries and graph revisions are not additional samples.
4. Every N new verified samples, the root Campaign publishes a read-only review card to the superior through the existing Kanban/Auto Queue dispatcher. The cycle continues after promotion: with N=10, reviews occur at 10, 20, 30 and subsequent samples. The frozen, hashed artifact contains the next N new verified graphs, bounded redacted receipt evidence, recent failure history, quality measurements and the existing active template when present. One pending review is reused while additional samples accumulate. Extra samples remain available for the next complete batch; for example, 25 samples produce reviews through 10 and 20 with five left for the next batch. Sample collection and review claims serialize on the family, so concurrent arrivals cannot change a frozen cohort.
5. The superior returns `dag_template_review` with the pinned `sample_digest`, a reason, and `decision: promote`, `keep` or `defer`. Promotion requires a template definition; keep/defer require a null template. The server checks that the scope, frozen input and current template remain unchanged, validates the proposed template against the sampled goals, verified node/effect classes and a fresh occurrence, and commits the versioned template atomically. Keep preserves the active template, and an identical promotion records keep without creating another version. Repeated identical decisions are idempotent; changed decisions are rejected.
6. A deferred or failed review remains visible and requires another configured batch of verified samples before another review. Review preparation gates are visible without stopping ordinary lower-agent occurrences. The campaign's existing finite execution budgets still apply.
7. A promoted template is instantiated with fresh task keys and current goal/window values. It becomes a candidate DAG and must pass independent graph approval before work publication. Completion receipts and runtime state are never copied from a previous run. A rejected or invalid template is blocked and bounded enrolled-agent planning remains available.

Existing published plans, running work, completion/failure evidence, contracts and migration checksums are preserved at upgrade. Previously completed scheduled occurrences can be collected on reconciliation if they meet the same sample requirements. Historical initial plans retain their original executor rather than being replayed solely to reassign the superior.

## Template bindings

`task_key` must be `{{task_key}}`; the server binds it to the family, occurrence and node. `agent` must be `{{planner}}` or `{{verifier}}`. Titles, details and acceptance conditions support `{{objective}}`, `{{repo}}`, `{{window}}`, `{{success_json}}` and `{{constraints_json}}`. Binding values are opaque and are never recursively evaluated. IDs, dependencies and artifact paths remain literal. Unknown or unterminated bindings are rejected.

The superior must assess whether target-specific branches, source revisions, PR IDs, failure recovery and no-work outcomes can be generalized. The server can validate structure and evidence integrity; it cannot prove semantic generalization from JSON alone. Every future run must freshly inspect inputs and reconcile effects before mutation.

## Prerequisites and recurrence

An execution goal may include explicit `prerequisites`: `{"kind":"file","path":"/absolute/input.json","sha256":"<64 hex characters>"}`, `{"kind":"binary","name":"git"}` or `{"kind":"skill","agent_id":"<enrolled executor>","name":"<skill name>"}`. The owner checks these before spending another planner or work turn. Missing or changed prerequisites appear as structured `preflight` reasons and wait for correction; they do not clear execution holds or authorize replay. Existing goals without prerequisites retain their admission behavior. File prerequisites pin exact input bytes and belong to the reusable goal scope; free-form source references still require the existing execution guards and fresh agent inspection.

`POST /api/campaigns/:id/execution/schedule` activates an audited recurring parent with `expected_revision`, a unique `request_id`, concrete `evidence` and a schedule such as `{"timezone":"Asia/Seoul","daily_minutes":[0,360,720,1080]}`. It requires enabled internal tasks, a paused or completed parent, settled owned dispatches/queues and reconciled prior unknown outcomes. It rejects execution holds and occurrence children. The operation preserves business goals, accepted graph/task contracts, samples and finite budgets. Admission starts at activation time, coalesces missed future slots and prevents overlapping occurrences; old slots are not backfilled.

Each child carries its time window in `observation_window`, separately from the objective. A new window therefore remains in the parent's DAG family and contributes a verified sample to that family after independent completion. During a pending or deferred template review, an existing valid template remains available and every instantiated candidate still requires independent approval.

## Inspection and evidence

`GET /api/campaigns/:id/execution` includes `dag_learning`: family, superior, bootstrap occurrence, configured threshold, unique verified sample count, new samples since review, samples remaining, recurrence, active template, review reasons/history and any review preparation gate. The Campaign dashboard shows the same lifecycle, structured prerequisite waits and execution measurements grouped by role. Published tasks pin prompt/goal/result-schema digests, requested provider and template version in `execution_profile`. Observed model and token usage come only from provider stream metadata attached to the owned dispatch; absent metadata stays unknown. Dispatch lifetime includes queue/wait time and is not model inference time. No price or cost is inferred from model prose.

Migration 0139 adds family, membership, sample, review, versioned-template and control-assignment records. It extends the existing dispatch guard for server-recorded superior control assignments without creating another scheduler or completion owner.

Migration 0140 adds continuous-review decisions, prerequisite reports, frozen execution profiles, observed provider metadata and audited schedule activation. Existing migration checksums and completion evidence remain immutable.

Tests use explicit PostgreSQL fixture receipts to cover default tenth-sample admission, configurable thresholds and persistence, concurrent claims, one-sample-per-occurrence collection, promotion/deferral, frozen-input integrity, bootstrap/follower assignment, replay fences, candidate approval and template rejection. These fixtures establish runtime contracts; they do not establish ten real model-generated successful occurrences or a production superior's promotion decision.
### Independent recurring work budgets (migration 141)

The parent goal's budget covers its lifetime control tasks, including superior
template reviews. `occurrence_budget` is persisted separately at enrollment or
the first audited schedule activation. Increasing the parent budget does not
increase existing or future occurrence budgets, reset usage, or change accepted
contracts. Migration 141 retains the budget at the earliest audited activation
for existing parents. Parents enrolled with a schedule retain their goal budget.

`POST /api/campaigns/{id}/execution/schedule` optionally accepts a complete finite
`occurrence_budget`. Omission preserves the existing policy. An explicit change
records both policies in `campaign_schedule_revisions`, affects future children
only, and retains the ordinary pause, CAS, ownership and unknown-outcome guards.
Each new occurrence has an initial plan and up to eight replans. Usage belongs
to that child and starts at zero for each new scheduled slot; historical usage
is never reset. When a later slot is due, an active child with an exhausted
replan budget may be cancelled only after execution, queues and observations
have settled and external effects have independent verification. Its hold,
failures, accepted work and receipts remain available, and cancellation is not
successful completion. Manually paused children and unresolved outcomes still
block admission. An existing occurrence can receive a separate audited lifetime
extension. A stale Hub tick cannot
admit a child using a goal read before a pause/revision/resume.

### Read-only operational observation

`python3 scripts/observe_campaign_learning.py --root ~/.adk/release
--campaign CAMPAIGN_ID --output OUTPUT_DIRECTORY` takes one authenticated GET
snapshot. It projects health, deployed identity, parent/occurrence budgets,
sample counts, review decisions and the latest 30 occurrence outcomes. It never
executes or resumes a campaign, stores provider output, or reads credentials.
Run it every 300 seconds with launchd/cron. `window.json` retains at most 600
samples; `first-24h-window.json` freezes the first elapsed-day window. Gaps over
660 seconds, clock regressions, unavailable observations, doctor failures and
binary/provenance changes prevent a continuous-health claim. Business holds
remain a separate observation, and `full_v5_acceptance` stays false.

Future occurrence policy changes can use `POST execution/schedule` with `activate:false` for an existing recurring parent to preserve its paused/completed status and admission start. First conversion must use activation so its admission clock excludes older slots. The default remains `true`, and omitted/true requests retain their legacy idempotency digest. Audited child budget changes cannot exceed eight replans. Structured `goal.budget` fields govern execution limits after policy changes; inherited business constraints and all prior receipts remain binding.
