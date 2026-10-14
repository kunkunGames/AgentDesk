# Six-campaign planning audit

Observed 2026-09-29 14:58 UTC (23:58 KST). Read-only canonical API/DB and
owned workspace/provider output inspection; no failure was converted to success.

All six manual campaigns have zero graph versions, zero accepted tasks, and one
initial planning card. These cards are control tasks, not generated business DAG
nodes. All five dispatched planning workspaces lack
`.agentdesk-campaign-result.json`. No independent graph validation was reached.

| Campaign | Observed outcome | Specific evidence |
| --- | --- | --- |
| Health | Cancelled and held | Node deadline, 629.60 seconds; 17 observed tools |
| Skills | Failed and held | Preserved Qwen exit reason: wall-clock budget of 1800 seconds exceeded; 21 tool-use events in preserved output |
| Upstream | Failed and held | Same Qwen wall-clock limit; 11 tool-use events, also counted in graph telemetry |
| AgentDesk review | Failed and held | Qwen exit reason: no output for 240 seconds before first progress; no tool-use events; total dispatch lifetime 1058.63 seconds |
| AgentDesk merge | Failed and held | OpenCode provider response `UnknownError`; four observed tools; underlying provider cause not established |
| Monolith | Queue pending, no dispatch | One planning card, active queue, reserved monitoring slot, no provider execution; admission cause not yet established |

The manual batch observer stopped after its observation allowance expired on
Monolith. Its `complete` field is false. An active campaign/queue label is not
proof of provider execution or business completion.

The Qwen database summaries only said `tmux session died mid-turn`. Preserved
owned `.jsonl.exit_reason` files establish the more specific limits above.
Skills telemetry originally counted zero because the old normalizer dropped
tool IDs; the preserved output contains 21 tool uses. Zero in that historical
counter must not be interpreted as no actual activity.

The controller requests original requirement discovery, current observations,
graph construction and final receipt in one initial planning card. Candidate
staging occurs only after a completed dispatch with its result file. This makes
late/no submission a bottleneck. It does not prove that stricter DAG validation
caused the failures: no candidate reached that validator. Provider speed/quality
and the cause of each long investigation have not been independently isolated.

The held-queue cleanup correction was installed at 2026-09-29 01:13 UTC.
`held-boundary-maintenance.json` confirms activation, recovery of the upstream
queue/slot, retained hold/failure evidence, and resumed batch admission. That
recovery succeeded, but did not make subsequent planning succeed.

Recommended priorities: explicit bounded planner-only work, sufficient known
runtime context, early draft artifacts separate from final acceptance, precise
durable provider error classification, and observable bounded queue admission.
Do not weaken DAG acceptance or blindly replay unknown external effects.
Only promote a graph or node template after real execution and independent
verification. Cross-occurrence reuse remains a design recommendation, not an
implemented feature or a remedy for provider startup failure.

Evidence: `target/24x7-native/campaign-graph-v15/six-outcome-audit.json`,
`provider-ending-audit.json`, `20260929-seq01-results.json`,
`held-boundary-maintenance.json`, and `activation-v15-held.json`.
