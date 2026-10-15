// @vitest-environment happy-dom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { getCampaignExecution, type CampaignExecution } from "../../api/campaignExecution";
import CampaignExecutionStatus from "./CampaignExecutionStatus";

vi.mock("../../api/campaignExecution", () => ({ getCampaignExecution: vi.fn() }));
const baseline: CampaignExecution = {
  enrolled: true, active_version: null, hold_reason: null, observed_tool_calls: 0,
  elapsed_dispatch_seconds: 0,
  goal: { objective: "Fresh independently verified campaign", success: ["checked"], constraints: [],
    budget: { max_dispatches: 12, max_elapsed_secs: 600, max_evolutions: 2 } },
  versions: [], tasks: [], failures: [], occurrences: [],
};
let container: HTMLDivElement;
let root: Root;
beforeEach(() => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  container = document.createElement("div"); document.body.appendChild(container);
  root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.clearAllMocks(); });
async function open(state: CampaignExecution) {
  vi.mocked(getCampaignExecution).mockResolvedValue(state);
  await act(async () => root.render(<CampaignExecutionStatus id="family" tr={(_ko, en) => en} />));
  await act(async () => container.querySelector("button")!.click());
}
it("keeps recurring work limits distinct from the enlarged parent review budget", async () => {
  await open({ ...baseline,
    goal: {
      objective: "Recurring independently checked work", success:["checked"], constraints:[],
      budget:{ max_dispatches:20, max_elapsed_secs:10800, max_evolutions:2 },
      schedule:{ timezone:"Asia/Seoul",daily_minutes:[240,600,960,1320] } },
    occurrence_budget:{ max_dispatches:20,max_elapsed_secs:1800,node_timeout_secs:600,max_evolutions:2,max_observed_tool_calls:300 },
  });
  expect(container.textContent).toContain("Parent control dispatch lifetime");
  expect(container.textContent).toContain("10800s");
  expect(container.textContent).toContain("Budget for each occurrence: 20 tasks · 1800s");
  expect(container.textContent).toContain("Node timeout 600s");
});
it("shows the configurable threshold, superior decision and waiting reason without claiming a template", async () => {
  await open({ ...baseline, dag_learning: { schema_version: 1, family_id: "dag-family", superior_agent_id: "codex",
    min_samples: 10, sample_count: 10, active_template: null, review_wait_reason: "superior temporarily unavailable",
    reviews: [{ id: "review-1", sample_count: 10, threshold: 10, status: "deferred", reason: "Need a distinct outcome", superior_agent_id: "codex" }] } });
  expect(container.textContent).toContain("10 / 10");
  expect(container.textContent).toContain("Accumulating / awaiting review");
  expect(container.textContent).toContain("Need a distinct outcome");
  expect(container.textContent).toContain("Template review waiting: superior temporarily unavailable");
  expect(container.textContent).toContain("codex");
});
it("shows the promoted version and still renders older execution responses", async () => {
  await open({ ...baseline, dag_learning: { schema_version: 1, family_id: "dag-family", superior_agent_id: "brain",
    min_samples: 4, sample_count: 4, active_template: { id: "template-1", version: 2, status: "active" }, reviews: [] } });
  expect(container.textContent).toContain("v2");
  await act(async () => container.querySelector("button")!.click());
  vi.mocked(getCampaignExecution).mockResolvedValue(baseline);
  await act(async () => container.querySelector("button")!.click());
  expect(container.textContent).toContain(baseline.enrolled && baseline.goal.objective);
  expect(container.textContent).not.toContain("DAG template");
});

it("shows remaining new samples, prerequisites and operator failures without a nested receipt", async()=>{
  await open({...baseline,preflight:{ready:false,checks:[{ready:false,reason_code:"source_digest_mismatch",requirement:{kind:"file",path:"source.py"}}]},
    dag_learning:{schema_version:1,family_id:"family",superior_agent_id:"codex",min_samples:10,sample_count:13,new_samples_since_review:3,samples_until_review:7,recurring:false,active_template:{id:"template",version:1,status:"active"},reviews:[]},
    failures:[{id:"operator-replan",category:"plan",task_key:"preserved",evidence:{reason:"Original input contract must be corrected"}}]});
  expect(container.textContent).toContain("3 / 7");expect(container.textContent).toContain("source_digest_mismatch");
  expect(container.textContent).toContain("Original input contract must be corrected");expect(container.textContent).toContain("Recurrence is not configured");
});
