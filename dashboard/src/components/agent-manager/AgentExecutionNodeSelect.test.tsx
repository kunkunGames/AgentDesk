// @vitest-environment happy-dom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { AgentExecutionNodeSelect } from "./AgentExecutionNodeSelect";
import { getAgentExecutionNode, setAgentExecutionNode } from "../../api/agentExecutionNode";
import { getClusterNodes } from "../../api/clusterNodes";

vi.mock("../../api/agentExecutionNode", () => ({ getAgentExecutionNode: vi.fn(), setAgentExecutionNode: vi.fn() }));
vi.mock("../../api/clusterNodes", () => ({ getClusterNodes: vi.fn() }));
let root: Root;
let container: HTMLDivElement;
const tr = (_ko: string, en: string) => en;
const nodes = ["single-node", "windows-runner-1"].map((instance_id, index) => ({
  instance_id, hostname: index ? "Windows PC" : "Mac mini", status: "online",
  effective_role: index ? "runner" : "hub", capabilities: {},
  execution_readiness: { providers: { codex: { eligible: true, reasons: [] } } },
}));
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.resetAllMocks();
  vi.mocked(getAgentExecutionNode).mockResolvedValue({ default_node_id: null, routing_enforced: true });
  vi.mocked(getClusterNodes).mockResolvedValue({ cluster: { enabled: true }, nodes });
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});
const render = () => act(async () => root.render(<AgentExecutionNodeSelect agentId="codex" provider="codex" tr={tr} />));
const button = (name: string) => [...container.querySelectorAll("button")].find((entry) => entry.textContent === name)!;
const select = async (value: string) => act(async () => {
  const element = container.querySelector("select")!;
  element.value = value;
  element.dispatchEvent(new Event("change", { bubbles: true }));
});

it("saves only the selected agent default after an explicit save", async () => {
  vi.mocked(setAgentExecutionNode).mockResolvedValue({ default_node_id: "windows-runner-1" });
  await render();
  await select("windows-runner-1");
  expect(setAgentExecutionNode).not.toHaveBeenCalled();
  await act(async () => button("Save").click());
  expect(setAgentExecutionNode).toHaveBeenCalledExactlyOnceWith("codex", "windows-runner-1");
  expect(container.textContent).toContain("Existing sessions keep their current device");
  expect(container.textContent).toContain("Applies to new sessions");
});

it("preserves a missing saved node and allows an explicit reset", async () => {
  vi.mocked(getAgentExecutionNode).mockResolvedValue({ default_node_id: "removed-runner", routing_enforced: true });
  vi.mocked(setAgentExecutionNode).mockResolvedValue({ default_node_id: null });
  await render();
  expect(container.querySelector("select")!.value).toBe("removed-runner");
  expect(container.textContent).toContain("Not registered");
  expect(container.textContent).toContain("not ready");
  await select("");
  await act(async () => button("Save").click());
  expect(setAgentExecutionNode).toHaveBeenCalledExactlyOnceWith("codex", null);
});

it("does not permit a save after policy loading failed", async () => {
  vi.mocked(getAgentExecutionNode).mockRejectedValue(new Error("Policy unavailable"));
  await render();
  expect(container.querySelector("select")!.disabled).toBe(true);
  expect(button("Save").disabled).toBe(true);
  expect(container.textContent).toContain("Policy unavailable");
  expect(setAgentExecutionNode).not.toHaveBeenCalled();
});

it("keeps a failed change unsaved and surfaces the failure", async () => {
  vi.mocked(setAgentExecutionNode).mockRejectedValue(new Error("Node no longer registered"));
  await render();
  await select("windows-runner-1");
  await act(async () => button("Save").click());
  expect(container.textContent).toContain("Node no longer registered");
  expect(container.textContent).not.toContain("Applies to new sessions");
  expect(button("Save").disabled).toBe(false);
});
