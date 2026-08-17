import { describe, expect, it } from "vitest";
import {
  catalogLabel,
  meetingCatalogIds,
  selectableCatalogIds,
  type ProviderCatalogEntry,
} from "./providers";

function entry(
  id: string,
  extras: Partial<ProviderCatalogEntry> = {},
): ProviderCatalogEntry {
  return {
    id,
    display_name: id,
    channel_suffix: null,
    binary_name: id,
    execution_surface: "stream_json_cli",
    supports_resume: true,
    supports_structured_output: false,
    supports_tool_stream: false,
    supports_restricted_tool_policy: true,
    supports_tui_hosting: false,
    system_prompt_transport: "native",
    context_window: "unknown",
    ...extras,
  };
}

describe("provider catalog presentation", () => {
  it("keeps grok and antigravity selectable and excludes ghosts", () => {
    const ids = selectableCatalogIds([
      entry("claude"),
      entry("grok"),
      entry("antigravity", { supports_restricted_tool_policy: false }),
      entry("copilot"),
    ]);
    expect(ids).toEqual(["claude", "grok", "antigravity"]);
  });

  it("keeps a legacy current id visible without adding it to create lists", () => {
    const ids = selectableCatalogIds([entry("claude"), entry("grok")], "api");
    expect(ids[0]).toBe("api");
    expect(ids).toContain("grok");
  });

  it("filters meeting providers to restricted-capable runtimes", () => {
    const ids = meetingCatalogIds([
      entry("claude"),
      entry("grok"),
      entry("antigravity", { supports_restricted_tool_policy: false }),
    ]);
    expect(ids).toEqual(["claude", "grok"]);
  });

  it("prefers catalog display names and falls back to themed labels", () => {
    expect(catalogLabel([entry("qwen", { display_name: "Qwen Code" })], "qwen")).toBe(
      "Qwen Code",
    );
    expect(catalogLabel([], "claude")).toBe("Claude");
    expect(catalogLabel([], "grok")).toBe("Grok");
    expect(catalogLabel([], "antigravity")).toBe("Antigravity");
  });
});
