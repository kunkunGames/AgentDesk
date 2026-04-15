import { describe, expect, it } from "vitest";
import {
  normalizeMiniRateLimitProviderLabel,
  transformRLProviders,
} from "./OfficeInsightPanel";

describe("OfficeInsightPanel mini rate-limit helpers", () => {
  it("normalizes provider labels for compact cards", () => {
    expect(normalizeMiniRateLimitProviderLabel("gemini")).toBe("Gemini");
    expect(normalizeMiniRateLimitProviderLabel("qwen")).toBe("Qwen");
  });

  it("keeps unsupported providers visible instead of dropping them", () => {
    const rows = transformRLProviders([
      {
        provider: "gemini",
        buckets: [],
        stale: false,
        unsupported: true,
        reason: "No Gemini rate-limit telemetry source is implemented yet.",
      },
      {
        provider: "qwen",
        buckets: [{ name: "1h", limit: 100, used: 92, remaining: 8, reset: 1_700_000_600 }],
        stale: false,
      },
    ]);

    expect(rows[0]).toMatchObject({
      provider: "Gemini",
      unsupported: true,
      reason: "No Gemini rate-limit telemetry source is implemented yet.",
    });
    expect(rows[1]?.buckets[0]).toMatchObject({
      label: "1h",
      utilization: 92,
      level: "warning",
    });
  });
});
