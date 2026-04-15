import { describe, expect, it } from "vitest";
import { normalizeRateLimitProviderLabel, transformRawData } from "./RateLimitWidget";

describe("RateLimitWidget helpers", () => {
  it("normalizes provider labels for supported dashboard providers", () => {
    expect(normalizeRateLimitProviderLabel("claude")).toBe("Claude");
    expect(normalizeRateLimitProviderLabel("gemini")).toBe("Gemini");
    expect(normalizeRateLimitProviderLabel("qwen")).toBe("Qwen");
  });

  it("preserves unsupported providers and computes visible bucket utilization", () => {
    const data = transformRawData(
      {
        providers: [
          {
            provider: "gemini",
            buckets: [],
            fetched_at: 1_700_000_000,
            stale: false,
            unsupported: true,
            reason: "No Gemini rate-limit telemetry source is implemented yet.",
          },
          {
            provider: "qwen",
            buckets: [
              { name: "1h", limit: 200, used: 50, remaining: 150, reset: 1_700_000_600 },
            ],
            fetched_at: 1_700_000_000,
            stale: false,
          },
        ],
      },
      80,
      95,
    );

    expect(data.providers).toHaveLength(2);
    expect(data.providers[0]).toMatchObject({
      provider: "Gemini",
      unsupported: true,
      reason: "No Gemini rate-limit telemetry source is implemented yet.",
    });
    expect(data.providers[1]?.provider).toBe("Qwen");
    expect(data.providers[1]?.buckets[0]).toMatchObject({
      label: "1h",
      utilization: 25,
      level: "normal",
    });
  });
});
