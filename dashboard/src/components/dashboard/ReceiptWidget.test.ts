import { describe, expect, it } from "vitest";
import { buildProviderOptions, deriveReceiptView, type ReceiptData } from "./ReceiptWidget";

function makeReceiptData(): ReceiptData {
  return {
    period_label: "This Month",
    period_start: "2026-04-01",
    period_end: "2026-04-15",
    models: [
      {
        model: "claude-sonnet-4",
        display_name: "Claude Sonnet 4",
        total_tokens: 1_200,
        cost: 4.5,
        provider: "claude",
      },
      {
        model: "gemini-3.1-pro-preview",
        display_name: "Gemini 3.1 Pro Preview",
        total_tokens: 700,
        cost: 0,
        provider: "gemini",
      },
      {
        model: "qwen3.5:397b-cloud",
        display_name: "Qwen 3.5 397B Cloud",
        total_tokens: 300,
        cost: 0,
        provider: "qwen",
      },
    ],
    subtotal: 4.5,
    cache_discount: 0.9,
    total: 3.6,
    stats: {
      total_messages: 20,
      total_sessions: 3,
    },
    providers: [
      {
        provider: "Claude",
        tokens: 1_200,
        percentage: 54.5,
      },
      {
        provider: "Gemini",
        tokens: 700,
        percentage: 31.8,
      },
    ],
    agents: [
      {
        agent: "codex",
        tokens: 2_200,
        cost: 4.5,
        percentage: 100,
      },
    ],
  };
}

describe("ReceiptWidget helpers", () => {
  it("builds provider options from both provider shares and model rows", () => {
    const options = buildProviderOptions(makeReceiptData());

    expect(options).toEqual(["Claude", "Gemini", "qwen"]);
  });

  it("filters the receipt view to the selected Gemini provider without fallback cost inflation", () => {
    const view = deriveReceiptView(makeReceiptData(), "Gemini");

    expect(view).not.toBeNull();
    expect(view?.models).toHaveLength(1);
    expect(view?.models[0]).toMatchObject({
      provider: "gemini",
      display_name: "Gemini 3.1 Pro Preview",
      total_tokens: 700,
      cost: 0,
    });
    expect(view?.subtotal).toBe(0);
    expect(view?.cache_discount).toBe(0);
    expect(view?.total).toBe(0);
    expect(view?.providers).toEqual([
      {
        provider: "gemini",
        tokens: 700,
        percentage: 100,
      },
    ]);
    expect(view?.agents).toEqual([]);
  });

  it("matches Qwen providers case-insensitively when deriving the slice view", () => {
    const view = deriveReceiptView(makeReceiptData(), "QWEN");

    expect(view).not.toBeNull();
    expect(view?.models).toHaveLength(1);
    expect(view?.models[0]).toMatchObject({
      provider: "qwen",
      display_name: "Qwen 3.5 397B Cloud",
      total_tokens: 300,
      cost: 0,
    });
    expect(view?.providers[0]).toMatchObject({
      provider: "qwen",
      tokens: 300,
      percentage: 100,
    });
  });
});
