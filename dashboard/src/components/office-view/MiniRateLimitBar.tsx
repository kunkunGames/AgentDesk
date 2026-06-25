import { useEffect, useState } from "react";
import {
  getProviderLevelColors,
  getProviderMeta,
} from "../../app/providerTheme";
import {
  formatRateLimitResetLabel,
  transformRLProviders,
  type RLBucket,
  type RLProvider,
  type RawRLProvider,
} from "./MiniRateLimitBarModel";

const RL_ICONS: Record<string, string> = {
  Claude: "🤖",
  Codex: "⚡",
  Gemini: "🔮",
  Qwen: "🧠",
  OpenCode: "🧩",
  Copilot: "🛩️",
  Antigravity: "🌀",
  API: "🔌",
};

function formatCurrentPct(bucket: RLBucket): string {
  return bucket.utilization === null ? "N/A" : `${bucket.utilization}%`;
}

function formatProjectedPct(bucket: RLBucket): string {
  return bucket.projectedUtilization === null ? "→ --" : `→ ${bucket.projectedUtilization}%`;
}

function shouldShowProjectedUtilization(bucket: RLBucket): boolean {
  return bucket.label.trim().toLowerCase() !== "5h";
}

function formatCompactPctPair(bucket: RLBucket, showProjectedUtilization: boolean): string {
  if (bucket.utilization === null) return "N/A";
  if (!showProjectedUtilization) return `${bucket.utilization}%`;
  if (bucket.projectedUtilization === null) return `${bucket.utilization}→--`;
  return `${bucket.utilization}→${bucket.projectedUtilization}%`;
}

function projectionChipStyle(provider: string, bucket: RLBucket) {
  const colors = getProviderLevelColors(provider, bucket.projectedLevel ?? bucket.level);
  return {
    color: bucket.projectedUtilization === null ? "var(--th-text-muted)" : colors.text,
    background:
      bucket.projectedUtilization === null
        ? "color-mix(in oklch, var(--fg-faint) 10%, var(--bg-2) 90%)"
        : `color-mix(in oklch, ${colors.bar} 14%, var(--bg-2) 86%)`,
    border:
      bucket.projectedUtilization === null
        ? "1px solid color-mix(in oklch, var(--fg-faint) 20%, var(--line) 80%)"
        : `1px solid color-mix(in oklch, ${colors.bar} 28%, var(--line) 72%)`,
  };
}

function RateLimitBucketGauge({
  bucket,
  provider,
  isKo,
  density,
}: {
  bucket: RLBucket;
  provider: string;
  isKo: boolean;
  density: "compact" | "comfortable";
}) {
  const colors = getProviderLevelColors(provider, "normal");
  const showProjectedUtilization = shouldShowProjectedUtilization(bucket);
  const utilizationWidth =
    bucket.utilization === null ? "0%" : `${Math.min(bucket.utilization, 100)}%`;
  const projectionWidth =
    !showProjectedUtilization || bucket.projectedUtilization === null
      ? "0%"
      : `${Math.min(Math.max(bucket.projectedUtilization, bucket.utilization ?? 0), 100)}%`;
  const nowMs = Date.now();
  const resetLabel = formatRateLimitResetLabel(bucket.resetAtMs, isKo, nowMs);
  const resetShortLabel = formatRateLimitResetLabel(bucket.resetAtMs, isKo, nowMs, "short");

  if (density === "comfortable") {
    return (
      <div key={bucket.id} className="min-w-0 space-y-1">
        <div className="grid min-w-0 grid-cols-[22px_minmax(0,1fr)_32px_48px] items-center gap-1.5">
          <span
            className="text-xs font-bold"
            style={{ color: colors.bar }}
          >
            {bucket.label}
          </span>
          <div className="flex-1 min-w-0">
            <div
              className="relative h-1.5 rounded-full overflow-hidden"
              style={{ background: "var(--line-soft)" }}
            >
              <div
                className="absolute inset-y-0 left-0 rounded-full"
                style={{
                  width: projectionWidth,
                  opacity: 0.38,
                  backgroundImage: `repeating-linear-gradient(90deg, ${colors.bar} 0 7px, transparent 7px 11px)`,
                }}
              />
              <div
                className="absolute inset-y-0 left-0 rounded-full"
                style={{
                  width: utilizationWidth,
                  background: bucket.utilization === null ? "transparent" : colors.bar,
                }}
              />
            </div>
          </div>
          <span
            className="text-xs font-mono font-bold text-right"
            style={{
              color: bucket.utilization === null ? "var(--th-text-muted)" : colors.bar,
            }}
          >
            {formatCurrentPct(bucket)}
          </span>
          {showProjectedUtilization ? (
            <span
              className="w-[48px] rounded px-1 py-0.5 text-center text-[10px] font-mono font-bold"
              style={projectionChipStyle(provider, bucket)}
            >
              {formatProjectedPct(bucket)}
            </span>
          ) : null}
        </div>
        <div
          className="truncate text-[10px] font-medium"
          style={{ color: "var(--th-text-muted)" }}
          title={resetLabel}
        >
          {resetShortLabel}
        </div>
      </div>
    );
  }

  return (
    <div key={bucket.id} className="min-w-0 space-y-0.5">
      <div className="flex min-w-0 items-center gap-1">
        <span
          className="text-xs font-bold shrink-0 w-[20px]"
          style={{ color: colors.bar }}
        >
          {bucket.label}
        </span>
        <div className="flex-1 min-w-0">
          <div
            className="relative h-[3px] rounded-full overflow-hidden"
            style={{ background: "var(--line-soft)" }}
          >
            <div
              className="absolute inset-y-0 left-0 rounded-full"
              style={{
                width: projectionWidth,
                opacity: 0.38,
                backgroundImage: `repeating-linear-gradient(90deg, ${colors.bar} 0 5px, transparent 5px 8px)`,
              }}
            />
            <div
              className="absolute inset-y-0 left-0 rounded-full"
              style={{
                width: utilizationWidth,
                background: bucket.utilization === null ? "transparent" : colors.bar,
              }}
            />
          </div>
        </div>
        <span
          className="text-[10px] font-mono font-bold shrink-0 w-[54px] text-right"
          style={{
            color: bucket.utilization === null ? "var(--th-text-muted)" : colors.bar,
          }}
        >
          {formatCompactPctPair(bucket, showProjectedUtilization)}
        </span>
      </div>
      <div
        className="truncate text-[9px] font-medium"
        style={{ color: "var(--th-text-muted)" }}
        title={resetLabel}
      >
        {resetShortLabel}
      </div>
    </div>
  );
}

export function MiniRateLimitBar({
  isKo,
  density = "compact",
}: {
  isKo: boolean;
  density?: "compact" | "comfortable";
}) {
  const [providers, setProviders] = useState<RLProvider[]>([]);
  const isComfy = density === "comfortable";

  useEffect(() => {
    let mounted = true;
    const load = async () => {
      try {
        const res = await fetch("/api/rate-limits", { credentials: "include" });
        if (!res.ok) return;
        const json = await res.json() as { providers: RawRLProvider[] };
        if (mounted) setProviders(transformRLProviders(json.providers ?? []));
      } catch { /* ignore */ }
    };
    load();
    const timer = setInterval(load, 30_000);
    return () => { mounted = false; clearInterval(timer); };
  }, []);

  if (providers.length === 0) return null;

  if (isComfy) {
    return (
      <div className="mt-3 space-y-3">
        {providers.map((p) => {
          const providerMeta = getProviderMeta(p.provider);
          const visible = p.buckets;
          return (
            <div key={p.provider} className="space-y-1.5">
              <div className="flex items-center gap-1">
                <span
                  className="text-sm font-bold uppercase truncate"
                  style={{ color: providerMeta.color }}
                >
                  {(RL_ICONS[p.provider] ?? "•")} {p.provider}
                </span>
                {p.stale ? (
                  <span
                    className="rounded px-1 text-[8px] font-medium shrink-0"
                    style={{
                      color: "var(--warn)",
                      background:
                        "color-mix(in oklch, var(--warn) 14%, var(--bg-2) 86%)",
                      border:
                        "1px solid color-mix(in oklch, var(--warn) 28%, var(--line) 72%)",
                    }}
                  >
                    {isKo ? "지연" : "STALE"}
                  </span>
                ) : null}
              </div>
              {p.unsupported || visible.length === 0 ? (
                <div className="flex items-center gap-2 overflow-hidden">
                  <span
                    className="rounded px-1.5 py-0.5 text-[10px] font-semibold shrink-0"
                    style={{
                      color: "var(--fg-dim)",
                      background:
                        "color-mix(in oklch, var(--fg-faint) 10%, var(--bg-2) 90%)",
                      border:
                        "1px solid color-mix(in oklch, var(--fg-faint) 20%, var(--line) 80%)",
                    }}
                  >
                    {p.unsupported ? "N/A" : (isKo ? "비어있음" : "EMPTY")}
                  </span>
                  <span
                    className="truncate text-[11px]"
                    style={{ color: "var(--th-text-muted)" }}
                  >
                    {p.unsupported
                      ? p.reason ??
                        (isKo
                          ? "한도 텔레메트리 미지원"
                          : "Rate-limit telemetry unavailable")
                      : isKo
                        ? "표시할 버킷 데이터 없음"
                        : "No bucket data"}
                  </span>
                </div>
              ) : (
                <div className="grid grid-cols-1 gap-y-1.5 min-[520px]:grid-cols-2 min-[520px]:gap-x-3 min-[520px]:gap-y-1">
                  {visible.map((b) => (
                    <RateLimitBucketGauge
                      key={b.id}
                      bucket={b}
                      provider={p.provider}
                      isKo={isKo}
                      density="comfortable"
                    />
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </div>
    );
  }

  return (
    <div className="mt-2 space-y-1">
      {providers.map((p) => {
        const providerMeta = getProviderMeta(p.provider);
        const visible = p.buckets;
        return (
          <div key={p.provider} className="flex items-center gap-0">
            <div className="flex items-center gap-1 shrink-0" style={{ width: 96 }}>
              <span
                className="text-xs font-bold uppercase truncate"
                style={{ color: providerMeta.color }}
              >
                {(RL_ICONS[p.provider] ?? "•")} {p.provider}
              </span>
              {p.stale ? (
                <span
                  className="rounded px-0.5 text-[7px] font-medium shrink-0"
                  style={{
                    color: "var(--warn)",
                    background:
                      "color-mix(in oklch, var(--warn) 14%, var(--bg-2) 86%)",
                    border:
                      "1px solid color-mix(in oklch, var(--warn) 28%, var(--line) 72%)",
                  }}
                >
                  {isKo ? "지연" : "STALE"}
                </span>
              ) : null}
            </div>
            {p.unsupported || visible.length === 0 ? (
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 overflow-hidden">
                  <span
                    className="rounded px-1.5 py-0.5 text-[9px] font-semibold shrink-0"
                    style={{
                      color: "var(--fg-dim)",
                      background:
                        "color-mix(in oklch, var(--fg-faint) 10%, var(--bg-2) 90%)",
                      border:
                        "1px solid color-mix(in oklch, var(--fg-faint) 20%, var(--line) 80%)",
                    }}
                  >
                    {p.unsupported ? "N/A" : (isKo ? "비어있음" : "EMPTY")}
                  </span>
                  <span className="truncate text-[11px]" style={{ color: "var(--th-text-muted)" }}>
                    {p.unsupported
                      ? (p.reason ?? (isKo ? "한도 텔레메트리 미지원" : "Rate-limit telemetry unavailable"))
                      : (isKo ? "표시할 버킷 데이터 없음" : "No bucket data")}
                  </span>
                </div>
              </div>
            ) : (
              <div className="flex-1 grid grid-cols-2 gap-x-2">
                {visible.map((b) => (
                  <RateLimitBucketGauge
                    key={b.id}
                    bucket={b}
                    provider={p.provider}
                    isKo={isKo}
                    density="compact"
                  />
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
