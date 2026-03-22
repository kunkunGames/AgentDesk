import { useState, useEffect } from "react";

interface BotInfo {
  valid: boolean;
  bot_id?: string;
  bot_name?: string;
  error?: string;
}

interface Guild {
  id: string;
  name: string;
  channels: Array<{ id: string; name: string; category_id?: string }>;
}

interface ChannelMapping {
  channel_id: string;
  channel_name: string;
  role_id: string;
  selected: boolean;
}

interface Props {
  isKo: boolean;
  onComplete: () => void;
}

export default function OnboardingWizard({ isKo, onComplete }: Props) {
  const tr = (ko: string, en: string) => (isKo ? ko : en);
  const [step, setStep] = useState(1);
  const [token, setToken] = useState("");
  const [botInfo, setBotInfo] = useState<BotInfo | null>(null);
  const [validating, setValidating] = useState(false);
  const [guilds, setGuilds] = useState<Guild[]>([]);
  const [selectedGuild, setSelectedGuild] = useState<string>("");
  const [mappings, setMappings] = useState<ChannelMapping[]>([]);
  const [provider, setProvider] = useState("claude");
  const [ownerId, setOwnerId] = useState("");
  const [completing, setCompleting] = useState(false);
  const [error, setError] = useState("");

  // Load existing config for pre-fill
  useEffect(() => {
    void fetch("/api/onboarding/status", { credentials: "include" })
      .then((r) => r.json())
      .then((d) => {
        if (d.owner_id) setOwnerId(d.owner_id);
        if (d.guild_id) setSelectedGuild(d.guild_id);
        if (d.bot_token) setToken(d.bot_token);
        // Pre-fill agent mappings from existing agents
        if (d.agents?.length > 0) {
          setMappings(d.agents.map((a: { agent_id: string; channel_id: string; name: string }) => ({
            channel_id: a.channel_id || "",
            channel_name: a.channel_id || "",
            role_id: a.agent_id,
            selected: true,
          })));
        }
      })
      .catch(() => {});
  }, []);

  const validateToken = async () => {
    setValidating(true);
    setError("");
    try {
      const r = await fetch("/api/onboarding/validate-token", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token }),
      });
      const d: BotInfo = await r.json();
      setBotInfo(d);
      if (d.valid) setStep(2);
      else setError(d.error || tr("토큰이 유효하지 않습니다.", "Invalid token."));
    } catch {
      setError(tr("검증 실패", "Validation failed"));
    }
    setValidating(false);
  };

  const fetchChannels = async () => {
    try {
      const r = await fetch(`/api/onboarding/channels?token=${encodeURIComponent(token)}`, {
        credentials: "include",
      });
      const d = await r.json();
      setGuilds(d.guilds || []);
      if (d.guilds?.length === 1) setSelectedGuild(d.guilds[0].id);
    } catch {
      setError(tr("채널 조회 실패", "Failed to fetch channels"));
    }
  };

  useEffect(() => {
    if (step === 2 && guilds.length === 0) void fetchChannels();
  }, [step]);

  const guild = guilds.find((g) => g.id === selectedGuild);

  useEffect(() => {
    if (guild) {
      setMappings(
        guild.channels.map((ch) => ({
          channel_id: ch.id,
          channel_name: ch.name,
          role_id: ch.name.replace(/-cc$|-cdx$/, ""),
          selected: false,
        })),
      );
    }
  }, [selectedGuild, guilds]);

  const handleComplete = async () => {
    setCompleting(true);
    setError("");
    try {
      const selected = mappings.filter((m) => m.selected);
      const r = await fetch("/api/onboarding/complete", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          token,
          guild_id: selectedGuild,
          owner_id: ownerId || null,
          provider,
          channels: selected.map((m) => ({
            channel_id: m.channel_id,
            channel_name: m.channel_name,
            role_id: m.role_id,
          })),
        }),
      });
      const d = await r.json();
      if (d.ok) {
        onComplete();
      } else {
        setError(d.error || tr("설정 저장 실패", "Failed to save"));
      }
    } catch {
      setError(tr("완료 실패", "Failed to complete"));
    }
    setCompleting(false);
  };

  const stepStyle = "rounded-2xl border p-6 space-y-4";
  const inputStyle = "w-full rounded-xl px-4 py-3 text-sm bg-white/5 border";
  const btnPrimary = "px-6 py-3 rounded-xl text-sm font-medium bg-indigo-600 text-white hover:bg-indigo-500 disabled:opacity-50 transition-colors";
  const btnSecondary = "px-6 py-3 rounded-xl text-sm font-medium border text-white/70 hover:text-white transition-colors";

  return (
    <div className="max-w-2xl mx-auto p-4 sm:p-8 space-y-6">
      <div className="text-center space-y-2">
        <h1 className="text-2xl font-bold" style={{ color: "var(--th-text-heading)" }}>
          {tr("AgentDesk 설정", "AgentDesk Setup")}
        </h1>
        <p className="text-sm" style={{ color: "var(--th-text-muted)" }}>
          {tr(`Step ${step}/5`, `Step ${step}/5`)}
        </p>
        <div className="flex gap-1 justify-center">
          {[1, 2, 3, 4, 5].map((s) => (
            <div
              key={s}
              className="h-1.5 rounded-full transition-all"
              style={{
                width: s <= step ? 40 : 20,
                backgroundColor: s <= step ? "#818cf8" : "rgba(148,163,184,0.3)",
              }}
            />
          ))}
        </div>
      </div>

      {error && (
        <div className="rounded-xl px-4 py-3 text-sm border" style={{ borderColor: "rgba(248,113,113,0.4)", color: "#fca5a5", backgroundColor: "rgba(127,29,29,0.2)" }}>
          {error}
        </div>
      )}

      {/* Step 1: Bot Token */}
      {step === 1 && (
        <div className={stepStyle} style={{ borderColor: "rgba(148,163,184,0.2)" }}>
          <h2 className="text-lg font-semibold" style={{ color: "var(--th-text-heading)" }}>
            {tr("Discord 봇 연결", "Connect Discord Bot")}
          </h2>
          <p className="text-sm" style={{ color: "var(--th-text-muted)" }}>
            {tr(
              "Discord Developer Portal에서 봇을 생성하고 토큰을 입력하세요.",
              "Create a bot in Discord Developer Portal and enter its token.",
            )}
          </p>
          <a
            href="https://discord.com/developers/applications"
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm text-indigo-400 hover:text-indigo-300"
          >
            {tr("Discord Developer Portal 열기 →", "Open Discord Developer Portal →")}
          </a>
          <input
            type="password"
            placeholder={tr("봇 토큰 입력", "Enter bot token")}
            value={token}
            onChange={(e) => setToken(e.target.value)}
            className={inputStyle}
            style={{ borderColor: "rgba(148,163,184,0.24)", color: "var(--th-text-primary)" }}
          />
          {botInfo?.valid && (
            <div className="text-sm text-emerald-400">
              ✅ {botInfo.bot_name} ({botInfo.bot_id})
            </div>
          )}
          <button onClick={() => void validateToken()} disabled={!token || validating} className={btnPrimary}>
            {validating ? tr("검증 중...", "Validating...") : tr("토큰 검증", "Validate Token")}
          </button>
        </div>
      )}

      {/* Step 2: Channel Selection */}
      {step === 2 && (
        <div className={stepStyle} style={{ borderColor: "rgba(148,163,184,0.2)" }}>
          <h2 className="text-lg font-semibold" style={{ color: "var(--th-text-heading)" }}>
            {tr("채널 선택", "Select Channels")}
          </h2>
          <p className="text-sm" style={{ color: "var(--th-text-muted)" }}>
            {tr("에이전트를 배정할 채널을 선택하세요.", "Select channels to assign agents to.")}
          </p>
          {guilds.length > 1 && (
            <select
              value={selectedGuild}
              onChange={(e) => setSelectedGuild(e.target.value)}
              className={inputStyle}
              style={{ borderColor: "rgba(148,163,184,0.24)", color: "var(--th-text-primary)" }}
            >
              <option value="">{tr("서버 선택", "Select server")}</option>
              {guilds.map((g) => (
                <option key={g.id} value={g.id}>{g.name}</option>
              ))}
            </select>
          )}
          {guild && (
            <div className="space-y-1.5 max-h-60 overflow-y-auto">
              {mappings.map((m, i) => (
                <label key={m.channel_id} className="flex items-center gap-3 rounded-xl px-4 py-2 border cursor-pointer hover:bg-white/5" style={{ borderColor: "rgba(148,163,184,0.15)" }}>
                  <input
                    type="checkbox"
                    checked={m.selected}
                    onChange={() => {
                      const next = [...mappings];
                      next[i] = { ...m, selected: !m.selected };
                      setMappings(next);
                    }}
                    className="accent-indigo-500"
                  />
                  <span className="text-sm" style={{ color: "var(--th-text-primary)" }}>#{m.channel_name}</span>
                </label>
              ))}
            </div>
          )}
          <div className="flex gap-3">
            <button onClick={() => setStep(1)} className={btnSecondary} style={{ borderColor: "rgba(148,163,184,0.3)" }}>
              {tr("이전", "Back")}
            </button>
            <button
              onClick={() => setStep(3)}
              disabled={mappings.filter((m) => m.selected).length === 0}
              className={btnPrimary}
            >
              {tr("다음", "Next")} ({mappings.filter((m) => m.selected).length}{tr("개 선택", " selected")})
            </button>
          </div>
        </div>
      )}

      {/* Step 3: Agent Config */}
      {step === 3 && (
        <div className={stepStyle} style={{ borderColor: "rgba(148,163,184,0.2)" }}>
          <h2 className="text-lg font-semibold" style={{ color: "var(--th-text-heading)" }}>
            {tr("에이전트 구성", "Agent Configuration")}
          </h2>
          <p className="text-sm" style={{ color: "var(--th-text-muted)" }}>
            {tr("각 채널에 역할 ID를 지정하세요.", "Assign a role ID to each channel.")}
          </p>
          <div className="space-y-2">
            {mappings.filter((m) => m.selected).map((m, i) => (
              <div key={m.channel_id} className="flex items-center gap-3 rounded-xl px-4 py-2 border" style={{ borderColor: "rgba(148,163,184,0.15)" }}>
                <span className="text-sm min-w-[120px]" style={{ color: "var(--th-text-muted)" }}>#{m.channel_name}</span>
                <input
                  type="text"
                  value={m.role_id}
                  onChange={(e) => {
                    const next = [...mappings];
                    const idx = mappings.findIndex((x) => x.channel_id === m.channel_id);
                    next[idx] = { ...m, role_id: e.target.value };
                    setMappings(next);
                  }}
                  className="flex-1 rounded-lg px-3 py-1.5 text-sm bg-white/5 border"
                  style={{ borderColor: "rgba(148,163,184,0.24)", color: "var(--th-text-primary)" }}
                  placeholder={tr("역할 ID", "Role ID")}
                />
              </div>
            ))}
          </div>
          <div className="flex items-center gap-4">
            <span className="text-sm" style={{ color: "var(--th-text-secondary)" }}>AI</span>
            <div className="flex rounded-xl overflow-hidden border" style={{ borderColor: "rgba(148,163,184,0.3)" }}>
              {["claude", "codex"].map((p) => (
                <button
                  key={p}
                  onClick={() => setProvider(p)}
                  className="px-4 py-2 text-sm transition-colors"
                  style={{
                    backgroundColor: provider === p ? "rgba(99,102,241,0.3)" : "transparent",
                    color: provider === p ? "#a5b4fc" : "var(--th-text-muted)",
                  }}
                >
                  {p === "claude" ? "Claude" : "Codex"}
                </button>
              ))}
            </div>
          </div>
          <div className="flex gap-3">
            <button onClick={() => setStep(2)} className={btnSecondary} style={{ borderColor: "rgba(148,163,184,0.3)" }}>{tr("이전", "Back")}</button>
            <button onClick={() => setStep(4)} className={btnPrimary}>{tr("다음", "Next")}</button>
          </div>
        </div>
      )}

      {/* Step 4: Owner */}
      {step === 4 && (
        <div className={stepStyle} style={{ borderColor: "rgba(148,163,184,0.2)" }}>
          <h2 className="text-lg font-semibold" style={{ color: "var(--th-text-heading)" }}>
            {tr("소유자 설정", "Owner Setup")}
          </h2>
          <p className="text-sm" style={{ color: "var(--th-text-muted)" }}>
            {tr(
              "Discord 사용자 ID를 입력하세요. 비워두면 첫 메시지 발신자가 자동 등록됩니다.",
              "Enter your Discord user ID. Leave blank to auto-register the first message sender.",
            )}
          </p>
          <input
            type="text"
            placeholder={tr("Discord 사용자 ID (선택)", "Discord User ID (optional)")}
            value={ownerId}
            onChange={(e) => setOwnerId(e.target.value)}
            className={inputStyle}
            style={{ borderColor: "rgba(148,163,184,0.24)", color: "var(--th-text-primary)" }}
          />
          <div className="flex gap-3">
            <button onClick={() => setStep(3)} className={btnSecondary} style={{ borderColor: "rgba(148,163,184,0.3)" }}>{tr("이전", "Back")}</button>
            <button onClick={() => setStep(5)} className={btnPrimary}>{tr("다음", "Next")}</button>
          </div>
        </div>
      )}

      {/* Step 5: Summary */}
      {step === 5 && (
        <div className={stepStyle} style={{ borderColor: "rgba(148,163,184,0.2)" }}>
          <h2 className="text-lg font-semibold" style={{ color: "var(--th-text-heading)" }}>
            {tr("설정 확인", "Confirm Setup")}
          </h2>
          <div className="space-y-2 text-sm" style={{ color: "var(--th-text-primary)" }}>
            <div>🤖 {botInfo?.bot_name}</div>
            <div>🏠 {guilds.find((g) => g.id === selectedGuild)?.name}</div>
            <div>🔧 {provider === "claude" ? "Claude" : "Codex"}</div>
            <div>📋 {mappings.filter((m) => m.selected).length}{tr("개 채널", " channels")}</div>
            {mappings.filter((m) => m.selected).map((m) => (
              <div key={m.channel_id} className="pl-6 text-xs" style={{ color: "var(--th-text-muted)" }}>
                #{m.channel_name} → {m.role_id}
              </div>
            ))}
          </div>
          <div className="flex gap-3">
            <button onClick={() => setStep(4)} className={btnSecondary} style={{ borderColor: "rgba(148,163,184,0.3)" }}>{tr("이전", "Back")}</button>
            <button onClick={() => void handleComplete()} disabled={completing} className={btnPrimary}>
              {completing ? tr("설정 중...", "Setting up...") : tr("설정 완료", "Complete Setup")}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
