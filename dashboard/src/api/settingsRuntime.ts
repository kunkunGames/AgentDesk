import { z } from "zod";

import type { CompanySettings, VoiceConfigPutBody, VoiceConfigResponse } from "../types";
import { clearCachedGet, readCachedGet, request, type CachedGetEntry } from "./httpClient";

export async function getSettings(): Promise<Partial<CompanySettings>> {
  return request("/api/settings");
}

export async function saveSettings(
  settings: Partial<CompanySettings>,
): Promise<{ ok: boolean }> {
  return request("/api/settings", {
    method: "PUT",
    body: JSON.stringify(settings),
  });
}

export class VoiceConfigApiError extends Error {
  status: number;
  payload: unknown;

  constructor(status: number, payload: unknown) {
    const message =
      typeof payload === "object" &&
      payload !== null &&
      "message" in payload &&
      typeof (payload as { message?: unknown }).message === "string"
        ? (payload as { message: string }).message
        : `HTTP ${status}`;
    super(message);
    this.name = "VoiceConfigApiError";
    this.status = status;
    this.payload = payload;
  }
}

export async function getVoiceConfig(): Promise<VoiceConfigResponse> {
  return request("/api/voice/config");
}

export async function saveVoiceConfig(
  body: VoiceConfigPutBody,
): Promise<VoiceConfigResponse> {
  const response = await fetch("/api/voice/config", {
    method: "PUT",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const payload = await response.json().catch(() => ({ error: "unknown" }));
  if (!response.ok) {
    throw new VoiceConfigApiError(response.status, payload);
  }
  clearCachedGet("/api/voice/config");
  return payload as VoiceConfigResponse;
}

// ── Runtime Config ──

export interface RuntimeConfigResponse {
  current: RuntimeConfigMap;
  defaults: RuntimeConfigMap;
  explicit_keys?: string[];
}

export type RuntimeConfigValue = number | string | boolean;
export type RuntimeConfigMap = Record<string, RuntimeConfigValue>;
export type RuntimeConfigSaveBody = Record<string, RuntimeConfigValue | string[] | undefined>;

export type EscalationMode = "pm" | "user" | "scheduled";

export interface EscalationSettings {
  mode: EscalationMode;
  owner_user_id: number | null;
  pm_channel_id: string | null;
  schedule: {
    pm_hours: string;
    timezone: string;
  };
}

export interface EscalationSettingsResponse {
  current: EscalationSettings;
  defaults: EscalationSettings;
}

export async function getRuntimeConfig(): Promise<RuntimeConfigResponse> {
  return request("/api/settings/runtime-config");
}

export async function saveRuntimeConfig(
  patch: RuntimeConfigSaveBody,
): Promise<{ ok: boolean }> {
  return request("/api/settings/runtime-config", {
    method: "PUT",
    body: JSON.stringify(patch),
  });
}

export type OperatorConnectorState =
  | "ready"
  | "skipped"
  | "missing_config"
  | "missing_path"
  | "missing_provider"
  | "invalid_config"
  | string;

export interface OperatorConnectorStatus {
  id: string;
  name: string;
  state: OperatorConnectorState;
  optional: boolean;
  kind: "filesystem" | "oauth" | string;
  env_var: string;
  env_vars: string[];
  source: string | null;
  reason: string | null;
  detail: string;
  setup_actions: string[];
  capabilities: string[];
  connection?: {
    state: string;
    reason: string | null;
    scopes: string[];
    access_expires_at: string | null;
    landing_url: string | null;
    accounts?: KakaoAccountSummary[];
  };
  actions: string[];
}

export interface OperatorConnectorsResponse {
  connectors: OperatorConnectorStatus[];
  summary: {
    ready: number;
    skipped: number;
    missing_config: number;
    missing_path: number;
    missing_provider: number;
    invalid_config: number;
    invalid: number;
    total: number;
    core_runtime_blocking: boolean;
  };
}

export async function getOperatorConnectors(): Promise<OperatorConnectorsResponse> {
  return request("/api/settings/operator-connectors");
}

const kakaoOAuthStartResponseSchema = z.object({
  authorize_url: z.string().url(),
  expires_in_seconds: z.number().int().positive(),
});

const kakaoDisconnectResponseSchema = z.object({
  ok: z.boolean(),
  account_id: z.string().min(1),
  remote_unlinked: z.literal(false),
});

const kakaoAccountSummarySchema = z.object({
  account_id: z.string().min(1),
  status: z.string().min(1),
  scopes: z.array(z.string()),
  access_expires_at: z.string().nullable(),
  is_legacy: z.boolean(),
});

const kakaoAccountsResponseSchema = z.object({ accounts: z.array(kakaoAccountSummarySchema) });

const kakaoFriendViewSchema = z.object({
  uuid: z.string().min(1),
  display_name: z.string(),
});

const kakaoFriendsPageSchema = z.object({
  friends: z.array(kakaoFriendViewSchema).max(100),
  total_count: z.number().int().nonnegative(),
  offset: z.number().int().nonnegative(),
  limit: z.number().int().min(1).max(100),
  next_offset: z.number().int().nonnegative().nullable(),
});

const kakaoSendStatusSchema = z.enum([
  "success",
  "partial_success",
  "failed",
  "unknown",
]);

const kakaoSendResultSchema = z
  .object({
    request_id: z.string().uuid(),
    status: kakaoSendStatusSchema,
    requested_count: z.number().int().min(1).max(5),
    successful_count: z.number().int().min(0).max(5),
    failed_count: z.number().int().min(0).max(5),
    replayed: z.boolean(),
    delivery_may_have_occurred: z.boolean(),
    automatic_retry_allowed: z.literal(false),
  })
  .refine(
    ({
      status,
      requested_count,
      successful_count,
      failed_count,
      delivery_may_have_occurred,
    }) => {
      switch (status) {
        case "success":
          return successful_count === requested_count
            && failed_count === 0
            && delivery_may_have_occurred;
        case "partial_success":
          return successful_count > 0
            && failed_count > 0
            && successful_count + failed_count === requested_count
            && delivery_may_have_occurred;
        case "failed":
          return successful_count === 0
            && failed_count === requested_count
            && !delivery_may_have_occurred;
        case "unknown":
          return successful_count === 0
            && failed_count === 0
            && delivery_may_have_occurred;
      }
    },
    { message: "Kakao result status, counts, and delivery risk are inconsistent" },
  );

export type KakaoOAuthStartResponse = z.infer<
  typeof kakaoOAuthStartResponseSchema
>;
export type KakaoDisconnectResponse = z.infer<typeof kakaoDisconnectResponseSchema>;
export type KakaoAccountSummary = z.infer<typeof kakaoAccountSummarySchema>;
export type KakaoFriendView = z.infer<typeof kakaoFriendViewSchema>;
export type KakaoFriendsPage = z.infer<typeof kakaoFriendsPageSchema>;
export type KakaoSendStatus = z.infer<typeof kakaoSendStatusSchema>;
export type KakaoSendResult = z.infer<typeof kakaoSendResultSchema>;

export async function startKakaoOAuth(): Promise<KakaoOAuthStartResponse> {
  return request(
    "/api/kakao/oauth/start",
    {
      method: "POST",
      maxRetries: 0,
    },
    kakaoOAuthStartResponseSchema,
  );
}

export async function getKakaoAccounts(): Promise<KakaoAccountSummary[]> {
  return request("/api/kakao/accounts", undefined, kakaoAccountsResponseSchema).then((response) => response.accounts);
}

export async function disconnectKakao(accountId: string): Promise<KakaoDisconnectResponse> {
  return request(
    `/api/kakao/accounts/${encodeURIComponent(accountId)}`,
    {
      method: "DELETE",
      maxRetries: 0,
    },
    kakaoDisconnectResponseSchema,
  );
}

export async function getKakaoFriends(accountId: string, offset = 0, limit = 20): Promise<KakaoFriendsPage> {
  return request(
    `/api/kakao/friends?account_id=${encodeURIComponent(accountId)}&offset=${offset}&limit=${limit}`,
    undefined,
    kakaoFriendsPageSchema,
  );
}

export async function sendKakaoFriendMessage(
  idempotencyKey: string,
  accountId: string,
  receiverUuids: string[],
  text: string,
  imageUrl?: string,
): Promise<KakaoSendResult> {
  return request(
    "/api/kakao/messages/send",
    {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
      body: JSON.stringify({
        account_id: accountId,
        receiver_uuids: receiverUuids,
        text,
        image_url: imageUrl,
        confirmed: true,
      }),
      maxRetries: 0,
    },
    kakaoSendResultSchema,
  );
}

export async function sendKakaoMemoMessage(
  idempotencyKey: string,
  accountId: string,
  text: string,
  imageUrl?: string,
): Promise<KakaoSendResult> {
  return request(
    "/api/kakao/messages/send-to-me",
    {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
      body: JSON.stringify({ account_id: accountId, text, image_url: imageUrl, confirmed: true }),
      maxRetries: 0,
    },
    kakaoSendResultSchema,
  );
}

export async function getEscalationSettings(): Promise<EscalationSettingsResponse> {
  return request("/api/settings/escalation");
}

export async function saveEscalationSettings(
  settings: EscalationSettings,
): Promise<EscalationSettingsResponse> {
  return request("/api/settings/escalation", {
    method: "PUT",
    body: JSON.stringify(settings),
  });
}

// ── Runtime Health ──

export interface HealthProviderStatus {
  name: string;
  connected: boolean;
  active_turns: number;
  queue_depth: number;
  sessions: number;
  restart_pending: boolean;
  last_turn_at: string | null;
}

export interface HealthDispatchOutboxStats {
  pending: number;
  retrying: number;
  permanent_failures: number;
  oldest_pending_age: number;
}

export interface HealthResponse {
  status: "healthy" | "degraded" | "unhealthy" | string;
  version?: string;
  uptime_secs?: number;
  global_active?: number;
  global_finalizing?: number;
  deferred_hooks?: number;
  queue_depth?: number;
  watcher_count?: number;
  recovery_duration?: number;
  degraded_reasons?: string[];
  providers?: HealthProviderStatus[];
  db?: boolean;
  dashboard?: boolean;
  outbox_age?: number;
  dispatch_outbox?: HealthDispatchOutboxStats;
}

// #2050 P3 finding 18 — normalize optional fields so consumers don't have
// to defensively `??` everything. Adds default `providers: []` /
// `degraded_reasons: []` / `dispatch_outbox` zero shape so UI render paths
// can rely on consistent typing under transient server omissions.
function normalizeHealth(raw: unknown): HealthResponse {
  const source = (raw ?? {}) as Partial<HealthResponse>;
  return {
    status: source.status ?? "unhealthy",
    version: source.version,
    uptime_secs: source.uptime_secs,
    global_active: source.global_active,
    global_finalizing: source.global_finalizing,
    deferred_hooks: source.deferred_hooks,
    queue_depth: source.queue_depth,
    watcher_count: source.watcher_count,
    recovery_duration: source.recovery_duration,
    degraded_reasons: Array.isArray(source.degraded_reasons)
      ? source.degraded_reasons
      : [],
    providers: Array.isArray(source.providers) ? source.providers : [],
    db: source.db,
    dashboard: source.dashboard,
    outbox_age: source.outbox_age,
    dispatch_outbox: source.dispatch_outbox ?? {
      pending: 0,
      retrying: 0,
      permanent_failures: 0,
      oldest_pending_age: 0,
    },
  };
}

export async function getHealth(): Promise<HealthResponse> {
  const raw = await request<unknown>("/api/health");
  return normalizeHealth(raw);
}

export function getCachedHealth(): CachedGetEntry<HealthResponse> | null {
  const cached = readCachedGet<unknown>("/api/health");
  if (!cached) return null;
  return {
    data: normalizeHealth(cached.data),
    fetchedAt: cached.fetchedAt,
  };
}

export interface PromptManifestRetentionStatus {
  total_stored_bytes: number;
  total_original_bytes: number;
  truncated_count: number;
  manifest_count: number;
  layer_count: number;
  oldest_full_content_at: string | null;
  retention_horizon_at: string | null;
  retention_days: number;
  per_layer_max_bytes_adk_provided: number;
  per_layer_max_bytes_user_derived: number;
  enabled: boolean;
  restart_required_for_config_changes: boolean;
  config_applied_at: string;
  config_source: string;
  hot_reload: boolean;
}

export async function getPromptManifestRetention(): Promise<PromptManifestRetentionStatus> {
  return request("/api/prompt-manifest/retention");
}

// ── Dispatches ──

export async function createDispatch(body: {
  kanban_card_id: string;
  to_agent_id: string;
  title: string;
  dispatch_type?: string;
}): Promise<{ dispatch: Record<string, unknown> }> {
  return request("/api/dispatches", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

// ── Stats ──
