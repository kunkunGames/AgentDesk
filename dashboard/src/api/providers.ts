import { useEffect, useMemo, useState } from "react";
import { getProviderLabel } from "../app/providerTheme";
import { request } from "./httpClient";
import { CLI_PROVIDERS, LEGACY_ONLY_PROVIDERS } from "../components/agent-manager/constants";
import type {
  PendingProviderLogin,
  ProviderAuthProfilesResponse,
  ProviderLoginStartResponse,
} from "../components/settings/SettingsProvidersModel";
import { loginAttachInstruction } from "../components/settings/SettingsProvidersModel";

export interface ProviderCatalogEntry {
  id: string;
  display_name: string;
  channel_suffix: string | null;
  binary_name: string;
  execution_surface: string;
  supports_resume: boolean;
  supports_structured_output: boolean;
  supports_tool_stream: boolean;
  supports_restricted_tool_policy: boolean;
  supports_tui_hosting: boolean;
  system_prompt_transport: string;
  context_window: string;
}

const LEGACY_ONLY = new Set<string>(LEGACY_ONLY_PROVIDERS);

export async function getProviderCatalog(): Promise<ProviderCatalogEntry[]> {
  const body = await request<{ providers: ProviderCatalogEntry[] }>("/api/providers");
  return body.providers ?? [];
}

export async function getProviderAuthProfiles(): Promise<ProviderAuthProfilesResponse> {
  return request("/api/provider-auth-profiles");
}

export async function startProviderAuthLogin(
  providerId: string,
  profileId?: string,
): Promise<PendingProviderLogin> {
  const body = await request<ProviderLoginStartResponse>(
    `/api/provider-auth-profiles/${encodeURIComponent(providerId)}/login-start`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(profileId ? { profile_id: profileId } : {}),
      maxRetries: 0,
    },
  );
  return {
    providerId,
    profileId: body.profile_id,
    home: body.home,
    tmuxSession: body.tmux_session,
    attach: body.attach || loginAttachInstruction(body.tmux_session),
  };
}

export async function completeProviderAuthLogin(
  providerId: string,
  profileId: string,
  home?: string,
): Promise<{ ok: boolean; profile_id: string; home: string }> {
  return request(`/api/provider-auth-profiles/${encodeURIComponent(providerId)}/login-complete`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ profile_id: profileId, home }),
  });
}

export async function removeProviderAuthProfile(
  providerId: string,
  profileId: string,
): Promise<{ ok: boolean; credentials_retained: boolean }> {
  return request(
    `/api/provider-auth-profiles/${encodeURIComponent(providerId)}/${encodeURIComponent(profileId)}`,
    { method: "DELETE" },
  );
}

export function selectableCatalogIds(
  entries: ProviderCatalogEntry[],
  currentId?: string | null,
): string[] {
  const ids = entries
    .map((entry) => entry.id)
    .filter((id) => !LEGACY_ONLY.has(id));
  if (currentId && !ids.includes(currentId)) {
    return [currentId, ...ids];
  }
  return ids;
}

export function meetingCatalogIds(entries: ProviderCatalogEntry[]): string[] {
  return entries
    .filter((entry) => entry.supports_restricted_tool_policy && !LEGACY_ONLY.has(entry.id))
    .map((entry) => entry.id);
}

export function catalogLabel(
  entries: ProviderCatalogEntry[],
  id: string,
  fallback?: string,
): string {
  const fromCatalog = entries
    .find((entry) => entry.id === id)
    ?.display_name?.trim();
  if (fromCatalog) return fromCatalog;
  if (fallback) return fallback;
  return getProviderLabel(id);
}

export function useProviderCatalog(currentId?: string | null) {
  const [entries, setEntries] = useState<ProviderCatalogEntry[] | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    let cancelled = false;
    void getProviderCatalog()
      .then((list) => {
        if (cancelled) return;
        setEntries(list);
        setError(false);
      })
      .catch(() => {
        if (cancelled) return;
        setEntries(null);
        setError(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const selectableIds = useMemo(() => {
    if (entries && entries.length > 0) {
      return selectableCatalogIds(entries, currentId);
    }
    const fallback = CLI_PROVIDERS.filter((id) => !LEGACY_ONLY.has(id));
    if (currentId && !fallback.includes(currentId as (typeof CLI_PROVIDERS)[number])) {
      return [currentId, ...fallback];
    }
    return fallback;
  }, [entries, currentId]);

  const meetingIds = useMemo(() => {
    if (entries && entries.length > 0) {
      return meetingCatalogIds(entries);
    }
    return CLI_PROVIDERS.filter((id) => id !== "antigravity" && !LEGACY_ONLY.has(id));
  }, [entries]);

  return {
    entries: entries ?? [],
    selectableIds,
    meetingIds,
    loading: entries === null && !error,
    error,
    catalogReady: Boolean(entries && entries.length > 0),
  };
}
