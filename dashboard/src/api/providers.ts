import { useEffect, useMemo, useState } from "react";
import { request } from "./httpClient";
import { CLI_PROVIDERS, LEGACY_ONLY_PROVIDERS } from "../components/agent-manager/constants";

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
  return entries.find((entry) => entry.id === id)?.display_name ?? fallback ?? id;
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
