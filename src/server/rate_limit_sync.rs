//! Rate limit sync.

use super::*;

/// Background task that periodically fetches rate-limit data from external providers
/// and caches it in the `rate_limit_cache` table for the dashboard API.
pub(super) async fn upsert_rate_limit_cache_entry(
    pg_pool: &PgPool,
    provider: &str,
    profile_id: &str,
    data: &str,
    fetched_at: i64,
) {
    let profile_id = if profile_id.trim().is_empty() {
        "default"
    } else {
        profile_id
    };
    if let Err(error) = sqlx::query(
        "INSERT INTO rate_limit_cache (provider, profile_id, data, fetched_at)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (provider, profile_id)
         DO UPDATE SET data = EXCLUDED.data, fetched_at = EXCLUDED.fetched_at",
    )
    .bind(provider)
    .bind(profile_id)
    .bind(data)
    .bind(fetched_at)
    .execute(pg_pool)
    .await
    {
        tracing::warn!(
            "[rate-limit-sync] failed to upsert rate_limit_cache row for {provider}: {error}"
        );
    }
}

pub(super) async fn sync_named_profile_rate_limits(pg_pool: &PgPool) {
    let catalog = crate::services::discord::org_schema::provider_auth_catalog();
    let now = chrono::Utc::now().timestamp();
    for (profile_id, def) in catalog {
        let Ok(provider) = crate::services::provider_auth_profile::intern_provider(&def.provider)
        else {
            continue;
        };
        if profile_id == crate::services::provider_auth_profile::DEFAULT_PROFILE_ID {
            continue;
        }
        let Ok(overlay) = crate::services::provider_auth_profile::resolve(
            provider.clone(),
            Some(&profile_id),
            None,
            &crate::services::discord::org_schema::provider_auth_catalog(),
        ) else {
            continue;
        };
        let Some(home) = overlay.home.as_ref() else {
            continue;
        };
        let result = match provider {
            crate::services::provider::ProviderKind::Claude => {
                crate::services::provider_auth::claude_oauth_token_from_home(home)
                    .ok_or_else(|| anyhow::anyhow!("no claude overlay token"))
            }
            crate::services::provider::ProviderKind::Codex => {
                crate::services::provider_auth::codex_access_token_from_home(home)
                    .ok_or_else(|| anyhow::anyhow!("no codex overlay token"))
            }
            crate::services::provider::ProviderKind::Grok => {
                crate::services::provider_auth::grok_token_from_home(home)
                    .ok_or_else(|| anyhow::anyhow!("no grok overlay token"))
            }
            _ => continue,
        };
        let token = match result {
            Ok(token) => token,
            Err(error) => {
                tracing::debug!("[rate-limit-sync] skip profile {profile_id}: {error}");
                continue;
            }
        };
        let buckets = match provider {
            crate::services::provider::ProviderKind::Claude => {
                fetch_claude_oauth_usage(&token).await
            }
            crate::services::provider::ProviderKind::Codex => fetch_codex_oauth_usage(&token).await,
            crate::services::provider::ProviderKind::Grok => fetch_grok_billing_usage(&token).await,
            _ => continue,
        };
        match buckets {
            Ok(buckets) => {
                let data = serde_json::json!({ "buckets": buckets }).to_string();
                upsert_rate_limit_cache_entry(pg_pool, provider.as_str(), &profile_id, &data, now)
                    .await;
            }
            Err(error) => {
                tracing::warn!(
                    "[rate-limit-sync] {provider} profile {profile_id} fetch failed: {error}",
                    provider = provider.as_str()
                );
            }
        }
    }
}

pub(super) async fn rate_limit_sync_loop(pg_pool: Arc<PgPool>) {
    use std::time::Duration;

    let interval = Duration::from_secs(120);
    // Run immediately on startup, then every 2 minutes
    let mut first = true;

    loop {
        if !first {
            tokio::time::sleep(interval).await;
        }
        first = false;

        let _ = sync_claude_rate_limit_cache_once_serialized(pg_pool.as_ref()).await;

        // --- Codex rate limits ---
        // Priority: 1) ~/.codex/auth.json (Codex CLI subscription), 2) OPENAI_API_KEY
        let codex_result = if let Some(token) = crate::services::provider_auth::codex_access_token()
        {
            fetch_codex_oauth_usage(&token).await
        } else if let Ok(api_key) = std::env::var("OPENAI_API_KEY") {
            fetch_openai_rate_limits(&api_key).await
        } else {
            Err(anyhow::anyhow!("no Codex credentials found"))
        };
        match codex_result {
            Ok(buckets) => {
                let data = serde_json::json!({ "buckets": buckets }).to_string();
                let now = chrono::Utc::now().timestamp();
                upsert_rate_limit_cache_entry(pg_pool.as_ref(), "codex", "default", &data, now)
                    .await;
                tracing::info!("[rate-limit-sync] Codex: {} buckets cached", buckets.len());
            }
            Err(e) => {
                tracing::warn!("[rate-limit-sync] Codex rate_limit fetch failed: {e}");
            }
        }

        // --- Gemini rate limits ---
        // Uses OAuth2 creds from ~/.gemini/oauth_creds.json.
        // Returns RPM/RPD buckets with known quota limits; usage fields are -1 (unavailable).
        match fetch_gemini_rate_limits().await {
            Ok(buckets) => {
                let n = buckets.len();
                let data = serde_json::json!({ "buckets": buckets }).to_string();
                let now = chrono::Utc::now().timestamp();
                upsert_rate_limit_cache_entry(pg_pool.as_ref(), "gemini", "default", &data, now)
                    .await;
                tracing::info!("[rate-limit-sync] Gemini: {} buckets cached", n);
            }
            Err(e) => {
                let msg = e.to_string();
                // Only suppress the genuine "not configured / file missing" case,
                // classified at the source (provider_auth) by `io::ErrorKind`:
                //   - "no home dir"            (no $HOME)
                //   - NotFound                 (oauth_creds.json does not exist)
                // PermissionDenied / IsADirectory / transient I/O are tagged
                // differently and corrupt/partial creds ("no access_token" /
                // "no refresh_token") are separate problems — all keep WARNing,
                // so we deliberately do NOT match on "oauth_creds.json" broadly
                // here (#3566 over-suppress fix, codex r2).
                let creds_missing =
                    crate::services::provider_auth::is_gemini_unconfigured_error(&e);
                if creds_missing {
                    // Gemini simply isn't configured — log once, then drop to DEBUG
                    // so the 2-minute sync loop doesn't spam an identical WARN (#3566).
                    if !GEMINI_CREDS_MISSING_WARNED.swap(true, Ordering::AcqRel) {
                        tracing::warn!(
                            "[rate-limit-sync] Gemini credentials not configured ({msg}); suppressing further repeats"
                        );
                    } else {
                        tracing::debug!(
                            "[rate-limit-sync] Gemini credentials absent; skipping (suppressed)"
                        );
                    }
                } else {
                    // Transient errors (network/API/token refresh) and corrupt/partial
                    // credentials keep WARNing.
                    tracing::warn!("[rate-limit-sync] Gemini rate_limit fetch failed: {e}");
                }
            }
        }

        sync_named_profile_rate_limits(pg_pool.as_ref()).await;

        // feature: rate-limit-aware-dispatch-gate — refresh the process-wide
        // in-memory pressure + agent→provider snapshots that the auto-queue
        // dispatch gate reads O(1) off the hot path (no DB on dispatch).
        refresh_dispatch_gate_snapshots_serialized(pg_pool.as_ref()).await;
    }
}

/// Fetch Codex usage via chatgpt.com backend API (subscription-based, no API key needed).
pub(super) async fn fetch_grok_billing_usage(
    token: &str,
) -> Result<Vec<serde_json::Value>, anyhow::Error> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()?;
    let resp = client
        .get("https://cli-chat-proxy.grok.com/v1/billing")
        .header("authorization", format!("Bearer {token}"))
        .header("xai-grok-cli", "1")
        .header("accept", "application/json")
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow::anyhow!(
            "Grok billing API returned {}",
            resp.status()
        ));
    }
    let data: serde_json::Value = resp.json().await?;
    if let Some(buckets) = data.get("buckets").and_then(|value| value.as_array()) {
        return Ok(buckets.clone());
    }
    Ok(vec![serde_json::json!({
        "label": "grok",
        "raw": data,
    })])
}
