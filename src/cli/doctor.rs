//! `agentdesk doctor` — environment diagnostics.

use std::process::Command;

use super::dcserver;
use crate::config;
use crate::services::provider::ProviderKind;
use serde_json::Value;

struct Check {
    name: &'static str,
    pass: bool,
    detail: String,
}

impl Check {
    fn ok(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            pass: true,
            detail: detail.into(),
        }
    }
    fn fail(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            pass: false,
            detail: detail.into(),
        }
    }
}

fn discord_bot_check_from_health(base: &str, body: &Value) -> Check {
    if let Some(providers) = body.get("providers").and_then(Value::as_array) {
        let total = providers.len();
        let connected: Vec<String> = providers
            .iter()
            .filter(|provider| provider.get("connected").and_then(Value::as_bool) == Some(true))
            .filter_map(|provider| provider.get("name").and_then(Value::as_str))
            .map(str::to_string)
            .collect();
        let disconnected: Vec<String> = providers
            .iter()
            .filter(|provider| provider.get("connected").and_then(Value::as_bool) != Some(true))
            .filter_map(|provider| provider.get("name").and_then(Value::as_str))
            .map(str::to_string)
            .collect();
        let overall = body
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        if total > 0 && connected.len() == total && overall == "healthy" {
            return Check::ok(
                "Discord Bot",
                format!(
                    "{}/{} connected — {}",
                    connected.len(),
                    total,
                    connected.join(", ")
                ),
            );
        }
        if total == 0 {
            return Check::fail(
                "Discord Bot",
                format!("no providers registered in unified health payload — {base}"),
            );
        }
        return Check::fail(
            "Discord Bot",
            format!(
                "overall={overall}, connected={}/{}, offline={}",
                connected.len(),
                total,
                if disconnected.is_empty() {
                    "-".to_string()
                } else {
                    disconnected.join(", ")
                }
            ),
        );
    }

    let ok = body.get("ok").and_then(Value::as_bool).unwrap_or(false);
    let db = body.get("db").and_then(Value::as_bool).unwrap_or(false);
    if ok && db {
        Check::fail(
            "Discord Bot",
            format!("standalone health only — provider status unavailable at {base}"),
        )
    } else {
        Check::fail(
            "Discord Bot",
            format!("server unhealthy or provider data missing: ok={ok} db={db}"),
        )
    }
}

fn check_discord_bot() -> Check {
    let base = super::client::api_base();
    match super::client::get_json("/api/health") {
        Ok(body) => discord_bot_check_from_health(&base, &body),
        Err(e) => Check::fail("Discord Bot", format!("unreachable ({e})")),
    }
}

fn check_tmux() -> Check {
    match Command::new("tmux").arg("-V").output() {
        Ok(out) if out.status.success() => {
            let ver = String::from_utf8_lossy(&out.stdout).trim().to_string();
            Check::ok("tmux", ver)
        }
        _ => Check::fail("tmux", "not found in PATH"),
    }
}

fn provider_capability_summary(provider: &ProviderKind) -> String {
    provider
        .capabilities()
        .map(|caps| {
            let mut parts = Vec::new();
            if caps.supports_structured_output {
                parts.push("structured-output");
            }
            if caps.supports_resume {
                parts.push("resume");
            }
            if caps.supports_tool_stream {
                parts.push("tool-stream");
            }
            parts.join(", ")
        })
        .unwrap_or_else(|| "unsupported".to_string())
}

fn check_provider_cli(name: &'static str, provider: ProviderKind, optional: bool) -> Check {
    let capability_summary = provider_capability_summary(&provider);
    match provider.probe_runtime() {
        Some(probe) => match probe.version {
            Some(ver) => {
                let path = probe
                    .binary_path
                    .unwrap_or_else(|| "unknown path".to_string());
                Check::ok(name, format!("{ver} — {path} [{capability_summary}]"))
            }
            None if optional => Check::ok(
                name,
                format!("not found in PATH (optional) [{capability_summary}]"),
            ),
            None => Check::fail(name, format!("not found in PATH [{capability_summary}]")),
        },
        None if optional => Check::ok(name, "unsupported provider (optional)"),
        None => Check::fail(name, "unsupported provider"),
    }
}

fn check_runtime_path() -> Check {
    let current = std::env::var("PATH").unwrap_or_default();
    match crate::services::platform::merged_runtime_path() {
        Some(merged) if merged == current => Check::ok(
            "Runtime PATH",
            "current PATH already matches provider runtime PATH",
        ),
        Some(merged) => {
            let entry_count = std::env::split_paths(&merged).count();
            Check::ok(
                "Runtime PATH",
                format!(
                    "provider subprocesses will use merged login-shell PATH ({entry_count} entries)"
                ),
            )
        }
        None => Check::fail("Runtime PATH", "unable to resolve provider runtime PATH"),
    }
}

fn check_claude_cli() -> Check {
    check_provider_cli("claude CLI", ProviderKind::Claude, false)
}

fn check_codex_cli() -> Check {
    check_provider_cli("codex CLI", ProviderKind::Codex, true)
}

fn check_gemini_cli() -> Check {
    check_provider_cli("gemini CLI", ProviderKind::Gemini, true)
}

fn check_server_running() -> Check {
    let base = super::client::api_base();
    match super::client::get_json("/api/health") {
        Ok(body) => {
            let ver = body
                .get("version")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let status = body
                .get("status")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| {
                    let ok = body.get("ok").and_then(Value::as_bool).unwrap_or(false);
                    let db = body.get("db").and_then(Value::as_bool).unwrap_or(false);
                    Some(if ok && db { "healthy" } else { "degraded" }.to_string())
                })
                .unwrap_or_else(|| "unknown".to_string());
            Check::ok("Server", format!("{status} v{ver} on {base}"))
        }
        Err(e) => Check::fail("Server", format!("not reachable — {base} ({e})")),
    }
}

#[cfg(target_os = "macos")]
fn check_launchd() -> Check {
    let label = dcserver::current_dcserver_launchd_label();
    if dcserver::is_launchd_job_loaded(&label) {
        Check::ok("launchd Job", format!("{label} — loaded"))
    } else {
        Check::fail("launchd Job", format!("{label} — not loaded"))
    }
}

#[cfg(not(target_os = "macos"))]
fn check_launchd() -> Check {
    Check::ok("launchd Job", "N/A (not macOS)")
}

fn check_db_integrity() -> Check {
    let cfg = config::load_graceful();
    let db_path = cfg.data.dir.join(&cfg.data.db_name);
    if !db_path.exists() {
        return Check::fail("DB File", format!("{} — not found", db_path.display()));
    }
    match rusqlite::Connection::open(&db_path) {
        Ok(conn) => {
            match conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0)) {
                Ok(result) if result == "ok" => {
                    let size = std::fs::metadata(&db_path)
                        .map(|m| format!("{:.1} MB", m.len() as f64 / 1_048_576.0))
                        .unwrap_or_default();
                    Check::ok("DB Integrity", format!("ok — {size}"))
                }
                Ok(result) => Check::fail("DB Integrity", format!("issues: {result}")),
                Err(e) => Check::fail("DB Integrity", format!("check failed: {e}")),
            }
        }
        Err(e) => Check::fail("DB File", format!("cannot open: {e}")),
    }
}

fn check_disk_usage() -> Check {
    let root = dcserver::agentdesk_runtime_root();
    match root {
        Some(path) => {
            let mut total: u64 = 0;
            if let Ok(entries) = std::fs::read_dir(&path) {
                for entry in entries.flatten() {
                    if let Ok(meta) = entry.metadata() {
                        total += meta.len();
                    }
                }
            }
            let mb = total as f64 / 1_048_576.0;
            Check::ok("Disk Usage", format!("{:.1} MB in {}", mb, path.display()))
        }
        None => Check::fail("Disk Usage", "cannot determine runtime root"),
    }
}

pub fn cmd_doctor() -> Result<(), String> {
    println!("AgentDesk Doctor v{}\n", env!("CARGO_PKG_VERSION"));

    let checks = vec![
        check_server_running(),
        check_discord_bot(),
        check_tmux(),
        check_runtime_path(),
        check_claude_cli(),
        check_codex_cli(),
        check_gemini_cli(),
        check_launchd(),
        check_db_integrity(),
        check_disk_usage(),
    ];

    let mut pass_count = 0;
    let mut fail_count = 0;

    for c in &checks {
        let icon = if c.pass { "✓" } else { "✗" };
        let label = if c.pass { "PASS" } else { "FAIL" };
        println!("  {icon} [{label}] {}: {}", c.name, c.detail);
        if c.pass {
            pass_count += 1;
        } else {
            fail_count += 1;
        }
    }

    println!(
        "\n  {pass_count} passed, {fail_count} failed out of {} checks",
        checks.len()
    );

    if fail_count > 0 {
        Err(format!("{fail_count} diagnostic check(s) failed"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{discord_bot_check_from_health, provider_capability_summary};
    use crate::config::ServerConfig;
    use crate::services::provider::ProviderKind;
    use serde_json::json;

    fn test_base_url() -> String {
        format!(
            "http://{}:{}",
            ServerConfig::loopback(),
            ServerConfig::default().port,
        )
    }

    #[test]
    fn unified_health_requires_connected_providers() {
        let check = discord_bot_check_from_health(
            &test_base_url(),
            &json!({
                "status": "healthy",
                "providers": [
                    {"name": "claude", "connected": true},
                    {"name": "codex", "connected": true}
                ]
            }),
        );
        assert!(check.pass);
        assert!(check.detail.contains("2/2 connected"));
    }

    #[test]
    fn standalone_health_does_not_count_as_discord_health() {
        let check = discord_bot_check_from_health(
            &test_base_url(),
            &json!({
                "ok": true,
                "db": true,
                "version": "0.1.0"
            }),
        );
        assert!(!check.pass);
        assert!(check.detail.contains("standalone health only"));
    }

    #[test]
    fn provider_capability_summary_mentions_structured_contract() {
        let summary = provider_capability_summary(&ProviderKind::Codex);
        assert!(summary.contains("structured-output"));
        assert!(summary.contains("resume"));
        assert!(summary.contains("tool-stream"));
    }
}
