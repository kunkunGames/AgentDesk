//! `agentdesk doctor` — environment diagnostics.

use std::process::Command;

use super::dcserver;
use crate::config;

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

fn check_discord_bot() -> Check {
    let base = super::client::api_base();
    let url = format!("{base}/api/health");
    match ureq::Agent::new().get(&url).call() {
        Ok(resp) => {
            if let Ok(body) = resp.into_json::<serde_json::Value>() {
                let ok = body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
                let db = body.get("db").and_then(|v| v.as_bool()).unwrap_or(false);
                if ok && db {
                    Check::ok("Discord Bot", format!("healthy — {base}"))
                } else {
                    Check::fail("Discord Bot", format!("degraded: ok={ok} db={db}"))
                }
            } else {
                Check::fail("Discord Bot", "invalid response body")
            }
        }
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

fn check_claude_cli() -> Check {
    match Command::new("claude").arg("--version").output() {
        Ok(out) if out.status.success() => {
            let ver = String::from_utf8_lossy(&out.stdout).trim().to_string();
            Check::ok("claude CLI", ver)
        }
        _ => Check::fail("claude CLI", "not found in PATH"),
    }
}

fn check_codex_cli() -> Check {
    match Command::new("codex").arg("--version").output() {
        Ok(out) if out.status.success() => {
            let ver = String::from_utf8_lossy(&out.stdout).trim().to_string();
            Check::ok("codex CLI", ver)
        }
        _ => Check::fail("codex CLI", "not found in PATH (optional)"),
    }
}

fn check_server_running() -> Check {
    let base = super::client::api_base();
    let url = format!("{base}/api/health");
    match ureq::Agent::new().get(&url).call() {
        Ok(resp) => {
            if let Ok(body) = resp.into_json::<serde_json::Value>() {
                let ver = body.get("version").and_then(|v| v.as_str()).unwrap_or("unknown");
                Check::ok("Server", format!("running v{ver} on {base}"))
            } else {
                Check::fail("Server", format!("responded but invalid JSON — {base}"))
            }
        }
        Err(_) => Check::fail("Server", format!("not reachable — {base}")),
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
        Ok(conn) => match conn.query_row("PRAGMA integrity_check", [], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(result) if result == "ok" => {
                let size = std::fs::metadata(&db_path)
                    .map(|m| format!("{:.1} MB", m.len() as f64 / 1_048_576.0))
                    .unwrap_or_default();
                Check::ok("DB Integrity", format!("ok — {size}"))
            }
            Ok(result) => Check::fail("DB Integrity", format!("issues: {result}")),
            Err(e) => Check::fail("DB Integrity", format!("check failed: {e}")),
        },
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

pub fn cmd_doctor() {
    println!("AgentDesk Doctor v{}\n", env!("CARGO_PKG_VERSION"));

    let checks = vec![
        check_server_running(),
        check_discord_bot(),
        check_tmux(),
        check_claude_cli(),
        check_codex_cli(),
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

    println!("\n  {pass_count} passed, {fail_count} failed out of {} checks", checks.len());
}
