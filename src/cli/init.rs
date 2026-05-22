use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::io::{self, BufRead, Write as IoWrite};
use std::path::{Path, PathBuf};

use super::{
    args::{EmitLaunchdPlistArgs, LaunchdPlistFlavorArg},
    dcserver,
};
use crate::services::provider::ProviderKind;

// ── Discord REST helpers ───────────────────────────────────────────

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";

#[derive(serde::Deserialize)]
struct DiscordUser {
    username: String,
    id: String,
}

#[derive(serde::Deserialize)]
struct DiscordGuild {
    id: String,
    name: String,
}

#[derive(serde::Deserialize)]
struct DiscordChannel {
    id: String,
    name: Option<String>,
    #[serde(rename = "type")]
    channel_type: u8,
}

async fn discord_get<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    token: &str,
    path: &str,
) -> Result<T, String> {
    let url = format!("{}{}", DISCORD_API_BASE, path);
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bot {}", token))
        .send()
        .await
        .map_err(|e| format!("HTTP error: {}", e))?;
    if !resp.status().is_success() {
        return Err(format!("Discord API {} — {}", resp.status(), path));
    }
    resp.json().await.map_err(|e| format!("Parse error: {}", e))
}

// ── Interactive helpers ────────────────────────────────────────────

fn prompt_line(msg: &str) -> String {
    print!("{}", msg);
    if let Err(e) = io::stdout().flush() {
        eprintln!("stdout flush failed: {e}");
        std::process::exit(1);
    }
    let mut buf = String::new();
    if let Err(e) = io::stdin().lock().read_line(&mut buf) {
        eprintln!("stdin read failed: {e}");
        std::process::exit(1);
    }
    buf.trim().to_string()
}

fn prompt_secret(msg: &str) -> String {
    // Simple secret prompt (no echo hiding — terminal may not support it)
    prompt_line(msg)
}

fn prompt_select(msg: &str, options: &[&str]) -> usize {
    println!("\n{}", msg);
    for (i, opt) in options.iter().enumerate() {
        println!("  [{}] {}", i + 1, opt);
    }
    loop {
        let input = prompt_line("선택: ");
        if let Ok(n) = input.parse::<usize>() {
            if n >= 1 && n <= options.len() {
                return n - 1;
            }
        }
        println!("1-{} 사이의 숫자를 입력하세요.", options.len());
    }
}

fn prompt_yes_no(msg: &str, default: bool) -> bool {
    let suffix = if default { "[Y/n]" } else { "[y/N]" };
    loop {
        let input = prompt_line(&format!("{msg} {suffix}: "));
        let normalized = input.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return default;
        }
        match normalized.as_str() {
            "y" | "yes" => return true,
            "n" | "no" => return false,
            _ => println!("y 또는 n으로 입력하세요."),
        }
    }
}

fn prompt_line_default(msg: &str, default: &str) -> String {
    let input = prompt_line(&format!("{msg} [{default}]: "));
    if input.trim().is_empty() {
        default.to_string()
    } else {
        input
    }
}

fn prompt_u16_default(msg: &str, default: u16) -> u16 {
    loop {
        let input = prompt_line_default(msg, &default.to_string());
        match input.trim().parse::<u16>() {
            Ok(value) => return value,
            Err(_) => println!("유효한 포트 번호를 입력하세요."),
        }
    }
}

fn prompt_u32_default(msg: &str, default: u32) -> u32 {
    loop {
        let input = prompt_line_default(msg, &default.to_string());
        match input.trim().parse::<u32>() {
            Ok(value) if value > 0 => return value,
            Ok(_) | Err(_) => println!("1 이상의 숫자를 입력하세요."),
        }
    }
}

fn cli_init_provider_labels() -> Vec<&'static str> {
    ProviderKind::cli_init_labels()
}

fn cli_init_provider_from_index(index: usize) -> &'static str {
    match ProviderKind::provider_for_cli_init_index(index)
        .or_else(ProviderKind::default_channel_provider)
        .unwrap_or(ProviderKind::Claude)
    {
        ProviderKind::Claude => "claude",
        ProviderKind::Codex => "codex",
        ProviderKind::Gemini => "gemini",
        ProviderKind::OpenCode => "opencode",
        ProviderKind::Qwen => "qwen",
        ProviderKind::Unsupported(_) => "claude",
    }
}

fn prompt_multi_select(msg: &str, options: &[(String, String)]) -> Vec<usize> {
    println!("\n{}", msg);
    for (i, (name, id)) in options.iter().enumerate() {
        println!("  [{}] {} ({})", i + 1, name, id);
    }
    println!("  (쉼표로 구분하여 여러 개 선택 가능, 예: 1,3,5)");
    loop {
        let input = prompt_line("선택: ");
        let selected: Vec<usize> = input
            .split(',')
            .filter_map(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n >= 1 && n <= options.len())
            .map(|n| n - 1)
            .collect();
        if !selected.is_empty() {
            return selected;
        }
        println!("최소 하나 이상 선택하세요.");
    }
}

#[cfg(unix)]
fn preferred_agentdesk_cli_dir(home: &Path) -> PathBuf {
    preferred_agentdesk_cli_dir_with_path(home, std::env::var_os("PATH").as_deref())
}

#[cfg(unix)]
fn preferred_agentdesk_cli_dir_with_path(home: &Path, path: Option<&OsStr>) -> PathBuf {
    let preferred_dirs = [home.join("bin"), home.join(".local").join("bin")];
    let Some(path) = path else {
        return preferred_dirs[0].clone();
    };
    for entry in std::env::split_paths(path) {
        if preferred_dirs.iter().any(|candidate| candidate == &entry) {
            return entry;
        }
    }
    preferred_dirs[0].clone()
}

#[cfg(unix)]
fn agentdesk_cli_wrapper_script(home: &Path) -> String {
    format!(
        r#"#!/usr/bin/env bash
set -euo pipefail

home_dir="${{HOME:-{home_dir}}}"
candidates=(
  "$home_dir/.adk/release/bin/agentdesk"
  "$home_dir/.adk/release/agentdesk"
)

for candidate in "${{candidates[@]}}"; do
  if [ -x "$candidate" ]; then
    exec "$candidate" "$@"
  fi
done

echo "agentdesk: no installed runtime binary found" >&2
echo "looked for:" >&2
for candidate in "${{candidates[@]}}"; do
  echo "  - $candidate" >&2
done
exit 127
"#,
        home_dir = home.display()
    )
}

#[cfg(unix)]
fn ensure_global_agentdesk_cli(home: &Path) -> Result<PathBuf, String> {
    use std::os::unix::fs::PermissionsExt;

    let wrapper_dir = preferred_agentdesk_cli_dir(home);
    fs::create_dir_all(&wrapper_dir).map_err(|e| {
        format!(
            "Failed to create CLI directory {}: {e}",
            wrapper_dir.display()
        )
    })?;
    let wrapper_path = wrapper_dir.join("agentdesk");
    fs::write(&wrapper_path, agentdesk_cli_wrapper_script(home)).map_err(|e| {
        format!(
            "Failed to write CLI wrapper {}: {e}",
            wrapper_path.display()
        )
    })?;
    let mut permissions = fs::metadata(&wrapper_path)
        .map_err(|e| format!("Failed to stat CLI wrapper {}: {e}", wrapper_path.display()))?
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&wrapper_path, permissions).map_err(|e| {
        format!(
            "Failed to make CLI wrapper executable {}: {e}",
            wrapper_path.display()
        )
    })?;
    Ok(wrapper_path)
}

// ── Template definitions ───────────────────────────────────────────

fn solo_org_yaml(channels: &[(String, String, String)]) -> String {
    // channels: Vec<(channel_id, channel_name, role_id)>
    let mut yaml = String::from(
        r#"version: 1
name: "My Agent Org"

prompts_root: "config"
skills_root: "skills"

agents:
  assistant:
    display_name: "Assistant"
    keywords: ["help", "assist"]

channels:
  by_id:
"#,
    );
    for (ch_id, _ch_name, _role) in channels {
        yaml.push_str(&format!("    \"{}\":\n      agent: assistant\n", ch_id));
    }
    yaml
}

fn small_team_org_yaml(channels: &[(String, String, String)]) -> String {
    let mut agents: HashMap<&str, bool> = HashMap::new();
    for (_, _, role) in channels {
        agents.insert(role, true);
    }

    let mut yaml = String::from(
        r#"version: 1
name: "Small Team Org"

prompts_root: "config"
skills_root: "skills"

agents:
"#,
    );
    for role in agents.keys() {
        yaml.push_str(&format!(
            "  {}:\n    display_name: \"{}\"\n    keywords: []\n",
            role,
            role.replace('-', " ")
        ));
    }
    yaml.push_str("\nchannels:\n  by_id:\n");
    for (ch_id, _ch_name, role) in channels {
        yaml.push_str(&format!("    \"{}\":\n      agent: {}\n", ch_id, role));
    }
    yaml
}

fn default_shared_prompt() -> &'static str {
    // Profile markers (`<!-- profile: full -->` / `review-lite` / `headless`) gate
    // sections per dispatch type — see settings::content::load_shared_prompt_for_profile.
    r#"<!-- profile: all -->
## Communication
- Respond in the user's language.
- Discord 발신자는 `[User: NAME (ID: N)]` prefix로 구분한다.

## Work Style
- Plan briefly, verify before reporting done, fix bugs autonomously.
- Check `GET /api/docs` or `GET /api/docs/{category}` before guessing ADK API calls.
- ADK API 반복 시행착오는 `api-friction`으로 기록 (DB 직접 접근 우회 금지).
<!-- /profile -->

<!-- profile: full -->
## Search Safety
- `grep -r` 직접 사용 대신 `rg` 또는 Grep tool (.gitignore 존중).
- 검색 범위는 필요한 하위 디렉터리로 한정.
- 부득이 시 `--exclude-dir={target,node_modules,.git,dist,build,.next}` 필수.
<!-- /profile -->

<!-- profile: review-lite -->
## Review 모드
- 한국어로 소통. 코드/문서 검증 후 평가. raw 로그 덤프 금지.
<!-- /profile -->

<!-- profile: headless -->
## Headless 모드
- 결과·다음 액션을 1~2줄로. 중간 서술 최소.
<!-- /profile -->
"#
}

fn default_agent_prompt(role_id: &str) -> String {
    format!(
        r#"# {}

## identity
- role: {}
- mission: Assist with tasks in this channel

## working_rules
- Follow the shared agent rules
- Ask clarifying questions only when requirements are genuinely ambiguous

## response_contract
- Be concise and actionable
"#,
        role_id, role_id
    )
}

// ── Launchd plist ──────────────────────────────────────────────────

#[cfg(target_os = "macos")]
fn launchd_path_env(home: &Path) -> String {
    let home_str = home.display();
    format!(
        "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:{home_str}/.cargo/bin:{home_str}/.local/bin:{home_str}/bin"
    )
}

#[cfg(target_os = "macos")]
fn launchd_label(flavor: LaunchdPlistFlavorArg) -> &'static str {
    match flavor {
        LaunchdPlistFlavorArg::Release => dcserver::AGENTDESK_DCSERVER_LAUNCHD_LABEL,
    }
}

#[cfg(target_os = "macos")]
fn launchd_plist_filename(flavor: LaunchdPlistFlavorArg) -> String {
    format!("{}.plist", launchd_label(flavor))
}

#[cfg(target_os = "macos")]
fn default_launchd_root_dir(home: &Path, flavor: LaunchdPlistFlavorArg) -> PathBuf {
    match flavor {
        LaunchdPlistFlavorArg::Release => {
            dcserver::agentdesk_runtime_root().unwrap_or_else(|| home.join(".adk").join("release"))
        }
    }
}

#[cfg(target_os = "macos")]
const LAUNCHD_NOFILE_SOFT_LIMIT_TARGET: u64 = 16_384;

#[cfg(target_os = "macos")]
fn clamp_launchd_nofile_soft_limit(hard_limit: u64) -> Option<u64> {
    if hard_limit == 0 {
        return None;
    }

    if hard_limit == libc::RLIM_INFINITY as u64 {
        return Some(LAUNCHD_NOFILE_SOFT_LIMIT_TARGET);
    }

    Some(LAUNCHD_NOFILE_SOFT_LIMIT_TARGET.min(hard_limit))
}

#[cfg(target_os = "macos")]
fn launchd_nofile_soft_limit() -> Option<u64> {
    let mut limits = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limits.as_mut_ptr()) };
    if result != 0 {
        return None;
    }

    let limits = unsafe { limits.assume_init() };
    clamp_launchd_nofile_soft_limit(limits.rlim_max as u64)
}

#[cfg(target_os = "macos")]
fn render_launchd_nofile_resource_limit_xml() -> String {
    let Some(nofile_soft) = launchd_nofile_soft_limit() else {
        return String::new();
    };

    format!(
        r#"  <key>SoftResourceLimits</key>
  <dict>
    <key>NumberOfFiles</key>
    <integer>{nofile_soft}</integer>
  </dict>
"#
    )
}

#[cfg(target_os = "macos")]
fn generate_launchd_plist(home: &Path, agentdesk_bin: &Path) -> String {
    let root_dir = default_launchd_root_dir(home, LaunchdPlistFlavorArg::Release);
    generate_launchd_plist_for_flavor_with_root(
        LaunchdPlistFlavorArg::Release,
        home,
        agentdesk_bin,
        &root_dir,
    )
}

#[cfg(target_os = "macos")]
fn generate_launchd_plist_for_flavor_with_root(
    flavor: LaunchdPlistFlavorArg,
    home: &Path,
    agentdesk_bin: &Path,
    root_dir: &Path,
) -> String {
    let home_str = home.display();
    let bin_str = agentdesk_bin.display();
    let label = launchd_label(flavor);
    let root_str = root_dir.display();
    let logs_dir = root_dir.join("logs");
    let logs_str = logs_dir.display();
    let path_env = launchd_path_env(home);
    let extra_env_xml =
        render_launchd_env_entries_xml(&root_dir.join("config").join("launchd.env"));
    let nofile_resource_limit_xml = render_launchd_nofile_resource_limit_xml();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{bin_str}</string>
    <string>dcserver</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>5</integer>
{nofile_resource_limit_xml}  <key>WorkingDirectory</key>
  <string>{root_str}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>{path_env}</string>
    <key>HOME</key>
    <string>{home_str}</string>
    <key>AGENTDESK_ROOT_DIR</key>
    <string>{root_str}</string>
{extra_env_xml}
  </dict>
  <key>StandardOutPath</key>
  <string>{logs_str}/dcserver.launchd.stdout.log</string>
  <key>StandardErrorPath</key>
  <string>{logs_str}/dcserver.launchd.stderr.log</string>
</dict>
</plist>"#,
    )
}

#[cfg(target_os = "macos")]
fn render_launchd_env_entries_xml(env_file: &Path) -> String {
    let mut xml = String::new();
    for (key, value) in read_launchd_env_entries(env_file) {
        let _ = writeln!(xml, "    <key>{}</key>", xml_escape(&key));
        let _ = writeln!(xml, "    <string>{}</string>", xml_escape(&value));
    }
    xml
}

fn read_launchd_env_entries(env_file: &Path) -> Vec<(String, String)> {
    let Ok(contents) = fs::read_to_string(env_file) else {
        return Vec::new();
    };

    contents
        .lines()
        .filter_map(parse_launchd_env_line)
        .collect()
}

fn parse_launchd_env_line(line: &str) -> Option<(String, String)> {
    let mut line = line.trim().trim_end_matches('\r');
    if line.is_empty() || line.starts_with('#') {
        return None;
    }

    if let Some(rest) = line.strip_prefix("export ") {
        line = rest.trim();
    }

    let (key, value) = line.split_once('=')?;
    let key = key.trim();
    if key.is_empty()
        || !key.chars().enumerate().all(|(idx, ch)| {
            ch == '_' || ch.is_ascii_alphanumeric() && (idx > 0 || !ch.is_ascii_digit())
        })
    {
        return None;
    }

    let mut value = value.trim().to_string();
    if value.len() >= 2 {
        let quoted_with_double = value.starts_with('"') && value.ends_with('"');
        let quoted_with_single = value.starts_with('\'') && value.ends_with('\'');
        if quoted_with_double || quoted_with_single {
            value = value[1..value.len() - 1].to_string();
        }
    }

    Some((key.to_string(), value))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

// ── AgentDesk config generation ────────────────────────────────────

fn init_config_path(root: &Path) -> PathBuf {
    let canonical = crate::runtime_layout::config_file_path(root);
    let legacy = crate::runtime_layout::legacy_config_file_path(root);
    if canonical.is_file() || !legacy.is_file() {
        canonical
    } else {
        legacy
    }
}

fn init_has_existing_configuration(root: &Path) -> bool {
    init_config_path(root).exists() || crate::runtime_layout::org_schema_path(root).exists()
}

fn parse_owner_id(owner_id: Option<&str>) -> Result<Option<u64>, String> {
    let Some(value) = owner_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    if !(17..=20).contains(&value.len()) || !value.chars().all(|ch| ch.is_ascii_digit()) {
        return Err("owner_id must be a Discord user id with 17-20 digits".to_string());
    }

    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| "owner_id must be a valid Discord user id".to_string())
}

fn upsert_command_bot(
    config: &mut crate::config::Config,
    token: &str,
    provider: &str,
    allowed_channel_ids: &[u64],
) {
    let mut bot = config
        .discord
        .bots
        .get("command")
        .cloned()
        .unwrap_or_default();
    bot.token = Some(token.trim().to_string());
    bot.provider = Some(provider.trim().to_string());
    bot.auth.allowed_channel_ids =
        (!allowed_channel_ids.is_empty()).then_some(allowed_channel_ids.to_vec());
    config.discord.bots.insert("command".to_string(), bot);
}

fn write_agentdesk_discord_config(
    root: &Path,
    guild_id: &str,
    token: &str,
    provider: &str,
    owner_id: Option<&str>,
    allowed_channel_ids: &[u64],
    database: &crate::config::DatabaseConfig,
    reconfigure: bool,
) -> Result<PathBuf, String> {
    let config_path = init_config_path(root);
    let mut config = if config_path.is_file() {
        crate::config::load_from_path(&config_path)
            .map_err(|e| format!("Failed to load config {}: {e}", config_path.display()))?
    } else {
        crate::config::Config::default()
    };

    config.discord.guild_id = Some(guild_id.trim().to_string());
    config.discord.owner_id = parse_owner_id(owner_id)?;
    upsert_command_bot(&mut config, token, provider, allowed_channel_ids);
    config.database = database.clone();

    let rendered = serde_yaml::to_string(&config)
        .map_err(|e| format!("Failed to serialize config {}: {e}", config_path.display()))?;
    write_with_backup(&config_path, &rendered, reconfigure)
        .map_err(|e| format!("Failed to write config {}: {e}", config_path.display()))?;

    Ok(config_path)
}

fn load_init_config_defaults(root: &Path) -> crate::config::Config {
    let config_path = init_config_path(root);
    if config_path.is_file() {
        crate::config::load_from_path(&config_path)
            .unwrap_or_else(|_| crate::config::Config::default())
    } else {
        crate::config::Config::default()
    }
}

fn prompt_postgres_config(
    existing: &crate::config::DatabaseConfig,
) -> crate::config::DatabaseConfig {
    let mut database = existing.clone();
    let enabled = prompt_yes_no("PostgreSQL 부트스트랩 설정을 추가할까요?", existing.enabled);
    database.enabled = enabled;
    if !enabled {
        return database;
    }

    database.host = prompt_line_default("  PostgreSQL host", &database.host);
    database.port = prompt_u16_default("  PostgreSQL port", database.port);
    database.dbname = prompt_line_default("  PostgreSQL database name", &database.dbname);
    database.user = prompt_line_default("  PostgreSQL user", &database.user);

    let current_password = database
        .password
        .clone()
        .unwrap_or_else(|| "agentdesk".to_string());
    let password = prompt_line_default("  PostgreSQL password", &current_password);
    database.password = Some(password);
    database.pool_max = prompt_u32_default("  PostgreSQL pool_max", database.pool_max.max(1));
    database
}

fn postgres_compose_yaml(database: &crate::config::DatabaseConfig) -> String {
    let password = database
        .password
        .clone()
        .unwrap_or_else(|| "agentdesk".to_string());
    format!(
        r#"services:
  postgres:
    image: postgres:17
    restart: unless-stopped
    ports:
      - "{port}:5432"
    environment:
      POSTGRES_DB: "{dbname}"
      POSTGRES_USER: "{user}"
      POSTGRES_PASSWORD: "{password}"
    volumes:
      - agentdesk-postgres:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U {user} -d {dbname}"]
      interval: 5s
      timeout: 5s
      retries: 10

volumes:
  agentdesk-postgres:
"#,
        port = database.port,
        dbname = database.dbname,
        user = database.user,
        password = password,
    )
}

fn write_postgres_compose_template(
    root: &Path,
    database: &crate::config::DatabaseConfig,
    reconfigure: bool,
) -> Result<Option<PathBuf>, String> {
    if !database.enabled {
        return Ok(None);
    }

    let compose_path = root.join("docker-compose.postgres.yml");
    write_with_backup(&compose_path, &postgres_compose_yaml(database), reconfigure)
        .map_err(|e| format!("Failed to write {}: {e}", compose_path.display()))?;
    Ok(Some(compose_path))
}

fn write_init_artifacts(
    root: &Path,
    channel_mappings: &[(String, String, String)],
    template_idx: usize,
    guild_id: &str,
    token: &str,
    provider: &str,
    owner_id: Option<&str>,
    allowed_channel_ids: &[u64],
    database: &crate::config::DatabaseConfig,
    reconfigure: bool,
) -> Result<(PathBuf, PathBuf, Option<PathBuf>), String> {
    // Validate before mutating the runtime layout so invalid owner_id input
    // never leaves partial init artifacts behind.
    parse_owner_id(owner_id)?;

    fs::create_dir_all(root)
        .map_err(|e| format!("Failed to create directory {}: {}", root.display(), e))?;
    crate::runtime_layout::ensure_runtime_layout(root)
        .map_err(|e| format!("Failed to prepare runtime layout {}: {}", root.display(), e))?;

    let config_dir = crate::runtime_layout::config_dir(root);
    let org_path = config_dir.join("org.yaml");
    let org_yaml = if reconfigure && org_path.exists() {
        let mut existing = fs::read_to_string(&org_path).unwrap_or_default();
        for (ch_id, _ch_name, role) in channel_mappings {
            let marker = format!("\"{}\":", ch_id);
            if !existing.contains(&marker) {
                let entry = format!("    \"{}\":\n      agent: {}\n", ch_id, role);
                if let Some(pos) = existing.find("  by_id:") {
                    let insert_at = existing[pos..]
                        .find('\n')
                        .map(|n| pos + n + 1)
                        .unwrap_or(existing.len());
                    existing.insert_str(insert_at, &entry);
                }
            }
        }
        existing
    } else {
        match template_idx {
            0 => solo_org_yaml(channel_mappings),
            _ => small_team_org_yaml(channel_mappings),
        }
    };
    write_with_backup(&org_path, &org_yaml, reconfigure)
        .map_err(|e| format!("Failed to write {}: {}", org_path.display(), e))?;

    let agentdesk_config_path = write_agentdesk_discord_config(
        root,
        guild_id,
        token,
        provider,
        owner_id,
        allowed_channel_ids,
        database,
        reconfigure,
    )?;
    if !agentdesk_config_path.exists() {
        return Err(format!(
            "Failed to write {}: file was not created",
            agentdesk_config_path.display()
        ));
    }

    let compose_path = write_postgres_compose_template(root, database, reconfigure)?;

    Ok((org_path, agentdesk_config_path, compose_path))
}

// ── Main init flow ─────────────────────────────────────────────────

pub fn handle_init(reconfigure: bool) {
    let root = dcserver::agentdesk_runtime_root().unwrap_or_else(|| {
        eprintln!("Error: cannot determine runtime directory");
        std::process::exit(1);
    });

    if !reconfigure && init_has_existing_configuration(&root) {
        println!("기존 설정이 발견되었습니다: {}", root.display());
        println!("재설정하려면 reconfigure를 사용하세요.");
        return;
    }

    println!("═══════════════════════════════════════");
    println!("  AgentDesk 초기 설정 (v{})", super::VERSION);
    println!("═══════════════════════════════════════\n");

    if reconfigure {
        println!("[재설정 모드] 기존 설정을 보존하며 변경합니다.\n");
    }

    let existing_config = load_init_config_defaults(&root);

    // Step 1: Bot token
    println!("Step 1/5: Discord 봇 토큰");
    println!("  Discord Developer Portal에서 봇을 생성하세요:");
    println!("  https://discord.com/developers/applications\n");

    let token = prompt_secret("봇 토큰 입력: ");
    if token.is_empty() {
        eprintln!("토큰이 비어있습니다. 종료합니다.");
        return;
    }

    // Validate token & fetch bot info
    println!("\n봇 정보를 확인 중...");
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Failed to create async runtime: {}", e);
            return;
        }
    };
    let client = reqwest::Client::new();

    let bot_user: DiscordUser = match rt.block_on(discord_get(&client, &token, "/users/@me")) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("토큰 검증 실패: {}", e);
            eprintln!("올바른 봇 토큰을 입력했는지 확인하세요.");
            return;
        }
    };
    println!("  봇 이름: {} (ID: {})", bot_user.username, bot_user.id);

    // Step 2: Fetch guilds + channels
    println!("\nStep 2/5: 서버 및 채널 스캔");
    let guilds: Vec<DiscordGuild> =
        match rt.block_on(discord_get(&client, &token, "/users/@me/guilds")) {
            Ok(g) => g,
            Err(e) => {
                eprintln!("서버 목록 조회 실패: {}", e);
                return;
            }
        };

    if guilds.is_empty() {
        eprintln!("봇이 참여한 서버가 없습니다.");
        eprintln!("먼저 봇을 서버에 초대하세요.");
        return;
    }

    // Select guild
    let guild_names: Vec<&str> = guilds.iter().map(|g| g.name.as_str()).collect();
    let guild_idx = if guilds.len() == 1 {
        println!("  서버: {}", guilds[0].name);
        0
    } else {
        prompt_select("사용할 서버를 선택하세요:", &guild_names)
    };
    let guild = &guilds[guild_idx];

    // Fetch text channels
    let channels: Vec<DiscordChannel> = match rt.block_on(discord_get(
        &client,
        &token,
        &format!("/guilds/{}/channels", guild.id),
    )) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("채널 목록 조회 실패: {}", e);
            return;
        }
    };

    // Filter text channels (type 0 = text)
    let text_channels: Vec<(String, String)> = channels
        .into_iter()
        .filter(|c| c.channel_type == 0)
        .map(|c| (c.name.unwrap_or_else(|| c.id.clone()), c.id))
        .collect();

    if text_channels.is_empty() {
        eprintln!("텍스트 채널을 찾을 수 없습니다.");
        return;
    }

    // Select channels for agents
    let selected = prompt_multi_select("에이전트를 배정할 채널을 선택하세요:", &text_channels);

    // Step 3: Template selection + role assignment
    println!("\nStep 3/5: 템플릿 선택");
    let template_idx = prompt_select(
        "조직 템플릿을 선택하세요:",
        &[
            "solo — 단일 에이전트 (모든 채널 동일)",
            "small-team — 채널별 역할 분리",
        ],
    );

    let mut channel_mappings: Vec<(String, String, String)> = Vec::new(); // (id, name, role)

    match template_idx {
        0 => {
            // Solo: all channels get "assistant"
            for &idx in &selected {
                let (name, id) = &text_channels[idx];
                channel_mappings.push((id.clone(), name.clone(), "assistant".into()));
            }
        }
        1 => {
            // Small team: assign role per channel
            println!("\n각 채널에 역할 ID를 지정하세요 (예: td, pd, designer):");
            for &idx in &selected {
                let (name, id) = &text_channels[idx];
                let role = prompt_line(&format!("  #{} → 역할: ", name));
                let role = if role.is_empty() { name.clone() } else { role };
                channel_mappings.push((id.clone(), name.clone(), role));
            }
        }
        _ => unreachable!(),
    }

    // Provider selection
    let provider_labels = cli_init_provider_labels();
    let provider_idx = prompt_select("AI 프로바이더를 선택하세요:", &provider_labels);
    let provider = cli_init_provider_from_index(provider_idx);

    // Owner user ID (optional)
    println!("\nStep 4/6: 소유자 설정");
    let owner_input =
        prompt_line("Discord 사용자 ID (Enter로 건너뛰기 — 첫 메시지 발신자가 자동 등록): ");
    let owner_id = if owner_input.is_empty() {
        None
    } else {
        Some(owner_input.as_str())
    };
    let allowed_channel_ids = selected
        .iter()
        .filter_map(|idx| text_channels.get(*idx))
        .filter_map(|(_, channel_id)| channel_id.parse::<u64>().ok())
        .collect::<Vec<_>>();

    println!("\nStep 5/6: PostgreSQL 설정");
    let database = prompt_postgres_config(&existing_config.database);

    // Generate configs
    println!("\nStep 6/6: 설정 파일 생성\n");
    let (org_path, agentdesk_config_path, compose_path) = match write_init_artifacts(
        &root,
        &channel_mappings,
        template_idx,
        &guild.id,
        &token,
        provider,
        owner_id,
        &allowed_channel_ids,
        &database,
        reconfigure,
    ) {
        Ok(paths) => paths,
        Err(e) => {
            eprintln!("설정 파일 생성 실패: {}", e);
            return;
        }
    };
    println!("  [OK] {}", org_path.display());
    println!("  [OK] {}", agentdesk_config_path.display());
    if let Some(compose_path) = compose_path {
        println!("  [OK] {}", compose_path.display());
    }

    // Create prompts
    let agents_root = crate::runtime_layout::managed_agents_root(&root);
    if let Err(e) = fs::create_dir_all(&agents_root) {
        eprintln!(
            "Failed to create {} directory: {}",
            agents_root.display(),
            e
        );
        return;
    }

    let shared_path = crate::runtime_layout::shared_prompt_path(&root);
    if !shared_path.exists() {
        if let Some(parent) = shared_path.parent() {
            if let Err(e) = fs::create_dir_all(parent) {
                eprintln!("Failed to create directory {}: {}", parent.display(), e);
                return;
            }
        }
        if let Err(e) = fs::write(&shared_path, default_shared_prompt()) {
            eprintln!("Failed to write {}: {}", shared_path.display(), e);
            return;
        }
        println!("  [OK] {}", shared_path.display());
    }

    let mut created_roles: Vec<String> = Vec::new();
    for (_, _, role) in &channel_mappings {
        if created_roles.contains(role) {
            continue;
        }
        let role_dir = agents_root.join(role);
        if let Err(e) = fs::create_dir_all(&role_dir) {
            eprintln!("Failed to create directory {}: {}", role_dir.display(), e);
            return;
        }
        let identity_path = role_dir.join("IDENTITY.md");
        if !identity_path.exists() {
            if let Err(e) = fs::write(&identity_path, default_agent_prompt(role)) {
                eprintln!("Failed to write {}: {}", identity_path.display(), e);
                return;
            }
            println!("  [OK] {}", identity_path.display());
        }
        created_roles.push(role.clone());
    }

    // Binary setup + platform-specific service installation
    {
        let Some(home) = dirs::home_dir() else {
            eprintln!("Error: cannot determine home directory");
            return;
        };
        let agentdesk_bin = root.join("bin").join("agentdesk");

        // Create wrapper bin dir
        let bin_dir = root.join("bin");
        if let Err(e) = fs::create_dir_all(&bin_dir) {
            eprintln!("Failed to create directory {}: {}", bin_dir.display(), e);
            return;
        }

        // If no binary installed yet, copy current executable
        if !agentdesk_bin.exists() {
            if let Ok(current_exe) = std::env::current_exe() {
                if let Err(e) = fs::copy(&current_exe, &agentdesk_bin) {
                    eprintln!("  [WARN] 바이너리 복사 실패: {} — 수동으로 복사하세요", e);
                } else {
                    println!("  [OK] {}", agentdesk_bin.display());
                }
            }
        }

        #[cfg(unix)]
        {
            match ensure_global_agentdesk_cli(&home) {
                Ok(wrapper_path) => println!("  [OK] {}", wrapper_path.display()),
                Err(e) => {
                    eprintln!("Failed to install global agentdesk CLI: {e}");
                    return;
                }
            }
        }

        // Platform-specific service installation (auto-detected)
        if let Err(e) = install_service(&home, &agentdesk_bin, reconfigure) {
            eprintln!("서비스 등록 실패: {e}");
            return;
        }

        println!("\n═══════════════════════════════════════");
        println!("  초기 설정 완료!");
        println!("═══════════════════════════════════════");
        println!("\n생성된 파일:");
        println!("  {} (org.yaml)", org_path.display());
        println!("  {} (agentdesk.yaml)", agentdesk_config_path.display());
        println!("  {} (agents)", agents_root.display());
        println!("\n다음 단계:");
        println!("  1. 프롬프트 파일을 편집하여 에이전트 성격을 정의하세요");
        println!("  2. Discord에서 봇에게 메시지를 보내 동작을 확인하세요");
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn handle_emit_launchd_plist(args: &EmitLaunchdPlistArgs) -> Result<(), String> {
    let home = args.home.clone().or_else(dirs::home_dir).ok_or_else(|| {
        "Failed to resolve home directory for launchd plist rendering".to_string()
    })?;
    let root_dir = args
        .root_dir
        .clone()
        .unwrap_or_else(|| default_launchd_root_dir(&home, args.flavor));
    let agentdesk_bin = match args.agentdesk_bin.clone() {
        Some(path) => path,
        None => std::env::current_exe()
            .map_err(|e| format!("Failed to resolve current agentdesk executable: {e}"))?,
    };
    let plist =
        generate_launchd_plist_for_flavor_with_root(args.flavor, &home, &agentdesk_bin, &root_dir);

    if let Some(output_path) = &args.output {
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "Failed to create plist output directory {}: {e}",
                    parent.display()
                )
            })?;
        }
        fs::write(output_path, plist).map_err(|e| {
            format!(
                "Failed to write launchd plist to {}: {e}",
                output_path.display()
            )
        })?;
    } else {
        print!("{plist}");
    }

    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub(crate) fn handle_emit_launchd_plist(_args: &EmitLaunchdPlistArgs) -> Result<(), String> {
    Err("emit-launchd-plist is only supported on macOS".to_string())
}

#[cfg(all(test, feature = "legacy-sqlite-tests"))]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[cfg(target_os = "macos")]
    fn assert_plist_xml_valid(plist: &str) {
        let temp_dir = tempfile::tempdir().unwrap();
        let plist_path = temp_dir.path().join("agentdesk.plist");
        fs::write(&plist_path, plist).unwrap();
        let status = std::process::Command::new("plutil")
            .args(["-lint", plist_path.to_str().unwrap()])
            .status()
            .unwrap();
        assert!(status.success(), "plist should pass plutil validation");
    }

    #[cfg(unix)]
    #[test]
    fn preferred_agentdesk_cli_dir_uses_first_path_match() {
        let home = Path::new("/tmp/agentdesk-home");
        let path = OsStr::new(
            "/usr/local/bin:/tmp/agentdesk-home/.local/bin:/tmp/agentdesk-home/bin:/usr/bin",
        );
        assert_eq!(
            preferred_agentdesk_cli_dir_with_path(home, Some(path)),
            PathBuf::from("/tmp/agentdesk-home/.local/bin")
        );
    }

    #[cfg(unix)]
    #[test]
    fn preferred_agentdesk_cli_dir_defaults_to_home_bin() {
        let home = Path::new("/tmp/agentdesk-home");
        assert_eq!(
            preferred_agentdesk_cli_dir_with_path(home, None),
            PathBuf::from("/tmp/agentdesk-home/bin")
        );
    }

    #[cfg(unix)]
    #[test]
    fn agentdesk_cli_wrapper_script_includes_release_candidate() {
        let script = agentdesk_cli_wrapper_script(Path::new("/tmp/agentdesk-home"));
        assert!(script.contains(".adk/release/bin/agentdesk"));
        assert!(!script.contains(".adk/dev"));
        assert!(script.contains("exec \"$candidate\" \"$@\""));
    }

    #[test]
    fn parse_launchd_env_line_accepts_plain_and_export_forms() {
        assert_eq!(
            parse_launchd_env_line("MEMENTO_ACCESS_KEY=abc123"),
            Some(("MEMENTO_ACCESS_KEY".to_string(), "abc123".to_string()))
        );
        assert_eq!(
            parse_launchd_env_line("export MEMENTO_ACCESS_KEY=\"abc123\""),
            Some(("MEMENTO_ACCESS_KEY".to_string(), "abc123".to_string()))
        );
    }

    #[test]
    fn parse_launchd_env_line_skips_comments_and_invalid_keys() {
        assert_eq!(parse_launchd_env_line("# comment"), None);
        assert_eq!(parse_launchd_env_line("1BAD=value"), None);
        assert_eq!(parse_launchd_env_line("NO_EQUALS"), None);
    }

    #[test]
    fn cli_init_provider_choices_follow_provider_registry() {
        assert_eq!(
            cli_init_provider_labels(),
            vec![
                "claude (Anthropic)",
                "codex (OpenAI)",
                "gemini (Google)",
                "opencode (OpenCode)",
                "qwen (Alibaba)"
            ]
        );
        assert_eq!(cli_init_provider_from_index(3), "opencode");
        assert_eq!(cli_init_provider_from_index(4), "qwen");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn generate_launchd_plist_includes_optional_launchd_env_entries() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root_dir = temp_dir.path().join(".adk").join("release");
        let config_dir = root_dir.join("config");
        fs::create_dir_all(&config_dir).unwrap();
        fs::write(
            config_dir.join("launchd.env"),
            "MEMENTO_ACCESS_KEY=abc123\nexport SAMPLE_FLAG=\"enabled\"\n",
        )
        .unwrap();

        let plist = generate_launchd_plist_for_flavor_with_root(
            LaunchdPlistFlavorArg::Release,
            temp_dir.path(),
            &root_dir.join("bin").join("agentdesk"),
            &root_dir,
        );

        assert!(plist.contains("<key>MEMENTO_ACCESS_KEY</key>"));
        assert!(plist.contains("<string>abc123</string>"));
        assert!(plist.contains("<key>SAMPLE_FLAG</key>"));
        assert!(plist.contains("<string>enabled</string>"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn generate_launchd_plist_release_sets_root_path_and_valid_xml() {
        let temp_dir = tempfile::tempdir().unwrap();
        let home = temp_dir.path().join("home");
        let root_dir = home.join(".adk").join("release");
        let plist = generate_launchd_plist_for_flavor_with_root(
            LaunchdPlistFlavorArg::Release,
            &home,
            &root_dir.join("bin").join("agentdesk"),
            &root_dir,
        );

        assert!(plist.contains("<string>com.agentdesk.release</string>"));
        assert!(plist.contains("<key>AGENTDESK_ROOT_DIR</key>"));
        assert!(plist.contains(&format!("<string>{}</string>", root_dir.display())));
        assert!(plist.contains("/opt/homebrew/bin"));
        assert!(plist.contains(&format!("{}/.local/bin", home.display())));
        assert!(plist.contains(&format!(
            "<string>{}</string>",
            root_dir
                .join("logs")
                .join("dcserver.launchd.stdout.log")
                .display()
        )));
        assert!(plist.contains(&format!(
            "<string>{}</string>",
            root_dir
                .join("logs")
                .join("dcserver.launchd.stderr.log")
                .display()
        )));
        assert_plist_xml_valid(&plist);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn emit_launchd_plist_writes_requested_output_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let home = temp_dir.path().join("home");
        let root_dir = home.join(".adk").join("release");
        let output_path = temp_dir.path().join("nested").join("agentdesk.plist");
        let args = EmitLaunchdPlistArgs {
            flavor: LaunchdPlistFlavorArg::Release,
            home: Some(home.clone()),
            root_dir: Some(root_dir.clone()),
            agentdesk_bin: Some(root_dir.join("bin").join("agentdesk")),
            output: Some(output_path.clone()),
        };

        handle_emit_launchd_plist(&args).unwrap();

        let written = fs::read_to_string(&output_path).unwrap();
        assert!(written.contains("<string>com.agentdesk.release</string>"));
        assert!(written.contains(&format!("<string>{}</string>", root_dir.display())));
        assert_plist_xml_valid(&written);
    }

    #[test]
    fn parse_owner_id_rejects_short_values() {
        let error = parse_owner_id(Some("7")).unwrap_err();
        assert!(error.contains("owner_id must be a Discord user id"));
    }

    #[test]
    fn parse_owner_id_accepts_discord_snowflakes() {
        assert_eq!(
            parse_owner_id(Some("1469509284508340276")).unwrap(),
            Some(1469509284508340276)
        );
    }

    #[test]
    fn write_init_artifacts_rejects_invalid_owner_id_before_writing_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path().join(".adk").join("runtime");
        let org_path = crate::runtime_layout::config_dir(&root).join("org.yaml");
        let agentdesk_config_path = init_config_path(&root);
        let database = crate::config::DatabaseConfig::default();

        let error = write_init_artifacts(
            &root,
            &[(
                "123456789012345678".to_string(),
                "general".to_string(),
                "assistant".to_string(),
            )],
            0,
            "guild-123",
            "test-token",
            "claude",
            Some("7"),
            &[123456789012345678],
            &database,
            false,
        )
        .unwrap_err();

        assert!(error.contains("owner_id must be a Discord user id"));
        assert!(!org_path.exists());
        assert!(!agentdesk_config_path.exists());
    }

    #[test]
    fn write_postgres_compose_template_skips_when_disabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path().join(".adk").join("runtime");
        let compose_path = write_postgres_compose_template(
            &root,
            &crate::config::DatabaseConfig::default(),
            false,
        )
        .unwrap();

        assert!(compose_path.is_none());
        assert!(!root.join("docker-compose.postgres.yml").exists());
    }

    #[test]
    fn write_init_artifacts_writes_postgres_compose_and_database_config_when_enabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path().join(".adk").join("runtime");
        let mut database = crate::config::DatabaseConfig::default();
        database.enabled = true;
        database.host = "db.internal".to_string();
        database.port = 5433;
        database.dbname = "agentdesk_dev".to_string();
        database.user = "agentdesk_app".to_string();
        database.password = Some("secretpw".to_string());
        database.pool_max = 16;

        let (org_path, config_path, compose_path) = write_init_artifacts(
            &root,
            &[(
                "123456789012345678".to_string(),
                "general".to_string(),
                "assistant".to_string(),
            )],
            0,
            "guild-123",
            "test-token",
            "claude",
            Some("1469509284508340276"),
            &[123456789012345678],
            &database,
            false,
        )
        .unwrap();

        let compose_path = compose_path.expect("postgres compose path should be created");
        let compose = fs::read_to_string(&compose_path).unwrap();
        let config = fs::read_to_string(&config_path).unwrap();

        assert!(org_path.exists());
        assert!(config_path.exists());
        assert!(compose_path.exists());
        assert!(compose.contains("image: postgres:17"));
        assert!(compose.contains("POSTGRES_DB: \"agentdesk_dev\""));
        assert!(compose.contains("POSTGRES_USER: \"agentdesk_app\""));
        assert!(compose.contains("POSTGRES_PASSWORD: \"secretpw\""));
        assert!(config.contains("database:"));
        assert!(config.contains("enabled: true"));
        assert!(config.contains("host: db.internal"));
        assert!(config.contains("port: 5433"));
        assert!(config.contains("dbname: agentdesk_dev"));
        assert!(config.contains("user: agentdesk_app"));
        assert!(config.contains("pool_max: 16"));
    }
}

#[cfg(all(test, target_os = "macos"))]
mod launchd_plist_tests {
    use super::*;

    fn assert_plist_xml_valid(plist: &str) {
        let temp_dir = tempfile::tempdir().unwrap();
        let plist_path = temp_dir.path().join("agentdesk.plist");
        fs::write(&plist_path, plist).unwrap();
        let status = std::process::Command::new("plutil")
            .args(["-lint", plist_path.to_str().unwrap()])
            .status()
            .unwrap();
        assert!(status.success(), "plist should pass plutil validation");
    }

    #[test]
    fn clamp_launchd_nofile_soft_limit_never_exceeds_host_hard_limit() {
        assert_eq!(
            clamp_launchd_nofile_soft_limit(LAUNCHD_NOFILE_SOFT_LIMIT_TARGET * 2),
            Some(LAUNCHD_NOFILE_SOFT_LIMIT_TARGET)
        );
        assert_eq!(
            clamp_launchd_nofile_soft_limit(LAUNCHD_NOFILE_SOFT_LIMIT_TARGET - 1),
            Some(LAUNCHD_NOFILE_SOFT_LIMIT_TARGET - 1)
        );
        assert_eq!(clamp_launchd_nofile_soft_limit(0), None);
    }

    #[test]
    fn generate_launchd_plist_release_sets_clamped_soft_number_of_files_limit() {
        let temp_dir = tempfile::tempdir().unwrap();
        let home = temp_dir.path().join("home");
        let root_dir = home.join(".adk").join("release");
        let plist = generate_launchd_plist_for_flavor_with_root(
            LaunchdPlistFlavorArg::Release,
            &home,
            &root_dir.join("bin").join("agentdesk"),
            &root_dir,
        );

        assert!(!plist.contains("<key>HardResourceLimits</key>"));
        if let Some(expected_soft_limit) = launchd_nofile_soft_limit() {
            assert!(plist.contains("<key>SoftResourceLimits</key>"));
            assert_eq!(plist.matches("<key>NumberOfFiles</key>").count(), 1);
            assert!(plist.contains(&format!("<integer>{expected_soft_limit}</integer>")));
        } else {
            assert!(!plist.contains("<key>SoftResourceLimits</key>"));
            assert_eq!(plist.matches("<key>NumberOfFiles</key>").count(), 0);
        }
        assert_plist_xml_valid(&plist);
    }
}

#[cfg(target_os = "macos")]
fn install_service(home: &Path, agentdesk_bin: &Path, reconfigure: bool) -> Result<(), String> {
    let flavor = LaunchdPlistFlavorArg::Release;
    let plist_content = generate_launchd_plist(home, agentdesk_bin);
    let launch_agents = home.join("Library").join("LaunchAgents");
    fs::create_dir_all(&launch_agents)
        .map_err(|e| format!("Failed to create LaunchAgents directory: {e}"))?;
    let plist_filename = launchd_plist_filename(flavor);
    let plist_path = launch_agents.join(&plist_filename);
    write_with_backup(&plist_path, &plist_content, reconfigure)
        .map_err(|e| format!("Failed to write plist {}: {e}", plist_path.display()))?;
    println!("  [OK] {}", plist_path.display());

    let load_answer = prompt_line("\ndcserver를 지금 시작할까요? (Y/n): ");
    if load_answer.is_empty() || load_answer.to_lowercase().starts_with('y') {
        let label = dcserver::AGENTDESK_DCSERVER_LAUNCHD_LABEL;
        let uid = get_uid().map_err(|e| {
            format!("UID를 가져올 수 없습니다: {e} — 수동으로 launchctl을 실행하세요")
        })?;
        if dcserver::is_launchd_job_loaded(label) {
            let _ = std::process::Command::new("launchctl")
                .args([
                    "bootout",
                    &format!("gui/{}", uid),
                    &plist_path.to_string_lossy().to_string(),
                ])
                .status();
        }
        let status = std::process::Command::new("launchctl")
            .args([
                "bootstrap",
                &format!("gui/{}", uid),
                &plist_path.to_string_lossy().to_string(),
            ])
            .status();
        match status {
            Ok(s) if s.success() => println!("  [OK] dcserver 시작됨"),
            _ => println!(
                "  [WARN] launchd 등록 실패 — 수동으로 실행: launchctl bootstrap gui/$(id -u) {}",
                plist_path.display()
            ),
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn install_service(home: &Path, agentdesk_bin: &Path, _reconfigure: bool) -> Result<(), String> {
    let service_name = "agentdesk-dcserver";
    let root_dir =
        dcserver::agentdesk_runtime_root().unwrap_or_else(|| home.join(".adk").join("release"));
    let logs_dir = root_dir.join("logs");
    fs::create_dir_all(&logs_dir).map_err(|e| format!("Failed to create logs directory: {e}"))?;
    let unit_content = format!(
        "[Unit]\n\
         Description=AgentDesk Discord Control Server\n\
         After=network.target\n\n\
         [Service]\n\
         Type=simple\n\
         ExecStart={bin} dcserver\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         Environment=AGENTDESK_ROOT_DIR={root}\n\
         StandardOutput=append:{logs}/dcserver.launchd.stdout.log\n\
         StandardError=append:{logs}/dcserver.launchd.stderr.log\n\n\
         [Install]\n\
         WantedBy=default.target\n",
        bin = agentdesk_bin.display(),
        root = root_dir.display(),
        logs = logs_dir.display()
    );

    let user_systemd = home.join(".config").join("systemd").join("user");
    fs::create_dir_all(&user_systemd)
        .map_err(|e| format!("Failed to create systemd user directory: {e}"))?;
    let unit_path = user_systemd.join(format!("{service_name}.service"));
    fs::write(&unit_path, &unit_content)
        .map_err(|e| format!("Failed to write systemd unit {}: {e}", unit_path.display()))?;
    println!("  [OK] {}", unit_path.display());

    let load_answer = prompt_line("\ndcserver를 지금 시작할까요? (Y/n): ");
    if load_answer.is_empty() || load_answer.to_lowercase().starts_with('y') {
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status();
        let status = std::process::Command::new("systemctl")
            .args(["--user", "enable", "--now", service_name])
            .status();
        match status {
            Ok(s) if s.success() => println!("  [OK] dcserver 시작됨 (systemd)"),
            _ => println!(
                "  [WARN] systemd 등록 실패 — 수동: systemctl --user enable --now {service_name}"
            ),
        }
    }
    Ok(())
}

#[cfg(target_os = "windows")]
fn install_service(_home: &Path, agentdesk_bin: &Path, _reconfigure: bool) -> Result<(), String> {
    let service_name = "AgentDeskDcserver";
    let root_dir = dcserver::agentdesk_runtime_root().unwrap_or_else(|| {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".adk")
            .join("release")
    });
    let logs_dir = root_dir.join("logs");
    fs::create_dir_all(&logs_dir).map_err(|e| format!("Failed to create logs directory: {e}"))?;

    println!("  Windows 서비스 등록:");
    println!("  NSSM 사용 시:");
    println!(
        "    nssm install {service_name} \"{}\" dcserver",
        agentdesk_bin.display()
    );
    println!(
        "    nssm set {service_name} AppStdout \"{}\"",
        logs_dir.join("dcserver.stdout.log").display()
    );
    println!(
        "    nssm set {service_name} AppStderr \"{}\"",
        logs_dir.join("dcserver.stderr.log").display()
    );
    println!("    nssm start {service_name}");
    println!("  sc.exe 사용 시:");
    println!(
        "    sc create {service_name} binPath=\"{} dcserver\" start=auto",
        agentdesk_bin.display()
    );
    println!("    sc start {service_name}");

    let load_answer = prompt_line("\nNSSM으로 지금 등록할까요? (y/N): ");
    if load_answer.to_lowercase().starts_with('y') {
        let status = std::process::Command::new("nssm")
            .args([
                "install",
                service_name,
                &agentdesk_bin.to_string_lossy(),
                "dcserver",
            ])
            .status();
        match status {
            Ok(s) if s.success() => {
                // Configure NSSM log routing
                let stdout_log = logs_dir.join("dcserver.stdout.log");
                let stderr_log = logs_dir.join("dcserver.stderr.log");
                let _ = std::process::Command::new("nssm")
                    .args([
                        "set",
                        service_name,
                        "AppStdout",
                        &stdout_log.to_string_lossy(),
                    ])
                    .status();
                let _ = std::process::Command::new("nssm")
                    .args([
                        "set",
                        service_name,
                        "AppStderr",
                        &stderr_log.to_string_lossy(),
                    ])
                    .status();
                let _ = std::process::Command::new("nssm")
                    .args(["start", service_name])
                    .status();
                println!("  [OK] dcserver 시작됨 (NSSM)");
            }
            _ => println!("  [WARN] NSSM 등록 실패 — nssm이 설치되어 있는지 확인하세요"),
        }
    }
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
fn install_service(_home: &Path, agentdesk_bin: &Path, _reconfigure: bool) -> Result<(), String> {
    println!("  이 플랫폼에서는 자동 서비스 등록이 지원되지 않습니다.");
    println!("  수동으로 실행: {} dcserver", agentdesk_bin.display());
    Ok(())
}

fn write_with_backup(path: &Path, content: &str, reconfigure: bool) -> Result<(), io::Error> {
    if reconfigure && path.exists() {
        let existing = fs::read_to_string(path).unwrap_or_default();
        if existing == content {
            return Ok(()); // No change
        }
        let backup = path.with_extension(format!(
            "{}.bak",
            path.extension().and_then(|e| e.to_str()).unwrap_or("bak")
        ));
        if !backup.exists() {
            let _ = fs::copy(path, &backup);
        }
    }
    fs::write(path, content)
}

#[cfg(target_os = "macos")]
fn get_uid() -> Result<String, String> {
    let output = std::process::Command::new("id")
        .arg("-u")
        .output()
        .map_err(|e| format!("failed to get uid: {e}"))?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}
