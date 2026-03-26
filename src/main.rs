#![recursion_limit = "256"]
mod cli;
mod config;
pub(crate) mod credential;
mod db;
mod dispatch;
mod engine;
mod error;
mod github;
pub(crate) mod kanban;
pub(crate) mod pipeline;
pub(crate) mod runtime;
mod server;
mod services;
mod ui;
mod utils;

#[cfg(test)]
mod integration_tests;

// Re-export for crate-level access (used by services::discord::mod.rs)
pub(crate) use cli::agentdesk_runtime_root;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

// ── Clap CLI definition ──────────────────────────────────────

#[derive(Parser)]
#[command(name = "agentdesk", version = env!("CARGO_PKG_VERSION"), about = "AI agent orchestration platform")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Show server health, active sessions, and auto-queue status
    Status,
    /// List kanban cards
    Cards {
        /// Filter by status (e.g. ready, in_progress, done)
        #[arg(long)]
        status: Option<String>,
    },
    /// Dispatch operations
    Dispatch {
        #[command(subcommand)]
        action: DispatchAction,
    },
    /// List agents and their status
    Agents,
    /// Runtime config get/set
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Call any API endpoint (curl replacement)
    Api {
        /// HTTP method (GET, POST, PATCH, PUT, DELETE)
        method: String,
        /// API path (e.g. /api/health)
        path: String,
        /// Optional JSON body
        body: Option<String>,
    },
    /// Environment diagnostics
    Doctor,
}

#[derive(Subcommand)]
enum DispatchAction {
    /// List active dispatches
    List,
    /// Retry a dispatch for a card
    Retry {
        /// Kanban card ID
        card_id: String,
    },
    /// Redispatch a card
    Redispatch {
        /// Kanban card ID
        card_id: String,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Get current runtime config
    Get,
    /// Set runtime config (JSON string)
    Set {
        /// JSON value to set
        json: String,
    },
}

fn exit_for_cli(result: std::result::Result<(), String>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // ── Legacy flag pre-check ────────────────────────────────
    // These flags use custom sub-argument parsing and must be handled
    // before clap takes over. They create their own tokio runtime.
    for arg in &args[1..] {
        match arg.as_str() {
            "--dcserver" | "dcserver" => {
                let token = args
                    .iter()
                    .skip_while(|a| a.as_str() != "--dcserver" && a.as_str() != "dcserver")
                    .nth(1)
                    .filter(|a| !a.starts_with('-'))
                    .cloned()
                    .or_else(|| std::env::var("AGENTDESK_TOKEN").ok());
                cli::handle_dcserver(token);
                return Ok(());
            }
            "--init" | "init" => {
                cli::handle_init(false);
                return Ok(());
            }
            "--reconfigure" | "reconfigure" => {
                cli::handle_init(true);
                return Ok(());
            }
            "--restart-dcserver" => {
                let start_index = args.iter().position(|a| a == "--restart-dcserver").unwrap() + 1;
                match cli::parse_restart_dcserver_report_context(&args, start_index) {
                    Ok(report_context) => cli::handle_restart_dcserver(report_context),
                    Err(err) => eprintln!("Error: {err}"),
                }
                return Ok(());
            }
            "--discord-sendfile" => {
                let mut file_path: Option<String> = None;
                let mut channel_id: Option<u64> = None;
                let mut key: Option<String> = None;
                let mut j = args.iter().position(|a| a == "--discord-sendfile").unwrap() + 1;
                while j < args.len() {
                    match args[j].as_str() {
                        "--channel" => {
                            channel_id = args.get(j + 1).and_then(|v| v.parse().ok());
                            j += 2;
                        }
                        "--key" => {
                            key = args.get(j + 1).cloned();
                            j += 2;
                        }
                        _ if file_path.is_none() && !args[j].starts_with("--") => {
                            file_path = Some(args[j].clone());
                            j += 1;
                        }
                        _ => {
                            j += 1;
                        }
                    }
                }
                match (file_path, channel_id, key) {
                    (Some(fp), Some(cid), Some(k)) => cli::handle_discord_sendfile(&fp, cid, &k),
                    _ => eprintln!(
                        "Error: --discord-sendfile requires <PATH>, --channel <ID>, and --key <HASH>"
                    ),
                }
                return Ok(());
            }
            "--discord-sendmessage" => {
                let mut message: Option<String> = None;
                let mut channel_id: Option<u64> = None;
                let mut key: Option<String> = None;
                let mut j = args
                    .iter()
                    .position(|a| a == "--discord-sendmessage")
                    .unwrap()
                    + 1;
                while j < args.len() {
                    match args[j].as_str() {
                        "--channel" => {
                            channel_id = args.get(j + 1).and_then(|v| v.parse().ok());
                            j += 2;
                        }
                        "--message" => {
                            message = args.get(j + 1).cloned();
                            j += 2;
                        }
                        "--key" => {
                            key = args.get(j + 1).cloned();
                            j += 2;
                        }
                        _ => {
                            j += 1;
                        }
                    }
                }
                match (message, channel_id) {
                    (Some(msg), Some(cid)) => {
                        cli::handle_discord_sendmessage(&msg, cid, key.as_deref())
                    }
                    _ => eprintln!(
                        "Error: --discord-sendmessage requires --channel <ID> and --message <TEXT>"
                    ),
                }
                return Ok(());
            }
            "--discord-senddm" => {
                let mut message: Option<String> = None;
                let mut user_id: Option<u64> = None;
                let mut key: Option<String> = None;
                let mut j = args.iter().position(|a| a == "--discord-senddm").unwrap() + 1;
                while j < args.len() {
                    match args[j].as_str() {
                        "--user" => {
                            user_id = args.get(j + 1).and_then(|v| v.parse().ok());
                            j += 2;
                        }
                        "--message" => {
                            message = args.get(j + 1).cloned();
                            j += 2;
                        }
                        "--key" => {
                            key = args.get(j + 1).cloned();
                            j += 2;
                        }
                        _ => {
                            j += 1;
                        }
                    }
                }
                match (message, user_id) {
                    (Some(msg), Some(uid)) => cli::handle_discord_senddm(&msg, uid, key.as_deref()),
                    _ => eprintln!(
                        "Error: --discord-senddm requires --user <ID> and --message <TEXT>"
                    ),
                }
                return Ok(());
            }
            #[cfg(unix)]
            "--tmux-wrapper" => {
                let i = args.iter().position(|a| a == "--tmux-wrapper").unwrap();
                let mut output_file: Option<String> = None;
                let mut input_fifo: Option<String> = None;
                let mut prompt_file: Option<String> = None;
                let mut cwd: Option<String> = None;
                let mut input_mode = services::tmux_wrapper::InputMode::Fifo;
                let mut claude_cmd: Vec<String> = Vec::new();
                let mut j = i + 1;
                let mut after_separator = false;
                while j < args.len() {
                    if after_separator {
                        claude_cmd.push(args[j].clone());
                        j += 1;
                        continue;
                    }
                    match args[j].as_str() {
                        "--" => {
                            after_separator = true;
                            j += 1;
                        }
                        "--output-file" => {
                            output_file = args.get(j + 1).cloned();
                            j += 2;
                        }
                        "--input-fifo" => {
                            input_fifo = args.get(j + 1).cloned();
                            j += 2;
                        }
                        "--prompt-file" => {
                            prompt_file = args.get(j + 1).cloned();
                            j += 2;
                        }
                        "--cwd" => {
                            cwd = args.get(j + 1).cloned();
                            j += 2;
                        }
                        "--input-mode" => {
                            if let Some(mode) = args.get(j + 1) {
                                input_mode = match mode.as_str() {
                                    "pipe" => services::tmux_wrapper::InputMode::Pipe,
                                    _ => services::tmux_wrapper::InputMode::Fifo,
                                };
                            }
                            j += 2;
                        }
                        _ => {
                            j += 1;
                        }
                    }
                }
                match (output_file, input_fifo, prompt_file) {
                    (Some(of), Some(inf), Some(pf)) => {
                        let wd = cwd.unwrap_or_else(|| ".".to_string());
                        services::tmux_wrapper::run(&of, &inf, &pf, &wd, &claude_cmd, input_mode);
                    }
                    _ => eprintln!(
                        "Error: --tmux-wrapper requires --output-file, --input-fifo, and --prompt-file"
                    ),
                }
                return Ok(());
            }
            #[cfg(unix)]
            "--codex-tmux-wrapper" => {
                let i = args
                    .iter()
                    .position(|a| a == "--codex-tmux-wrapper")
                    .unwrap();
                let mut output_file: Option<String> = None;
                let mut input_fifo: Option<String> = None;
                let mut prompt_file: Option<String> = None;
                let mut cwd: Option<String> = None;
                let mut codex_bin: Option<String> = None;
                let mut codex_model: Option<String> = None;
                let mut input_mode = services::tmux_wrapper::InputMode::Fifo;
                let mut j = i + 1;
                while j < args.len() {
                    match args[j].as_str() {
                        "--output-file" => {
                            output_file = args.get(j + 1).cloned();
                            j += 2;
                        }
                        "--input-fifo" => {
                            input_fifo = args.get(j + 1).cloned();
                            j += 2;
                        }
                        "--prompt-file" => {
                            prompt_file = args.get(j + 1).cloned();
                            j += 2;
                        }
                        "--cwd" => {
                            cwd = args.get(j + 1).cloned();
                            j += 2;
                        }
                        "--codex-bin" => {
                            codex_bin = args.get(j + 1).cloned();
                            j += 2;
                        }
                        "--codex-model" => {
                            codex_model = args.get(j + 1).cloned();
                            j += 2;
                        }
                        "--input-mode" => {
                            if let Some(mode) = args.get(j + 1) {
                                input_mode = match mode.as_str() {
                                    "pipe" => services::tmux_wrapper::InputMode::Pipe,
                                    _ => services::tmux_wrapper::InputMode::Fifo,
                                };
                            }
                            j += 2;
                        }
                        _ => {
                            j += 1;
                        }
                    }
                }
                match (output_file, input_fifo, prompt_file, codex_bin) {
                    (Some(of), Some(inf), Some(pf), Some(bin)) => {
                        let wd = cwd.unwrap_or_else(|| ".".to_string());
                        services::codex_tmux_wrapper::run(
                            &of,
                            &inf,
                            &pf,
                            &wd,
                            &bin,
                            codex_model.as_deref(),
                            input_mode,
                        );
                    }
                    _ => eprintln!(
                        "Error: --codex-tmux-wrapper requires --output-file, --input-fifo, --prompt-file, and --codex-bin"
                    ),
                }
                return Ok(());
            }
            _ => {}
        }
    }

    // ── Clap subcommand parsing ──────────────────────────────
    let parsed = Cli::try_parse();
    match parsed {
        Ok(cli) => match cli.command {
            Some(Commands::Status) => {
                return exit_for_cli(cli::client::cmd_status());
            }
            Some(Commands::Cards { status }) => {
                return exit_for_cli(cli::client::cmd_cards(status.as_deref()));
            }
            Some(Commands::Dispatch { action }) => {
                return exit_for_cli(match action {
                    DispatchAction::List => cli::client::cmd_dispatch_list(),
                    DispatchAction::Retry { card_id } => cli::client::cmd_dispatch_retry(&card_id),
                    DispatchAction::Redispatch { card_id } => {
                        cli::client::cmd_dispatch_redispatch(&card_id)
                    }
                });
            }
            Some(Commands::Agents) => {
                return exit_for_cli(cli::client::cmd_agents());
            }
            Some(Commands::Config { action }) => {
                return exit_for_cli(match action {
                    ConfigAction::Get => cli::client::cmd_config_get(),
                    ConfigAction::Set { json } => cli::client::cmd_config_set(&json),
                });
            }
            Some(Commands::Api { method, path, body }) => {
                return exit_for_cli(cli::client::cmd_api(&method, &path, body.as_deref()));
            }
            Some(Commands::Doctor) => {
                return exit_for_cli(cli::doctor::cmd_doctor());
            }
            None => {
                // No subcommand — fall through to server start
            }
        },
        Err(e) => {
            // --help and --version exit with 0; actual errors exit with 1
            if e.kind() == clap::error::ErrorKind::DisplayHelp
                || e.kind() == clap::error::ErrorKind::DisplayVersion
            {
                e.print().ok();
                std::process::exit(0);
            }
            if args.len() > 1 {
                e.print().ok();
                std::process::exit(1);
            }
            // No args — fall through to server start
        }
    }

    // ── Default: start full AgentDesk server ─────────────────
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::from_default_env().add_directive("agentdesk=info".parse().unwrap()),
            )
            .init();

        let config = config::load().expect("Failed to load config");
        let db = db::init(&config).expect("Failed to init DB");

        // Load data-driven pipeline definition (#106)
        let pipeline_path = config.policies.dir.join("default-pipeline.yaml");
        if pipeline_path.exists() {
            pipeline::load(&pipeline_path).expect("Failed to load pipeline definition");
            tracing::info!("Pipeline loaded: {}", pipeline_path.display());
        }

        let engine =
            engine::PolicyEngine::new(&config, db.clone()).expect("Failed to init policy engine");

        tracing::info!(
            "AgentDesk v{} starting on {}:{}",
            env!("CARGO_PKG_VERSION"),
            config.server.host,
            config.server.port
        );

        tokio::try_join!(server::run(
            config.clone(),
            db.clone(),
            engine.clone(),
            None
        ),)
        .expect("Server error");
    });

    Ok(())
}
