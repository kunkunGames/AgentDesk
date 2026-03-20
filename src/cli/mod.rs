pub(crate) mod dcserver;
pub(crate) mod discord;
pub(crate) mod init;
pub(crate) mod utils;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// Re-export commonly used items
pub use dcserver::{
    agentdesk_runtime_root, handle_dcserver, handle_restart_dcserver,
    parse_restart_dcserver_report_context,
};
pub use discord::{handle_discord_senddm, handle_discord_sendfile, handle_discord_sendmessage};
pub use init::handle_init;
