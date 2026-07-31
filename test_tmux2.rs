fn main() {
    let tmux_session = "AgentDesk-codex-4615-cap";
    let status = std::process::Command::new("tmux")
        .args(["new-session", "-d", "-s", tmux_session])
        .status()
        .expect("start tmux fixture");
    println!("success: {}", status.success());
}
