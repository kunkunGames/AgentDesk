fn main() { let output = std::process::Command::new("tmux").args(["new-session", "-d", "-s", "test"]).output().unwrap(); println!("{:?}", output); }
