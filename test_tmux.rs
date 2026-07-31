fn main() {
    let output = std::process::Command::new("tmux").arg("-V").output().unwrap();
    println!("{:?}", String::from_utf8(output.stdout).unwrap());
}
