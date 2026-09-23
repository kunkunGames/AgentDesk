use super::{GitCommand, ensure_git_worktree, repo_id_for_dir};

#[test]
fn repository_metadata_preserves_origin_and_rejects_non_worktrees() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().to_str().unwrap();
    assert!(ensure_git_worktree(path).is_err());
    GitCommand::new()
        .repo(path)
        .args(["init", "--quiet"])
        .run_output()
        .unwrap();
    GitCommand::new()
        .repo(path)
        .args([
            "remote",
            "add",
            "origin",
            "https://github.com/example/project.git",
        ])
        .run_output()
        .unwrap();
    ensure_git_worktree(path).unwrap();
    assert_eq!(repo_id_for_dir(path).as_deref(), Some("example/project"));
}

#[cfg(unix)]
#[test]
fn stalled_git_configuration_is_terminated_by_metadata_deadline() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().to_str().unwrap();
    GitCommand::new()
        .repo(path)
        .args(["init", "--quiet"])
        .run_output()
        .unwrap();
    let fifo = directory.path().join("unavailable-config");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    let config = directory.path().join(".git/config");
    let original = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        format!("{original}\n[include]\npath = {}\n", fifo.display()),
    )
    .unwrap();
    let started = std::time::Instant::now();
    let error = ensure_git_worktree(path).unwrap_err();
    assert!(error.contains("timed out"), "{error}");
    assert!(started.elapsed() < std::time::Duration::from_secs(15));
    // A timed-out repository must not prevent the following probe from succeeding.
    std::fs::write(&config, original).unwrap();
    ensure_git_worktree(path).unwrap();
}
