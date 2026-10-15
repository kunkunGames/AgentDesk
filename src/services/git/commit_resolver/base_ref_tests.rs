use super::*;

fn git(repo: &str, args: &[&str]) {
    let output = git_command().current_dir(repo).args(args).output().unwrap();
    assert!(output.status.success(), "{:?}: {:?}", args, output);
}

fn repository(branch: &str) -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().to_str().unwrap();
    git(path, &["init", "-b", branch]);
    git(
        path,
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "baseline",
        ],
    );
    temp
}

#[test]
fn upstream_base_supports_master_and_preserves_remote_baseline() {
    let temp = repository("master");
    let path = temp.path().to_str().unwrap();
    assert_eq!(upstream_base_ref(path), "master");
    let baseline = git_head_commit(path).unwrap();
    git(
        path,
        &["update-ref", "refs/remotes/origin/master", &baseline],
    );
    git(
        path,
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "local ahead",
        ],
    );
    let selected = upstream_base_ref(path);
    assert_eq!(selected, "origin/master");
    assert_eq!(git_commit_for_ref(path, &selected).unwrap(), baseline);
    assert_ne!(git_head_commit(path).unwrap(), baseline);
}

#[test]
fn upstream_base_follows_custom_default_and_rejects_unborn_default() {
    let temp = repository("trunk");
    let path = temp.path().to_str().unwrap();
    let baseline = git_head_commit(path).unwrap();
    git(
        path,
        &["update-ref", "refs/remotes/origin/trunk", &baseline],
    );
    git(
        path,
        &[
            "symbolic-ref",
            "refs/remotes/origin/HEAD",
            "refs/remotes/origin/trunk",
        ],
    );
    git(path, &["branch", "main"]);
    assert_eq!(upstream_base_ref(path), "origin/HEAD");
    assert_eq!(
        git_commit_for_ref(path, &upstream_base_ref(path)).unwrap(),
        baseline
    );

    let empty = tempfile::tempdir().unwrap();
    let empty_path = empty.path().to_str().unwrap();
    git(empty_path, &["init", "-b", "feature"]);
    let selected = upstream_base_ref(empty_path);
    assert!(git_commit_for_ref(empty_path, &selected).is_none());
}
