use super::*;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::process::Stdio;

fn owned(path: &Path) {
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::chown(path.as_ptr(), 65534, 65534) }, 0);
}

fn workspace(root: &Path) -> std::path::PathBuf {
    fs::set_permissions(root, fs::Permissions::from_mode(0o755)).unwrap();
    let workspace = root.join("workspace");
    fs::create_dir(&workspace).unwrap();
    fs::set_permissions(&workspace, fs::Permissions::from_mode(0o700)).unwrap();
    owned(&workspace);
    workspace
}

#[test]
#[ignore = "requires disposable Linux root launcher, Python3 and Landlock ABI >= 3"]
fn kernel_denies_alias_proc_exec_and_inherited_fd_but_allows_workspace_edits() {
    let root = tempfile::tempdir().unwrap();
    let workspace = workspace(root.path());
    let allowed = workspace.join("allowed.txt");
    fs::write(&allowed, "before").unwrap();
    owned(&allowed);
    let outside = root.path().join("outside.txt");
    fs::write(&outside, "outside").unwrap();
    // DAC alone would permit access: the Landlock rule must stop this alias.
    fs::set_permissions(&outside, fs::Permissions::from_mode(0o666)).unwrap();
    symlink(&outside, workspace.join("alias.txt")).unwrap();
    symlink("/proc/self/environ", workspace.join("environment.txt")).unwrap();
    let leaked = File::open(&outside).unwrap();
    assert_eq!(
        unsafe { libc::fcntl(leaked.as_raw_fd(), libc::F_SETFD, 0) },
        0
    );
    let binary = fs::canonicalize("/usr/bin/python3").unwrap();
    let boundary = FilesystemBoundary::prepare(&binary, &workspace).unwrap();
    let runtime_path = boundary.runtime_home().to_owned();
    let mut command = Command::new(&binary);
    command
        .env_clear()
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env(
            "SYNTHETIC_PROVIDER_TOKEN",
            "fixture-secret-not-readable-from-proc",
        )
        .current_dir(&workspace)
        .stdin(Stdio::null())
        .arg("-c")
        .arg(
            r#"
import errno, os, pathlib, subprocess, sys
root = pathlib.Path('.')
assert os.getuid() == os.geteuid() == 65534
assert os.getgroups() == []
assert root.joinpath('allowed.txt').read_text() == 'before'
root.joinpath('allowed.txt').write_text('after')
pathlib.Path(sys.argv[2], 'runtime.txt').write_text('runtime-ok')
def denied(action, expected=(errno.EACCES, errno.EPERM, errno.EBADF)):
    try: action()
    except OSError as error:
        assert error.errno in expected, error
    else: raise AssertionError('boundary was bypassed')
for alias in ('alias.txt', 'environment.txt'):
    denied(lambda: root.joinpath(alias).read_bytes())
denied(lambda: root.joinpath('alias.txt').write_text('escaped'))
denied(lambda: os.link(sys.argv[3], 'hardlink.txt'), (errno.EXDEV, errno.EACCES, errno.EPERM))
denied(lambda: os.read(int(sys.argv[1]), 16))
denied(lambda: subprocess.run(['/bin/sh', '-c', 'true'], check=True))
print('filesystem-boundary-ok')
"#,
        )
        .arg(leaked.as_raw_fd().to_string())
        .arg(&runtime_path)
        .arg(&outside);
    let runtime = boundary.configure(&mut command).unwrap();
    let result = command.output().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&result.stdout).trim(),
        "filesystem-boundary-ok"
    );
    assert_eq!(fs::read_to_string(&outside).unwrap(), "outside");
    assert_eq!(fs::read_to_string(&allowed).unwrap(), "after");
    assert_eq!(
        fs::read_to_string(runtime.path().join("runtime.txt")).unwrap(),
        "runtime-ok"
    );
}

#[test]
#[ignore = "requires disposable Linux root launcher"]
fn admission_rejects_preexisting_hardlinks_and_service_owned_workspaces() {
    let root = tempfile::tempdir().unwrap();
    assert!(inspect_workspace(root.path()).is_err());
    let workspace = workspace(root.path());
    let source = root.path().join("shared.txt");
    fs::write(&source, "shared").unwrap();
    owned(&source);
    fs::hard_link(&source, workspace.join("alias.txt")).unwrap();
    let error = inspect_workspace(&workspace).unwrap_err();
    assert!(error.to_string().contains("hardlinks"), "{error}");
}

#[test]
#[ignore = "requires disposable Linux root launcher"]
fn protected_config_rejects_aliases_and_writable_control_files() {
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("config.json");
    fs::write(&config, "{}").unwrap();
    fs::set_permissions(&config, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(read_protected_config(&config).unwrap(), "{}");
    let alias = root.path().join("alias.json");
    symlink(&config, &alias).unwrap();
    assert!(read_protected_config(&alias).is_err());
    fs::set_permissions(&config, fs::Permissions::from_mode(0o666)).unwrap();
    assert!(read_protected_config(&config).is_err());
    fs::set_permissions(&config, fs::Permissions::from_mode(0o600)).unwrap();
    fs::hard_link(&config, root.path().join("hardlink.json")).unwrap();
    assert!(read_protected_config(&config).is_err());
}
