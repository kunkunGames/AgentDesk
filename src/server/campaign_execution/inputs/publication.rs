//! Crash-safe input publication, with private staging outside the agent tree.
use std::{collections::BTreeMap, path::Path};

#[cfg(target_os = "linux")]
pub(super) fn install(
    root: &Path,
    artifacts: &Path,
    inputs: &BTreeMap<String, Vec<u8>>,
) -> anyhow::Result<()> {
    use crate::{
        server::test_phase_runs::checks::snapshot,
        services::{managed_checks::contract, platform::managed_process},
    };
    use std::{
        fs,
        io::Write,
        os::{
            fd::AsRawFd,
            unix::{
                ffi::OsStrExt,
                fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
            },
        },
    };
    let identity = managed_process::inspect_workspace(root)?;
    let existing = snapshot::files(root).map_err(anyhow::Error::msg)?;
    if inputs.is_empty() {
        return Ok(());
    }
    snapshot::protected_directory(artifacts).map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        fs::metadata(root)?.dev() == fs::metadata(artifacts)?.dev(),
        "atomic campaign inputs require workspace and protected artifacts on the same filesystem"
    );
    let staging = artifacts.join(".campaign-inputs");
    match fs::create_dir(&staging) {
        Ok(()) => fs::set_permissions(&staging, fs::Permissions::from_mode(0o700))?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    snapshot::protected_directory(&staging).map_err(anyhow::Error::msg)?;
    // Recover a crash immediately after mkdir, before its private mode was set.
    fs::set_permissions(&staging, fs::Permissions::from_mode(0o700))?;
    let stage_name = |kind: &str, path: &Path| {
        staging.join(format!(
            "{kind}-{}",
            contract::digest(path.as_os_str().as_bytes())
        ))
    };
    let publish = |source: &Path, target: &Path| -> anyhow::Result<()> {
        let source_name = std::ffi::CString::new(source.as_os_str().as_bytes())?;
        let target_name = std::ffi::CString::new(target.as_os_str().as_bytes())?;
        // NOREPLACE is the commit point. No named partial file or hardlink is
        // ever visible in the workspace, even if the server dies immediately.
        anyhow::ensure!(
            unsafe {
                libc::renameat2(
                    libc::AT_FDCWD,
                    source_name.as_ptr(),
                    libc::AT_FDCWD,
                    target_name.as_ptr(),
                    libc::RENAME_NOREPLACE,
                )
            } == 0,
            "atomic input publication failed: {}",
            std::io::Error::last_os_error()
        );
        crate::services::discord::runtime_store::fsync_parent_dir(source)?;
        Ok(())
    };
    for (relative, bytes) in inputs {
        contract::relative(relative).map_err(anyhow::Error::msg)?;
        if let Some(previous) = existing.get(relative) {
            anyhow::ensure!(
                previous == bytes,
                "existing input differs from verified dependency: {relative}"
            );
            continue;
        }
        let target = root.join(relative);
        let parent = target
            .parent()
            .ok_or_else(|| anyhow::anyhow!("input needs a parent"))?;
        let mut path = root.to_owned();
        for part in parent.strip_prefix(root)?.components() {
            path.push(part);
            match fs::symlink_metadata(&path) {
                Ok(meta) => anyhow::ensure!(meta.is_dir(), "input parent is not a directory"),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    let staged = stage_name("dir", &path);
                    match fs::create_dir(&staged) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                        Err(e) => return Err(e.into()),
                    }
                    anyhow::ensure!(
                        fs::symlink_metadata(&staged)?.is_dir()
                            && fs::read_dir(&staged)?.next().is_none(),
                        "staged input directory must be an empty non-aliased directory"
                    );
                    fs::set_permissions(&staged, fs::Permissions::from_mode(0o700))?;
                    let raw = std::ffi::CString::new(staged.as_os_str().as_bytes())?;
                    anyhow::ensure!(
                        unsafe { libc::chown(raw.as_ptr(), identity.uid, identity.gid) } == 0,
                        "cannot set input directory ownership"
                    );
                    publish(&staged, &path)?;
                    crate::services::discord::runtime_store::fsync_parent_dir(&path)?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        let staged = stage_name("file", &target);
        // Deterministic names let a subsequent owner recover interrupted private
        // staging without scanning or deleting unrelated files.
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&staged)?;
        let meta = file.metadata()?;
        anyhow::ensure!(
            meta.is_file() && meta.nlink() == 1,
            "invalid staged input file"
        );
        file.set_len(0)?;
        file.write_all(bytes)?;
        anyhow::ensure!(
            unsafe { libc::fchown(file.as_raw_fd(), identity.uid, identity.gid) } == 0,
            "cannot set input file ownership"
        );
        file.sync_all()?;
        publish(&staged, &target)?;
        crate::services::discord::runtime_store::fsync_parent_dir(&target)?;
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(super) fn install(_: &Path, _: &Path, _: &BTreeMap<String, Vec<u8>>) -> anyhow::Result<()> {
    anyhow::bail!("protected campaign inputs require the managed Linux launcher")
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::{server::test_phase_runs::checks, services::managed_checks::contract};
    use std::{fs, os::unix::ffi::OsStrExt};

    #[test]
    #[ignore = "requires a disposable Linux root filesystem fixture"]
    fn interrupted_private_staging_recovers_without_replacing_conflicting_input() {
        let (_root, contract) = checks::native_tests::fixture("", contract::Review::None);
        let workspace = &contract.template.workspace;
        let artifacts = &contract.template.artifact_root;
        let staging = artifacts.join(".campaign-inputs");
        fs::create_dir(&staging).unwrap();
        let target = workspace.join("input/a.txt");
        let partial = staging.join(format!(
            "file-{}",
            contract::digest(target.as_os_str().as_bytes())
        ));
        fs::write(&partial, "interrupted write").unwrap();
        let inputs = BTreeMap::from([("input/a.txt".to_string(), b"42".to_vec())]);
        install(workspace, artifacts, &inputs).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"42");
        assert!(!partial.exists());
        install(workspace, artifacts, &inputs).unwrap();
        crate::services::platform::managed_process::inspect_workspace(workspace).unwrap();
        fs::write(&target, "conflict").unwrap();
        assert!(install(workspace, artifacts, &inputs).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"conflict");
    }
}
