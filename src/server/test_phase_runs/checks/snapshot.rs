//! Bounded, complete file manifests for the first small Linux check template.
//! Git metadata is not executable input; every other entry is accounted for.
use super::contract::{Contract, Template, digest};
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
};

pub(crate) fn files(root: &Path) -> Result<BTreeMap<String, Vec<u8>>, String> {
    crate::services::platform::managed_process::inspect_workspace(root)
        .map_err(|e| e.to_string())?;
    read_tree(root)
}

pub(super) fn read_tree(root: &Path) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = BTreeMap::new();
    let mut bytes = 0_u64;
    let mut entries = 0;
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(dir).map_err(|e| e.to_string())? {
            let path = entry.map_err(|e| e.to_string())?.path();
            let relative = path
                .strip_prefix(root)
                .map_err(|e| e.to_string())?
                .to_str()
                .ok_or("non-UTF8 candidate path")?
                .to_owned();
            if relative == ".git" {
                continue;
            }
            // Linux filenames may contain a literal backslash. Never turn one
            // into a separator: that could alias another input or escape the
            // staging tree (for example a file named "..\\outside").
            super::contract::relative(&relative)?;
            entries += 1;
            if entries > 4096 {
                return Err("candidate exceeds 4096 entries".into());
            }
            let meta = fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
            if meta.is_dir() {
                pending.push(path);
                continue;
            }
            if !meta.is_file() {
                return Err("candidate aliases and special files are unsupported".into());
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if meta.nlink() != 1 {
                    return Err("candidate hardlinks are unsupported".into());
                }
            }
            let mut options = fs::OpenOptions::new();
            options.read(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.custom_flags(libc::O_NOFOLLOW);
            }
            let mut contents = Vec::new();
            options
                .open(&path)
                .map_err(|e| e.to_string())?
                .take(32 * 1024 * 1024 + 1 - bytes)
                .read_to_end(&mut contents)
                .map_err(|e| e.to_string())?;
            bytes += contents.len() as u64;
            if bytes > 32 * 1024 * 1024 {
                return Err("candidate exceeds 32 MiB".into());
            }
            files.insert(relative, contents);
        }
    }
    Ok(files)
}

fn manifest(files: &BTreeMap<String, Vec<u8>>) -> BTreeMap<String, String> {
    files
        .iter()
        .map(|(path, contents)| (path.clone(), digest(contents)))
        .collect()
}

pub(crate) fn enroll(template: Template) -> Result<Contract, String> {
    template.validate()?;
    if fs::canonicalize(&template.workspace).map_err(|e| e.to_string())? != template.workspace
        || fs::canonicalize(&template.artifact_root).map_err(|e| e.to_string())?
            != template.artifact_root
    {
        return Err("verification roots must be canonical, non-aliased paths".into());
    }
    protected_directory(&template.artifact_root)?;
    for check in template.checks.values() {
        super::process::validate_check(check)?;
    }
    let baseline = manifest(&files(&template.workspace)?);
    let digest = digest(&serde_json::to_vec(&(&template, &baseline)).map_err(|e| e.to_string())?);
    Ok(Contract {
        template,
        baseline,
        digest,
    })
}

pub(crate) struct Candidate {
    pub digest: String,
    pub path: PathBuf,
}

pub(crate) fn freeze(contract: &Contract) -> Result<Candidate, String> {
    protected_directory(&contract.template.artifact_root)?;
    let inputs = files(&contract.template.workspace)?;
    let current = manifest(&inputs);
    for path in contract.baseline.keys().chain(current.keys()) {
        if contract.baseline.get(path) != current.get(path)
            && !contract.template.allowed_changes.contains(path)
        {
            return Err(format!("candidate changed an unapproved input: {path}"));
        }
    }
    let digest = digest(&serde_json::to_vec(&current).map_err(|e| e.to_string())?);
    let path = contract.template.artifact_root.join(&digest);
    if path.exists() {
        verify(&path, &digest)?;
        return Ok(Candidate { digest, path });
    }
    let staging = tempfile::Builder::new()
        .prefix(".freeze-")
        .tempdir_in(&contract.template.artifact_root)
        .map_err(|e| e.to_string())?;
    for (name, contents) in inputs {
        let target = staging.path().join(name);
        fs::create_dir_all(target.parent().ok_or("invalid candidate file")?)
            .map_err(|e| e.to_string())?;
        fs::write(&target, contents).map_err(|e| e.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(target, fs::Permissions::from_mode(0o444))
                .map_err(|e| e.to_string())?;
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(staging.path(), fs::Permissions::from_mode(0o755))
            .map_err(|e| e.to_string())?;
    }
    // A competing collector may have frozen identical bytes. Verify that tree.
    if let Err(error) = fs::rename(staging.path(), &path) {
        if !path.exists() {
            return Err(error.to_string());
        }
    }
    verify(&path, &digest)?;
    Ok(Candidate { digest, path })
}

pub(crate) fn verify(path: &Path, expected: &str) -> Result<(), String> {
    protected_directory(path)?;
    let current = manifest(&read_tree(path)?);
    for name in current.keys() {
        crate::services::platform::managed_process::protected_file(&path.join(name), false)
            .map_err(|e| e.to_string())?;
    }
    if digest(&serde_json::to_vec(&current).map_err(|e| e.to_string())?) != expected {
        return Err("frozen candidate digest mismatch".into());
    }
    Ok(())
}

pub(crate) fn protected_directory(path: &Path) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        if !path.is_absolute() {
            return Err("artifact root must be absolute".into());
        }
        for parent in path.ancestors() {
            let m = fs::symlink_metadata(parent).map_err(|e| e.to_string())?;
            let sticky = parent != path && m.mode() & libc::S_ISVTX != 0;
            if !m.is_dir() || m.uid() != 0 || (m.mode() & 0o022 != 0 && !sticky) {
                return Err("candidate storage must have protected root-owned directories".into());
            }
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        Err("protected check snapshots require Linux".into())
    }
}
