//! Shared bounded native artifact handoff. This does not claim Linux managed containment.
use crate::db::campaigns::graph::{
    Format, Input, MAX_ARTIFACT_BUNDLE_BYTES, MAX_ARTIFACT_FILE_BYTES, Output, Task, digest,
    relative,
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::Write,
    path::{Path, PathBuf},
};

fn file(root: &Path, name: &str) -> Result<PathBuf> {
    relative(name)?;
    let root = root.canonicalize()?;
    let mut path = root.clone();
    for part in name.split('/') {
        path.push(part);
        match std::fs::symlink_metadata(&path) {
            Ok(meta) => ensure!(!meta.file_type().is_symlink(), "symlink in artifact path"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    ensure!(path.starts_with(&root), "artifact escapes workspace");
    Ok(path)
}
pub fn capture(workspace: &Path, task: &Task, root: &Path) -> Result<Value> {
    capture_outputs(workspace, &task.outputs, root)
}
pub fn capture_outputs(workspace: &Path, outputs: &[Output], root: &Path) -> Result<Value> {
    let mut contents = BTreeMap::new();
    let mut total = 0;
    for output in outputs {
        let path = file(workspace, &output.path)?;
        let meta = std::fs::metadata(&path)?;
        ensure!(
            meta.is_file() && meta.len() > 0,
            "artifact {} must be a nonempty regular file",
            output.path
        );
        ensure!(
            meta.len() <= MAX_ARTIFACT_FILE_BYTES,
            "artifact {} has {} bytes; maximum is {} bytes",
            output.path,
            meta.len(),
            MAX_ARTIFACT_FILE_BYTES
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            ensure!(meta.nlink() == 1, "hardlinked output rejected");
        }
        total += meta.len();
        ensure!(
            total <= MAX_ARTIFACT_BUNDLE_BYTES,
            "artifact bundle has {} bytes after {}; maximum is {} bytes",
            total,
            output.path,
            MAX_ARTIFACT_BUNDLE_BYTES
        );
        let bytes = std::fs::read(&path)?;
        ensure!(
            bytes.len() as u64 == meta.len()
                && std::fs::metadata(&path)?.modified()? == meta.modified()?,
            "artifact changed during capture"
        );
        match output.format {
            Format::Json => {
                let _: Value = serde_json::from_slice(&bytes)?;
            }
            Format::Text | Format::Python => {
                std::str::from_utf8(&bytes)?;
            }
        }
        // Python syntax/tests are executed by the explicitly independent verify
        // node, not by eval/import in the Hub process.
        contents.insert(output.path.clone(), bytes);
    }
    let hashes: BTreeMap<_, _> = contents
        .iter()
        .map(|(p, b)| (p.clone(), digest(b)))
        .collect();
    let bundle = digest(&serde_json::to_vec(&hashes)?);
    std::fs::create_dir_all(root)?;
    let dest = root.join(&bundle);
    if !dest.exists() {
        let stage = tempfile::Builder::new()
            .prefix(".candidate-")
            .tempdir_in(root)?;
        for (name, bytes) in &contents {
            let p = stage.path().join(name);
            std::fs::create_dir_all(p.parent().unwrap())?;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(p)?;
            f.write_all(bytes)?;
            f.sync_all()?;
        }
        match std::fs::rename(stage.path(), &dest) {
            Ok(()) => {}
            Err(e) if dest.exists() => {
                let _ = e;
            }
            Err(e) => return Err(e.into()),
        }
    }
    let manifest = json!({"bundle":bundle,"files":hashes});
    verify(root, &manifest)?;
    Ok(manifest)
}
pub fn verify(root: &Path, manifest: &Value) -> Result<()> {
    let bundle = manifest["bundle"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing bundle"))?;
    ensure!(
        bundle.len() == 64 && bundle.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid bundle ID"
    );
    let files = manifest["files"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("missing manifest"))?;
    ensure!(
        digest(&serde_json::to_vec(files)?) == bundle,
        "manifest digest mismatch"
    );
    for (name, hash) in files {
        let p = file(&root.join(bundle), name)?;
        let meta = std::fs::metadata(&p)?;
        ensure!(
            meta.is_file() && meta.len() <= MAX_ARTIFACT_FILE_BYTES,
            "invalid stored artifact"
        );
        ensure!(
            Some(digest(&std::fs::read(p)?).as_str()) == hash.as_str(),
            "stored artifact changed"
        );
    }
    Ok(())
}
pub fn materialize(
    root: &Path,
    workspace: &Path,
    task: &Task,
    manifests: &BTreeMap<String, Value>,
) -> Result<Value> {
    materialize_inputs(root, workspace, &task.inputs, manifests)
}
pub fn materialize_inputs(
    root: &Path,
    workspace: &Path,
    inputs: &[Input],
    manifests: &BTreeMap<String, Value>,
) -> Result<Value> {
    let mut result = BTreeMap::new();
    for input in inputs {
        let m = manifests
            .get(&input.dependency)
            .ok_or_else(|| anyhow::anyhow!("missing predecessor artifact"))?;
        verify(root, m)?;
        let hash = m["files"][&input.source]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("undeclared predecessor output"))?;
        let bytes = std::fs::read(file(
            &root.join(m["bundle"].as_str().unwrap()),
            &input.source,
        )?)?;
        ensure!(digest(&bytes) == hash, "dependency changed");
        let dst = file(workspace, &input.destination)?;
        if dst.exists() {
            ensure!(
                digest(&std::fs::read(&dst)?) == hash,
                "input conflicts with existing work"
            );
        } else {
            std::fs::create_dir_all(dst.parent().unwrap())?;
            let mut tmp = tempfile::NamedTempFile::new_in(dst.parent().unwrap())?;
            tmp.write_all(&bytes)?;
            tmp.as_file().sync_all()?;
            tmp.persist_noclobber(&dst).map_err(|e| e.error)?;
        }
        result.insert(
            input.destination.clone(),
            json!({"sha256":hash,"producer":input.dependency,"source":input.source}),
        );
    }
    Ok(serde_json::to_value(result)?)
}

/// Recheck pinned inputs after the provider has stopped, before accepting output.
/// Never repair a modified input here: doing so would hide what the task used.
pub fn verify_inputs(
    root: &Path,
    workspace: &Path,
    task: &Task,
    manifests: &BTreeMap<String, Value>,
) -> Result<()> {
    verify_input_files(root, workspace, &task.inputs, manifests)
}
pub fn verify_input_files(
    root: &Path,
    workspace: &Path,
    inputs: &[Input],
    manifests: &BTreeMap<String, Value>,
) -> Result<()> {
    for input in inputs {
        let manifest = manifests
            .get(&input.dependency)
            .ok_or_else(|| anyhow::anyhow!("missing pinned input manifest"))?;
        verify(root, manifest)?;
        let path = file(workspace, &input.destination)?;
        let meta = std::fs::metadata(&path)?;
        ensure!(
            meta.is_file() && meta.len() <= MAX_ARTIFACT_FILE_BYTES,
            "invalid input file"
        );
        ensure!(
            Some(digest(&std::fs::read(path)?).as_str())
                == manifest["files"][&input.source].as_str(),
            "task modified its pinned input"
        );
    }
    Ok(())
}
