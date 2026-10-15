//! Bounded, cycle-safe metadata scans run on the blocking pool.
use anyhow::{Result, ensure};
use std::{
    collections::{BTreeSet, HashSet},
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant, UNIX_EPOCH},
};

const MAX_ENTRIES: usize = 100_000;
const MAX_DEPTH: usize = 64;
const MAX_DURATION: Duration = Duration::from_secs(5);

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum LinkState {
    Dangling,
    Resolved,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Record {
    Markdown {
        path: PathBuf,
        len: u64,
        modified: u128,
    },
    Symlink {
        path: PathBuf,
        target: PathBuf,
        state: LinkState,
    },
}

/// Missing targets are a stable link state, not a failed directory scan. All
/// other metadata/read-link failures retain their normal error semantics.
fn follow_link(
    path: &Path,
    meta: fs::Metadata,
    records: &mut BTreeSet<Record>,
) -> Result<Option<fs::Metadata>> {
    if !meta.file_type().is_symlink() {
        return Ok(Some(meta));
    }
    let target = fs::read_link(path)?;
    let (state, resolved) = match fs::metadata(path) {
        Ok(meta) => (LinkState::Resolved, Some(meta)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (LinkState::Dangling, None),
        Err(error) => return Err(error.into()),
    };
    records.insert(Record::Symlink {
        path: path.to_owned(),
        target,
        state,
    });
    Ok(resolved)
}

fn hash_path(hash: &mut blake3::Hasher, path: &Path) {
    let bytes = path.as_os_str().as_encoded_bytes();
    hash.update(&(bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
}

pub(super) fn scan(roots: Vec<PathBuf>) -> Result<(usize, u64)> {
    scan_with_limits(roots, MAX_ENTRIES, MAX_DEPTH, MAX_DURATION)
}

fn scan_with_limits(
    roots: Vec<PathBuf>,
    entry_limit: usize,
    depth_limit: usize,
    time_limit: Duration,
) -> Result<(usize, u64)> {
    let started = Instant::now();
    let mut pending: Vec<_> = roots.into_iter().map(|p| (p, 0)).collect();
    let mut visited = HashSet::new();
    let mut records = BTreeSet::new();
    let mut entries_seen = 0;
    while let Some((path, depth)) = pending.pop() {
        ensure!(
            started.elapsed() <= time_limit,
            "skill scan exceeded time budget"
        );
        let meta = match fs::symlink_metadata(&path) {
            Ok(meta) => meta,
            Err(e) if depth == 0 && e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        if follow_link(&path, meta, &mut records)?.is_none() {
            continue;
        }
        let canonical = fs::canonicalize(&path)?;
        if !visited.insert(canonical.clone()) {
            continue;
        }
        ensure!(depth <= depth_limit, "skill scan exceeded depth budget");
        let entries = fs::read_dir(&canonical)?;
        for entry in entries {
            entries_seen += 1;
            ensure!(
                entries_seen <= entry_limit,
                "skill scan exceeded entry budget"
            );
            ensure!(
                started.elapsed() <= time_limit,
                "skill scan exceeded time budget"
            );
            let entry = entry?;
            let path = entry.path();
            // Follow intentional shared-skill links once, including directory
            // links, without allowing a link cycle to recurse indefinitely.
            let Some(meta) = follow_link(&path, fs::symlink_metadata(&path)?, &mut records)? else {
                continue;
            };
            if meta.is_dir() {
                pending.push((path, depth + 1));
            } else if path.extension().is_some_and(|e| e == "md") {
                let modified = meta.modified()?.duration_since(UNIX_EPOCH)?.as_nanos();
                records.insert(Record::Markdown {
                    path,
                    len: meta.len(),
                    modified,
                });
            }
        }
    }
    let mut hash = blake3::Hasher::new();
    let mut files = 0;
    for record in &records {
        match record {
            Record::Markdown {
                path,
                len,
                modified,
            } => {
                files += 1;
                hash.update(b"markdown\0");
                hash_path(&mut hash, path);
                hash.update(&len.to_le_bytes());
                hash.update(&modified.to_le_bytes());
            }
            Record::Symlink {
                path,
                target,
                state,
            } => {
                hash.update(b"symlink\0");
                hash_path(&mut hash, path);
                hash_path(&mut hash, target);
                hash.update(match state {
                    LinkState::Dangling => b"dangling\0",
                    LinkState::Resolved => b"resolved\0",
                });
            }
        }
    }
    let bytes = hash.finalize();
    Ok((files, u64::from_le_bytes(bytes.as_bytes()[..8].try_into()?)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn edits_below_max_mtime_and_renames_change_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.md");
        let b = dir.path().join("b.md");
        fs::write(&a, "a").unwrap();
        fs::write(&b, "b").unwrap();
        filetime::set_file_mtime(&a, filetime::FileTime::from_unix_time(1, 0)).unwrap();
        filetime::set_file_mtime(&b, filetime::FileTime::from_unix_time(10, 0)).unwrap();
        let roots = vec![dir.path().to_owned()];
        let before = scan(roots.clone()).unwrap();
        filetime::set_file_mtime(&a, filetime::FileTime::from_unix_time(2, 0)).unwrap();
        let edited = scan(roots.clone()).unwrap();
        assert_ne!(before, edited);
        fs::rename(a, dir.path().join("c.md")).unwrap();
        assert_ne!(edited, scan(roots).unwrap());
    }
    #[cfg(unix)]
    #[test]
    fn shared_directory_links_are_counted_once_and_cycles_terminate() {
        let dir = tempfile::tempdir().unwrap();
        let skill = dir.path().join("skill");
        fs::create_dir(&skill).unwrap();
        fs::write(skill.join("SKILL.md"), "linked skill").unwrap();
        std::os::unix::fs::symlink(dir.path(), skill.join("cycle")).unwrap();
        std::os::unix::fs::symlink(&skill, dir.path().join("alias")).unwrap();
        let first = scan(vec![dir.path().to_owned(), skill.clone()]).unwrap();
        assert_eq!(first.0, 1);
        assert_eq!(
            first,
            scan(vec![skill, dir.path().to_owned(), dir.path().to_owned()]).unwrap()
        );
    }

    #[cfg(unix)]
    #[test]
    fn dangling_links_preserve_live_edits_and_detect_target_repair_and_removal() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("skills");
        let target = dir.path().join("missing-skill");
        fs::create_dir(&root).unwrap();
        let live = root.join("live.md");
        fs::write(&live, "live").unwrap();
        filetime::set_file_mtime(&live, filetime::FileTime::from_unix_time(10, 0)).unwrap();
        let link = root.join("backup");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let roots = vec![root.clone()];
        let broken = scan(roots.clone()).unwrap();
        assert_eq!(broken.0, 1, "dangling links are not usable Markdown files");
        assert_eq!(broken, scan(vec![root.clone(), root.clone()]).unwrap());

        fs::write(&live, "edit").unwrap();
        filetime::set_file_mtime(&live, filetime::FileTime::from_unix_time(11, 0)).unwrap();
        let edited = scan(roots.clone()).unwrap();
        assert_eq!(edited.0, 1);
        assert_ne!(broken, edited, "a dangling link must not hide a live edit");

        fs::create_dir(&target).unwrap();
        fs::write(target.join("SKILL.md"), "repaired skill").unwrap();
        let repaired = scan(roots.clone()).unwrap();
        assert_eq!(repaired.0, 2);
        assert_ne!(edited, repaired);
        assert_eq!(repaired, scan(roots.clone()).unwrap());

        fs::remove_file(target.join("SKILL.md")).unwrap();
        fs::remove_dir(&target).unwrap();
        assert_eq!(edited, scan(roots.clone()).unwrap());
        fs::remove_file(&link).unwrap();
        let removed = scan(roots).unwrap();
        assert_eq!(removed.0, 1);
        assert_ne!(
            edited, removed,
            "removing a dangling link changes its state"
        );
    }

    #[cfg(unix)]
    #[test]
    fn retargeting_dangling_and_live_file_links_changes_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("skills");
        fs::create_dir(&root).unwrap();
        let link = root.join("linked.md");
        let target_a = dir.path().join("a.md");
        let target_b = dir.path().join("b.md");
        std::os::unix::fs::symlink(&target_a, &link).unwrap();
        let roots = vec![root];
        let first = scan(roots.clone()).unwrap();
        assert_eq!(first.0, 0);
        fs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(&target_b, &link).unwrap();
        let retargeted = scan(roots.clone()).unwrap();
        assert_eq!(retargeted.0, 0);
        assert_ne!(first, retargeted);

        for target in [&target_a, &target_b] {
            fs::write(target, "same skill").unwrap();
            filetime::set_file_mtime(target, filetime::FileTime::from_unix_time(10, 0)).unwrap();
        }
        let live_b = scan(roots.clone()).unwrap();
        assert_eq!(live_b.0, 1);
        assert_ne!(retargeted, live_b);
        fs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(&target_a, &link).unwrap();
        assert_ne!(
            live_b,
            scan(roots).unwrap(),
            "equal file metadata is not an equal link target"
        );
    }

    #[cfg(unix)]
    #[test]
    fn missing_roots_and_dangling_root_links_have_distinct_stable_fingerprints() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        let link = dir.path().join("root-link");
        let absent = scan(vec![missing.clone()]).unwrap();
        std::os::unix::fs::symlink(&missing, &link).unwrap();
        let dangling = scan(vec![link.clone()]).unwrap();
        assert_eq!(dangling.0, 0);
        assert_ne!(absent, dangling);
        assert_eq!(
            dangling,
            scan(vec![link.clone(), missing.clone(), link.clone()]).unwrap()
        );

        fs::create_dir(&missing).unwrap();
        fs::write(missing.join("SKILL.md"), "now available").unwrap();
        let repaired = scan(vec![link.clone()]).unwrap();
        assert_eq!(repaired.0, 1);
        assert_ne!(dangling, repaired);
        assert_eq!(repaired, scan(vec![missing.clone(), link.clone()]).unwrap());

        fs::remove_file(missing.join("SKILL.md")).unwrap();
        fs::remove_dir(&missing).unwrap();
        assert_eq!(dangling, scan(vec![link.clone()]).unwrap());
        fs::remove_file(link).unwrap();
        assert_eq!(absent, scan(vec![missing]).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn symlink_resolution_io_errors_are_not_mistaken_for_missing_targets() {
        let dir = tempfile::tempdir().unwrap();
        let regular = dir.path().join("regular-file");
        fs::write(&regular, "not a directory").unwrap();
        std::os::unix::fs::symlink(regular.join("child"), dir.path().join("invalid")).unwrap();
        let error = scan(vec![dir.path().to_owned()]).unwrap_err();
        let io = error.downcast_ref::<std::io::Error>().unwrap();
        assert_ne!(io.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn scan_budgets_still_reject_incomplete_traversal() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("one.md"), "one").unwrap();
        fs::write(dir.path().join("two.md"), "two").unwrap();
        let roots = vec![dir.path().to_owned()];
        let error = scan_with_limits(roots.clone(), 1, MAX_DEPTH, MAX_DURATION).unwrap_err();
        assert!(error.to_string().contains("entry budget"));

        let nested = dir.path().join("nested");
        fs::create_dir(&nested).unwrap();
        fs::write(nested.join("SKILL.md"), "nested").unwrap();
        let error = scan_with_limits(roots.clone(), MAX_ENTRIES, 0, MAX_DURATION).unwrap_err();
        assert!(error.to_string().contains("depth budget"));
        let error = scan_with_limits(roots, MAX_ENTRIES, MAX_DEPTH, Duration::ZERO).unwrap_err();
        assert!(error.to_string().contains("time budget"));
    }
}
