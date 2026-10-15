//! Conservative orphan cleanup under the exact session source authority.
use crate::services::discord::input_runtime::fence::{BootTarget, boot_skip};

pub(super) fn source_authority_stem_for_orphan_file(filename: &str) -> &str {
    [
        ".codex-tui-rollout.json",
        ".claude-tui-settings.json",
        ".claude-tui.sh",
    ]
    .into_iter()
    .find_map(|suffix| filename.strip_suffix(suffix))
    .or_else(|| filename.rsplit_once('.').map(|(stem, _)| stem))
    .unwrap_or(filename)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OrphanFileIdentity {
    modified: std::time::SystemTime,
    length: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    changed_seconds: i64,
    #[cfg(unix)]
    changed_nanos: i64,
}

impl OrphanFileIdentity {
    fn capture(path: &std::path::Path) -> std::io::Result<Self> {
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.is_file() {
            return Err(std::io::Error::other("orphan source is not a regular file"));
        }
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(Self {
            modified: metadata.modified()?,
            length: metadata.len(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(unix)]
            changed_seconds: metadata.ctime(),
            #[cfg(unix)]
            changed_nanos: metadata.ctime_nsec(),
        })
    }
}

pub(super) type OrphanGroupSnapshot =
    std::collections::BTreeMap<std::path::PathBuf, OrphanFileIdentity>;

pub(super) fn capture_orphan_group(
    dir: &std::path::Path,
    stem: &str,
) -> std::io::Result<OrphanGroupSnapshot> {
    let mut snapshot = OrphanGroupSnapshot::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let Some(filename) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if source_authority_stem_for_orphan_file(&filename) == stem {
            let path = entry.path();
            snapshot.insert(path.clone(), OrphanFileIdentity::capture(&path)?);
        }
    }
    Ok(snapshot)
}

/// The caller holds this exact stem's source authority. A new/replaced file,
/// current native registry, unknown evidence, or durable native intent forbids
/// deleting a group judged earlier. None of these facts infer OS process death.
pub(super) fn remove_judged_orphan_group_under_source_authority(
    dir: &std::path::Path,
    stem: &str,
    judged: &OrphanGroupSnapshot,
    minimum_age: std::time::Duration,
) -> bool {
    use crate::services::session_backend::{
        NativeRegistryPresence, native_registry_presence_for_source_stem,
    };
    if boot_skip(BootTarget::Unknown) {
        return false;
    }
    let ext = crate::services::tmux_common::TMUX_CHANNEL_TEMP_EXT;
    let channel = std::fs::read_to_string(dir.join(format!("{stem}.{ext}")))
        .ok()
        .and_then(|id| id.trim().parse().ok());
    let target = channel
        .filter(|id| *id != 0)
        .map_or(BootTarget::Unknown, BootTarget::AnyProvider);
    if boot_skip(target) {
        return false;
    }
    if native_registry_presence_for_source_stem(stem) != NativeRegistryPresence::Absent {
        return false;
    }
    let marker = dir.join(format!(
        "{stem}.{}",
        crate::services::tmux_common::TMUX_RUNTIME_KIND_TEMP_EXT
    ));
    if crate::services::tmux_common::hosting_intent_at_path_requires_preservation(&marker) {
        return false;
    }
    let Ok(current) = capture_orphan_group(dir, stem) else {
        return false;
    };
    if current.is_empty() || current != *judged {
        return false;
    }
    let now = std::time::SystemTime::now();
    if current
        .values()
        .any(|file| now.duration_since(file.modified).unwrap_or_default() < minimum_age)
    {
        return false;
    }
    let mut removed = false;
    for (path, identity) in current {
        // Keep a final file fence as well as the group fence. An unowned write
        // outside source authority is uncertainty, never a stale-delete permit.
        if OrphanFileIdentity::capture(&path).ok().as_ref() != Some(&identity) {
            return false;
        }
        if std::fs::remove_file(path).is_err() {
            return false;
        }
        removed = true;
    }
    removed
}

/// Remove jsonl/input/prompt/owner/etc files in the persistent sessions
/// directory that no longer belong to a running tmux session. Conservative:
/// require an owner marker (or the jsonl) to be older than
/// `ORPHAN_MIN_AGE_SECS` and require the session to be absent from tmux
/// before deleting. Legacy `/tmp/` files are *never* swept at startup —
/// pre-migration wrappers may still be writing into them.
pub(in crate::services::discord) async fn sweep_orphan_session_files() {
    if boot_skip(BootTarget::Unknown) {
        return;
    }

    let Some(dir) = crate::services::tmux_common::persistent_sessions_dir() else {
        return;
    };
    if !dir.exists() {
        return;
    }

    // List live tmux sessions.
    let live: std::collections::HashSet<String> = match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::task::spawn_blocking(crate::services::platform::tmux::list_session_names),
    )
    .await
    {
        Ok(Ok(Ok(names))) => names.into_iter().collect(),
        _ => return, // tmux unavailable — skip sweep rather than risk false positives
    };

    sweep_orphan_session_files_in(&dir, &live);
}

pub(super) fn sweep_orphan_session_files_in(
    dir: &std::path::Path,
    live: &std::collections::HashSet<String>,
) {
    if boot_skip(BootTarget::Unknown) {
        return;
    }
    #[cfg(test)]
    super::input_fence_tests::scan_started();
    const ORPHAN_MIN_AGE_SECS: u64 = 10 * 60;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    // Group files under the sessions dir by the `agentdesk-<hash>-<host>-<session>`
    // prefix. Any prefix whose session name is not in `live` *and* whose
    // newest file mtime is older than ORPHAN_MIN_AGE_SECS is considered.
    let mut groups = std::collections::HashSet::new();
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if !name.starts_with("agentdesk-") {
            continue;
        }
        // Strip the complete extension, including the multi-dot source marker.
        let stem = source_authority_stem_for_orphan_file(&name).to_string();
        // Session name is the last token after the fourth dash — but our
        // prefix format is `agentdesk-<12hex>-<host>-<session>` and host
        // may contain dashes. The simplest robust approach: split_once on
        // `agentdesk-<hash>-<host>-` is hard to reverse, so instead we use
        // the owner file's prefix as the grouping key directly — any file
        // whose stem matches some live session (ends with `-<live>`) is kept.
        groups.insert(stem);
    }

    let now = std::time::SystemTime::now();
    let mut swept = 0usize;
    for stem in groups {
        // Is this stem associated with any live tmux session? We check
        // whether ANY live session name appears as a suffix of the stem.
        // Since session names are distinctive (provider:channel shape), a
        // conservative suffix match keeps ambiguity low; we also require
        // that the match is preceded by a dash so we don't match e.g.
        // "claude:foo" against a stem ending with "-thisisnotclaude:foo".
        let is_live = live.iter().any(|live_name| {
            let needle = format!("-{}", live_name);
            stem.ends_with(&needle) || stem == *live_name
        });
        if is_live {
            continue;
        }
        let Ok(judged) = capture_orphan_group(&dir, &stem) else {
            continue;
        };
        let minimum_age = std::time::Duration::from_secs(ORPHAN_MIN_AGE_SECS);
        if judged.is_empty()
            || judged
                .values()
                .any(|file| now.duration_since(file.modified).unwrap_or_default() < minimum_age)
        {
            continue;
        }
        let removed =
            crate::services::tmux_common::with_session_temp_source_authority(&stem, || {
                remove_judged_orphan_group_under_source_authority(&dir, &stem, &judged, minimum_age)
            });
        swept += usize::from(removed);
    }
    if swept > 0 {
        let ts = chrono::Local::now().format("%H:%M:%S");
        tracing::info!(
            "  [{ts}] 🧹 Swept {} orphan session file group(s) from {}",
            swept,
            dir.display()
        );
    }
}
