//! Persist only OS identity, never command lines, environments or credentials.
use super::{RuntimeIdentity, StopObservation};
use crate::services::process::ProcessIdentity;

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use crate::services::process::ProcessIdentityProbe;
    use std::collections::BTreeSet;
    use std::fs;
    use std::os::unix::fs::MetadataExt;

    pub(super) struct Scope {
        boot_id: String,
        pid_namespace: u64,
    }
    impl Scope {
        fn current() -> Result<Self, String> {
            let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")
                .map_err(|_| "kernel boot identity unavailable")?
                .trim()
                .to_owned();
            uuid::Uuid::parse_str(&boot_id).map_err(|_| "invalid kernel boot identity")?;
            let pid_namespace = fs::metadata("/proc/self/ns/pid")
                .map_err(|_| "PID namespace identity unavailable")?
                .ino();
            Ok(Self {
                boot_id,
                pid_namespace,
            })
        }
    }

    fn credentials(pid: u32) -> Result<([u32; 4], bool), String> {
        let status = fs::read_to_string(format!("/proc/{pid}/status"))
            .map_err(|_| "process credentials unavailable")?;
        let ids = status
            .lines()
            .find_map(|line| line.strip_prefix("Uid:"))
            .ok_or("process UID missing")?
            .split_whitespace()
            .map(str::parse::<u32>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "invalid process UID")?;
        let ids: [u32; 4] = ids
            .try_into()
            .map_err(|_| "invalid process UID field count")?;
        let state = status
            .lines()
            .find_map(|line| line.strip_prefix("State:"))
            .and_then(|value| value.trim().chars().next())
            .ok_or("process state missing")?;
        Ok((ids, matches!(state, 'Z' | 'X')))
    }

    pub(crate) fn capture(pid: u32, original: ProcessIdentity) -> Result<RuntimeIdentity, String> {
        let scope = Scope::current()?;
        if original.probe(pid) != ProcessIdentityProbe::Same {
            return Err("managed runtime birth identity cannot be verified".into());
        }
        let (uids, zombie) = credentials(pid)?;
        if uids.iter().any(|uid| *uid != uids[0]) {
            return Err("managed runtime has ambiguous process credentials".into());
        }
        if zombie || original.probe(pid) != ProcessIdentityProbe::Same {
            return Err("managed runtime exited during identity binding".into());
        }
        let identity = RuntimeIdentity {
            version: 1,
            boot_id: scope.boot_id,
            pid_namespace: scope.pid_namespace,
            pid,
            starttime: original
                .persisted_starttime()
                .ok_or("managed runtime starttime unavailable")?
                .to_string(),
            uid: uids[0],
        };
        identity.validate()?;
        Ok(identity)
    }

    pub(crate) struct Snapshot {
        scope: Result<Scope, String>,
        live_uids: Result<BTreeSet<u32>, String>,
    }
    impl Snapshot {
        pub(crate) fn capture() -> Self {
            Self {
                scope: Scope::current(),
                live_uids: live_uids(),
            }
        }

        pub(crate) fn observe(&self, identity: &RuntimeIdentity) -> StopObservation {
            let (Ok(scope), Ok(uids)) = (&self.scope, &self.live_uids) else {
                return StopObservation::new("unobservable", "process_inventory_unavailable");
            };
            if scope.boot_id != identity.boot_id || scope.pid_namespace != identity.pid_namespace {
                // A different namespace/host is not evidence that the old one
                // died. Cross-boot/container recovery needs a separate proof.
                return StopObservation::new("unobservable", "boot_or_pid_namespace_changed");
            }
            let original = ProcessIdentity::from_persisted(identity.starttime.parse().ok(), None);
            match original.probe(identity.pid) {
                ProcessIdentityProbe::ProbeError => {
                    StopObservation::new("unobservable", "process_identity_probe_failed")
                }
                ProcessIdentityProbe::Same
                    if !credentials(identity.pid).is_ok_and(|(_, zombie)| zombie) =>
                {
                    StopObservation::new("alive", "original_runtime_alive")
                }
                _ if uids.contains(&identity.uid) => {
                    StopObservation::new("alive", "dedicated_uid_has_live_processes")
                }
                // /proc is an observation, not an atomic containment boundary.
                // Even an empty scan must not authorize release/refund.
                _ => StopObservation::new("not_observed", "original_exited_and_uid_scan_empty"),
            }
        }
    }

    fn live_uids() -> Result<BTreeSet<u32>, String> {
        let mut uids = BTreeSet::new();
        let entries = fs::read_dir("/proc").map_err(|_| "process inventory unavailable")?;
        for (count, entry) in entries.enumerate() {
            if count >= 65_536 {
                return Err("process inventory exceeds observation bound".into());
            }
            let entry = entry.map_err(|_| "process inventory entry unavailable")?;
            let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
                continue;
            };
            match credentials(pid) {
                Ok((ids, false)) => {
                    uids.extend(ids);
                }
                Ok((_, true)) => {}
                Err(_) if matches!(fs::symlink_metadata(entry.path()), Err(error) if error.kind() == std::io::ErrorKind::NotFound) =>
                    {}
                Err(error) => return Err(error),
            }
        }
        Ok(uids)
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::{Snapshot, capture};

#[cfg(not(target_os = "linux"))]
pub(crate) fn capture(_: u32, _: ProcessIdentity) -> Result<RuntimeIdentity, String> {
    Err("managed runtime identity requires a verified OS observer".into())
}

#[cfg(not(target_os = "linux"))]
pub(crate) struct Snapshot;
#[cfg(not(target_os = "linux"))]
impl Snapshot {
    pub(crate) fn capture() -> Self {
        Self
    }
    pub(crate) fn observe(&self, _: &RuntimeIdentity) -> StopObservation {
        StopObservation::new("unobservable", "unsupported_runtime_observer")
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
