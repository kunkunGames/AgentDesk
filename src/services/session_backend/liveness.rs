//! Evidence for maintenance policies. A missing registry entry is not OS death.

use super::{PROCESS_SESSIONS, SessionHandle};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum ProcessSessionLiveness {
    Live { reason: &'static str },
    Exited { reason: &'static str },
    Unknown { reason: &'static str },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeRegistryPresence {
    Registered,
    Absent,
    Unknown,
}

pub(crate) struct ProcessSessionEvidence {
    pub(crate) registration: NativeRegistryPresence,
    pub(crate) liveness: ProcessSessionLiveness,
}

/// Startup file sweeps own the canonical source stem rather than a parsed
/// session name. Never guess a name across runtime/host prefixes or inspect a
/// candidate PID: match only current registered names to that exact authority.
pub(crate) fn native_registry_presence_for_source_stem(stem: &str) -> NativeRegistryPresence {
    let registry = match PROCESS_SESSIONS.try_lock() {
        Ok(registry) => registry,
        Err(_) => return NativeRegistryPresence::Unknown,
    };
    if registry
        .handles
        .keys()
        .any(|name| crate::services::tmux_common::session_temp_prefix(name.trim()) == stem)
    {
        NativeRegistryPresence::Registered
    } else {
        NativeRegistryPresence::Absent
    }
}

#[cfg(test)]
pub(crate) fn with_process_registry_busy_for_test<R>(operation: impl FnOnce() -> R) -> R {
    let _registry = super::process_sessions();
    operation()
}

pub(crate) fn process_session_liveness(session_name: &str) -> ProcessSessionLiveness {
    process_session_evidence(session_name).liveness
}

pub(crate) fn process_session_evidence(session_name: &str) -> ProcessSessionEvidence {
    process_session_evidence_in_registry(&PROCESS_SESSIONS, session_name)
}

fn process_session_evidence_in_registry(
    registry: &std::sync::Mutex<super::ProcessSessionRegistry>,
    session_name: &str,
) -> ProcessSessionEvidence {
    let registry = match registry.try_lock() {
        Ok(registry) => registry,
        Err(std::sync::TryLockError::Poisoned(_)) => {
            return ProcessSessionEvidence {
                registration: NativeRegistryPresence::Unknown,
                liveness: ProcessSessionLiveness::Unknown {
                    reason: "registry_lock_poisoned",
                },
            };
        }
        Err(std::sync::TryLockError::WouldBlock) => {
            return ProcessSessionEvidence {
                registration: NativeRegistryPresence::Unknown,
                liveness: ProcessSessionLiveness::Unknown {
                    reason: "registry_busy",
                },
            };
        }
    };
    let Some(entry) = registry.handles.get(session_name) else {
        // After a Hub restart this registry is empty while an old wrapper may
        // still be present. Neither absence nor a stopped name proves identity.
        return ProcessSessionEvidence {
            registration: NativeRegistryPresence::Absent,
            liveness: ProcessSessionLiveness::Unknown {
                reason: "registry_entry_absent",
            },
        };
    };
    let liveness = match &entry.handle {
        SessionHandle::Process { child, pid, .. } => {
            let Ok(mut child) = child.try_lock() else {
                return ProcessSessionEvidence {
                    registration: NativeRegistryPresence::Registered,
                    liveness: ProcessSessionLiveness::Unknown {
                        reason: "child_handle_busy",
                    },
                };
            };
            let Some(child) = child.as_mut() else {
                return ProcessSessionEvidence {
                    registration: NativeRegistryPresence::Registered,
                    liveness: ProcessSessionLiveness::Unknown {
                        reason: "child_handle_unavailable",
                    },
                };
            };
            match child.try_wait() {
                Ok(Some(_)) => ProcessSessionLiveness::Exited {
                    reason: "owned_child_exit_observed",
                },
                Ok(None) => {
                    #[cfg(unix)]
                    {
                        use crate::services::process::ProcessIdentityProbe;
                        match entry.identity.probe(*pid) {
                            ProcessIdentityProbe::Same => ProcessSessionLiveness::Live {
                                reason: "owned_child_identity_verified",
                            },
                            ProcessIdentityProbe::GoneOrReused => ProcessSessionLiveness::Unknown {
                                reason: "child_identity_changed",
                            },
                            ProcessIdentityProbe::ProbeError => ProcessSessionLiveness::Unknown {
                                reason: "child_identity_unreadable",
                            },
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        let _ = pid;
                        ProcessSessionLiveness::Unknown {
                            reason: "identity_probe_unsupported",
                        }
                    }
                }
                Err(_) => ProcessSessionLiveness::Unknown {
                    reason: "child_exit_probe_failed",
                },
            }
        }
        #[cfg(test)]
        SessionHandle::TestProcess { alive, .. } => {
            if alive.load(std::sync::atomic::Ordering::Relaxed) {
                ProcessSessionLiveness::Live {
                    reason: "test_child_live",
                }
            } else {
                ProcessSessionLiveness::Exited {
                    reason: "test_child_exited",
                }
            }
        }
    };
    ProcessSessionEvidence {
        registration: NativeRegistryPresence::Registered,
        liveness,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::session_backend::ProcessSessionRegistry;
    use std::sync::{Arc, Mutex};

    #[cfg(unix)]
    struct OwnedChild(Arc<Mutex<Option<std::process::Child>>>);

    #[cfg(unix)]
    impl Drop for OwnedChild {
        fn drop(&mut self) {
            let mut owned = self.0.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(mut child) = owned.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    #[test]
    fn missing_registry_after_restart_is_unknown_not_exited() {
        let name = format!("missing-native-{}", uuid::Uuid::new_v4());
        let registry = Mutex::new(ProcessSessionRegistry::default());
        let absent = process_session_evidence_in_registry(&registry, &name);
        assert_eq!(absent.registration, NativeRegistryPresence::Absent);
        assert_eq!(
            absent.liveness,
            ProcessSessionLiveness::Unknown {
                reason: "registry_entry_absent"
            }
        );
        {
            let _held = registry.lock().unwrap();
            let busy = process_session_evidence_in_registry(&registry, &name);
            assert_eq!(busy.registration, NativeRegistryPresence::Unknown);
            assert_eq!(
                busy.liveness,
                ProcessSessionLiveness::Unknown {
                    reason: "registry_busy"
                }
            );
        }
        let poisoned = Arc::new(Mutex::new(ProcessSessionRegistry::default()));
        let lock = poisoned.clone();
        assert!(
            std::thread::spawn(move || {
                let _held = lock.lock().unwrap();
                panic!("owned registry poison fixture");
            })
            .join()
            .is_err()
        );
        let unknown = process_session_evidence_in_registry(&poisoned, &name);
        assert_eq!(unknown.registration, NativeRegistryPresence::Unknown);
        assert_eq!(
            unknown.liveness,
            ProcessSessionLiveness::Unknown {
                reason: "registry_lock_poisoned"
            }
        );
    }

    #[cfg(unix)]
    #[test]
    fn native_liveness_verifies_real_child_and_observes_its_exit_without_tmux() {
        let name = format!("native-liveness-{}", uuid::Uuid::new_v4());
        let child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let pid = child.id();
        let owned_child = Arc::new(Mutex::new(Some(child)));
        let _cleanup = OwnedChild(owned_child.clone());
        let mut registry = Mutex::new(ProcessSessionRegistry::default());
        let output = tempfile::tempfile().unwrap();
        registry.get_mut().unwrap().insert(
            name.clone(),
            SessionHandle::Process {
                child_stdin: Arc::new(Mutex::new(None)),
                child: owned_child.clone(),
                pid,
                output: Arc::new(Mutex::new(output)),
            },
        );
        let live = process_session_evidence_in_registry(&registry, &name);
        assert_eq!(live.registration, NativeRegistryPresence::Registered);
        assert_eq!(
            live.liveness,
            ProcessSessionLiveness::Live {
                reason: "owned_child_identity_verified"
            }
        );
        {
            let mut child = owned_child.lock().unwrap();
            let child = child.as_mut().unwrap();
            child.kill().unwrap();
            child.wait().unwrap();
        }
        let exited = process_session_evidence_in_registry(&registry, &name);
        assert_eq!(exited.registration, NativeRegistryPresence::Registered);
        assert_eq!(
            exited.liveness,
            ProcessSessionLiveness::Exited {
                reason: "owned_child_exit_observed"
            }
        );
        registry.get_mut().unwrap().handles.remove(&name);
        let removed = process_session_evidence_in_registry(&registry, &name);
        assert_eq!(removed.registration, NativeRegistryPresence::Absent);
        assert_eq!(
            removed.liveness,
            ProcessSessionLiveness::Unknown {
                reason: "registry_entry_absent"
            }
        );
    }
}
