//! Filesystem boundary for the first Linux managed read/edit template.
//!
//! This is not a network or IPC sandbox. The operator supplies a private,
//! exclusively owned workspace and a dedicated non-root UID. Legacy launches
//! do not use this module; unsupported managed hosts fail before spawning.

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WorkspaceIdentity {
    pub(crate) uid: u32,
    pub(crate) gid: u32,
}

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(crate) use linux::{
    FilesystemBoundary, inspect_workspace, protected_file, read_protected_config,
};

#[cfg(not(target_os = "linux"))]
mod unsupported {
    use super::WorkspaceIdentity;
    use std::{io, path::Path, process::Command};

    fn unavailable() -> io::Error {
        io::Error::other("managed filesystem boundary requires Linux Landlock ABI >= 3")
    }

    pub(crate) fn inspect_workspace(_: &Path) -> io::Result<WorkspaceIdentity> {
        Err(unavailable())
    }

    pub(crate) fn read_protected_config(_: &Path) -> io::Result<String> {
        Err(unavailable())
    }

    pub(crate) fn protected_file(_: &Path, _: bool) -> io::Result<()> {
        Err(unavailable())
    }

    pub(crate) struct FilesystemBoundary;
    impl FilesystemBoundary {
        pub(crate) fn prepare(_: &Path, _: &Path) -> io::Result<Self> {
            Err(unavailable())
        }

        pub(crate) fn prepare_check(
            _: &Path,
            _: &Path,
            _: &Path,
            _: WorkspaceIdentity,
        ) -> io::Result<Self> {
            Err(unavailable())
        }

        pub(crate) fn runtime_home(&self) -> &Path {
            unreachable!("unsupported boundary cannot be constructed")
        }

        pub(crate) fn configure(self, _: &mut Command) -> io::Result<tempfile::TempDir> {
            Err(unavailable())
        }
    }
}
#[cfg(not(target_os = "linux"))]
pub(crate) use unsupported::*;
