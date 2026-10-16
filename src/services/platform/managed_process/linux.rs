use super::WorkspaceIdentity;
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Command;

const EXECUTE: u64 = 1;
const WRITE_FILE: u64 = 1 << 1;
const READ_FILE: u64 = 1 << 2;
const READ_DIR: u64 = 1 << 3;
const TRUNCATE: u64 = 1 << 14;
const READ: u64 = READ_FILE | READ_DIR;
// No execution, device/FIFO/socket creation, or cross-directory REFER grant.
const WRITE: u64 =
    READ | WRITE_FILE | TRUNCATE | (1 << 4) | (1 << 5) | (1 << 7) | (1 << 8) | (1 << 12);
const HANDLED: u64 = (1 << 15) - 1;

#[repr(C)]
struct RulesetAttr {
    handled_access_fs: u64,
}

#[repr(C, packed)]
struct PathBeneathAttr {
    allowed_access: u64,
    parent_fd: i32,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

pub(crate) fn inspect_workspace(workspace: &Path) -> io::Result<WorkspaceIdentity> {
    // The service only drops privilege in its child. It never changes the
    // ownership of an operator workspace or requires a privileged host service.
    if unsafe { libc::geteuid() } != 0 {
        return Err(invalid(
            "managed Linux template requires a root launcher inside its isolated deployment and a non-root workspace owner",
        ));
    }
    let root = fs::symlink_metadata(workspace)?;
    if !root.is_dir() || root.uid() == 0 || root.gid() == 0 || root.mode() & 0o077 != 0 {
        return Err(invalid(
            "managed workspace must be a private directory owned by a dedicated non-root UID/GID (mode 0700)",
        ));
    }
    let identity = WorkspaceIdentity {
        uid: root.uid(),
        gid: root.gid(),
    };
    let mut pending = vec![workspace.to_path_buf()];
    let mut inspected = 0_usize;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            inspected += 1;
            if inspected > 100_000 {
                return Err(invalid(
                    "managed read/edit template workspace exceeds its 100000-entry admission limit",
                ));
            }
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                // The kernel follows the target at access time. Do not turn a
                // mutable symlink resolution check into the security boundary.
                continue;
            }
            if metadata.uid() != identity.uid || metadata.gid() != identity.gid {
                return Err(invalid(
                    "managed workspace entries must belong to its dedicated UID/GID",
                ));
            }
            if metadata.is_dir() {
                pending.push(path);
            } else if !metadata.is_file() || metadata.nlink() != 1 {
                return Err(invalid(
                    "managed workspace forbids pre-existing hardlinks and special files",
                ));
            }
        }
    }
    Ok(identity)
}

/// Open a service-owned file without following aliases. Its entire parent
/// chain must be protected from executor writes. Contents never enter logs.
pub(crate) fn read_protected_config(path: &Path) -> io::Result<String> {
    protected_file(path, true)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != 0
        || metadata.mode() & 0o077 != 0
        || metadata.nlink() != 1
    {
        return Err(invalid(
            "managed config must be a private root-owned regular file without hardlinks",
        ));
    }
    let mut content = String::new();
    file.take(1_048_577).read_to_string(&mut content)?;
    if content.len() > 1_048_576 {
        return Err(invalid("managed config exceeds one MiB"));
    }
    Ok(content)
}

pub(crate) fn protected_file(path: &Path, private: bool) -> io::Result<()> {
    if !path.is_absolute() {
        return Err(invalid("managed protected paths must be absolute"));
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.uid() != 0
        || metadata.nlink() != 1
        || metadata.mode() & if private { 0o077 } else { 0o022 } != 0
    {
        return Err(invalid(
            "managed executable/config must be protected root-owned regular files",
        ));
    }
    for parent in path.ancestors().skip(1) {
        let metadata = fs::symlink_metadata(parent)?;
        // /tmp is permitted only as a root-owned sticky ancestor. The private
        // directory below it must still be root-owned and non-writable.
        let sticky_root = metadata.uid() == 0 && metadata.mode() & libc::S_ISVTX != 0;
        if !metadata.is_dir() || metadata.uid() != 0 || metadata.mode() & 0o022 != 0 && !sticky_root
        {
            return Err(invalid(
                "managed protected file parent is writable, aliased, or not root-owned",
            ));
        }
    }
    Ok(())
}

pub(crate) struct FilesystemBoundary {
    ruleset: OwnedFd,
    runtime: tempfile::TempDir,
    identity: WorkspaceIdentity,
}

impl FilesystemBoundary {
    pub(crate) fn prepare(binary: &Path, workspace: &Path) -> io::Result<Self> {
        let identity = inspect_workspace(workspace)?;
        let boundary = Self::new(binary, identity)?;
        boundary.allow(workspace, WRITE)?;
        Ok(boundary)
    }

    /// A verifier can only read the frozen candidate and protected wrapper.
    /// Its sole writable directory is disposable and never an implementation input.
    pub(crate) fn prepare_check(
        binary: &Path,
        wrapper: &Path,
        candidate: &Path,
        identity: WorkspaceIdentity,
    ) -> io::Result<Self> {
        protected_file(wrapper, false)?;
        let boundary = Self::new(binary, identity)?;
        boundary.allow(candidate, READ)?;
        boundary.allow(wrapper, READ_FILE)?;
        Ok(boundary)
    }

    fn new(binary: &Path, identity: WorkspaceIdentity) -> io::Result<Self> {
        protected_file(binary, false)?;
        // Do not silently degrade to an ABI without REFER/TRUNCATE mediation.
        let abi = unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                std::ptr::null::<u8>(),
                0,
                1,
            )
        };
        if abi < 3 {
            return Err(invalid(
                "managed filesystem boundary requires enabled Landlock ABI >= 3",
            ));
        }
        let attr = RulesetAttr {
            handled_access_fs: HANDLED,
        };
        let fd = unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                &attr,
                size_of::<RulesetAttr>(),
                0,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let ruleset = unsafe { OwnedFd::from_raw_fd(fd as i32) };
        let runtime = tempfile::Builder::new()
            .prefix("agentdesk-managed-runtime-")
            .tempdir()?;
        let path = CString::new(runtime.path().as_os_str().as_bytes())?;
        if unsafe { libc::chown(path.as_ptr(), identity.uid, identity.gid) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let boundary = Self {
            ruleset,
            runtime,
            identity,
        };
        boundary.allow(boundary.runtime.path(), WRITE)?;
        boundary.allow(binary, READ_FILE | EXECUTE)?;
        for path in [
            "/usr/lib",
            "/lib",
            "/lib64",
            "/usr/share/zoneinfo",
            "/etc/ssl/certs",
        ] {
            boundary.allow_if_present(Path::new(path), READ)?;
        }
        // The ELF interpreter itself needs execute permission. Other programs
        // and workspace scripts do not inherit an executable-directory grant.
        for path in ["/lib64/ld-linux-x86-64.so.2", "/lib/ld-linux-aarch64.so.1"] {
            boundary.allow_if_present(Path::new(path), READ_FILE | EXECUTE)?;
        }
        for path in [
            "/etc/ld.so.cache",
            "/etc/resolv.conf",
            "/etc/hosts",
            "/etc/nsswitch.conf",
            "/etc/passwd",
            "/etc/group",
            "/etc/localtime",
            "/dev/urandom",
            "/dev/random",
            "/proc/sys/vm/overcommit_memory",
            "/proc/sys/vm/mmap_min_addr",
            "/proc/stat",
            "/sys/devices/system/cpu/online",
            "/sys/kernel/mm/transparent_hugepage/enabled",
        ] {
            boundary.allow_if_present(Path::new(path), READ_FILE)?;
        }
        boundary.allow(Path::new("/dev/null"), READ_FILE | WRITE_FILE)?;
        Ok(boundary)
    }

    fn allow_if_present(&self, path: &Path, access: u64) -> io::Result<()> {
        match fs::metadata(path) {
            Ok(_) => self.allow(path, access),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn allow(&self, path: &Path, access: u64) -> io::Result<()> {
        let path_fd: File = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_PATH | libc::O_CLOEXEC)
            .open(path)?;
        let attr = PathBeneathAttr {
            allowed_access: access,
            parent_fd: path_fd.as_raw_fd(),
        };
        let result = unsafe {
            libc::syscall(
                libc::SYS_landlock_add_rule,
                self.ruleset.as_raw_fd(),
                1,
                &attr,
                0,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(crate) fn runtime_home(&self) -> &Path {
        self.runtime.path()
    }

    pub(crate) fn configure(self, cmd: &mut Command) -> io::Result<tempfile::TempDir> {
        let Self {
            ruleset,
            runtime,
            identity,
        } = self;
        // No allocation, locks, logging, or Rust library I/O after fork. The
        // ruleset fd lives until spawn and the kernel retains the applied rules.
        unsafe {
            cmd.pre_exec(move || {
                let no_core = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                if libc::setrlimit(libc::RLIMIT_CORE, &no_core) != 0
                    || libc::setgroups(0, std::ptr::null()) != 0
                    || libc::setgid(identity.gid) != 0
                    || libc::setuid(identity.uid) != 0
                    || libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0
                {
                    return Err(io::Error::last_os_error());
                }
                // Bun requires its own memory/cgroup description at startup.
                // Resolve these two files after fork: resolving /proc/self in
                // the parent would authorize the service's proc entries. Never
                // grant the proc directory, environ, mem, or another PID.
                for path in [c"/proc/self/maps", c"/proc/self/cgroup"] {
                    let fd = libc::open(path.as_ptr(), libc::O_PATH | libc::O_CLOEXEC);
                    if fd < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    let attr = PathBeneathAttr {
                        allowed_access: READ_FILE,
                        parent_fd: fd,
                    };
                    let result = libc::syscall(
                        libc::SYS_landlock_add_rule,
                        ruleset.as_raw_fd(),
                        1,
                        &attr,
                        0,
                    );
                    let error = if result != 0 {
                        Some(io::Error::last_os_error())
                    } else {
                        None
                    };
                    libc::close(fd);
                    if let Some(error) = error {
                        return Err(error);
                    }
                }
                if libc::syscall(libc::SYS_landlock_restrict_self, ruleset.as_raw_fd(), 0) != 0
                    // CLOEXEC keeps Rust's spawn-error pipe usable until exec,
                    // while preventing inherited descriptors bypassing rules.
                    || libc::syscall(libc::SYS_close_range, 3_u32, u32::MAX, 4_u32) != 0
                {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(runtime)
    }
}

#[cfg(test)]
mod tests;
