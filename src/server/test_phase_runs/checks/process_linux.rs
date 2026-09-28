//! Keep a checker a single, networkless process. Landlock separately limits
//! executable/file access. No credentials, inherited service FDs, or child writers.
use std::{io, os::unix::process::CommandExt, process::Command};

fn statement(code: u16, k: u32) -> libc::sock_filter {
    libc::sock_filter {
        code,
        jt: 0,
        jf: 0,
        k,
    }
}
fn equal(k: u32, jt: u8, jf: u8) -> libc::sock_filter {
    libc::sock_filter {
        code: 0x15,
        jt,
        jf,
        k,
    }
}

pub(super) fn configure(command: &mut Command) {
    #[cfg(target_arch = "x86_64")]
    const ARCH: u32 = 0xc000003e;
    #[cfg(target_arch = "aarch64")]
    const ARCH: u32 = 0xc00000b7;
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    const ARCH: u32 = 0; // Fail closed on a host without a verified syscall ABI.
    let mut filters = vec![
        statement(0x20, 4),
        equal(ARCH, 1, 0),
        statement(0x06, 0x80000000),
        statement(0x20, 0),
    ];
    // Reject x32/unknown syscall spaces, even when the audit arch is x86_64.
    filters.push(libc::sock_filter {
        code: 0x35,
        jt: 0,
        jf: 1,
        k: 0x40000000,
    });
    filters.push(statement(0x06, 0x80000000));
    let mut denied = vec![
        libc::SYS_clone,
        libc::SYS_clone3,
        libc::SYS_socket,
        libc::SYS_socketpair,
        libc::SYS_connect,
        libc::SYS_bind,
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_kill,
        libc::SYS_tkill,
        libc::SYS_tgkill,
        libc::SYS_unshare,
        libc::SYS_setns,
        libc::SYS_mount,
        libc::SYS_io_uring_setup,
    ];
    #[cfg(target_arch = "x86_64")]
    denied.extend([libc::SYS_fork, libc::SYS_vfork]);
    for syscall in denied {
        filters.push(equal(syscall as u32, 0, 1));
        filters.push(statement(0x06, 0x00050000 | libc::EPERM as u32));
    }
    filters.push(statement(0x06, 0x7fff0000));
    let parent = unsafe { libc::getpid() };
    // Registered after FilesystemBoundary's privilege drop and no_new_privs.
    unsafe {
        command.pre_exec(move || {
            let output = libc::rlimit {
                rlim_cur: 1_048_576,
                rlim_max: 1_048_576,
            };
            let filter = libc::sock_fprog {
                len: filters.len() as u16,
                filter: filters.as_ptr().cast_mut(),
            };
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0
                || libc::getppid() != parent
                || libc::setrlimit(libc::RLIMIT_FSIZE, &output) != 0
                || libc::prctl(libc::PR_SET_SECCOMP, 2, &filter) != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
}
