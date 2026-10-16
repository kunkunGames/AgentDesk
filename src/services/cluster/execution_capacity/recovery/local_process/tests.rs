use super::*;
use crate::services::process::{
    ProcessGroupProbe, configure_child_process_group, kill_pid_tree_if_identity_matches,
    process_group_probe,
};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};

struct OwnedTree {
    child: Child,
    birth: ProcessIdentity,
    escaped: Option<(u32, ProcessIdentity)>,
}
impl Drop for OwnedTree {
    fn drop(&mut self) {
        if let Some((pid, birth)) = self.escaped {
            kill_pid_tree_if_identity_matches(pid, birth);
        }
        kill_pid_tree_if_identity_matches(self.child.id(), self.birth);
        let _ = self.child.wait();
    }
}

fn child(command: &mut Command, uid: u32) -> OwnedTree {
    configure_child_process_group(command);
    unsafe {
        command.pre_exec(move || {
            if libc::setgroups(0, std::ptr::null()) != 0
                || libc::setgid(uid) != 0
                || libc::setuid(uid) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().unwrap();
    let birth = ProcessIdentity::capture(child.id());
    OwnedTree {
        child,
        birth,
        escaped: None,
    }
}

#[test]
#[ignore = "requires disposable Linux root launcher and Python3; creates only owned synthetic writers"]
fn escaped_writer_and_namespace_mismatch_do_not_become_stop_proof() {
    assert_eq!(unsafe { libc::geteuid() }, 0);
    let workspace = tempfile::tempdir().unwrap();
    let path = std::ffi::CString::new(workspace.path().as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::chown(path.as_ptr(), 65532, 65532) }, 0);
    std::fs::set_permissions(workspace.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let writer = workspace.path().join("writer.txt");
    let mut command = Command::new("python3");
    command
        .arg("-c")
        .arg(
            r#"
import os, pathlib, sys, time
read_end, write_end = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(read_end)
    os.setsid()
    target = pathlib.Path(sys.argv[1])
    target.write_text('0')
    os.write(write_end, str(os.getpid()).encode())
    os.close(write_end)
    for fd in (0, 1, 2): os.close(fd)
    count = 0
    while True:
        count += 1
        target.write_text(str(count))
        time.sleep(.05)
else:
    os.close(write_end)
    print(os.read(read_end, 64).decode(), flush=True)
    os.close(read_end)
    sys.stdin.readline()
"#,
        )
        .arg(&writer)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut owned = child(&mut command, 65532);
    let mut ready = String::new();
    BufReader::new(owned.child.stdout.take().unwrap())
        .read_line(&mut ready)
        .unwrap();
    let escaped: u32 = ready.trim().parse().unwrap();
    owned.escaped = Some((escaped, ProcessIdentity::capture(escaped)));
    let identity = capture(owned.child.id(), owned.birth).unwrap();
    assert_eq!(
        Snapshot::capture().observe(&identity).local_writer_status,
        "alive"
    );
    for changed in [
        RuntimeIdentity {
            boot_id: uuid::Uuid::nil().to_string(),
            ..identity.clone()
        },
        RuntimeIdentity {
            pid_namespace: identity.pid_namespace.checked_add(1).unwrap(),
            ..identity.clone()
        },
    ] {
        assert_eq!(
            Snapshot::capture().observe(&changed).local_writer_status,
            "unobservable"
        );
    }

    let mut healthy_command = Command::new("/bin/sleep");
    healthy_command
        .arg("60")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut healthy = child(&mut healthy_command, 65531);
    owned
        .child
        .stdin
        .take()
        .unwrap()
        .write_all(b"stop\n")
        .unwrap();
    assert!(owned.child.wait().unwrap().success());
    assert_eq!(process_group_probe(identity.pid), ProcessGroupProbe::Gone);
    let before = std::fs::read_to_string(&writer).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(180));
    assert_ne!(
        std::fs::read_to_string(&writer).unwrap(),
        before,
        "escaped child must really keep writing"
    );
    let observation = Snapshot::capture().observe(&identity);
    assert_eq!(observation.local_writer_status, "alive");
    assert_eq!(observation.reason, "dedicated_uid_has_live_processes");
    assert_eq!(observation.inference_status, "unknown");
    assert!(healthy.child.try_wait().unwrap().is_none());
    let (pid, birth) = owned.escaped.take().unwrap();
    assert!(kill_pid_tree_if_identity_matches(pid, birth));
    let until = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let observation = Snapshot::capture().observe(&identity);
        if observation.local_writer_status == "not_observed" {
            break;
        }
        assert!(std::time::Instant::now() < until, "{observation:?}");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        healthy.child.try_wait().unwrap().is_none(),
        "observations must not kill an independent runtime"
    );
}
