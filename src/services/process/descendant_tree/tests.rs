use super::{capture, descendant_pids};
use crate::services::process::{
    ProcessIdentity, configure_child_process_group, kill_pid_tree,
    kill_pid_tree_if_identity_matches,
};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct UnrelatedChild(Child);

impl Drop for UnrelatedChild {
    fn drop(&mut self) {
        // Keep ownership of the unreaped child even if an assertion fails.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct ToolTree {
    root: Child,
    root_identity: ProcessIdentity,
    worker: u32,
    worker_identity: ProcessIdentity,
    files: tempfile::TempDir,
}

impl ToolTree {
    fn spawn() -> Self {
        let files = tempfile::tempdir().unwrap();
        let mut command = Command::new("python3");
        command
            .args([
                "-c",
                r#"
import os, subprocess, sys, time
worker = '''import os, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
open(sys.argv[1] + '/worker.pid', 'w').write(str(os.getpid()))
while True:
    with open(sys.argv[1] + '/writes', 'a') as stream:
        stream.write('x')
    time.sleep(0.02)
'''
child = subprocess.Popen([sys.executable, '-c', worker, sys.argv[1]], start_new_session=True)
child.wait()
open(sys.argv[1] + '/after-child-exit', 'w').write('parent advanced after tool exit')
time.sleep(30)
"#,
            ])
            .arg(files.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        configure_child_process_group(&mut command);
        let root = command.spawn().expect("Python process fixture must start");
        let root_identity = ProcessIdentity::capture(root.id());
        let deadline = Instant::now() + Duration::from_secs(5);
        let worker = loop {
            if let Ok(text) = std::fs::read_to_string(files.path().join("worker.pid")) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    break pid;
                }
            }
            if Instant::now() >= deadline {
                kill_pid_tree(root.id());
                panic!("separate process-group worker did not become ready");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        let worker_identity = ProcessIdentity::capture(worker);
        assert!(worker_identity.has_baseline());
        Self {
            root,
            root_identity,
            worker,
            worker_identity,
            files,
        }
    }

    fn writes(&self) -> u64 {
        std::fs::metadata(self.files.path().join("writes"))
            .unwrap()
            .len()
    }
}

impl Drop for ToolTree {
    fn drop(&mut self) {
        kill_pid_tree_if_identity_matches(self.worker, self.worker_identity);
        kill_pid_tree_if_identity_matches(self.root.id(), self.root_identity);
        let _ = self.root.wait();
    }
}

fn wait_for_writes(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::fs::metadata(path).map(|m| m.len()).unwrap_or(0) == 0 {
        assert!(Instant::now() < deadline, "tool fixture never wrote");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn cancel_and_assert_stopped(tree: &mut ToolTree) {
    kill_pid_tree(tree.root.id());
    tree.root.wait().unwrap();
    let stopped_at = tree.writes();
    std::thread::sleep(Duration::from_millis(350));
    assert_eq!(
        tree.writes(),
        stopped_at,
        "detached tool wrote after wrapper cancellation"
    );
    assert!(
        !tree.files.path().join("after-child-exit").exists(),
        "waiting parent advanced after its tool was killed"
    );
}

#[test]
fn cancellation_stops_tool_in_separate_process_group_without_touching_sibling() {
    let mut tree = ToolTree::spawn();
    let unrelated = UnrelatedChild(Command::new("sleep").arg("30").spawn().unwrap());
    let unrelated_identity = ProcessIdentity::capture(unrelated.0.id());
    wait_for_writes(&tree.files.path().join("writes"));
    let captured = capture(tree.root.id(), ProcessIdentity::capture(tree.root.id()));
    assert!(captured.iter().any(|(pid, _)| *pid == tree.worker));
    assert!(!captured.iter().any(|(pid, _)| *pid == unrelated.0.id()));

    cancel_and_assert_stopped(&mut tree);
    assert!(
        unrelated_identity.matches(unrelated.0.id()),
        "cleanup killed an unrelated process"
    );
}

#[test]
fn concurrent_cancellation_stops_separate_groups_without_touching_sibling() {
    let unrelated = UnrelatedChild(Command::new("sleep").arg("30").spawn().unwrap());
    let unrelated_identity = ProcessIdentity::capture(unrelated.0.id());
    let trees: Vec<_> = (0..4).map(|_| ToolTree::spawn()).collect();
    for tree in &trees {
        wait_for_writes(&tree.files.path().join("writes"));
    }
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let mut starts = Vec::new();
    let callers: Vec<_> = trees
        .into_iter()
        .map(|mut tree| {
            let (start_tx, start_rx) = std::sync::mpsc::channel();
            starts.push(start_tx);
            let ready_tx = ready_tx.clone();
            std::thread::Builder::new()
                .name("owned-concurrent-cancellation".into())
                .spawn(move || {
                    ready_tx.send(()).unwrap();
                    start_rx
                        .recv_timeout(Duration::from_secs(10))
                        .expect("owned cancellation start gate timed out or was aborted");
                    cancel_and_assert_stopped(&mut tree);
                })
                .expect("owned cancellation worker must start")
        })
        .collect();
    for _ in 0..4 {
        ready_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("owned cancellation worker did not become ready");
    }
    for start in starts {
        start.send(()).unwrap();
    }
    for caller in callers {
        caller.join().unwrap();
    }
    assert!(
        unrelated_identity.matches(unrelated.0.id()),
        "unrelated sibling was killed"
    );
}

#[test]
fn stale_root_identity_cannot_capture_or_kill_its_current_descendants() {
    let tree = ToolTree::spawn();
    wait_for_writes(&tree.files.path().join("writes"));
    let stale = ProcessIdentity::from_raw_for_test(Some(u128::MAX));
    assert!(capture(tree.root.id(), stale).is_empty());
    assert!(!kill_pid_tree_if_identity_matches(tree.root.id(), stale));
    let before = tree.writes();
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        tree.writes() > before,
        "stale cleanup interrupted the current tool"
    );
}

#[test]
fn ancestry_walk_excludes_ancestors_unrelated_pids_and_cycles() {
    let rows = [(10, 1), (20, 10), (30, 20), (40, 1), (10, 30), (30, 20)];
    assert_eq!(descendant_pids(10, &rows), vec![20, 30]);
}
