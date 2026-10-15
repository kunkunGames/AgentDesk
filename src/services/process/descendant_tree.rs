//! Capture owned descendants before a wrapper exits and reparents its tools.
//!
//! This supplements process-group cleanup; it is not an OS sandbox or proof
//! against a process which already detached before the snapshot.

use super::ProcessIdentity;
use std::collections::HashSet;
use std::process::Command;
use std::time::Duration;

pub(super) fn capture(root: u32, identity: ProcessIdentity) -> Vec<(u32, ProcessIdentity)> {
    if root <= 1
        || root == std::process::id()
        || !identity.has_baseline()
        || !identity.matches(root)
    {
        return Vec::new();
    }
    let Some(rows) = process_parents() else {
        tracing::warn!(
            root,
            "could not capture descendant identities before process cleanup"
        );
        return Vec::new();
    };
    if !identity.matches(root) {
        return Vec::new();
    }
    let captured: Vec<_> = descendant_pids(root, &rows)
        .into_iter()
        .filter(|pid| *pid > 1 && *pid != std::process::id())
        .filter_map(|pid| {
            let identity = ProcessIdentity::capture(pid);
            identity.has_baseline().then_some((pid, identity))
        })
        .collect();
    // A PID from the first table may exit and be reused before identity capture.
    // Require that the captured incarnation still belongs to this live tree.
    let Some(current) = process_parents() else {
        return Vec::new();
    };
    if !identity.matches(root) {
        return Vec::new();
    }
    let owned: HashSet<_> = descendant_pids(root, &current).into_iter().collect();
    captured
        .into_iter()
        .filter(|(pid, identity)| owned.contains(pid) && identity.matches(*pid))
        .collect()
}

fn process_parents() -> Option<Vec<(u32, u32)>> {
    // No command arguments or environment are collected. Drain stdout while ps
    // runs so a large host process table cannot block on the pipe buffer.
    static LANE: std::sync::LazyLock<super::bounded_probe::ProbeLane> =
        std::sync::LazyLock::new(Default::default);
    let mut command = Command::new("/bin/ps");
    command.args(["-axo", "pid=,ppid="]);
    let output = LANE.output(command, Duration::from_secs(2), 8 * 1024 * 1024)?;
    let text = String::from_utf8(output.stdout).ok()?;
    text.lines()
        .map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse().ok()?;
            let parent = fields.next()?.parse().ok()?;
            fields.next().is_none().then_some((pid, parent))
        })
        .collect()
}

fn descendant_pids(root: u32, rows: &[(u32, u32)]) -> Vec<u32> {
    let mut visited = HashSet::from([root]);
    let mut pending = vec![root];
    let mut descendants = Vec::new();
    while let Some(parent) = pending.pop() {
        for &(pid, ppid) in rows {
            if ppid == parent && visited.insert(pid) {
                descendants.push(pid);
                pending.push(pid);
            }
        }
    }
    descendants
}

#[cfg(test)]
mod tests;
