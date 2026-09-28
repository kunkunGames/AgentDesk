//! Protected, single-process checker protocol. Output is evidence only after
//! wait/reap, manifest revalidation and type-specific report validation.
use super::{
    contract::{Check, CheckKind, digest},
    snapshot,
};
use crate::services::platform::managed_process::{self, FilesystemBoundary, WorkspaceIdentity};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
#[path = "process_linux.rs"]
mod linux;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Report {
    passed: bool,
    #[serde(default)]
    suites: Vec<Suite>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Suite {
    id: String,
    discovered: u64,
    executed: u64,
    skipped: u64,
    failed: u64,
}

#[cfg(target_os = "linux")]
struct ChildGuard(std::process::Child);

#[cfg(target_os = "linux")]
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

pub(crate) fn validate_check(check: &Check) -> Result<(), String> {
    for (path, expected) in [
        (&check.executable, &check.executable_sha256),
        (&check.wrapper, &check.wrapper_sha256),
    ] {
        managed_process::protected_file(path, false).map_err(|e| e.to_string())?;
        if digest(&fs::read(path).map_err(|e| e.to_string())?) != *expected {
            return Err("protected executable/wrapper digest changed".into());
        }
    }
    Ok(())
}

fn validate_report(check: &Check, bytes: &[u8]) -> Result<Report, String> {
    let report: Report = serde_json::from_slice(bytes)
        .map_err(|e| format!("missing or malformed check report: {e}"))?;
    if let CheckKind::TestSuite {
        suites,
        min_tests,
        max_skipped,
    } = &check.kind
    {
        let names: std::collections::BTreeSet<_> = report.suites.iter().map(|s| &s.id).collect();
        if names.len() != report.suites.len()
            || names != suites.iter().collect()
            || report.suites.iter().any(|s| {
                s.executed == 0
                    || s.discovered != s.executed.saturating_add(s.skipped)
                    || s.failed > s.executed
            })
            || report
                .suites
                .iter()
                .map(|s| s.executed)
                .fold(0_u64, u64::saturating_add)
                < *min_tests
            || report
                .suites
                .iter()
                .map(|s| s.skipped)
                .fold(0_u64, u64::saturating_add)
                > *max_skipped
        {
            return Err("check report does not cover the required executed suites".into());
        }
    }
    Ok(report)
}

pub(crate) fn run(
    check: &Check,
    candidate: &Path,
    expected: &str,
    identity: WorkspaceIdentity,
    cancelled: Arc<AtomicBool>,
) -> Result<serde_json::Value, String> {
    validate_check(check)?;
    snapshot::verify(candidate, expected)?;
    let boundary =
        FilesystemBoundary::prepare_check(&check.executable, &check.wrapper, candidate, identity)
            .map_err(|e| e.to_string())?;
    let stdout = tempfile::tempfile().map_err(|e| e.to_string())?;
    let stderr = tempfile::tempfile().map_err(|e| e.to_string())?;
    let mut command = Command::new(&check.executable);
    command
        .arg(&check.wrapper)
        .arg(candidate)
        .current_dir(candidate)
        .env_clear()
        .env("HOME", boundary.runtime_home())
        .env("TMPDIR", boundary.runtime_home())
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env("PYTHONNOUSERSITE", "1")
        .stdin(Stdio::null())
        .stdout(stdout.try_clone().map_err(|e| e.to_string())?)
        .stderr(stderr.try_clone().map_err(|e| e.to_string())?);
    let _runtime = boundary
        .configure(&mut command)
        .map_err(|e| e.to_string())?;
    #[cfg(target_os = "linux")]
    linux::configure(&mut command);
    #[cfg(not(target_os = "linux"))]
    return Err("protected checks require Linux".into());
    #[cfg(target_os = "linux")]
    {
        let mut guarded = ChildGuard(command.spawn().map_err(|e| e.to_string())?);
        let child = &mut guarded.0;
        let deadline = Instant::now() + Duration::from_secs(check.timeout_secs);
        let (status, reason) = loop {
            if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
                break (status, None);
            }
            let reason = if cancelled.load(Ordering::Acquire) {
                Some("cancelled")
            } else if Instant::now() >= deadline {
                Some("deadline")
            } else {
                None
            };
            if let Some(reason) = reason {
                let _ = child.kill();
                break (child.wait().map_err(|e| e.to_string())?, Some(reason));
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        use std::io::{Read, Seek};
        let mut bytes = Vec::new();
        let mut stdout = stdout;
        stdout.rewind().map_err(|e| e.to_string())?;
        stdout
            .take(1_048_577)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        let mut diagnostic = Vec::new();
        let mut stderr = stderr;
        stderr.rewind().map_err(|e| e.to_string())?;
        stderr
            .take(65536)
            .read_to_end(&mut diagnostic)
            .map_err(|e| e.to_string())?;
        // Never forward arbitrary checker text/secrets to logs or card prompts.
        let mut evidence = serde_json::json!({"exit_code":status.code(), "stdout_sha256":digest(&bytes), "stderr_sha256":digest(&diagnostic), "process_reaped":true});
        if let Some(reason) = reason {
            evidence["outcome"] = serde_json::json!(if reason == "cancelled" {
                "cancelled"
            } else {
                "unverifiable"
            });
            evidence["reason"] = serde_json::json!(reason);
            return Ok(evidence);
        }
        snapshot::verify(candidate, expected)?;
        let report = validate_report(check, &bytes)?;
        let passed =
            status.success() && report.passed && report.suites.iter().all(|s| s.failed == 0);
        evidence["outcome"] = serde_json::json!(if passed { "passed" } else { "failed" });
        evidence["report"] = serde_json::to_value(report).map_err(|e| e.to_string())?;
        Ok(evidence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_empty_skipped_or_partial_suites_cannot_pass() {
        let check = Check {
            executable: "/python".into(),
            wrapper: "/check".into(),
            executable_sha256: "a".repeat(64),
            wrapper_sha256: "b".repeat(64),
            timeout_secs: 5,
            kind: CheckKind::TestSuite {
                suites: vec!["unit".into(), "integration".into()],
                min_tests: 2,
                max_skipped: 0,
            },
        };
        for report in [
            "",
            r#"{"passed":true}"#,
            r#"{"passed":true,"suites":[{"id":"unit","discovered":0,"executed":0,"skipped":0,"failed":0}]}"#,
            r#"{"passed":true,"suites":[{"id":"unit","discovered":2,"executed":1,"skipped":1,"failed":0},{"id":"integration","discovered":1,"executed":1,"skipped":0,"failed":0}]}"#,
        ] {
            assert!(validate_report(&check, report.as_bytes()).is_err());
        }
        assert!(validate_report(&check,br#"{"passed":true,"suites":[{"id":"unit","discovered":1,"executed":1,"skipped":0,"failed":0},{"id":"integration","discovered":1,"executed":1,"skipped":0,"failed":0}]}"#).is_ok());
    }
}
