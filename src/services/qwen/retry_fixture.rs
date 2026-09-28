//! A provider process that records invocations before emitting a controlled stream.
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

pub(crate) struct RetryFixture {
    dir: tempfile::TempDir,
    pub(crate) cli: PathBuf,
}

impl RetryFixture {
    pub(crate) fn new(mode: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let cli = dir.path().join("qwen-fixture");
        // Feed the native Qwen envelope, before the tmux normalizer removes role.
        let progress = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"one","name":"write_file","input":{"path":"effect"}}]}}"#;
        let done = r#"{"type":"result","subtype":"success","result":"done"}"#;
        let error = r#"{"type":"result","subtype":"error_during_execution","is_error":true,"error":{"message":"native budget exhausted"}}"#;
        std::fs::write(
            &cli,
            format!(
                r#"#!/usr/bin/env python3
import pathlib, sys
root = pathlib.Path(__file__).parent
counter = root / "invocations"
with counter.open("a") as f: f.write("invoked\n")
attempt = len(counter.read_text().splitlines())
mode = {mode:?}
if mode == "startup_once" and attempt == 1: sys.exit(7)
if mode == "terminal_error":
    print({error:?}, flush=True)
    sys.exit(7)
(root / "effect").write_text("effect-performed")
print({progress:?}, flush=True)
if mode == "startup_once":
    print({done:?}, flush=True)
    sys.exit(0)
if mode == "read_error":
    sys.stdout.buffer.write(b"\xff\n")
    sys.stdout.buffer.flush()
sys.exit(0 if mode == "eof_after_progress" else 7)
"#
            ),
        )
        .unwrap();
        std::fs::set_permissions(&cli, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self { dir, cli }
    }

    pub(crate) fn path(&self) -> &Path {
        self.dir.path()
    }

    pub(crate) fn invocations(&self) -> usize {
        std::fs::read_to_string(self.path().join("invocations"))
            .unwrap()
            .lines()
            .count()
    }

    pub(crate) fn resolution(&self) -> crate::services::platform::BinaryResolution {
        crate::services::platform::BinaryResolution {
            requested_binary: self.cli.display().to_string(),
            resolved_path: Some(self.cli.display().to_string()),
            canonical_path: None,
            source: None,
            attempts: vec![],
            failure_kind: None,
            exec_path: None,
        }
    }
}
