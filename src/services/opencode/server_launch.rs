//! Server launch.

use super::*;

pub(super) fn spawn_server(
    bin: &str,
    resolution: &crate::services::platform::BinaryResolution,
    port: u16,
    password: &str,
    working_dir: &str,
    overlay: &crate::services::provider_auth_profile::ProviderAuthOverlay,
    managed: bool,
) -> Result<OpenCodeServerProcess, String> {
    let mut cmd = Command::new(bin);
    if managed {
        if overlay.home.is_none() || overlay.is_default() {
            return Err("managed OpenCode requires a named isolated auth profile".into());
        }
        // AgentDesk's database/control-plane credentials must not be inherited
        // by a managed provider. Only platform bootstrap and the explicit
        // provider overlay cross this boundary.
        cmd.env_clear();
        for name in [
            "PATH",
            "SYSTEMROOT",
            "WINDIR",
            "COMSPEC",
            "PATHEXT",
            "LANG",
            "LC_ALL",
            "TMPDIR",
            "TEMP",
            "TMP",
        ] {
            if let Some(value) = std::env::var_os(name) {
                cmd.env(name, value);
            }
        }
    }
    crate::services::platform::apply_binary_resolution(&mut cmd, resolution);
    configure_child_process_group(&mut cmd);
    crate::services::provider_auth_profile::apply_overlay_to_command(&mut cmd, overlay);
    let managed_runtime = if managed {
        let boundary = crate::services::platform::managed_process::FilesystemBoundary::prepare(
            std::path::Path::new(bin),
            std::path::Path::new(working_dir),
        )
        .map_err(|error| format!("managed OpenCode filesystem boundary: {error}"))?;
        let home = boundary.runtime_home();
        // The service-owned profile and its credential/config files are never
        // exposed as the provider's writable home. Only the validated snapshot
        // and explicit credential environment cross into the process.
        cmd.env("HOME", home)
            .env("XDG_CONFIG_HOME", home.join("config"))
            .env("XDG_DATA_HOME", home.join("data"))
            .env("XDG_CACHE_HOME", home.join("cache"))
            .env("XDG_STATE_HOME", home.join("state"))
            .env("TMPDIR", home);
        Some(
            boundary
                .configure(&mut cmd)
                .map_err(|error| format!("managed OpenCode child boundary: {error}"))?,
        )
    } else {
        None
    };
    cmd.arg("serve")
        .arg("--hostname")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .env("OPENCODE_SERVER_PASSWORD", password)
        .current_dir(working_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if managed {
        // Match the probed native contract. Never reuse this process for a
        // legacy request with project plugins or automatic auxiliary setup.
        cmd.arg("--pure")
            .env("OPENCODE_DISABLE_DEFAULT_PLUGINS", "true")
            .env("OPENCODE_DISABLE_MODELS_FETCH", "true")
            .env("OPENCODE_DISABLE_AUTOUPDATE", "true");
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("Failed to spawn opencode serve: {e}"))?;
    let identity = crate::services::process::ProcessIdentity::capture(child.id());
    let startup_output = Arc::new(Mutex::new(OpenCodeStartupOutput::default()));
    if let Some(stdout) = child.stdout.take() {
        drain_startup_output(stdout, startup_output.clone(), StartupStream::Stdout);
    }
    if let Some(stderr) = child.stderr.take() {
        drain_startup_output(stderr, startup_output.clone(), StartupStream::Stderr);
    }
    Ok(OpenCodeServerProcess {
        child,
        identity,
        startup_output,
        _managed_runtime: managed_runtime,
    })
}

pub(super) fn drain_startup_output<R>(
    mut reader: R,
    output: Arc<Mutex<OpenCodeStartupOutput>>,
    stream: StartupStream,
) where
    R: Read + Send + 'static,
{
    let _ = thread::spawn(move || {
        let mut buffer = [0_u8; 1024];
        loop {
            let read = match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => read,
                Err(_) => break,
            };
            let chunk = String::from_utf8_lossy(&buffer[..read]);
            let mut output = output.lock().unwrap_or_else(|e| {
                tracing::warn!("Recovered poisoned lock for OpenCodeStartupOutput");
                e.into_inner()
            });
            match stream {
                StartupStream::Stdout => append_bounded(&mut output.stdout, &chunk),
                StartupStream::Stderr => append_bounded(&mut output.stderr, &chunk),
            }
        }
    });
}
