//! Process watchdog on an OS thread, independent of the runtime it watches.

use std::time::{Duration, Instant};

mod driver;
mod logging;
mod policy;
#[cfg(test)]
mod tests;

pub(crate) const CHECK_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const TCP_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const MAX_FAILURES: u32 = 3;
pub(crate) const STARTUP_GRACE: Duration = Duration::from_secs(30);
pub(crate) const BLACKOUT_LIMIT: Duration = Duration::from_secs(300);
pub(crate) const DUMP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WatchdogMode {
    Enforce,
    Observe,
}

impl WatchdogMode {
    fn parse(value: Option<&str>) -> Self {
        match value {
            Some("observe") => Self::Observe,
            _ => Self::Enforce,
        }
    }
}

pub fn spawn_watchdog(port: u16) {
    let armed = crate::services::hang_forensics::spawn_runtime_liveness_beacon();
    let mode = WatchdogMode::parse(
        std::env::var("AGENTDESK_SELF_WATCHDOG_MODE")
            .ok()
            .as_deref(),
    );
    spawn_watchdog_thread(port, armed, mode);
}

fn spawn_watchdog_thread(
    port: u16,
    armed: crate::services::hang_forensics::BeaconArmed,
    mode: WatchdogMode,
) {
    match armed.boot_report() {
        Ok(line) => tracing::info!("{line}"),
        Err(line) => tracing::error!("{line}"),
    }
    std::thread::Builder::new()
        .name("health-watchdog".into())
        .spawn(move || {
            let first_check_at = Instant::now() + STARTUP_GRACE + CHECK_INTERVAL;
            let dump = if mode == WatchdogMode::Enforce {
                driver::prepare_dump(capture_dump)
            } else {
                None
            };
            let logger = logging::PreparedLog::spawn(mode)
                .map_err(|error| {
                    tracing::warn!("watchdog: diagnostic helper could not start ({error})");
                })
                .ok();
            // Prepare within the existing grace, before starting the blackout clock.
            std::thread::sleep(first_check_at.saturating_duration_since(Instant::now()));
            let loopback = crate::config::loopback();
            let addr = format!("{loopback}:{port}");
            let started = Instant::now();
            driver::run(
                &policy::WatchdogPolicy::DEFAULT,
                mode,
                policy::WatchdogState::new(started),
                started,
                driver::Hooks {
                    probe: |deadline| driver::tcp_probe(&addr, &loopback, deadline),
                    snapshot: crate::services::hang_forensics::snapshot,
                    dump,
                    log: |state: &policy::WatchdogState,
                          decision: &policy::Decision,
                          sample: Option<&driver::Sample>,
                          decided_at| {
                        if let Some(logger) = &logger {
                            logger.record(*state, *decision, sample.cloned(), decided_at);
                        }
                    },
                    exit: |_, _| std::process::exit(1),
                },
            );
        })
        .expect("Failed to spawn watchdog thread");
}

fn capture_dump() -> Result<(), String> {
    let pid = std::process::id();
    let dump_dir = crate::agentdesk_runtime_root()
        .map(|root| root.join("logs"))
        .unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&dump_dir).map_err(|error| error.to_string())?;
    let path = dump_dir.join(format!(
        "adk-hang-{pid}-{}.txt",
        chrono::Local::now().format("%Y%m%d-%H%M%S")
    ));
    crate::services::platform::capture_process_dump(pid, &path.to_string_lossy())?;
    tracing::warn!("watchdog: dump saved to {}", path.display());
    Ok(())
}
