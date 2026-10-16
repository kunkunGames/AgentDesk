//! Routine schedule, initial state, timeout and retry validation.

use crate::services::scheduling::validate_schedule;
use anyhow::{Result, anyhow};

pub(super) fn validate_execution_strategy(strategy: &str) -> Result<()> {
    match strategy {
        "fresh" | "persistent" => Ok(()),
        other => Err(anyhow!(
            "unsupported routine execution_strategy '{other}'; expected fresh or persistent"
        )),
    }
}

pub fn validate_routine_schedule(schedule: &str) -> Result<()> {
    validate_schedule(schedule)
}

pub(super) fn normalize_schedule(schedule: Option<String>) -> Result<Option<String>> {
    schedule
        .map(|schedule| {
            let schedule = schedule.trim().to_string();
            validate_routine_schedule(&schedule)?;
            Ok(schedule)
        })
        .transpose()
}

pub(super) fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(super) fn normalize_new_routine_status(status: Option<&str>) -> Result<&'static str> {
    match status.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("enabled") => Ok("enabled"),
        Some("paused") => Ok("paused"),
        Some(other) => Err(anyhow!(
            "routine initial status must be enabled or paused, got {other}"
        )),
    }
}

pub(super) fn validate_timeout_secs(timeout_secs: Option<i32>) -> Result<()> {
    if let Some(value) = timeout_secs
        && value <= 0
    {
        return Err(anyhow!("routine timeout_secs must be greater than zero"));
    }
    Ok(())
}

pub(super) fn validate_max_retries(max_retries: Option<i32>) -> Result<()> {
    if let Some(value) = max_retries
        && value < 0
    {
        return Err(anyhow!(
            "routine max_retries must be greater than or equal to zero"
        ));
    }
    Ok(())
}
