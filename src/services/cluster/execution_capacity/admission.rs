//! Bind managed policy and resource identity to the existing execution context.
use super::*;

pub(super) fn limits(
    pool: &PgPool,
    provider: &str,
    channel: &str,
) -> Result<Option<ExecutionLimits>, String> {
    // Capture the blocking thread's context before entering the async lookup.
    // Discord threads and headless/Routine turns already resolve their agent;
    // looking up only the execution channel would lose that parent's policy.
    let agent = crate::services::platform::active_provider_context(provider)
        .and_then(|context| context.agent_id);
    tokio::runtime::Handle::current().block_on(async {
        tokio::time::timeout(LEASE_IO_TIMEOUT, async {
            let policy = if let Some(agent) = agent.as_deref()
                && let Some(value) = super::super::execution_requirements::get(pool, agent)
                    .await
                    .map_err(|error| format!("execution policy lookup: {error}"))?
            {
                // A present canonical policy (including an empty one) wins.
                // Invalid policy must fail, never fall back to another agent.
                super::super::execution_requirements::ExecutionRequirements::parse(value)?
            } else {
                // Preserve channel lookup for legacy callers/roles that have
                // no corresponding canonical agent row in PostgreSQL.
                super::super::execution_requirements::for_channel(pool, channel).await?
            };
            Ok(policy.execution_limits)
        })
        .await
        .map_err(|_| "execution policy lookup timed out".to_string())?
    })
}

pub(super) fn resource(
    working_dir: &str,
    remote: bool,
    managed: bool,
) -> Result<Option<String>, String> {
    if remote {
        return if managed {
            Err("managed execution requires a verified local resource; remote admission is not supported".into())
        } else {
            // A remote workspace is not a path on the AgentDesk host.
            Ok(None)
        };
    }
    match std::fs::canonicalize(working_dir) {
        Ok(path) => Ok(Some(path.to_string_lossy().into_owned())),
        Err(error) if managed => Err(format!("execution workspace cannot be resolved: {error}")),
        // Existing providers own their legacy workspace validation/creation.
        Err(_) => Ok(None),
    }
}
