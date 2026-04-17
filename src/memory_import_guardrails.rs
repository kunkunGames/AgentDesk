pub(crate) const DIRECT_IMPORT_SHARED_AGENT_KNOWLEDGE_RELATIVE_PATH: &str =
    "memories/shared-agent-knowledge/shared_knowledge.md";
pub(crate) const DIRECT_IMPORT_SHARED_AGENT_MEMORY_RELATIVE_ROOT: &str =
    "memories/shared-agent-memory";
pub(crate) const DIRECT_IMPORT_LONG_TERM_RELATIVE_ROOT: &str = "memories/long-term";

pub(crate) const MANAGED_MEMORIES_DIR_NAME: &str = "memories";
pub(crate) const SHARED_AGENT_KNOWLEDGE_DIR_NAME: &str = "shared-agent-knowledge";
pub(crate) const SHARED_AGENT_KNOWLEDGE_FILE_NAME: &str = "shared_knowledge.md";
pub(crate) const SHARED_AGENT_MEMORY_DIR_NAME: &str = "shared-agent-memory";
pub(crate) const LONG_TERM_MEMORY_DIR_NAME: &str = "long-term";

#[allow(dead_code)]
pub(crate) const DIRECT_IMPORT_ALLOWED_RELATIVE_PATHS: &[&str] = &[
    DIRECT_IMPORT_SHARED_AGENT_KNOWLEDGE_RELATIVE_PATH,
    DIRECT_IMPORT_SHARED_AGENT_MEMORY_RELATIVE_ROOT,
    DIRECT_IMPORT_LONG_TERM_RELATIVE_ROOT,
];

#[allow(dead_code)]
pub(crate) const DIRECT_IMPORT_EXCLUDED_SURFACES: &[&str] = &[
    "sessions",
    "dispatched_sessions",
    "dispatches",
    "discord bindings",
    "raw DB files",
    "scheduler state",
    "prompts",
    "workspaces",
];

#[cfg(test)]
mod tests {
    use super::{
        DIRECT_IMPORT_ALLOWED_RELATIVE_PATHS, DIRECT_IMPORT_EXCLUDED_SURFACES,
        DIRECT_IMPORT_LONG_TERM_RELATIVE_ROOT, DIRECT_IMPORT_SHARED_AGENT_KNOWLEDGE_RELATIVE_PATH,
        DIRECT_IMPORT_SHARED_AGENT_MEMORY_RELATIVE_ROOT,
    };

    #[test]
    fn direct_import_allowlist_matches_runtime_memory_artifacts() {
        assert_eq!(
            DIRECT_IMPORT_ALLOWED_RELATIVE_PATHS,
            &[
                DIRECT_IMPORT_SHARED_AGENT_KNOWLEDGE_RELATIVE_PATH,
                DIRECT_IMPORT_SHARED_AGENT_MEMORY_RELATIVE_ROOT,
                DIRECT_IMPORT_LONG_TERM_RELATIVE_ROOT,
            ]
        );
    }

    #[test]
    fn direct_import_exclusion_set_blocks_operational_state() {
        assert!(DIRECT_IMPORT_EXCLUDED_SURFACES.contains(&"sessions"));
        assert!(DIRECT_IMPORT_EXCLUDED_SURFACES.contains(&"dispatches"));
        assert!(DIRECT_IMPORT_EXCLUDED_SURFACES.contains(&"raw DB files"));
        assert!(DIRECT_IMPORT_EXCLUDED_SURFACES.contains(&"workspaces"));
    }
}
