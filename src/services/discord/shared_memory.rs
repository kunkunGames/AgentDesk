use std::fs;

use super::runtime_store::shared_agent_memory_root;

/// Maximum number of recent notes to inject into the system prompt.
/// Keeps token cost bounded as notes accumulate over time.
const MAX_AGENT_NOTES: usize = 20;

/// Load agent-specific notes from {role_id}.json's notes[] field.
/// Returns formatted [Shared Agent Memory] section with the most recent N notes,
/// or None if empty.
pub(super) fn load_agent_notes(role_id: &str) -> Option<String> {
    let root = shared_agent_memory_root()?;
    let path = root.join(format!("{}.json", role_id));
    let content = fs::read_to_string(&path).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(&content).ok()?;
    let notes = parsed.get("notes")?.as_array()?;
    if notes.is_empty() {
        return None;
    }
    // Take only the most recent N notes (notes are appended chronologically,
    // so the tail contains the most recent entries).
    let recent = if notes.len() > MAX_AGENT_NOTES {
        &notes[notes.len() - MAX_AGENT_NOTES..]
    } else {
        notes.as_slice()
    };
    let mut lines = vec!["[Shared Agent Memory]".to_string()];
    for note in recent {
        if let Some(content) = note.get("content").and_then(|v| v.as_str()) {
            let source = note
                .get("source")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let created = note
                .get("created_at")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            lines.push(format!("- [{}|{}] {}", created, source, content));
        }
    }
    if lines.len() <= 1 {
        return None;
    }
    if notes.len() > MAX_AGENT_NOTES {
        lines.push(format!(
            "(showing {} of {} total notes)",
            MAX_AGENT_NOTES,
            notes.len()
        ));
    }
    Some(lines.join("\n"))
}

/// Read shared_knowledge.md from the shared_agent_memory directory.
/// Returns the file content wrapped in a [Shared Agent Knowledge] section,
/// or None if the file doesn't exist or is empty.
pub(super) fn load_shared_knowledge() -> Option<String> {
    let root = shared_agent_memory_root()?;
    let path = root.join("shared_knowledge.md");
    let content = fs::read_to_string(&path).ok()?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(format!("[Shared Agent Knowledge]\n{}", trimmed))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_temp_root<F>(f: F)
    where
        F: FnOnce(&std::path::Path),
    {
        let _guard = super::super::runtime_store::test_env_lock().lock().unwrap();
        let temp = tempfile::TempDir::new().unwrap();
        let root = temp.path().join(".agentdesk");
        let sam = root.join("shared_agent_memory");
        std::fs::create_dir_all(&sam).unwrap();
        let prev = std::env::var_os("AGENTDESK_ROOT_DIR");
        unsafe { std::env::set_var("AGENTDESK_ROOT_DIR", &root) };
        f(&sam);
        match prev {
            Some(v) => unsafe { std::env::set_var("AGENTDESK_ROOT_DIR", v) },
            None => unsafe { std::env::remove_var("AGENTDESK_ROOT_DIR") },
        }
    }

    #[test]
    fn test_load_agent_notes_empty_returns_none() {
        with_temp_root(|sam| {
            let json = serde_json::json!({"notes": []});
            std::fs::write(sam.join("test-agent.json"), json.to_string()).unwrap();
            assert!(load_agent_notes("test-agent").is_none());
        });
    }

    #[test]
    fn test_load_agent_notes_returns_formatted() {
        with_temp_root(|sam| {
            let json = serde_json::json!({
                "notes": [
                    {"content": "first note", "source": "agent-a", "created_at": "2026-03-23"},
                    {"content": "second note", "source": "agent-b", "created_at": "2026-03-24"}
                ]
            });
            std::fs::write(sam.join("test-agent.json"), json.to_string()).unwrap();
            let result = load_agent_notes("test-agent").unwrap();
            assert!(result.starts_with("[Shared Agent Memory]"));
            assert!(result.contains("first note"));
            assert!(result.contains("second note"));
            assert!(result.contains("agent-a"));
        });
    }

    #[test]
    fn test_load_agent_notes_limits_to_max() {
        with_temp_root(|sam| {
            let mut notes = Vec::new();
            for i in 0..30 {
                notes.push(serde_json::json!({
                    "content": format!("note-{}", i),
                    "source": "test",
                    "created_at": format!("2026-03-{:02}", (i % 28) + 1)
                }));
            }
            let json = serde_json::json!({"notes": notes});
            std::fs::write(sam.join("test-agent.json"), json.to_string()).unwrap();
            let result = load_agent_notes("test-agent").unwrap();
            // Should NOT contain early notes (0-9)
            assert!(!result.contains("note-0\n"));
            assert!(!result.contains("note-9\n"));
            // Should contain recent notes (10-29)
            assert!(result.contains("note-10"));
            assert!(result.contains("note-29"));
            // Should show truncation notice
            assert!(result.contains("showing 20 of 30 total notes"));
        });
    }

    #[test]
    fn test_load_shared_knowledge_empty_returns_none() {
        with_temp_root(|sam| {
            std::fs::write(sam.join("shared_knowledge.md"), "   ").unwrap();
            assert!(load_shared_knowledge().is_none());
        });
    }

    #[test]
    fn test_load_shared_knowledge_returns_wrapped() {
        with_temp_root(|sam| {
            std::fs::write(sam.join("shared_knowledge.md"), "Some knowledge").unwrap();
            let result = load_shared_knowledge().unwrap();
            assert_eq!(result, "[Shared Agent Knowledge]\nSome knowledge");
        });
    }
}
