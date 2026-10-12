//! Validate terminal-safe prompts and split literal delivery at Unicode boundaries.

pub(super) fn validate_prompt_text(input: &str) -> Result<(), String> {
    // Block terminal control channels such as ESC bracketed-paste markers,
    // DEL, and C1 controls before either literal send or tmux paste-buffer
    // delivery can relay them into the hosted Codex TUI. Mirrors
    // claude_tui::input::validate_prompt_text.
    if input
        .chars()
        .any(|ch| ch.is_control() && !matches!(ch, '\n' | '\r' | '\t'))
    {
        return Err("prompt contains unsupported terminal control characters".to_string());
    }
    Ok(())
}

pub(super) fn validate_prompt_not_empty(input: &str) -> Result<(), String> {
    if input.trim().is_empty() {
        return Err("prompt must contain non-whitespace text".to_string());
    }
    Ok(())
}

pub(super) fn split_literal_chunks(input: &str, max_chars: usize) -> Vec<String> {
    if input.is_empty() {
        return Vec::new();
    }
    let max_chars = max_chars.max(1);
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_chars = 0usize;
    for ch in input.chars() {
        if current_chars >= max_chars {
            chunks.push(std::mem::take(&mut current));
            current_chars = 0;
        }
        current.push(ch);
        current_chars += 1;
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}
