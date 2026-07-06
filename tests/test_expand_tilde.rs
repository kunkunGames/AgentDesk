use std::path::PathBuf;

fn expand_tilde(path: &str) -> String {
    if path == "~" || path.starts_with("~/") || path.starts_with("~\\") {
        if let Some(expanded) = crate::runtime_layout::expand_user_path(path) {
            return expanded.to_string_lossy().into_owned();
        }
    }
    path.to_string()
}
