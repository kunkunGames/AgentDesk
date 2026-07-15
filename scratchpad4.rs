fn main() {
    let raw = "provider:codex:pending_queue_depth:2";
    if let Some(stripped) = raw.strip_prefix("provider:") {
        let parts: Vec<&str> = stripped.rsplitn(3, ':').collect();
        println!("rsplitn 3 parts: {:?}", parts);
    }
}
