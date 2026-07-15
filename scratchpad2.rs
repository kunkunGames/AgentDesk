fn main() {
    let raw = "provider:prod-mini-01:customerA:pending_queue_depth:2";
    if let Some(stripped) = raw.strip_prefix("provider:") {
        let parts: Vec<&str> = stripped.rsplitn(3, ':').collect();
        println!("rsplitn 3 parts: {:?}", parts);

        // This splits from right to left.
        // For "prod-mini-01:customerA:pending_queue_depth:2":
        // rsplitn(3, ':') -> ["2", "pending_queue_depth", "prod-mini-01:customerA"]
    }
}
