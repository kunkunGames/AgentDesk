fn main() {
    let raw = "provider:prod-mini-01:customerA:disconnected";
    if let Some(stripped) = raw.strip_prefix("provider:") {
        let parts: Vec<&str> = stripped.rsplitn(2, ':').collect();
        println!("rsplitn 2 parts: {:?}", parts);
        // For "prod-mini-01:customerA:disconnected":
        // rsplitn(2, ':') -> ["disconnected", "prod-mini-01:customerA"]
    }
}
