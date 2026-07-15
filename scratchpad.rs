fn main() {
    let raw = "provider:prod-mini-01:customerA:pending_queue_depth:2";
    let parts: Vec<&str> = raw.split(':').collect();
    println!("{:?}", parts);
}
