#[test]
fn test_classify_degraded_reason_provider_with_colon() {
    let reason = crate::cli::doctor::health::classify_degraded_reason("provider:prod-mini-01:customerA:pending_queue_depth:2");
    println!("{:?}", reason);
}
