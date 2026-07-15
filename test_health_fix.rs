#[test]
fn test_classify_degraded_reason_provider_with_colon() {
    let reason1 = crate::cli::doctor::health::classify_degraded_reason("provider:prod-mini-01:customerA:pending_queue_depth:2");
    assert_eq!(reason1.summary, "provider prod-mini-01:customerA has pending queue depth 2");

    let reason2 = crate::cli::doctor::health::classify_degraded_reason("provider:prod-mini-01:customerA:disconnected");
    assert_eq!(reason2.summary, "provider prod-mini-01:customerA is disconnected");
}
