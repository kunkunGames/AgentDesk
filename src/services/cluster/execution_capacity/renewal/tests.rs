use super::*;

#[test]
fn transient_database_failure_retries_inside_original_lease_without_extending_it() {
    let start = Instant::now();
    let mut window = RenewalWindow::admitted(start);
    let failed_at = start + Duration::from_secs(15);
    assert!(window.retry_transport_error(failed_at));
    assert_eq!(window.delay(failed_at), Duration::from_secs(1));
    assert_eq!(
        window.io_budget(start + Duration::from_secs(28)),
        Duration::from_secs(2)
    );
    assert!(!window.retry_transport_error(start + Duration::from_secs(30)));
    assert_eq!(
        window.io_budget(start + Duration::from_secs(31)),
        Duration::ZERO
    );
}

#[test]
fn successful_renewal_uses_request_start_instead_of_delayed_response_time() {
    let start = Instant::now();
    let mut window = RenewalWindow::admitted(start);
    let request_started = start + Duration::from_secs(10);
    window.renewed(request_started);
    assert_eq!(
        window.delay(start + Duration::from_secs(16)),
        LEASE_RENEW_INTERVAL
    );
    assert_eq!(
        window.io_budget(start + Duration::from_secs(39)),
        Duration::from_secs(1)
    );
    assert!(!window.retry_transport_error(start + Duration::from_secs(40)));
}

#[test]
fn exhausted_window_never_starts_new_lease_io() {
    let start = Instant::now();
    let window = RenewalWindow::admitted(start);
    assert_eq!(
        window.delay(start + Duration::from_secs(60)),
        Duration::ZERO
    );
    assert_eq!(
        window.io_budget(start + Duration::from_secs(60)),
        Duration::ZERO
    );
}
