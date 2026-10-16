//! Retry transport failures only inside the last conservatively known lease.

use super::{LEASE_IO_TIMEOUT, LEASE_RENEW_INTERVAL, LEASE_SECONDS};
use std::time::{Duration, Instant};

pub(super) struct RenewalWindow {
    valid_until: Instant,
    next_delay: Duration,
}

impl RenewalWindow {
    pub(super) fn admitted(started: Instant) -> Self {
        Self {
            valid_until: started + Duration::from_secs(LEASE_SECONDS as u64),
            next_delay: LEASE_RENEW_INTERVAL,
        }
    }

    pub(super) fn delay(&self, now: Instant) -> Duration {
        self.next_delay
            .min(self.valid_until.saturating_duration_since(now))
    }

    pub(super) fn io_budget(&self, now: Instant) -> Duration {
        LEASE_IO_TIMEOUT.min(self.valid_until.saturating_duration_since(now))
    }

    pub(super) fn renewed(&mut self, request_started: Instant) {
        // The database extends from its own clock during the request. Starting
        // locally before I/O is conservative and never grants extra lease time.
        self.valid_until = request_started + Duration::from_secs(LEASE_SECONDS as u64);
        self.next_delay = LEASE_RENEW_INTERVAL;
    }

    pub(super) fn retry_transport_error(&mut self, now: Instant) -> bool {
        self.next_delay = Duration::from_secs(1);
        now < self.valid_until
    }
}

#[cfg(test)]
mod tests;
