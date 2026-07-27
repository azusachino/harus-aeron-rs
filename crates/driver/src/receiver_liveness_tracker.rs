//! Tracks which remote receivers are currently live for a network publication.
//!
//! Mirrors `io.aeron.driver.ReceiverLivenessTracker.java`. A receiver is considered live from the
//! last Status Message it sent until it either closes or its last SM ages past the liveness
//! timeout. A publication uses this to decide whether it still has any connected subscribers.

use std::collections::HashMap;

/// Per-publication liveness map keyed by receiver id, holding each receiver's last SM timestamp.
#[derive(Debug, Default)]
pub struct ReceiverLivenessTracker {
    last_sm_timestamp_ns_by_receiver_id: HashMap<i64, i64>,
}

impl ReceiverLivenessTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether any receiver is currently tracked as live.
    pub fn has_receivers(&self) -> bool {
        !self.last_sm_timestamp_ns_by_receiver_id.is_empty()
    }

    /// Record a Status Message from `receiver_id`, refreshing its liveness deadline.
    pub fn on_status_message(&mut self, receiver_id: i64, now_ns: i64) {
        self.last_sm_timestamp_ns_by_receiver_id
            .insert(receiver_id, now_ns);
    }

    /// Drop a receiver that closed; returns whether it had been tracked.
    pub fn on_remote_close(&mut self, receiver_id: i64) -> bool {
        self.last_sm_timestamp_ns_by_receiver_id
            .remove(&receiver_id)
            .is_some()
    }

    /// Evict receivers whose last Status Message is at or beyond the liveness timeout.
    pub fn on_idle(&mut self, now_ns: i64, timeout_ns: i64) {
        let timeout_threshold_ns = now_ns - timeout_ns;
        self.last_sm_timestamp_ns_by_receiver_id
            .retain(|_, &mut last_sm_ns| last_sm_ns > timeout_threshold_ns);
    }
}

#[cfg(test)]
mod tests {
    //! Faithful port of
    //! vendor/aeron/aeron-driver/src/test/java/io/aeron/driver/ReceiverLivenessTrackerTest.java

    use super::*;

    const RECEIVER_ID: i64 = 10001;
    const NOW_NS: i64 = 10_000_000_000;
    const TIMEOUT_NS: i64 = 5_000_000;

    #[test]
    fn should_not_be_live_if_no_receivers_added() {
        let tracker = ReceiverLivenessTracker::new();
        assert!(!tracker.has_receivers());
    }

    #[test]
    fn should_live_if_one_receiver_added() {
        let mut tracker = ReceiverLivenessTracker::new();
        tracker.on_status_message(RECEIVER_ID, NOW_NS);
        assert!(tracker.has_receivers());
    }

    #[test]
    fn should_be_live_if_receiver_not_timed_out() {
        let mut tracker = ReceiverLivenessTracker::new();
        tracker.on_status_message(RECEIVER_ID, NOW_NS);
        tracker.on_idle(NOW_NS + (TIMEOUT_NS - 1), TIMEOUT_NS);
        assert!(tracker.has_receivers());
    }

    #[test]
    fn should_not_be_live_if_receiver_timed_out_exactly() {
        let mut tracker = ReceiverLivenessTracker::new();
        tracker.on_status_message(RECEIVER_ID, NOW_NS);
        tracker.on_idle(NOW_NS + TIMEOUT_NS, TIMEOUT_NS);
        assert!(!tracker.has_receivers());
    }

    #[test]
    fn should_not_be_live_if_receiver_timed_out_after() {
        let mut tracker = ReceiverLivenessTracker::new();
        tracker.on_status_message(RECEIVER_ID, NOW_NS);
        tracker.on_idle(NOW_NS + (TIMEOUT_NS + 1), TIMEOUT_NS);
        assert!(!tracker.has_receivers());
    }

    #[test]
    fn should_not_be_live_if_receiver_removed() {
        let mut tracker = ReceiverLivenessTracker::new();
        tracker.on_status_message(10001, NOW_NS);
        tracker.on_status_message(10002, NOW_NS);
        tracker.on_status_message(10003, NOW_NS);

        tracker.on_remote_close(10001);
        assert!(tracker.has_receivers());

        tracker.on_remote_close(10002);
        assert!(tracker.has_receivers());

        tracker.on_remote_close(10003);
        assert!(!tracker.has_receivers());
    }

    #[test]
    fn should_return_false_if_already_removed() {
        let mut tracker = ReceiverLivenessTracker::new();
        tracker.on_status_message(10001, NOW_NS);
        tracker.on_status_message(10002, NOW_NS);
        tracker.on_status_message(10003, NOW_NS);

        assert!(tracker.on_remote_close(10001));
        assert!(!tracker.on_remote_close(10001));
        assert!(tracker.has_receivers());

        assert!(tracker.on_remote_close(10002));
        assert!(!tracker.on_remote_close(10002));
        assert!(tracker.has_receivers());

        assert!(tracker.on_remote_close(10003));
        assert!(!tracker.on_remote_close(10003));
        assert!(!tracker.has_receivers());
    }
}
