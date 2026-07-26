//! Driver status counters used by duty-cycle instrumentation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

/// Tracks the maximum observed duty-cycle duration and counts measurements beyond a threshold.
pub struct DutyCycleStallTracker {
    max_cycle_time_ns: Arc<AtomicI64>,
    cycle_time_threshold_exceeded_count: Arc<AtomicI64>,
    closed: Arc<AtomicBool>,
    cycle_time_threshold_ns: i64,
}

impl DutyCycleStallTracker {
    pub fn new(
        max_cycle_time_ns: Arc<AtomicI64>,
        cycle_time_threshold_exceeded_count: Arc<AtomicI64>,
        cycle_time_threshold_ns: i64,
    ) -> Self {
        Self {
            max_cycle_time_ns,
            cycle_time_threshold_exceeded_count,
            closed: Arc::new(AtomicBool::new(false)),
            cycle_time_threshold_ns,
        }
    }

    pub fn report_measurement(&self, measurement_ns: i64) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.max_cycle_time_ns
            .fetch_max(measurement_ns, Ordering::Release);
        if measurement_ns > self.cycle_time_threshold_ns {
            self.cycle_time_threshold_exceeded_count
                .fetch_add(1, Ordering::Release);
        }
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_maximum_and_only_counts_threshold_exceedances() {
        let max_cycle_time = Arc::new(AtomicI64::new(0));
        let exceeded = Arc::new(AtomicI64::new(0));
        let tracker = DutyCycleStallTracker::new(max_cycle_time.clone(), exceeded.clone(), 1000);
        tracker.report_measurement(555);
        tracker.report_measurement(1000);
        tracker.report_measurement(1001);
        assert_eq!(max_cycle_time.load(Ordering::Acquire), 1001);
        assert_eq!(exceeded.load(Ordering::Acquire), 1);
    }

    #[test]
    fn closed_tracker_does_not_update_counters() {
        let max_cycle_time = Arc::new(AtomicI64::new(0));
        let exceeded = Arc::new(AtomicI64::new(0));
        let tracker = DutyCycleStallTracker::new(max_cycle_time.clone(), exceeded.clone(), 1);
        tracker.close();
        tracker.report_measurement(555);
        assert_eq!(max_cycle_time.load(Ordering::Acquire), 0);
        assert_eq!(exceeded.load(Ordering::Acquire), 0);
    }
}
