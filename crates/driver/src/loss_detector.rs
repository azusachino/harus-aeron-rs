//! Gap loss detection with feedback-delayed, retry-backed NAK timing.
//!
//! Mirrors `io.aeron.driver.LossDetector.java` and the unicast `StaticDelayGenerator`. A gap is not
//! NAK'd the instant it is observed: the detector waits one feedback delay before the first NAK,
//! then re-NAKs on a retry cadence for as long as the same gap persists. This avoids the packet
//! storm an eager "NAK on every out-of-order frame" produces when a burst arrives reordered.

/// A contiguous run of missing frames at the front of the reconstructed stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gap {
    pub term_id: i32,
    pub term_offset: i32,
    pub length: i32,
}

/// Upstream `Configuration.NAK_UNICAST_DELAY_DEFAULT_NS`: 1us feedback delay before the first NAK.
const NAK_UNICAST_DELAY_NS: i64 = 1_000;
/// Upstream `Configuration.NAK_UNICAST_RETRY_DELAY_RATIO_DEFAULT`: retry delay = delay * ratio.
const NAK_UNICAST_RETRY_DELAY_RATIO: i64 = 100;

/// A fixed initial/retry delay pair (upstream `StaticDelayGenerator`).
#[derive(Debug, Clone, Copy)]
pub struct StaticDelayGenerator {
    delay_ns: i64,
    retry_delay_ns: i64,
}

impl StaticDelayGenerator {
    /// The unicast defaults the driver builds for a `PublicationImage` on a unicast channel.
    pub fn unicast() -> Self {
        Self {
            delay_ns: NAK_UNICAST_DELAY_NS,
            retry_delay_ns: NAK_UNICAST_DELAY_NS * NAK_UNICAST_RETRY_DELAY_RATIO,
        }
    }

    fn generate_delay_ns(&self) -> i64 {
        self.delay_ns
    }

    fn retry_delay_ns(&self) -> i64 {
        self.retry_delay_ns
    }
}

/// Tracks a single run of a gap and decides when a NAK is due.
///
/// Each detector only signals one gap at a time (upstream: "each detector only notifies a single
/// run of a gap in a message stream").
#[derive(Debug)]
pub struct LossDetector {
    delay_generator: StaticDelayGenerator,
    active_gap: Option<Gap>,
    deadline_ns: i64,
}

impl LossDetector {
    pub fn unicast() -> Self {
        Self {
            delay_generator: StaticDelayGenerator::unicast(),
            active_gap: None,
            deadline_ns: 0,
        }
    }

    /// Reconcile the detector with the currently observed gap and return a NAK if one is due.
    ///
    /// Mirrors `LossDetector.scan` + `checkTimerExpiry`: a newly observed gap is armed with a
    /// feedback delay; once the deadline passes the gap is returned to NAK and the deadline is
    /// pushed out by the retry delay; `None` clears the detector when the gap has been repaired.
    pub fn scan(&mut self, gap: Option<Gap>, now_ns: i64) -> Option<Gap> {
        match gap {
            None => {
                self.active_gap = None;
                None
            }
            Some(observed) => {
                if self.active_gap != Some(observed) {
                    self.active_gap = Some(observed);
                    self.deadline_ns = now_ns + self.delay_generator.generate_delay_ns();
                    return None;
                }
                if self.deadline_ns - now_ns <= 0 {
                    self.deadline_ns = now_ns + self.delay_generator.retry_delay_ns();
                    return Some(observed);
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Cross-checked against:
    // vendor/aeron/aeron-driver/src/test/java/io/aeron/driver/LossDetectorTest.java

    fn gap() -> Gap {
        Gap {
            term_id: 3,
            term_offset: 128,
            length: 64,
        }
    }

    #[test]
    fn does_not_nak_a_freshly_observed_gap() {
        let mut detector = LossDetector::unicast();
        // Armed with the feedback delay; nothing is due yet.
        assert_eq!(detector.scan(Some(gap()), 0), None);
    }

    #[test]
    fn naks_once_the_feedback_delay_elapses_then_backs_off_for_retries() {
        let mut detector = LossDetector::unicast();
        assert_eq!(detector.scan(Some(gap()), 0), None);

        // Just short of the 1us feedback delay: still nothing.
        assert_eq!(detector.scan(Some(gap()), NAK_UNICAST_DELAY_NS - 1), None);
        // At the deadline the NAK fires.
        assert_eq!(
            detector.scan(Some(gap()), NAK_UNICAST_DELAY_NS),
            Some(gap())
        );

        // The retry delay (delay * ratio) now gates the next NAK.
        let retry = NAK_UNICAST_DELAY_NS * NAK_UNICAST_RETRY_DELAY_RATIO;
        let fired_at = NAK_UNICAST_DELAY_NS;
        assert_eq!(detector.scan(Some(gap()), fired_at + retry - 1), None);
        assert_eq!(detector.scan(Some(gap()), fired_at + retry), Some(gap()));
    }

    #[test]
    fn repairing_a_gap_clears_the_detector_and_rearms_the_next() {
        let mut detector = LossDetector::unicast();
        assert_eq!(detector.scan(Some(gap()), 0), None);
        assert_eq!(
            detector.scan(Some(gap()), NAK_UNICAST_DELAY_NS),
            Some(gap())
        );

        // Gap repaired.
        assert_eq!(detector.scan(None, NAK_UNICAST_DELAY_NS + 1), None);

        // A new gap is re-armed from scratch, not fired immediately.
        let later = Gap {
            term_offset: 512,
            ..gap()
        };
        assert_eq!(detector.scan(Some(later), NAK_UNICAST_DELAY_NS + 1), None);
        assert_eq!(
            detector.scan(Some(later), NAK_UNICAST_DELAY_NS + 1 + NAK_UNICAST_DELAY_NS),
            Some(later)
        );
    }
}
