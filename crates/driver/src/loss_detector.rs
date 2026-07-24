//! Gap loss detection with feedback-delayed, retry-backed NAK timing.
//!
//! Mirrors `io.aeron.driver.LossDetector.java` and `StaticDelayGenerator`. Each detector tracks a
//! single run of a gap in the rebuilt term buffer: a newly observed gap is armed with a feedback
//! delay before the first NAK, then re-NAK'd on a retry cadence until the gap is repaired. This
//! avoids the packet storm an eager "NAK on every out-of-order frame" produces.

use aeron_client::logbuffer::TermGapScanner;
use agrona::AtomicBuffer;

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
    /// A generator using `delay_ns` for both the initial and the retry delay.
    pub fn new(delay_ns: i64) -> Self {
        Self {
            delay_ns,
            retry_delay_ns: delay_ns,
        }
    }

    /// A generator with a distinct retry delay.
    pub fn with_retry(delay_ns: i64, retry_delay_ns: i64) -> Self {
        Self {
            delay_ns,
            retry_delay_ns,
        }
    }

    /// The unicast defaults the driver builds for a `PublicationImage` on a unicast channel.
    pub fn unicast() -> Self {
        Self::with_retry(
            NAK_UNICAST_DELAY_NS,
            NAK_UNICAST_DELAY_NS * NAK_UNICAST_RETRY_DELAY_RATIO,
        )
    }

    /// The initial delay before the first feedback (NAK / retransmit).
    pub fn generate_delay_ns(&self) -> i64 {
        self.delay_ns
    }

    fn retry_delay_ns(&self) -> i64 {
        self.retry_delay_ns
    }
}

/// Sentinel matching upstream `Aeron.NULL_VALUE` for an unset deadline / gap offset.
const NULL_VALUE: i64 = -1;

/// Pack a scan's rebuild offset and loss-found flag into one word (upstream `LossDetector.pack`).
pub fn pack(rebuild_offset: i32, loss_found: bool) -> u64 {
    ((rebuild_offset as u32 as u64) << 32) | (loss_found as u64)
}

/// Whether the packed scan outcome found loss (upstream `LossDetector.lossFound`).
pub fn loss_found(scan_outcome: u64) -> bool {
    (scan_outcome as u32) != 0
}

/// The offset up to which the log has been rebuilt (upstream `LossDetector.rebuildOffset`).
pub fn rebuild_offset(scan_outcome: u64) -> i32 {
    (scan_outcome >> 32) as u32 as i32
}

/// Tracks a single run of a gap and decides when a NAK is due.
///
/// Each detector only signals one gap at a time (upstream: "each detector only notifies a single
/// run of a gap in a message stream").
#[derive(Debug)]
pub struct LossDetector {
    delay_generator: StaticDelayGenerator,
    deadline_ns: i64,
    scanned_term_id: i32,
    scanned_term_offset: i32,
    scanned_length: i32,
    active_term_id: i32,
    active_term_offset: i32,
    active_length: i32,
}

impl LossDetector {
    /// Build a detector with the given delay generator (upstream constructor).
    pub fn new(delay_generator: StaticDelayGenerator) -> Self {
        Self {
            delay_generator,
            deadline_ns: NULL_VALUE,
            scanned_term_id: 0,
            scanned_term_offset: -1,
            scanned_length: 0,
            active_term_id: 0,
            active_term_offset: -1,
            active_length: 0,
        }
    }

    /// A detector with the unicast delay defaults.
    pub fn unicast() -> Self {
        Self::new(StaticDelayGenerator::unicast())
    }

    /// Scan for gaps between `rebuild_position` and `hwm_position` and NAK a due gap.
    ///
    /// Mirrors `LossDetector.scan`: the term buffer is scanned for the first gap; a newly seen gap
    /// is armed with the feedback delay, and once its deadline passes `on_gap` is invoked and the
    /// deadline is pushed out by the retry delay. Returns the packed rebuild offset + loss flag.
    #[allow(clippy::too_many_arguments)]
    pub fn scan(
        &mut self,
        term_buffer: &AtomicBuffer,
        rebuild_position: i64,
        hwm_position: i64,
        now_ns: i64,
        term_length_mask: i32,
        position_bits_to_shift: i32,
        initial_term_id: i32,
        on_gap: &mut dyn FnMut(i32, i32, i32),
    ) -> u64 {
        let mut loss_found = false;
        let mut rebuild_offset = (rebuild_position & term_length_mask as i64) as i32;

        if rebuild_position < hwm_position {
            let rebuild_term_count = (rebuild_position >> position_bits_to_shift) as i32;
            let hwm_term_count = (hwm_position >> position_bits_to_shift) as i32;

            let rebuild_term_id = initial_term_id.wrapping_add(rebuild_term_count);
            let hwm_term_offset = (hwm_position & term_length_mask as i64) as i32;
            let limit_offset = if rebuild_term_count == hwm_term_count {
                hwm_term_offset
            } else {
                term_length_mask + 1
            };

            let mut scanned: Option<(i32, i32, i32)> = None;
            rebuild_offset = TermGapScanner::scan_for_gap(
                term_buffer,
                rebuild_term_id,
                rebuild_offset as usize,
                limit_offset as usize,
                |term_id, offset, length| {
                    scanned = Some((term_id, offset as i32, length as i32));
                },
            ) as i32;
            if let Some((term_id, offset, length)) = scanned {
                self.scanned_term_id = term_id;
                self.scanned_term_offset = offset;
                self.scanned_length = length;
            }

            if rebuild_offset < limit_offset {
                if self.scanned_term_offset != self.active_term_offset
                    || self.scanned_term_id != self.active_term_id
                    || self.scanned_length != self.active_length
                {
                    self.activate_gap(now_ns);
                    loss_found = true;
                }

                self.check_timer_expiry(now_ns, on_gap);
            }
        }

        pack(rebuild_offset, loss_found)
    }

    fn activate_gap(&mut self, now_ns: i64) {
        self.active_term_id = self.scanned_term_id;
        self.active_term_offset = self.scanned_term_offset;
        self.active_length = self.scanned_length;
        self.deadline_ns = now_ns + self.delay_generator.generate_delay_ns();
    }

    fn check_timer_expiry(&mut self, now_ns: i64, on_gap: &mut dyn FnMut(i32, i32, i32)) {
        if self.deadline_ns - now_ns <= 0 {
            on_gap(
                self.active_term_id,
                self.active_term_offset,
                self.active_length,
            );
            self.deadline_ns = now_ns + self.delay_generator.retry_delay_ns();
        }
    }
}

#[cfg(test)]
mod tests {
    //! Faithful port of
    //! vendor/aeron/aeron-driver/src/test/java/io/aeron/driver/LossDetectorTest.java

    use super::*;
    use aeron_client::logbuffer::{
        FRAME_ALIGNMENT, TERM_MIN_LENGTH, TermRebuilder, align, compute_position,
        position_bits_to_shift,
    };
    use aeron_client::protocol::{DATA_HEADER_LENGTH, DataHeaderFlyweight};

    const TERM_BUFFER_LENGTH: i32 = TERM_MIN_LENGTH;
    const MASK: i32 = TERM_BUFFER_LENGTH - 1;
    const DATA_LENGTH: usize = 36;
    const MESSAGE_LENGTH: usize = DATA_HEADER_LENGTH + DATA_LENGTH;
    const SESSION_ID: i32 = 0x5E55101D;
    const STREAM_ID: i32 = 0x000C400E;
    const TERM_ID: i32 = 0x000EE81D;

    const MS_20: i64 = 20_000_000;
    const MS_200: i64 = 200_000_000;

    fn bits() -> i32 {
        position_bits_to_shift(TERM_BUFFER_LENGTH as usize) as i32
    }

    fn aligned_frame_length() -> i32 {
        align(MESSAGE_LENGTH as i32, FRAME_ALIGNMENT)
    }

    fn active_term_position() -> i64 {
        compute_position(TERM_ID, 0, bits(), TERM_ID)
    }

    fn offset_of_message(index: i32) -> i32 {
        index * aligned_frame_length()
    }

    fn insert_data_frame(term_buffer: &AtomicBuffer, offset: i32) {
        let mut packet = vec![0u8; MESSAGE_LENGTH];
        {
            let header = DataHeaderFlyweight::new(AtomicBuffer::new(&mut packet));
            header
                .set_frame_length(MESSAGE_LENGTH as i32)
                .set_term_offset(offset)
                .set_session_id(SESSION_ID)
                .set_stream_id(STREAM_ID)
                .set_term_id(TERM_ID);
        }
        for (i, byte) in packet.iter_mut().skip(DATA_HEADER_LENGTH).enumerate() {
            *byte = i as u8;
        }
        TermRebuilder::insert(
            term_buffer,
            offset as usize,
            &AtomicBuffer::new(&mut packet),
            MESSAGE_LENGTH,
        );
    }

    /// Test harness mirroring the JUnit fixture: a term buffer, a detector, and captured NAKs.
    struct Fixture {
        term_bytes: Vec<u8>,
        detector: LossDetector,
        calls: Vec<(i32, i32, i32)>,
    }

    impl Fixture {
        fn with_delay(generator: StaticDelayGenerator) -> Self {
            Self {
                term_bytes: vec![0u8; TERM_BUFFER_LENGTH as usize],
                detector: LossDetector::new(generator),
                calls: Vec::new(),
            }
        }

        fn new() -> Self {
            Self::with_delay(StaticDelayGenerator::new(MS_20))
        }

        fn long_retry() -> Self {
            Self::with_delay(StaticDelayGenerator::with_retry(MS_20, MS_200))
        }

        fn term_buffer(&mut self) -> AtomicBuffer<'_> {
            AtomicBuffer::new(&mut self.term_bytes)
        }

        fn insert(&mut self, offset: i32) {
            let buffer = self.term_buffer();
            insert_data_frame(&buffer, offset);
        }

        fn scan(&mut self, rebuild_position: i64, hwm_position: i64, now_ns: i64) -> u64 {
            // SAFETY-free split of the two &mut self fields the scan touches.
            let Fixture {
                term_bytes,
                detector,
                calls,
            } = self;
            let term_buffer = AtomicBuffer::new(term_bytes);
            detector.scan(
                &term_buffer,
                rebuild_position,
                hwm_position,
                now_ns,
                MASK,
                bits(),
                TERM_ID,
                &mut |term_id, offset, length| calls.push((term_id, offset, length)),
            )
        }

        fn count(&self, expected: (i32, i32, i32)) -> usize {
            self.calls.iter().filter(|&&call| call == expected).count()
        }
    }

    #[test]
    fn should_not_send_nak_when_buffer_is_empty() {
        let mut fixture = Fixture::new();
        let rebuild = active_term_position();
        let hwm = active_term_position();

        fixture.scan(rebuild, hwm, 0);
        fixture.scan(rebuild, hwm, 100_000_000);

        assert!(fixture.calls.is_empty());
    }

    #[test]
    fn should_not_nak_if_no_missing_data() {
        let mut fixture = Fixture::new();
        let rebuild = active_term_position();
        let hwm = active_term_position() + (aligned_frame_length() as i64 * 3);

        fixture.insert(offset_of_message(0));
        fixture.insert(offset_of_message(1));
        fixture.insert(offset_of_message(2));

        fixture.scan(rebuild, hwm, 0);
        fixture.scan(rebuild, hwm, MS_20 * 2);

        assert!(fixture.calls.is_empty());
    }

    #[test]
    fn should_nak_missing_data() {
        let mut fixture = Fixture::new();
        let rebuild = active_term_position();
        let hwm = active_term_position() + (aligned_frame_length() as i64 * 3);

        fixture.insert(offset_of_message(0));
        fixture.insert(offset_of_message(2));

        fixture.scan(rebuild, hwm, 0);
        fixture.scan(rebuild, hwm, MS_20 * 2);

        assert_eq!(
            fixture.calls,
            vec![(TERM_ID, offset_of_message(1), aligned_frame_length())]
        );
    }

    #[test]
    fn should_retransmit_nak_for_missing_data() {
        let mut fixture = Fixture::new();
        let rebuild = active_term_position();
        let hwm = active_term_position() + (aligned_frame_length() as i64 * 3);

        fixture.insert(offset_of_message(0));
        fixture.insert(offset_of_message(2));

        fixture.scan(rebuild, hwm, 0);
        fixture.scan(rebuild, hwm, 30_000_000);
        fixture.scan(rebuild, hwm, 60_000_000);

        assert!(fixture.count((TERM_ID, offset_of_message(1), aligned_frame_length())) >= 2);
    }

    #[test]
    fn should_stop_nak_on_receiving_data() {
        let mut fixture = Fixture::new();
        let mut rebuild = active_term_position();
        let hwm = active_term_position() + (aligned_frame_length() as i64 * 3);

        fixture.insert(offset_of_message(0));
        fixture.insert(offset_of_message(2));

        fixture.scan(rebuild, hwm, 0);
        fixture.insert(offset_of_message(1));
        rebuild += aligned_frame_length() as i64 * 3;
        fixture.scan(rebuild, hwm, MS_20);
        fixture.scan(rebuild, hwm, 100_000_000);

        assert!(fixture.calls.is_empty());
    }

    #[test]
    fn should_handle_more_than_2_gaps() {
        let mut fixture = Fixture::new();
        let mut rebuild = active_term_position();
        let hwm = active_term_position() + (aligned_frame_length() as i64 * 7);

        fixture.insert(offset_of_message(0));
        fixture.insert(offset_of_message(2));
        fixture.insert(offset_of_message(4));
        fixture.insert(offset_of_message(6));

        fixture.scan(rebuild, hwm, 0);
        fixture.scan(rebuild, hwm, MS_20 * 2);
        fixture.insert(offset_of_message(1));
        rebuild += aligned_frame_length() as i64 * 3;
        fixture.scan(rebuild, hwm, MS_20 * 2);
        fixture.scan(rebuild, hwm, 80_000_000);

        let one = fixture
            .calls
            .iter()
            .position(|&c| c == (TERM_ID, offset_of_message(1), aligned_frame_length()));
        let three = fixture
            .calls
            .iter()
            .position(|&c| c == (TERM_ID, offset_of_message(3), aligned_frame_length()));
        assert!(one.is_some(), "gap at message 1 should be NAK'd");
        assert!(three.is_some(), "gap at message 3 should be NAK'd");
        assert!(one < three, "message 1 must be NAK'd before message 3");
        assert_eq!(
            fixture.count((TERM_ID, offset_of_message(5), aligned_frame_length())),
            0,
            "message 5 must never be NAK'd"
        );
    }

    #[test]
    fn should_replace_old_nak_with_new_nak() {
        let mut fixture = Fixture::new();
        let mut rebuild = active_term_position();
        let mut hwm = active_term_position() + (aligned_frame_length() as i64 * 3);

        fixture.insert(offset_of_message(0));
        fixture.insert(offset_of_message(2));

        fixture.scan(rebuild, hwm, 0);
        fixture.scan(rebuild, hwm, MS_20);

        fixture.insert(offset_of_message(4));
        fixture.insert(offset_of_message(1));
        rebuild += aligned_frame_length() as i64 * 3;
        hwm = aligned_frame_length() as i64 * 5;

        fixture.scan(rebuild, hwm, MS_20);
        fixture.scan(rebuild, hwm, 100_000_000);

        assert!(fixture.count((TERM_ID, offset_of_message(3), aligned_frame_length())) >= 1);
    }

    #[test]
    fn should_handle_longer_retry_delay() {
        let mut fixture = Fixture::long_retry();
        let rebuild = active_term_position();
        let hwm = active_term_position() + (aligned_frame_length() as i64 * 3);

        fixture.insert(offset_of_message(0));
        fixture.insert(offset_of_message(2));

        fixture.scan(rebuild, hwm, 0);
        assert!(fixture.calls.is_empty());

        fixture.scan(rebuild, hwm, MS_20 * 2);
        assert_eq!(
            fixture.count((TERM_ID, offset_of_message(1), aligned_frame_length())),
            1
        );

        fixture.scan(rebuild, hwm, 80_000_000);
        assert_eq!(
            fixture.count((TERM_ID, offset_of_message(1), aligned_frame_length())),
            1
        );

        fixture.scan(rebuild, hwm, 240_000_000);
        assert_eq!(
            fixture.count((TERM_ID, offset_of_message(1), aligned_frame_length())),
            2
        );
    }

    #[test]
    fn should_not_nak_immediately_by_default() {
        let mut fixture = Fixture::new();
        let rebuild = active_term_position();
        let hwm = active_term_position() + (aligned_frame_length() as i64 * 3);

        fixture.insert(offset_of_message(0));
        fixture.insert(offset_of_message(2));

        fixture.scan(rebuild, hwm, 0);

        assert!(fixture.calls.is_empty());
    }

    #[test]
    fn should_only_send_naks_once_on_multiple_scans() {
        let mut fixture = Fixture::long_retry();
        let rebuild = active_term_position();
        let hwm = active_term_position() + (aligned_frame_length() as i64 * 3);

        fixture.insert(offset_of_message(0));
        fixture.insert(offset_of_message(2));

        fixture.scan(rebuild, hwm, 0);
        fixture.scan(rebuild, hwm, MS_20 * 2);
        fixture.scan(rebuild, hwm, MS_20 * 2);

        assert_eq!(
            fixture.count((TERM_ID, offset_of_message(1), aligned_frame_length())),
            1
        );
    }

    #[test]
    fn should_handle_hwm_greater_than_completed_buffer() {
        let mut fixture = Fixture::long_retry();
        let mut rebuild = active_term_position();
        let hwm =
            active_term_position() + TERM_BUFFER_LENGTH as i64 + aligned_frame_length() as i64;

        fixture.insert(offset_of_message(0));
        rebuild += aligned_frame_length() as i64;

        fixture.scan(rebuild, hwm, 0);
        fixture.scan(rebuild, hwm, MS_20 * 2);

        let expected_length = TERM_BUFFER_LENGTH - rebuild as i32;
        assert_eq!(
            fixture.count((TERM_ID, offset_of_message(1), expected_length)),
            1
        );
    }

    #[test]
    fn should_handle_non_zero_initial_term_offset() {
        let mut fixture = Fixture::long_retry();
        let rebuild = active_term_position() + (aligned_frame_length() as i64 * 3);
        let hwm = active_term_position() + (aligned_frame_length() as i64 * 5);

        fixture.insert(offset_of_message(2));
        fixture.insert(offset_of_message(4));

        fixture.scan(rebuild, hwm, 0);
        fixture.scan(rebuild, hwm, MS_20 * 2);

        assert_eq!(
            fixture.calls,
            vec![(TERM_ID, offset_of_message(3), aligned_frame_length())]
        );
    }

    #[test]
    fn should_detect_changes_in_the_gap_length() {
        let mut fixture = Fixture::long_retry();
        let rebuild = active_term_position() + (aligned_frame_length() as i64 * 3);

        fixture.insert(offset_of_message(2));
        fixture.insert(offset_of_message(5));

        assert!(loss_found(fixture.scan(rebuild, rebuild + 32, 0)));
        assert!(!loss_found(fixture.scan(
            rebuild,
            rebuild + 32,
            100_000_000
        )));
        assert!(loss_found(fixture.scan(rebuild, rebuild + 64, 100_000_000)));
        assert!(!loss_found(fixture.scan(rebuild, rebuild + 64, MS_200)));

        let two_frames = aligned_frame_length() as i64 * 2;
        assert!(loss_found(fixture.scan(
            rebuild,
            rebuild + aligned_frame_length() as i64,
            300_000_000
        )));
        assert!(loss_found(fixture.scan(
            rebuild,
            rebuild + two_frames,
            300_000_000
        )));
        assert!(!loss_found(fixture.scan(
            rebuild,
            rebuild + two_frames,
            400_000_000
        )));

        fixture.insert(offset_of_message(4));
        assert!(loss_found(fixture.scan(
            rebuild,
            rebuild + two_frames,
            400_000_000
        )));
        assert!(!loss_found(fixture.scan(
            rebuild,
            rebuild + two_frames,
            500_000_000
        )));
        assert!(loss_found(fixture.scan(rebuild, rebuild + 64, 500_000_000)));
        assert!(!loss_found(fixture.scan(
            rebuild,
            rebuild + 64,
            600_000_000
        )));

        let al = aligned_frame_length();
        assert_eq!(
            fixture.calls,
            vec![
                (TERM_ID, offset_of_message(3), 32),
                (TERM_ID, offset_of_message(3), 64),
                (TERM_ID, offset_of_message(3), al * 2),
                (TERM_ID, offset_of_message(3), al),
                (TERM_ID, offset_of_message(3), 64),
            ]
        );
    }
}
