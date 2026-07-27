//! Sender-side tracking and handling of retransmit requests (NAKs).
//!
//! Mirrors `io.aeron.driver.RetransmitHandler.java`. With multicast/MDC group semantics a bounded
//! pool of concurrent retransmits is permitted; with unicast a single outstanding retransmit is
//! allowed and a non-overlapping NAK replaces the previous one. Each accepted NAK is optionally
//! delayed, sent, then lingered so overlapping NAKs during the linger are suppressed.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use aeron_client::protocol::DATA_HEADER_LENGTH;

use crate::loss_detector::StaticDelayGenerator;

/// Clamps a requested retransmit length (upstream `FlowControl.maxRetransmissionLength`).
pub trait RetransmitFlowControl {
    fn max_retransmission_length(
        &self,
        term_offset: i32,
        resend_length: i32,
        term_length: i32,
        mtu_length: i32,
    ) -> i32;
}

/// Callback invoked to actually retransmit a range (upstream `RetransmitSender.resend`).
pub type RetransmitSender<'a> = dyn FnMut(i32, i32, i32) + 'a;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetransmitActionState {
    Delayed,
    Lingering,
    Inactive,
}

#[derive(Debug, Clone, Copy)]
struct RetransmitAction {
    expiry_ns: i64,
    term_id: i32,
    term_offset: i32,
    length: i32,
    state: RetransmitActionState,
}

impl RetransmitAction {
    fn inactive() -> Self {
        Self {
            expiry_ns: 0,
            term_id: 0,
            term_offset: 0,
            length: 0,
            state: RetransmitActionState::Inactive,
        }
    }

    fn delay(&mut self, delay_ns: i64, now_ns: i64) {
        self.state = RetransmitActionState::Delayed;
        self.expiry_ns = now_ns + delay_ns;
    }

    fn linger(&mut self, timeout_ns: i64, now_ns: i64) {
        self.state = RetransmitActionState::Lingering;
        self.expiry_ns = now_ns + timeout_ns;
    }

    fn cancel(&mut self) {
        self.state = RetransmitActionState::Inactive;
    }
}

/// Handles reception of NAKs and the timing of resends.
pub struct RetransmitHandler {
    retransmit_action_pool: Vec<RetransmitAction>,
    delay_generator: StaticDelayGenerator,
    linger_timeout_generator: StaticDelayGenerator,
    invalid_packets: Arc<AtomicI64>,
    has_group_semantics: bool,
    retransmit_overflow_counter: Arc<AtomicI64>,
    active_retransmit_count: usize,
}

impl RetransmitHandler {
    /// `max_retransmits` applies only with group semantics; unicast always uses a single slot.
    pub fn new(
        invalid_packets: Arc<AtomicI64>,
        delay_generator: StaticDelayGenerator,
        linger_timeout_generator: StaticDelayGenerator,
        has_group_semantics: bool,
        max_retransmits: usize,
        retransmit_overflow_counter: Arc<AtomicI64>,
    ) -> Self {
        let actual_max_retransmits = if has_group_semantics {
            max_retransmits
        } else {
            1
        };
        Self {
            retransmit_action_pool: vec![RetransmitAction::inactive(); actual_max_retransmits],
            delay_generator,
            linger_timeout_generator,
            invalid_packets,
            has_group_semantics,
            retransmit_overflow_counter,
            active_retransmit_count: 0,
        }
    }

    /// Handle a received NAK: clamp, schedule (or immediately send) a retransmit.
    #[allow(clippy::too_many_arguments)]
    pub fn on_nak(
        &mut self,
        term_id: i32,
        term_offset: i32,
        length: i32,
        term_length: i32,
        mtu_length: i32,
        now_ns: i64,
        flow_control: &dyn RetransmitFlowControl,
        sender: &mut RetransmitSender<'_>,
    ) {
        if !self.is_invalid(term_offset, term_length, length) && 0 != length {
            let retransmit_length = flow_control.max_retransmission_length(
                term_offset,
                length,
                term_length,
                mtu_length,
            );
            if let Some(index) =
                self.scan_for_available_retransmit(term_id, term_offset, retransmit_length)
            {
                let action = &mut self.retransmit_action_pool[index];
                action.term_id = term_id;
                action.term_offset = term_offset;
                action.length = retransmit_length;

                let delay = self.delay_generator.generate_delay_ns();
                if 0 == delay {
                    let length = action.length;
                    sender(term_id, term_offset, length);
                    let linger = self.linger_timeout_generator.generate_delay_ns();
                    self.retransmit_action_pool[index].linger(linger, now_ns);
                } else {
                    action.delay(delay, now_ns);
                }
            }
        }
    }

    /// A retransmission was observed on the wire; cancel a still-delayed matching retransmit.
    pub fn on_retransmit_received(&mut self, term_id: i32, term_offset: i32) {
        if let Some(index) = self.scan_for_existing_retransmit(term_id, term_offset) {
            if RetransmitActionState::Delayed == self.retransmit_action_pool[index].state {
                self.remove_retransmit(index);
            }
        }
    }

    /// Process outstanding delays (fire resends) and expired lingers (release slots).
    pub fn process_timeouts(&mut self, now_ns: i64, sender: &mut RetransmitSender<'_>) {
        if self.active_retransmit_count > 0 {
            for index in 0..self.retransmit_action_pool.len() {
                let action = self.retransmit_action_pool[index];
                if RetransmitActionState::Delayed == action.state && (action.expiry_ns - now_ns < 0)
                {
                    sender(action.term_id, action.term_offset, action.length);
                    let linger = self.linger_timeout_generator.generate_delay_ns();
                    self.retransmit_action_pool[index].linger(linger, now_ns);
                } else if RetransmitActionState::Lingering == action.state
                    && (action.expiry_ns - now_ns < 0)
                {
                    self.remove_retransmit(index);
                }
            }
        }
    }

    fn is_invalid(&self, term_offset: i32, term_length: i32, length: i32) -> bool {
        let is_invalid = (term_offset > (term_length - DATA_HEADER_LENGTH as i32))
            || (term_offset < 0)
            || (length < 0);

        if is_invalid {
            self.invalid_packets.fetch_add(1, Ordering::Relaxed);
        }

        is_invalid
    }

    fn scan_for_available_retransmit(
        &mut self,
        term_id: i32,
        term_offset: i32,
        // Present to mirror the upstream signature; the overlap test uses each action's own length.
        _length: i32,
    ) -> Option<usize> {
        if 0 == self.active_retransmit_count {
            return Some(self.add_retransmit(0));
        }

        let mut available_action: Option<usize> = None;
        for index in 0..self.retransmit_action_pool.len() {
            let action = self.retransmit_action_pool[index];
            match action.state {
                RetransmitActionState::Inactive => {
                    if available_action.is_none() {
                        available_action = Some(index);
                    }
                }
                RetransmitActionState::Delayed | RetransmitActionState::Lingering => {
                    if action.term_id == term_id
                        && action.term_offset <= term_offset
                        && term_offset < action.term_offset + action.length
                    {
                        return None;
                    }

                    if !self.has_group_semantics {
                        // unicast, and the NAK does NOT overlap the previous one, so reuse it
                        available_action = Some(index);
                    }
                }
            }
        }

        if self.has_group_semantics {
            if let Some(index) = available_action {
                return Some(self.add_retransmit(index));
            }

            self.retransmit_overflow_counter
                .fetch_add(1, Ordering::Relaxed);
        }

        available_action
    }

    fn scan_for_existing_retransmit(&self, term_id: i32, term_offset: i32) -> Option<usize> {
        if 0 == self.active_retransmit_count {
            return None;
        }

        for index in 0..self.retransmit_action_pool.len() {
            let action = self.retransmit_action_pool[index];
            if matches!(
                action.state,
                RetransmitActionState::Delayed | RetransmitActionState::Lingering
            ) && action.term_id == term_id
                && action.term_offset == term_offset
            {
                return Some(index);
            }
        }

        None
    }

    fn add_retransmit(&mut self, index: usize) -> usize {
        self.active_retransmit_count += 1;
        index
    }

    fn remove_retransmit(&mut self, index: usize) {
        self.active_retransmit_count -= 1;
        self.retransmit_action_pool[index].cancel();
    }
}

#[cfg(test)]
mod tests {
    //! Faithful port of
    //! vendor/aeron/aeron-driver/src/test/java/io/aeron/driver/RetransmitHandlerTest.java
    //!
    //! The upstream test parameterizes each case over two ways of populating the term buffer
    //! (sent vs received frames), but `RetransmitHandler` never reads the buffer — only term id,
    //! offset, and length matter — so the two variants are behaviorally identical and the port
    //! runs each case once.

    use super::*;

    const MTU_LENGTH: i32 = 1024;
    const TERM_BUFFER_LENGTH: i32 = aeron_client::logbuffer::TERM_MIN_LENGTH;
    const DATA_LEN: i32 = 16;
    const MESSAGE_LENGTH: i32 = DATA_HEADER_LENGTH as i32 + DATA_LEN;
    const ALIGNED_FRAME_LENGTH: i32 = (MESSAGE_LENGTH + 31) & !31;
    const TWO_MESSAGE_FRAME_LENGTH: i32 = 2 * ALIGNED_FRAME_LENGTH;
    const TERM_ID: i32 = 0x7F003355;

    const MS_20: i64 = 20_000_000;
    const MS_40: i64 = 40_000_000;
    const MS_100: i64 = 100_000_000;
    const MS_200: i64 = 200_000_000;

    fn offset_of_frame(index: i32) -> i32 {
        index * ALIGNED_FRAME_LENGTH
    }

    /// Flow control that returns the requested resend length (the upstream default mock).
    struct PassThroughFlowControl;
    impl RetransmitFlowControl for PassThroughFlowControl {
        fn max_retransmission_length(&self, _: i32, resend_length: i32, _: i32, _: i32) -> i32 {
            resend_length
        }
    }

    /// Flow control that clamps every retransmit to a fixed length.
    struct ClampFlowControl(i32);
    impl RetransmitFlowControl for ClampFlowControl {
        fn max_retransmission_length(&self, _: i32, _: i32, _: i32, _: i32) -> i32 {
            self.0
        }
    }

    struct Fixture {
        handler: RetransmitHandler,
        invalid_packets: Arc<AtomicI64>,
        retransmit_overflow: Arc<AtomicI64>,
        resends: Vec<(i32, i32, i32)>,
    }

    impl Fixture {
        fn with_delay(delay: StaticDelayGenerator) -> Self {
            let invalid_packets = Arc::new(AtomicI64::new(0));
            let retransmit_overflow = Arc::new(AtomicI64::new(0));
            let handler = RetransmitHandler::new(
                invalid_packets.clone(),
                delay,
                StaticDelayGenerator::new(MS_40),
                true,
                16,
                retransmit_overflow.clone(),
            );
            Self {
                handler,
                invalid_packets,
                retransmit_overflow,
                resends: Vec::new(),
            }
        }

        fn new() -> Self {
            Self::with_delay(StaticDelayGenerator::new(MS_20))
        }

        fn zero_delay() -> Self {
            Self::with_delay(StaticDelayGenerator::new(0))
        }

        #[allow(clippy::too_many_arguments)]
        fn on_nak(
            &mut self,
            term_offset: i32,
            length: i32,
            term_length: i32,
            now_ns: i64,
            flow_control: &dyn RetransmitFlowControl,
        ) {
            let resends = &mut self.resends;
            self.handler.on_nak(
                TERM_ID,
                term_offset,
                length,
                term_length,
                MTU_LENGTH,
                now_ns,
                flow_control,
                &mut |t, o, l| resends.push((t, o, l)),
            );
        }

        fn process_timeouts(&mut self, now_ns: i64) {
            let resends = &mut self.resends;
            self.handler
                .process_timeouts(now_ns, &mut |t, o, l| resends.push((t, o, l)));
        }

        fn count(&self, expected: (i32, i32, i32)) -> usize {
            self.resends.iter().filter(|&&r| r == expected).count()
        }
    }

    #[test]
    fn should_retransmit_on_nak() {
        let mut f = Fixture::new();
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_100);
        assert_eq!(
            f.resends,
            vec![(TERM_ID, offset_of_frame(0), ALIGNED_FRAME_LENGTH)]
        );
    }

    #[test]
    fn should_clamp_retransmit_via_flow_control_on_nak() {
        let mut f = Fixture::new();
        let expected_resend_length = 32;
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &ClampFlowControl(expected_resend_length),
        );
        f.process_timeouts(MS_100);
        assert_eq!(
            f.resends,
            vec![(TERM_ID, offset_of_frame(0), expected_resend_length)]
        );
    }

    #[test]
    fn should_not_retransmit_on_nak_while_in_linger() {
        let mut f = Fixture::new();
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_40);
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            MS_40,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_100);
        assert_eq!(
            f.count((TERM_ID, offset_of_frame(0), ALIGNED_FRAME_LENGTH)),
            1
        );
    }

    #[test]
    fn should_not_retransmit_on_nak_while_in_linger_contained_within_existing() {
        let mut f = Fixture::new();
        f.on_nak(
            offset_of_frame(0),
            TWO_MESSAGE_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_40);
        f.on_nak(
            offset_of_frame(1),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            MS_40,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_100);
        assert_eq!(f.resends.len(), 1);
    }

    #[test]
    fn should_retransmit_on_nak_after_linger() {
        let mut f = Fixture::new();
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_40);
        f.process_timeouts(MS_100);
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            MS_100,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_200);
        assert_eq!(
            f.count((TERM_ID, offset_of_frame(0), ALIGNED_FRAME_LENGTH)),
            2
        );
    }

    #[test]
    fn should_retransmit_on_multiple_naks() {
        let mut f = Fixture::new();
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.on_nak(
            offset_of_frame(1),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_100);
        assert_eq!(
            f.resends,
            vec![
                (TERM_ID, offset_of_frame(0), ALIGNED_FRAME_LENGTH),
                (TERM_ID, offset_of_frame(1), ALIGNED_FRAME_LENGTH),
            ]
        );
    }

    #[test]
    fn should_retransmit_on_nak_over_message_length() {
        let mut f = Fixture::new();
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH * 5,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_100);
        assert_eq!(
            f.resends,
            vec![(TERM_ID, offset_of_frame(0), ALIGNED_FRAME_LENGTH * 5)]
        );
    }

    #[test]
    fn should_retransmit_on_nak_over_mtu_length() {
        let mut f = Fixture::new();
        f.on_nak(
            offset_of_frame(0),
            MTU_LENGTH * 2,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_100);
        assert_eq!(
            f.resends,
            vec![(TERM_ID, offset_of_frame(0), MTU_LENGTH * 2)]
        );
    }

    #[test]
    fn should_not_retransmit_on_nak_offset_in_range_of_existing() {
        let mut f = Fixture::new();
        f.on_nak(
            offset_of_frame(0),
            TWO_MESSAGE_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_40);
        f.on_nak(
            offset_of_frame(1),
            TWO_MESSAGE_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            MS_40,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_100);
        assert_eq!(
            f.count((TERM_ID, offset_of_frame(0), TWO_MESSAGE_FRAME_LENGTH)),
            1
        );
        assert_eq!(
            f.count((TERM_ID, offset_of_frame(1), TWO_MESSAGE_FRAME_LENGTH)),
            0
        );
    }

    #[test]
    fn should_retransmit_on_nak_just_outside_range_of_existing() {
        let mut f = Fixture::new();
        f.on_nak(
            offset_of_frame(0),
            TWO_MESSAGE_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_40);
        f.on_nak(
            offset_of_frame(2),
            TWO_MESSAGE_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            MS_40,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_100);
        assert_eq!(
            f.count((TERM_ID, offset_of_frame(0), TWO_MESSAGE_FRAME_LENGTH)),
            1
        );
        assert_eq!(
            f.count((TERM_ID, offset_of_frame(2), TWO_MESSAGE_FRAME_LENGTH)),
            1
        );
    }

    #[test]
    fn should_stop_retransmit_on_retransmit_reception() {
        let mut f = Fixture::new();
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.handler
            .on_retransmit_received(TERM_ID, offset_of_frame(0));
        f.process_timeouts(MS_100);
        assert!(f.resends.is_empty());
    }

    #[test]
    fn should_stop_one_retransmit_on_retransmit_reception() {
        let mut f = Fixture::new();
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.on_nak(
            offset_of_frame(1),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.handler
            .on_retransmit_received(TERM_ID, offset_of_frame(0));
        f.process_timeouts(MS_100);
        assert_eq!(
            f.resends,
            vec![(TERM_ID, offset_of_frame(1), ALIGNED_FRAME_LENGTH)]
        );
    }

    #[test]
    fn should_immediate_retransmit_on_nak() {
        let mut f = Fixture::zero_delay();
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        assert_eq!(
            f.resends,
            vec![(TERM_ID, offset_of_frame(0), ALIGNED_FRAME_LENGTH)]
        );
    }

    #[test]
    fn should_go_into_linger_on_immediate_retransmit() {
        let mut f = Fixture::zero_delay();
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        f.process_timeouts(MS_40);
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            MS_40,
            &PassThroughFlowControl,
        );
        assert_eq!(
            f.count((TERM_ID, offset_of_frame(0), ALIGNED_FRAME_LENGTH)),
            1
        );
    }

    #[test]
    fn should_only_retransmit_on_nak_when_configured_to() {
        let mut f = Fixture::new();
        f.on_nak(
            offset_of_frame(0),
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        assert!(f.resends.is_empty());
    }

    #[test]
    fn should_increment_overflow_counter() {
        let mut f = Fixture::new();
        let term_length = 128 * 1024;
        for i in 0..16 {
            f.on_nak(
                offset_of_frame(i * 64),
                ALIGNED_FRAME_LENGTH,
                term_length,
                0,
                &PassThroughFlowControl,
            );
        }
        f.on_nak(
            offset_of_frame(16 * 64),
            ALIGNED_FRAME_LENGTH,
            term_length,
            0,
            &PassThroughFlowControl,
        );
        assert_eq!(f.retransmit_overflow.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn should_detect_invalid_naks_invalid_offset() {
        let mut f = Fixture::zero_delay();
        f.on_nak(
            TERM_BUFFER_LENGTH - 16,
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        assert_eq!(f.invalid_packets.load(Ordering::Relaxed), 1);
        assert!(f.resends.is_empty());
    }

    #[test]
    fn should_detect_invalid_naks_negative_offset() {
        let mut f = Fixture::zero_delay();
        f.on_nak(
            -128,
            ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        assert_eq!(f.invalid_packets.load(Ordering::Relaxed), 1);
        assert!(f.resends.is_empty());
    }

    #[test]
    fn should_detect_invalid_naks_negative_length() {
        let mut f = Fixture::zero_delay();
        f.on_nak(
            0,
            -ALIGNED_FRAME_LENGTH,
            TERM_BUFFER_LENGTH,
            0,
            &PassThroughFlowControl,
        );
        assert_eq!(f.invalid_packets.load(Ordering::Relaxed), 1);
        assert!(f.resends.is_empty());
    }

    #[test]
    fn should_ignore_empty_nak() {
        let mut f = Fixture::zero_delay();
        f.on_nak(0, 0, TERM_BUFFER_LENGTH, 0, &PassThroughFlowControl);
        assert!(f.resends.is_empty());
    }
}
