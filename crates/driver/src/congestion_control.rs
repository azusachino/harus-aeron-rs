//! Congestion control for received publication images.
//!
//! Mirrors `io.aeron.driver.CongestionControl` (the packed-outcome helpers) and
//! `io.aeron.driver.StaticWindowCongestionControl`, whose window is the min of the configured
//! initial window and half a term length and never changes over the life of the image.

use crate::flow_control;
use crate::media::udp_channel::UdpChannel;

/// Lower-bit mask / high-bit flag used by [`pack_outcome`] (upstream `FORCE_STATUS_MESSAGE_BIT`).
const FORCE_STATUS_MESSAGE_BIT: u64 = 0x1;

/// Pack a receiver window length and a force-status-message flag into one word.
pub fn pack_outcome(receiver_window_length: i32, force_status_message: bool) -> u64 {
    let flags = if force_status_message {
        FORCE_STATUS_MESSAGE_BIT
    } else {
        0
    };
    (flags << 32) | (receiver_window_length as u32 as u64)
}

/// Extract the receiver window length from a packed outcome.
pub fn receiver_window_length(outcome: u64) -> i32 {
    (outcome & 0xFFFF_FFFF) as u32 as i32
}

/// Extract the force-status-message flag from a packed outcome.
pub fn should_force_status_message(outcome: u64) -> bool {
    0 != (outcome >> 32) & FORCE_STATUS_MESSAGE_BIT
}

/// Static-window congestion control: a fixed receiver window for the image's whole lifetime.
pub struct StaticWindowCongestionControl {
    cc_outcome: u64,
}

/// Cubic congestion-control configuration surface used by the driver image setup.
///
/// The current Rust driver keeps the feedback algorithm static, but preserves Cubic's
/// negotiated maximum congestion-window boundary: channel `rcv-wnd`, then context default,
/// both clamped to half a term and expressed in MTUs.
pub struct CubicCongestionControl {
    max_congestion_window: i32,
}

impl CubicCongestionControl {
    pub const CC_PARAM_VALUE: &'static str = "cubic";

    pub fn new(
        udp_channel: &UdpChannel,
        term_length: i32,
        mtu_length: i32,
        context_initial_window_length: i32,
    ) -> Self {
        let channel_window = udp_channel.receiver_window_length as i32;
        let initial_window_length = if channel_window != 0 {
            channel_window
        } else {
            context_initial_window_length
        };
        let byte_window = flow_control::receiver_window_length(term_length, initial_window_length);
        Self {
            max_congestion_window: byte_window / mtu_length.max(1),
        }
    }

    pub fn max_congestion_window(&self) -> i32 {
        self.max_congestion_window
    }
}

impl StaticWindowCongestionControl {
    /// URI `cc` value identifying this strategy (upstream `CC_PARAM_VALUE`).
    pub const CC_PARAM_VALUE: &'static str = "static";

    /// The window is the channel's `rcv-wnd` if set, else the context default, clamped to half a
    /// term (upstream `Configuration.receiverWindowLength`).
    pub fn new(
        udp_channel: &UdpChannel,
        term_length: i32,
        context_initial_window_length: i32,
    ) -> Self {
        let channel_window = udp_channel.receiver_window_length as i32;
        let initial_window_length = if channel_window != 0 {
            channel_window
        } else {
            context_initial_window_length
        };
        let cc_outcome = pack_outcome(
            flow_control::receiver_window_length(term_length, initial_window_length),
            false,
        );
        Self { cc_outcome }
    }

    /// A static window never asks for an RTT measurement.
    pub fn should_measure_rtt(&self, _now_ns: i64) -> bool {
        false
    }

    /// The window is constant, so tracking a rebuild just returns the fixed outcome.
    #[allow(clippy::too_many_arguments)]
    pub fn on_track_rebuild(
        &self,
        _now_ns: i64,
        _new_consumption_position: i64,
        _last_sm_position: i64,
        _hwm_position: i64,
        _starting_rebuild_position: i64,
        _ending_rebuild_position: i64,
        _loss_occurred: bool,
    ) -> u64 {
        self.cc_outcome
    }

    pub fn initial_window_length(&self) -> i32 {
        receiver_window_length(self.cc_outcome)
    }

    pub fn max_window_length(&self) -> i32 {
        receiver_window_length(self.cc_outcome)
    }
}

#[cfg(test)]
mod tests {
    //! Faithful port of
    //! vendor/aeron/aeron-driver/src/test/java/io/aeron/driver/StaticWindowCongestionControlTest.java

    use super::*;

    const CONTEXT_INITIAL_WINDOW_LENGTH: i32 = 16536;

    #[test]
    fn should_set_window_length_from_channel() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=127.0.0.1:9999|rcv-wnd=8192").unwrap();
        let cc =
            StaticWindowCongestionControl::new(&channel, 1_000_000, CONTEXT_INITIAL_WINDOW_LENGTH);
        assert_eq!(cc.initial_window_length(), 8192);
    }

    #[test]
    fn should_set_window_length_from_context() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=127.0.0.1:9999").unwrap();
        let cc =
            StaticWindowCongestionControl::new(&channel, 1_000_000, CONTEXT_INITIAL_WINDOW_LENGTH);
        assert_eq!(cc.initial_window_length(), CONTEXT_INITIAL_WINDOW_LENGTH);
    }

    #[test]
    fn should_set_window_length_from_term_length() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=127.0.0.1:9999|rcv-wnd=8192").unwrap();
        let term_length = 8192;
        let cc = StaticWindowCongestionControl::new(
            &channel,
            term_length,
            CONTEXT_INITIAL_WINDOW_LENGTH,
        );
        assert_eq!(cc.initial_window_length(), term_length / 2);
    }

    #[test]
    fn cubic_should_set_window_length_from_channel() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=127.0.0.1:9999|rcv-wnd=8192").unwrap();
        let cc = CubicCongestionControl::new(&channel, 1_000_000, 1024, 65_536);
        assert_eq!(cc.max_congestion_window(), 8192 / 1024);
    }

    #[test]
    fn cubic_should_set_window_length_from_context() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=127.0.0.1:9999").unwrap();
        let cc = CubicCongestionControl::new(&channel, 1_000_000, 1024, 65_536);
        assert_eq!(cc.max_congestion_window(), 65_536 / 1024);
    }

    #[test]
    fn cubic_should_set_window_length_from_term_length() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=127.0.0.1:9999|rcv-wnd=8192").unwrap();
        let cc = CubicCongestionControl::new(&channel, 8192, 1024, 65_536);
        assert_eq!(cc.max_congestion_window(), 8192 / 2 / 1024);
    }
}
