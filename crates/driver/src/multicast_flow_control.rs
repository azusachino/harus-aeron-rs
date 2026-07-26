//! Selection of the multicast flow-control strategy for a channel.
//!
//! Mirrors `io.aeron.driver.DefaultMulticastFlowControlSupplier`: the `fc=` URI param names the
//! strategy (`min`/`max`/`tagged`, optionally followed by `,`-separated options), overriding the
//! `aeron.multicast.flow.control.strategy` default. The concrete `Min`/`Max`/`Tagged` strategy
//! implementations land with their own parity units; this supplier resolves which one applies.

use aeron_client::logbuffer::compute_position;
use aeron_client::protocol::StatusMessageFlyweight;
use aeron_client::uri::FC_PARAM_NAME;

use crate::error::{DriverError, Result};
use crate::media::udp_channel::UdpChannel;
use crate::publication_params::parse_duration;

const DEFAULT_GROUP_MIN_SIZE: usize = 0;
const DEFAULT_RECEIVER_TIMEOUT_NS: i64 = 5_000_000_000;

/// The multicast flow-control strategy a channel selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MulticastFlowControlStrategy {
    Max,
    Min,
    Tagged,
}

impl MulticastFlowControlStrategy {
    /// `fc=` value for the max strategy (upstream `MaxMulticastFlowControl.FC_PARAM_VALUE`).
    pub const MAX_FC_PARAM_VALUE: &'static str = "max";
    /// `fc=` value for the min strategy (upstream `MinMulticastFlowControl.FC_PARAM_VALUE`).
    pub const MIN_FC_PARAM_VALUE: &'static str = "min";
    /// `fc=` value for the tagged strategy (upstream `TaggedMulticastFlowControl.FC_PARAM_VALUE`).
    pub const TAGGED_FC_PARAM_VALUE: &'static str = "tagged";
}

/// Default supplier resolving the multicast flow-control strategy from the channel URI.
#[derive(Debug, Default)]
pub struct DefaultMulticastFlowControlSupplier;

impl DefaultMulticastFlowControlSupplier {
    pub fn new() -> Self {
        Self
    }

    /// Resolve the strategy for `udp_channel`, honouring `fc=` before the configured default.
    pub fn new_instance(
        &self,
        udp_channel: &UdpChannel,
        _stream_id: i32,
        _registration_id: i64,
    ) -> Result<MulticastFlowControlStrategy> {
        if let Some(fc_str) = udp_channel.channel_uri.get(FC_PARAM_NAME) {
            let strategy_str = fc_str.split(',').next().unwrap_or(fc_str);
            return match strategy_str {
                MulticastFlowControlStrategy::MAX_FC_PARAM_VALUE => {
                    Ok(MulticastFlowControlStrategy::Max)
                }
                MulticastFlowControlStrategy::MIN_FC_PARAM_VALUE => {
                    Ok(MulticastFlowControlStrategy::Min)
                }
                MulticastFlowControlStrategy::TAGGED_FC_PARAM_VALUE => {
                    Ok(MulticastFlowControlStrategy::Tagged)
                }
                _ => Err(DriverError::IllegalArgument(format!(
                    "unsupported multicast flow control strategy: fc={fc_str}"
                ))),
            };
        }

        // Upstream default `Configuration.MULTICAST_FLOW_CONTROL_STRATEGY` is MaxMulticastFlowControl.
        Ok(MulticastFlowControlStrategy::Max)
    }
}

#[derive(Debug, Clone, Copy)]
struct ReceiverState {
    receiver_id: i64,
    last_position_plus_window: i64,
    time_of_last_status_message_ns: i64,
    eos_flagged: bool,
}

/// Minimum multicast sender flow control.
///
/// This tracks every receiver and limits the sender to the minimum receiver position once the
/// configured group size has been reached. The state transitions mirror
/// `io.aeron.driver.AbstractMinMulticastFlowControl` and `MinMulticastFlowControl`.
#[derive(Debug)]
pub struct MinMulticastFlowControl {
    receivers: Vec<ReceiverState>,
    group_min_size: usize,
    receiver_timeout_ns: i64,
    has_required_receivers: bool,
    last_setup_sender_limit: Option<i64>,
    time_of_last_setup_ns: i64,
}

impl Default for MinMulticastFlowControl {
    fn default() -> Self {
        Self::new()
    }
}

impl MinMulticastFlowControl {
    /// URI param value identifying this strategy.
    pub const FC_PARAM_VALUE: &'static str = "min";

    pub fn new() -> Self {
        Self {
            receivers: Vec::new(),
            group_min_size: DEFAULT_GROUP_MIN_SIZE,
            receiver_timeout_ns: DEFAULT_RECEIVER_TIMEOUT_NS,
            has_required_receivers: true,
            last_setup_sender_limit: None,
            time_of_last_setup_ns: 0,
        }
    }

    /// Initialise strategy state from context defaults and the channel's `fc=` options.
    pub fn initialize(
        &mut self,
        udp_channel: &UdpChannel,
        context_group_min_size: usize,
        context_receiver_timeout_ns: i64,
    ) -> Result<()> {
        self.receivers.clear();
        self.group_min_size = context_group_min_size;
        self.receiver_timeout_ns = context_receiver_timeout_ns;
        self.last_setup_sender_limit = None;
        self.time_of_last_setup_ns = 0;

        if let Some(fc_value) = udp_channel.channel_uri.get(FC_PARAM_NAME) {
            for argument in fc_value.split(',') {
                if let Some(timeout) = argument.strip_prefix("t:") {
                    self.receiver_timeout_ns =
                        parse_duration("fc receiver timeout", timeout)? as i64;
                } else if let Some(group) = argument.strip_prefix("g:") {
                    if let Some((_, min_size)) = group.split_once('/') {
                        self.group_min_size = min_size.parse::<usize>().map_err(|_| {
                            DriverError::IllegalArgument(format!(
                                "invalid flow control group minimum size: {min_size}"
                            ))
                        })?;
                    }
                }
            }
        }

        self.has_required_receivers = self.receivers.len() >= self.group_min_size;
        Ok(())
    }

    pub fn group_min_size(&self) -> usize {
        self.group_min_size
    }

    pub fn receiver_timeout_ns(&self) -> i64 {
        self.receiver_timeout_ns
    }

    pub fn has_required_receivers(&self) -> bool {
        self.has_required_receivers
    }

    /// Process a received Status Message and return the sender's new position limit.
    pub fn on_status_message(
        &mut self,
        status_message: StatusMessageFlyweight<'_>,
        sender_limit: i64,
        initial_term_id: i32,
        position_bits_to_shift: i32,
        time_ns: i64,
    ) -> i64 {
        let position = compute_position(
            status_message.consumption_term_id(),
            status_message.consumption_term_offset(),
            position_bits_to_shift,
            initial_term_id,
        );
        let last_position_plus_window =
            position + i64::from(status_message.receiver_window_length());
        let receiver_id = status_message.receiver_id();
        let mut min_position = self.last_setup_sender_limit(time_ns);
        let mut is_existing = false;

        for receiver in &mut self.receivers {
            if receiver.receiver_id == receiver_id {
                receiver.last_position_plus_window = last_position_plus_window;
                receiver.time_of_last_status_message_ns = time_ns;
                receiver.eos_flagged = false;
                is_existing = true;
            }
            min_position = min_position.min(receiver.last_position_plus_window);
        }

        if !is_existing
            && (self.receivers.is_empty()
                || last_position_plus_window
                    >= min_position - i64::from(status_message.receiver_window_length()))
        {
            self.receivers.push(ReceiverState {
                receiver_id,
                last_position_plus_window,
                time_of_last_status_message_ns: time_ns,
                eos_flagged: false,
            });
            self.has_required_receivers = self.receivers.len() >= self.group_min_size;
            min_position = min_position.min(last_position_plus_window);
        }

        if self.receivers.len() < self.group_min_size {
            sender_limit
        } else if self.receivers.is_empty() {
            sender_limit.max(last_position_plus_window)
        } else {
            sender_limit.max(min_position)
        }
    }

    /// Remove inactive receivers and return the current sender limit.
    pub fn on_idle(&mut self, time_ns: i64, sender_limit: i64) -> i64 {
        let mut min_limit_position = self.last_setup_sender_limit(time_ns);
        self.receivers.retain(|receiver| {
            let active = receiver.time_of_last_status_message_ns + self.receiver_timeout_ns
                >= time_ns
                && !receiver.eos_flagged;
            if active {
                min_limit_position = min_limit_position.min(receiver.last_position_plus_window);
            }
            active
        });
        self.has_required_receivers = self.receivers.len() >= self.group_min_size;

        if self.receivers.len() < self.group_min_size || self.receivers.is_empty() {
            sender_limit
        } else {
            min_limit_position
        }
    }

    fn last_setup_sender_limit(&mut self, now_ns: i64) -> i64 {
        if let Some(limit) = self.last_setup_sender_limit {
            if self.time_of_last_setup_ns + self.receiver_timeout_ns >= now_ns {
                return limit;
            }
            self.last_setup_sender_limit = None;
        }
        i64::MAX
    }
}

#[cfg(test)]
mod tests {
    //! Faithful port of
    //! vendor/aeron/aeron-driver/src/test/java/io/aeron/driver/DefaultMulticastFlowControlSupplierTest.java

    use super::*;

    fn resolve(uri: &str) -> Result<MulticastFlowControlStrategy> {
        let supplier = DefaultMulticastFlowControlSupplier::new();
        supplier.new_instance(&UdpChannel::parse(uri).unwrap(), 0, 0)
    }

    #[test]
    fn should_return_min_flow_control() {
        for uri in [
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=min",
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=min,t:100ms",
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=min,t:100ms,g:10",
        ] {
            assert_eq!(resolve(uri).unwrap(), MulticastFlowControlStrategy::Min);
        }
    }

    #[test]
    fn should_return_max_flow_control() {
        for uri in [
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=max",
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=max,t:100ms",
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=max,t:100ms,g:10",
        ] {
            assert_eq!(resolve(uri).unwrap(), MulticastFlowControlStrategy::Max);
        }
    }

    #[test]
    fn should_return_tagged_flow_control() {
        for uri in [
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged",
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,t:100ms",
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=tagged,t:100ms,g:10",
        ] {
            assert_eq!(resolve(uri).unwrap(), MulticastFlowControlStrategy::Tagged);
        }
    }

    #[test]
    fn should_reject_invalid_flow_control() {
        for uri in [
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=minute",
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=maximillian",
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=taggedalong",
            "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=foobar",
        ] {
            assert!(resolve(uri).is_err());
        }
    }
}

#[cfg(test)]
mod min_flow_control_tests {
    //! Faithful port of
    //! vendor/aeron/aeron-driver/src/test/java/io/aeron/driver/MinMulticastFlowControlTest.java

    use super::*;
    use agrona::AtomicBuffer;

    const DEFAULT_GROUP_SIZE: usize = 3;
    const DEFAULT_TIMEOUT_NS: i64 = 5_000_000_000;
    const WINDOW_LENGTH: i32 = 16 * 1024;

    fn channel(uri: &str) -> UdpChannel {
        UdpChannel::parse(uri).unwrap()
    }

    fn initialized(uri: &str) -> MinMulticastFlowControl {
        let mut flow_control = MinMulticastFlowControl::new();
        flow_control
            .initialize(&channel(uri), DEFAULT_GROUP_SIZE, DEFAULT_TIMEOUT_NS)
            .unwrap();
        flow_control
    }

    fn on_status_message(
        flow_control: &mut MinMulticastFlowControl,
        receiver_id: i64,
        term_offset: i32,
        sender_limit: i64,
    ) -> i64 {
        let mut bytes = [0u8; 128];
        let status = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        status
            .set_receiver_id(receiver_id)
            .set_consumption_term_id(0)
            .set_consumption_term_offset(term_offset)
            .set_receiver_window_length(WINDOW_LENGTH);
        flow_control.on_status_message(status, sender_limit, 0, 0, 0)
    }

    #[test]
    fn should_parse_valid_flow_control_configuration() {
        for (uri, group_size, timeout) in [
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min",
                DEFAULT_GROUP_SIZE,
                DEFAULT_TIMEOUT_NS,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,t:100ms",
                DEFAULT_GROUP_SIZE,
                100_000_000,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:123",
                DEFAULT_GROUP_SIZE,
                DEFAULT_TIMEOUT_NS,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:3000000000",
                DEFAULT_GROUP_SIZE,
                DEFAULT_TIMEOUT_NS,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:123,t:100ms",
                DEFAULT_GROUP_SIZE,
                100_000_000,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:100/10",
                10,
                DEFAULT_TIMEOUT_NS,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/10",
                10,
                DEFAULT_TIMEOUT_NS,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:100/10,t:100ms",
                10,
                100_000_000,
            ),
        ] {
            let flow_control = initialized(uri);
            assert_eq!(flow_control.group_min_size(), group_size);
            assert_eq!(flow_control.receiver_timeout_ns(), timeout);
        }
    }

    #[test]
    fn should_not_be_connected_until_group_min_size_reached() {
        let mut flow_control =
            initialized("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/3");
        assert!(!flow_control.has_required_receivers());
        on_status_message(&mut flow_control, 1, 0, 5000);
        assert!(!flow_control.has_required_receivers());
        on_status_message(&mut flow_control, 2, 0, 5000);
        assert!(!flow_control.has_required_receivers());
        on_status_message(&mut flow_control, 2, 0, 5000);
        assert!(!flow_control.has_required_receivers());
        on_status_message(&mut flow_control, 3, 0, 5000);
        assert!(flow_control.has_required_receivers());
    }

    #[test]
    fn should_report_sender_limit_until_group_min_size_is_reached() {
        let mut flow_control =
            initialized("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/3");
        assert_eq!(on_status_message(&mut flow_control, 1, 6000, 5000), 5000);
        assert_eq!(flow_control.on_idle(0, 5000), 5000);
        assert_eq!(on_status_message(&mut flow_control, 2, 6000, 5000), 5000);
        assert_eq!(flow_control.on_idle(0, 5000), 5000);
        assert_eq!(on_status_message(&mut flow_control, 2, 6000, 5000), 5000);
        assert_eq!(flow_control.on_idle(0, 5000), 5000);
        assert_eq!(on_status_message(&mut flow_control, 3, 6000, 5000), 22384);
        assert_eq!(flow_control.on_idle(0, 5000), 22384);
    }

    #[test]
    fn should_not_include_receiver_more_than_window_size_behind_min_position() {
        let mut flow_control =
            initialized("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/2");
        assert_eq!(
            on_status_message(&mut flow_control, 1, WINDOW_LENGTH * 2, 5000),
            5000
        );
        assert_eq!(
            on_status_message(
                &mut flow_control,
                2,
                WINDOW_LENGTH * 2 - (WINDOW_LENGTH + 1),
                5000
            ),
            5000
        );
        assert_eq!(
            on_status_message(
                &mut flow_control,
                3,
                WINDOW_LENGTH * 2 - WINDOW_LENGTH,
                5000
            ),
            i64::from(WINDOW_LENGTH * 2)
        );
    }
}
