//! Selection of the multicast flow-control strategy for a channel.
//!
//! Mirrors `io.aeron.driver.DefaultMulticastFlowControlSupplier`: the `fc=` URI param names the
//! strategy (`min`/`max`/`tagged`, optionally followed by `,`-separated options), overriding the
//! `aeron.multicast.flow.control.strategy` default. The concrete `Min`/`Max`/`Tagged` strategy
//! implementations land with their own parity units; this supplier resolves which one applies.

use aeron_client::logbuffer::compute_position;
use aeron_client::protocol::{END_OF_STREAM_FLAG, ErrorFlyweight, StatusMessageFlyweight};
use aeron_client::uri::FC_PARAM_NAME;

use crate::error::{DriverError, Result};
use crate::media::udp_channel::UdpChannel;
use crate::publication_params::parse_duration;

const DEFAULT_GROUP_MIN_SIZE: usize = 0;
const DEFAULT_RECEIVER_TIMEOUT_NS: i64 = 5_000_000_000;
const DEFAULT_FLOW_CONTROL_GROUP_TAG: i64 = -1;

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
    group_tag: i64,
    tag_aware: bool,
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
            group_tag: DEFAULT_FLOW_CONTROL_GROUP_TAG,
            tag_aware: false,
        }
    }

    /// Initialise strategy state from context defaults and the channel's `fc=` options.
    pub fn initialize(
        &mut self,
        udp_channel: &UdpChannel,
        context_group_min_size: usize,
        context_receiver_timeout_ns: i64,
    ) -> Result<()> {
        self.initialize_mode(
            udp_channel,
            context_group_min_size,
            context_receiver_timeout_ns,
            false,
            DEFAULT_FLOW_CONTROL_GROUP_TAG,
        )
    }

    fn initialize_tagged(
        &mut self,
        udp_channel: &UdpChannel,
        context_group_min_size: usize,
        context_receiver_timeout_ns: i64,
    ) -> Result<()> {
        self.initialize_mode(
            udp_channel,
            context_group_min_size,
            context_receiver_timeout_ns,
            true,
            DEFAULT_FLOW_CONTROL_GROUP_TAG,
        )
    }

    fn initialize_mode(
        &mut self,
        udp_channel: &UdpChannel,
        context_group_min_size: usize,
        context_receiver_timeout_ns: i64,
        tag_aware: bool,
        default_group_tag: i64,
    ) -> Result<()> {
        self.receivers.clear();
        self.group_min_size = context_group_min_size;
        self.receiver_timeout_ns = context_receiver_timeout_ns;
        self.last_setup_sender_limit = None;
        self.time_of_last_setup_ns = 0;
        self.group_tag = default_group_tag;
        self.tag_aware = tag_aware;

        if let Some(fc_value) = udp_channel.channel_uri.get(FC_PARAM_NAME) {
            for argument in fc_value.split(',') {
                if let Some(timeout) = argument.strip_prefix("t:") {
                    self.receiver_timeout_ns =
                        parse_duration("fc receiver timeout", timeout)? as i64;
                } else if let Some(group) = argument.strip_prefix("g:") {
                    let (tag, min_size) = group.split_once('/').unwrap_or((group, ""));
                    if self.tag_aware && !tag.is_empty() {
                        self.group_tag = tag.parse::<i64>().map_err(|_| {
                            DriverError::IllegalArgument(format!(
                                "invalid flow control group tag: {tag}"
                            ))
                        })?;
                    }
                    if self.tag_aware && !group.contains('/') && tag.is_empty() {
                        return Err(DriverError::IllegalArgument(
                            "invalid flow control group tag".to_string(),
                        ));
                    }
                    if group.contains('/') {
                        if min_size.is_empty() {
                            return Err(DriverError::IllegalArgument(
                                "invalid flow control group minimum size".to_string(),
                            ));
                        }
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

    pub fn group_tag(&self) -> i64 {
        self.group_tag
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
        self.on_status_message_if(
            status_message,
            sender_limit,
            initial_term_id,
            position_bits_to_shift,
            time_ns,
            true,
        )
    }

    fn on_status_message_if(
        &mut self,
        status_message: StatusMessageFlyweight<'_>,
        sender_limit: i64,
        initial_term_id: i32,
        position_bits_to_shift: i32,
        time_ns: i64,
        matches_tag: bool,
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
            if matches_tag && receiver.receiver_id == receiver_id {
                receiver.last_position_plus_window = last_position_plus_window;
                receiver.time_of_last_status_message_ns = time_ns;
                receiver.eos_flagged = status_message.flags() & END_OF_STREAM_FLAG != 0;
                is_existing = true;
            }
            min_position = min_position.min(receiver.last_position_plus_window);
        }

        if matches_tag
            && !is_existing
            && (self.receivers.is_empty()
                || last_position_plus_window
                    >= min_position - i64::from(status_message.receiver_window_length()))
        {
            self.receivers.push(ReceiverState {
                receiver_id,
                last_position_plus_window,
                time_of_last_status_message_ns: time_ns,
                eos_flagged: status_message.flags() & END_OF_STREAM_FLAG != 0,
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

    fn on_error(&mut self, error: ErrorFlyweight<'_>, matches_tag: bool) {
        if self.tag_aware && !matches_tag {
            return;
        }
        if let Some(receiver) = self
            .receivers
            .iter_mut()
            .find(|receiver| receiver.receiver_id == error.receiver_id())
        {
            receiver.eos_flagged = true;
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

/// Tagged multicast flow control, tracking only receivers carrying the configured group tag.
#[derive(Debug, Default)]
pub struct TaggedMulticastFlowControl {
    inner: MinMulticastFlowControl,
}

impl TaggedMulticastFlowControl {
    pub const FC_PARAM_VALUE: &'static str = "tagged";

    pub fn new() -> Self {
        Self::default()
    }

    pub fn initialize(
        &mut self,
        udp_channel: &UdpChannel,
        context_group_min_size: usize,
        context_receiver_timeout_ns: i64,
    ) -> Result<()> {
        self.inner.initialize_tagged(
            udp_channel,
            context_group_min_size,
            context_receiver_timeout_ns,
        )
    }

    pub fn group_tag(&self) -> i64 {
        self.inner.group_tag()
    }

    pub fn group_min_size(&self) -> usize {
        self.inner.group_min_size()
    }

    pub fn receiver_timeout_ns(&self) -> i64 {
        self.inner.receiver_timeout_ns()
    }

    pub fn has_required_receivers(&self) -> bool {
        self.inner.has_required_receivers()
    }

    pub fn on_status_message(
        &mut self,
        status_message: StatusMessageFlyweight<'_>,
        sender_limit: i64,
        initial_term_id: i32,
        position_bits_to_shift: i32,
        time_ns: i64,
    ) -> i64 {
        let matches_tag =
            status_message.has_group_tag() && status_message.group_tag() == self.group_tag();
        self.inner.on_status_message_if(
            status_message,
            sender_limit,
            initial_term_id,
            position_bits_to_shift,
            time_ns,
            matches_tag,
        )
    }

    pub fn on_idle(&mut self, time_ns: i64, sender_limit: i64) -> i64 {
        self.inner.on_idle(time_ns, sender_limit)
    }

    pub fn on_error(&mut self, error: ErrorFlyweight<'_>) {
        let matches_tag = error.has_group_tag() && error.group_tag() == self.group_tag();
        self.inner.on_error(error, matches_tag);
    }
}

/// A network publication's live flow-control state, dispatching to whichever strategy its
/// channel selected. `Max` carries no extra state: it is exactly the previous unconditional
/// `receiver_limit.max(new_receiver_limit)` behavior, now made one option among three instead of
/// the only one every publication got regardless of its `fc=` parameter.
pub enum NetworkFlowControl {
    Max,
    Min(MinMulticastFlowControl),
    Tagged(TaggedMulticastFlowControl),
}

impl NetworkFlowControl {
    /// Resolve `udp_channel`'s `fc=` strategy and initialize its state, using vendor default
    /// group-min-size/receiver-timeout context values (the channel's own `fc=g:.../t:...`
    /// options can still override them per `initialize`).
    pub fn new_for_channel(
        udp_channel: &UdpChannel,
        stream_id: i32,
        registration_id: i64,
    ) -> Result<Self> {
        let strategy = DefaultMulticastFlowControlSupplier::new().new_instance(
            udp_channel,
            stream_id,
            registration_id,
        )?;
        Ok(match strategy {
            MulticastFlowControlStrategy::Max => NetworkFlowControl::Max,
            MulticastFlowControlStrategy::Min => {
                let mut min = MinMulticastFlowControl::new();
                min.initialize(
                    udp_channel,
                    DEFAULT_GROUP_MIN_SIZE,
                    DEFAULT_RECEIVER_TIMEOUT_NS,
                )?;
                NetworkFlowControl::Min(min)
            }
            MulticastFlowControlStrategy::Tagged => {
                let mut tagged = TaggedMulticastFlowControl::new();
                tagged.initialize(
                    udp_channel,
                    DEFAULT_GROUP_MIN_SIZE,
                    DEFAULT_RECEIVER_TIMEOUT_NS,
                )?;
                NetworkFlowControl::Tagged(tagged)
            }
        })
    }

    /// Compute the sender's new position limit from a received Status Message.
    pub fn on_status_message(
        &mut self,
        status_message: StatusMessageFlyweight<'_>,
        sender_limit: i64,
        initial_term_id: i32,
        position_bits_to_shift: i32,
        time_ns: i64,
    ) -> i64 {
        match self {
            NetworkFlowControl::Max => {
                let position = compute_position(
                    status_message.consumption_term_id(),
                    status_message.consumption_term_offset(),
                    position_bits_to_shift,
                    initial_term_id,
                );
                let window = i64::from(status_message.receiver_window_length().max(0));
                sender_limit.max(position.saturating_add(window))
            }
            NetworkFlowControl::Min(min) => min.on_status_message(
                status_message,
                sender_limit,
                initial_term_id,
                position_bits_to_shift,
                time_ns,
            ),
            NetworkFlowControl::Tagged(tagged) => tagged.on_status_message(
                status_message,
                sender_limit,
                initial_term_id,
                position_bits_to_shift,
                time_ns,
            ),
        }
    }

    /// Expire receivers that have gone quiet past their timeout, matching real disconnect
    /// detection for `Min`/`Tagged`. A no-op for `Max`, which tracks no per-receiver state.
    pub fn on_idle(&mut self, time_ns: i64, sender_limit: i64) -> i64 {
        match self {
            NetworkFlowControl::Max => sender_limit,
            NetworkFlowControl::Min(min) => min.on_idle(time_ns, sender_limit),
            NetworkFlowControl::Tagged(tagged) => tagged.on_idle(time_ns, sender_limit),
        }
    }

    /// Whether the required number of receivers have been observed according to group constraints.
    pub fn has_required_receivers(&self) -> bool {
        match self {
            NetworkFlowControl::Max => true,
            NetworkFlowControl::Min(min) => min.has_required_receivers(),
            NetworkFlowControl::Tagged(tagged) => tagged.has_required_receivers(),
        }
    }

    /// The receiver timeout configured for this flow control strategy, in nanoseconds.
    pub fn receiver_timeout_ns(&self) -> i64 {
        match self {
            NetworkFlowControl::Max => DEFAULT_RECEIVER_TIMEOUT_NS,
            NetworkFlowControl::Min(min) => min.receiver_timeout_ns(),
            NetworkFlowControl::Tagged(tagged) => tagged.receiver_timeout_ns(),
        }
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
        on_status_message_at(flow_control, receiver_id, term_offset, sender_limit, 0)
    }

    fn on_status_message_at(
        flow_control: &mut MinMulticastFlowControl,
        receiver_id: i64,
        term_offset: i32,
        sender_limit: i64,
        time_ns: i64,
    ) -> i64 {
        let mut bytes = [0u8; 128];
        let status = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        status
            .set_receiver_id(receiver_id)
            .set_consumption_term_id(0)
            .set_consumption_term_offset(term_offset)
            .set_receiver_window_length(WINDOW_LENGTH);
        flow_control.on_status_message(status, sender_limit, 0, 0, time_ns)
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

    #[test]
    fn should_remove_only_timed_out_receivers_when_the_last_receiver_has_also_timed_out() {
        let mut flow_control = initialized(
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/1,t:100ms",
        );

        let sender_limit = 5000;
        let term_offset_one = 1000;
        let term_offset_two = 2000;
        let term_offset_three = 3000;

        on_status_message_at(&mut flow_control, 1, term_offset_one, sender_limit, 0);
        on_status_message_at(&mut flow_control, 2, term_offset_two, sender_limit, 0);
        on_status_message_at(&mut flow_control, 3, term_offset_three, sender_limit, 0);

        on_status_message_at(
            &mut flow_control,
            2,
            term_offset_two,
            sender_limit,
            50_000_000,
        );

        // receivers one and three have timed out, receiver two has not
        assert_eq!(
            flow_control.on_idle(100_000_001, sender_limit),
            i64::from(term_offset_two + WINDOW_LENGTH)
        );

        // receiver two must be the receiver that survived the removal pass
        assert_eq!(
            flow_control.on_idle(100_000_002, sender_limit),
            i64::from(term_offset_two + WINDOW_LENGTH)
        );
    }
}

#[cfg(test)]
mod tagged_flow_control_tests {
    //! Faithful port of
    //! vendor/aeron/aeron-driver/src/test/java/io/aeron/driver/TaggedMulticastFlowControlTest.java

    use super::*;
    use agrona::AtomicBuffer;

    const DEFAULT_GROUP_SIZE: usize = 0;
    const DEFAULT_GROUP_TAG: i64 = -1;
    const DEFAULT_TIMEOUT_NS: i64 = 5_000_000_000;
    const WINDOW_LENGTH: i32 = 16 * 1024;

    fn initialized(uri: &str) -> TaggedMulticastFlowControl {
        let mut flow_control = TaggedMulticastFlowControl::new();
        flow_control
            .initialize(
                &UdpChannel::parse(uri).unwrap(),
                DEFAULT_GROUP_SIZE,
                DEFAULT_TIMEOUT_NS,
            )
            .unwrap();
        flow_control
    }

    fn status(
        flow_control: &mut TaggedMulticastFlowControl,
        receiver_id: i64,
        term_offset: i32,
        sender_limit: i64,
        group_tag: Option<i64>,
        eos: bool,
    ) -> i64 {
        let mut bytes = [0u8; 128];
        let message = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        message
            .set_receiver_id(receiver_id)
            .set_consumption_term_id(0)
            .set_consumption_term_offset(term_offset)
            .set_receiver_window_length(WINDOW_LENGTH)
            .set_flags(if eos { END_OF_STREAM_FLAG } else { 0 })
            .set_group_tag(group_tag);
        flow_control.on_status_message(message, sender_limit, 0, 0, 0)
    }

    #[test]
    fn should_parse_valid_flow_control_configuration() {
        for (uri, group_tag, group_size, timeout) in [
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged",
                DEFAULT_GROUP_TAG,
                DEFAULT_GROUP_SIZE,
                DEFAULT_TIMEOUT_NS,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,t:100ms",
                DEFAULT_GROUP_TAG,
                DEFAULT_GROUP_SIZE,
                100_000_000,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123",
                123,
                DEFAULT_GROUP_SIZE,
                DEFAULT_TIMEOUT_NS,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:3000000000",
                3_000_000_000,
                DEFAULT_GROUP_SIZE,
                DEFAULT_TIMEOUT_NS,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123,t:100ms",
                123,
                DEFAULT_GROUP_SIZE,
                100_000_000,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:100/10",
                100,
                10,
                DEFAULT_TIMEOUT_NS,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:/10",
                DEFAULT_GROUP_TAG,
                10,
                DEFAULT_TIMEOUT_NS,
            ),
            (
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:100/10,t:100ms",
                100,
                10,
                100_000_000,
            ),
        ] {
            let flow_control = initialized(uri);
            assert_eq!(flow_control.group_tag(), group_tag);
            assert_eq!(flow_control.group_min_size(), group_size);
            assert_eq!(flow_control.receiver_timeout_ns(), timeout);
        }
    }

    #[test]
    fn should_fail_with_invalid_uris() {
        for uri in [
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:100/",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:/",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,t:",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:100,t:",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,t:100ms,g:100/",
        ] {
            let result = TaggedMulticastFlowControl::new().initialize(
                &UdpChannel::parse(uri).unwrap(),
                DEFAULT_GROUP_SIZE,
                DEFAULT_TIMEOUT_NS,
            );
            assert!(result.is_err(), "expected invalid URI: {uri}");
        }
    }

    #[test]
    fn should_clamp_to_sender_limit_until_minimum_group_size_is_met() {
        let mut flow_control = initialized(
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123/3",
        );
        for (receiver_id, group_tag) in [(0, None), (1, Some(123)), (2, Some(123)), (3, None)] {
            assert_eq!(
                status(
                    &mut flow_control,
                    receiver_id,
                    10_000,
                    5000,
                    group_tag,
                    false
                ),
                5000
            );
            assert_eq!(flow_control.on_idle(0, 5000), 5000);
        }
        assert_eq!(
            status(&mut flow_control, 4, 10_000, 5000, Some(123), false),
            26_384
        );
        assert_eq!(flow_control.on_idle(0, 5000), 26_384);
    }

    #[test]
    fn should_return_last_window_when_receivers_are_in_group_with_no_min_size() {
        let mut flow_control = initialized(
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123",
        );
        assert_eq!(
            status(&mut flow_control, 0, 10_000, 5000, None, false),
            26_384
        );
        assert_eq!(
            status(&mut flow_control, 1, 9_999, 5000, Some(123), false),
            26_383
        );
        assert_eq!(
            status(&mut flow_control, 0, 10_000, 5000, None, false),
            26_383
        );
    }

    #[test]
    fn should_not_include_receiver_more_than_window_size_behind_min_position() {
        let mut flow_control = initialized(
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123/2",
        );
        assert_eq!(
            status(
                &mut flow_control,
                1,
                WINDOW_LENGTH * 2,
                5000,
                Some(123),
                false
            ),
            5000
        );
        assert_eq!(
            status(
                &mut flow_control,
                2,
                WINDOW_LENGTH - 1,
                5000,
                Some(123),
                false
            ),
            5000
        );
        assert_eq!(
            status(&mut flow_control, 3, WINDOW_LENGTH, 5000, Some(123), false),
            i64::from(WINDOW_LENGTH * 2)
        );
    }

    #[test]
    fn should_remove_entry_on_end_of_stream_and_error() {
        let mut flow_control = initialized(
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123/2",
        );
        assert_eq!(
            status(&mut flow_control, 1, WINDOW_LENGTH, 5000, Some(123), false),
            5000
        );
        assert_eq!(
            status(
                &mut flow_control,
                2,
                WINDOW_LENGTH + 1,
                5000,
                Some(123),
                false
            ),
            32_768
        );
        assert_eq!(
            status(
                &mut flow_control,
                3,
                WINDOW_LENGTH + 2,
                5000,
                Some(123),
                false
            ),
            32_768
        );
        assert_eq!(
            status(&mut flow_control, 1, WINDOW_LENGTH, 5000, Some(123), true),
            32_768
        );
        assert_eq!(flow_control.on_idle(0, 5000), 32_769);

        let mut bytes = [0u8; 128];
        let error = ErrorFlyweight::new(AtomicBuffer::new(&mut bytes));
        let mut error_flow_control = initialized(
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123/2",
        );
        status(
            &mut error_flow_control,
            1,
            WINDOW_LENGTH,
            5000,
            Some(123),
            false,
        );
        status(
            &mut error_flow_control,
            2,
            WINDOW_LENGTH + 1,
            5000,
            Some(123),
            false,
        );
        status(
            &mut error_flow_control,
            3,
            WINDOW_LENGTH + 2,
            5000,
            Some(123),
            false,
        );
        error.set_receiver_id(1).set_group_tag(Some(123));
        error_flow_control.on_error(error);
        assert_eq!(error_flow_control.on_idle(0, 5000), 32_769);
    }
}
