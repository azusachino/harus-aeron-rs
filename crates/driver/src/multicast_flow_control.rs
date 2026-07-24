//! Selection of the multicast flow-control strategy for a channel.
//!
//! Mirrors `io.aeron.driver.DefaultMulticastFlowControlSupplier`: the `fc=` URI param names the
//! strategy (`min`/`max`/`tagged`, optionally followed by `,`-separated options), overriding the
//! `aeron.multicast.flow.control.strategy` default. The concrete `Min`/`Max`/`Tagged` strategy
//! implementations land with their own parity units; this supplier resolves which one applies.

use aeron_client::uri::FC_PARAM_NAME;

use crate::error::{DriverError, Result};
use crate::media::udp_channel::UdpChannel;

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
