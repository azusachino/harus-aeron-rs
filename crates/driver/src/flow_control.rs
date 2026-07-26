//! Flow-control calculations shared by network publications.
//!
//! The formulas and `rrwm:` URI parsing mirror the checked-in upstream sources:
//! `vendor/aeron/aeron-driver/src/main/java/io/aeron/driver/FlowControl.java` and
//! `vendor/aeron/aeron-driver/src/main/java/io/aeron/driver/Configuration.java`.

use aeron_client::uri::FC_PARAM_NAME;

use crate::error::{DriverError, Result};
use crate::media::udp_channel::UdpChannel;

pub const INITIAL_WINDOW_LENGTH_DEFAULT: i32 = 128 * 1024;
pub const UNICAST_RRWM_DEFAULT: i32 = 16;
pub const MULTICAST_RRWM_DEFAULT: i32 = 4;

/// Match upstream `Configuration.receiverWindowLength`.
pub fn receiver_window_length(term_buffer_length: i32, initial_window_length: i32) -> i32 {
    initial_window_length.min(term_buffer_length >> 1)
}

/// Clamp a retransmit to the end of term and the estimated receiver window.
pub fn calculate_retransmission_length(
    resend_length: i32,
    term_buffer_length: i32,
    term_offset: i32,
    retransmit_receiver_window_multiple: i32,
) -> i32 {
    let length_to_end_of_term = term_buffer_length - term_offset;
    let estimated_retransmit_length =
        receiver_window_length(term_buffer_length, INITIAL_WINDOW_LENGTH_DEFAULT)
            * retransmit_receiver_window_multiple;
    length_to_end_of_term
        .min(estimated_retransmit_length)
        .min(resend_length)
}

/// Read `rrwm:<positive integer>` from the channel's `fc` value, or use the media default.
pub fn retransmit_receiver_window_multiple(
    udp_channel: &UdpChannel,
    default_value: i32,
) -> Result<i32> {
    let Some(flow_control) = udp_channel.channel_uri.get(FC_PARAM_NAME) else {
        return Ok(default_value);
    };
    for argument in flow_control.split(',') {
        if let Some(value) = argument.strip_prefix("rrwm:") {
            let rrwm = value.parse::<i32>().map_err(|_| {
                DriverError::IllegalArgument(format!(
                    "invalid retransmit receiver window multiple: {value}"
                ))
            })?;
            if rrwm <= 0 {
                return Err(DriverError::IllegalArgument(format!(
                    "invalid retransmit receiver window multiple: {rrwm}"
                )));
            }
            return Ok(rrwm);
        }
    }
    Ok(default_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MULTICAST_DEFAULT: i32 = MULTICAST_RRWM_DEFAULT;
    const UNICAST_DEFAULT: i32 = UNICAST_RRWM_DEFAULT;

    fn rrwm(uri: &str, default_value: i32) -> Result<i32> {
        retransmit_receiver_window_multiple(&UdpChannel::parse(uri).unwrap(), default_value)
    }

    #[test]
    fn should_use_resend_length_if_smallest() {
        assert_eq!(
            calculate_retransmission_length(1024, 64 * 1024, 0, 16),
            1024
        );
    }

    #[test]
    fn should_clamp_to_end_of_term() {
        assert_eq!(
            calculate_retransmission_length(1024, 64 * 1024, 64 * 1024 - 512, 16),
            512
        );
    }

    #[test]
    fn should_clamp_to_receiver_window() {
        assert_eq!(
            calculate_retransmission_length(4 * 1024 * 1024, 8 * 1024 * 1024, 0, 16),
            INITIAL_WINDOW_LENGTH_DEFAULT * 16
        );
    }

    #[test]
    fn should_parse_rrwm_from_uri_and_reject_invalid_values() {
        let channel = UdpChannel::parse(
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max,rrwm:8",
        )
        .unwrap();
        assert_eq!(retransmit_receiver_window_multiple(&channel, 2).unwrap(), 8);

        for value in ["a", "", " ", "foo", "0", "-1"] {
            let channel = UdpChannel::parse(&format!(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max,rrwm:{value}"
            ))
            .unwrap();
            assert!(retransmit_receiver_window_multiple(&channel, 2).is_err());
        }
    }

    #[test]
    fn should_use_default_multicast_rrwm_when_not_set_in_uri() {
        for uri in [
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost",
        ] {
            assert_eq!(rrwm(uri, MULTICAST_DEFAULT).unwrap(), MULTICAST_DEFAULT);
        }
    }

    #[test]
    fn should_use_rrwm_from_uri_for_all_multicast_strategies() {
        for uri in [
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max,rrwm:8",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max,foo:bar,rrwm:8,a:b",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,rrwm:8",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:100/10,t:100ms,rrwm:8",
        ] {
            assert_eq!(rrwm(uri, 2).unwrap(), 8);
        }
    }

    #[test]
    fn should_reject_invalid_rrwm_values() {
        for value in ["a", "", " ", "foo", "0", "-1"] {
            let uri = format!(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max,rrwm:{value}"
            );
            assert!(rrwm(&uri, 2).is_err(), "expected invalid rrwm: {value:?}");
        }
    }

    #[test]
    fn should_use_multicast_default_for_max_min_and_tagged() {
        for strategy in ["max", "min", "tagged"] {
            let uri =
                format!("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc={strategy}");
            assert_eq!(rrwm(&uri, MULTICAST_DEFAULT).unwrap(), MULTICAST_DEFAULT);
        }
    }

    #[test]
    fn should_use_multicast_context_value_for_max_min_and_tagged() {
        for strategy in ["max", "min", "tagged"] {
            let uri =
                format!("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc={strategy}");
            assert_eq!(rrwm(&uri, 2).unwrap(), 2);
        }
    }

    #[test]
    fn should_use_uri_value_for_max_min_and_tagged() {
        for strategy in ["max", "min", "tagged"] {
            let uri = format!(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc={strategy},rrwm:8"
            );
            assert_eq!(rrwm(&uri, 2).unwrap(), 8);
        }
    }

    #[test]
    fn should_use_default_unicast_rrwm_when_not_set_in_context() {
        assert_eq!(
            rrwm("aeron:udp?endpoint=localhost:0", UNICAST_DEFAULT).unwrap(),
            UNICAST_DEFAULT
        );
    }

    #[test]
    fn should_use_unicast_context_rrwm_when_set() {
        assert_eq!(rrwm("aeron:udp?endpoint=localhost:0", 2).unwrap(), 2);
    }
}
