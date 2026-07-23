//! Media configuration for Aeron UDP channels parsed from channel URIs.
//!
//! Mirrors `io.aeron.driver.media.UdpChannel.java`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};

use aeron_client::uri::{
    ChannelUri, ENDPOINT_PARAM_NAME, INTERFACE_PARAM_NAME, SOCKET_RCVBUF_PARAM_NAME,
    SOCKET_SNDBUF_PARAM_NAME, TTL_PARAM_NAME,
};

use crate::error::{DriverError, Result};
use crate::media::control_mode::ControlMode;
use crate::media::interface_search_address::UnresolvedInterface;
use crate::media::socket_address_parser::{DefaultNameResolver, NameResolver, SocketAddressParser};
use crate::publication_params::{parse_duration, parse_size};

pub const RESERVED_VALUE_MESSAGE_OFFSET: i32 = -8;
pub const NULL_VALUE: i32 = -1;

pub const MDC_CONTROL_PARAM_NAME: &str = "control";
pub const MDC_CONTROL_MODE_PARAM_NAME: &str = "control-mode";
pub const NAK_DELAY_PARAM_NAME: &str = "nak-delay";
pub const RECEIVER_WINDOW_LENGTH_PARAM_NAME: &str = "rcv-wnd";
pub const CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME: &str = "channel-rcv-ts-offset";
pub const CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME: &str = "channel-snd-ts-offset";
pub const GROUP_TAG_PARAM_NAME: &str = "gtag";
pub const RESERVED_OFFSET_STR: &str = "reserved";

static UNIQUE_CANONICAL_FORM_VALUE: AtomicU64 = AtomicU64::new(0);

const ANY_IPV4: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
const ANY_IPV6: SocketAddr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpChannel {
    pub control_mode: ControlMode,
    pub has_explicit_control: bool,
    pub has_explicit_endpoint: bool,
    pub is_multicast: bool,
    pub has_multicast_ttl: bool,
    pub multicast_ttl: i32,
    pub has_tag: bool,
    pub tag: i64,
    pub remote_data: SocketAddr,
    pub local_data: SocketAddr,
    pub remote_control: SocketAddr,
    pub local_control: SocketAddr,
    pub original_uri_string: String,
    pub canonical_form: String,
    pub channel_uri: ChannelUri,
    pub socket_rcvbuf_length: usize,
    pub socket_sndbuf_length: usize,
    pub receiver_window_length: usize,
    pub channel_receive_timestamp_offset: i32,
    pub channel_send_timestamp_offset: i32,
    pub group_tag: Option<i64>,
    pub nak_delay_ns: Option<u64>,
}

impl UdpChannel {
    pub fn parse(channel_uri_string: &str) -> Result<Self> {
        Self::parse_with_resolver(channel_uri_string, &DefaultNameResolver, false)
    }

    pub fn parse_with_resolver(
        channel_uri_string: &str,
        name_resolver: &impl NameResolver,
        is_destination: bool,
    ) -> Result<Self> {
        let channel_uri = ChannelUri::parse(channel_uri_string)
            .map_err(|msg| DriverError::InvalidChannel(format!("invalid channel URI: {msg}")))?;

        if !channel_uri.is_udp() {
            return Err(DriverError::InvalidChannel(format!(
                "UdpChannel only supports UDP media: {channel_uri_string}"
            )));
        }

        let endpoint_address = if let Some(endpoint_val) = channel_uri.get(ENDPOINT_PARAM_NAME) {
            Some(SocketAddressParser::parse(
                endpoint_val,
                ENDPOINT_PARAM_NAME,
                false,
                name_resolver,
            )?)
        } else {
            None
        };

        let control_address = if let Some(control_val) = channel_uri.get(MDC_CONTROL_PARAM_NAME) {
            Some(SocketAddressParser::parse(
                control_val,
                MDC_CONTROL_PARAM_NAME,
                false,
                name_resolver,
            )?)
        } else {
            None
        };

        let tag_id_str = channel_uri.channel_tag();
        let control_mode = parse_control_mode(&channel_uri);

        let socket_rcvbuf_length = parse_buffer_length(&channel_uri, SOCKET_RCVBUF_PARAM_NAME)?;
        let socket_sndbuf_length = parse_buffer_length(&channel_uri, SOCKET_SNDBUF_PARAM_NAME)?;
        let receiver_window_length =
            parse_buffer_length(&channel_uri, RECEIVER_WINDOW_LENGTH_PARAM_NAME)?;

        let requires_additional_suffix = !is_destination
            && ((endpoint_address.is_none() && control_address.is_none())
                || endpoint_address.is_some_and(|a| a.port() == 0)
                || control_address.is_some_and(|a| a.port() == 0));

        let has_no_distinguishing_characteristic =
            endpoint_address.is_none() && control_address.is_none() && tag_id_str.is_none();

        if control_mode == ControlMode::Dynamic && control_address.is_none() {
            return Err(DriverError::InvalidChannel(format!(
                "explicit control expected with dynamic control mode: {channel_uri_string}"
            )));
        }

        if has_no_distinguishing_characteristic
            && control_mode != ControlMode::Manual
            && control_mode != ControlMode::Response
        {
            return Err(DriverError::InvalidChannel(format!(
                "URIs for UDP must specify an endpoint, control, tags, or control-mode=manual/response: {channel_uri_string}"
            )));
        }

        let has_explicit_endpoint = endpoint_address.is_some();
        let effective_endpoint = endpoint_address.unwrap_or_else(|| {
            if control_address.is_some_and(|a| a.is_ipv6()) {
                ANY_IPV6
            } else {
                ANY_IPV4
            }
        });

        let mut is_multicast = false;
        let mut has_multicast_ttl = false;
        let mut multicast_ttl = 0;
        let remote_data;
        let local_data;
        let remote_control;
        let local_control;
        let canonical_form;

        let mut has_explicit_control = false;

        let is_endpoint_multicast =
            SocketAddressParser::is_multicast_address(&effective_endpoint.to_string())
                .unwrap_or(false);

        if is_endpoint_multicast {
            is_multicast = true;
            remote_data = effective_endpoint;
            remote_control = get_multicast_control_address(effective_endpoint)?;

            let unresolved_ifc = get_interface(&channel_uri)?;
            let local_addr = match unresolved_ifc {
                UnresolvedInterface::Search(isa) => isa.address,
                UnresolvedInterface::Named(ni) => {
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ni.port())
                }
            };

            local_data = local_addr;
            local_control = local_addr;
            canonical_form = canonicalise(None, local_addr, None, effective_endpoint);

            if let Some(ttl_val) = channel_uri.get(TTL_PARAM_NAME) {
                has_multicast_ttl = true;
                multicast_ttl = ttl_val
                    .parse::<i32>()
                    .map_err(|_| DriverError::InvalidChannel(format!("invalid ttl: {ttl_val}")))?;
            }
        } else if let Some(ctrl_addr) = control_address {
            has_explicit_control = true;
            remote_data = effective_endpoint;
            remote_control = effective_endpoint;
            local_data = ctrl_addr;
            local_control = ctrl_addr;

            let control_val = channel_uri.get(MDC_CONTROL_PARAM_NAME);
            let endpoint_val = channel_uri.get(ENDPOINT_PARAM_NAME);

            let suffix = if requires_additional_suffix {
                if let Some(tag) = tag_id_str {
                    format!("#{tag}")
                } else {
                    format!(
                        "-{}",
                        UNIQUE_CANONICAL_FORM_VALUE.fetch_add(1, Ordering::Relaxed)
                    )
                }
            } else {
                String::new()
            };

            canonical_form = format!(
                "{}{suffix}",
                canonicalise(control_val, ctrl_addr, endpoint_val, effective_endpoint)
            );
        } else {
            remote_data = effective_endpoint;
            remote_control = effective_endpoint;

            let unresolved_ifc = get_interface(&channel_uri)?;
            let local_addr = match unresolved_ifc {
                UnresolvedInterface::Search(isa) => isa.address,
                UnresolvedInterface::Named(ni) => {
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ni.port())
                }
            };

            local_data = local_addr;
            local_control = local_addr;

            let endpoint_val = channel_uri.get(ENDPOINT_PARAM_NAME);
            let suffix = if requires_additional_suffix {
                if let Some(tag) = tag_id_str {
                    format!("#{tag}")
                } else {
                    format!(
                        "-{}",
                        UNIQUE_CANONICAL_FORM_VALUE.fetch_add(1, Ordering::Relaxed)
                    )
                }
            } else {
                String::new()
            };

            canonical_form = format!(
                "{}{suffix}",
                canonicalise(None, local_addr, endpoint_val, effective_endpoint)
            );
        }

        let has_tag = tag_id_str.is_some();
        let tag = if let Some(t) = tag_id_str {
            t.parse::<i64>()
                .map_err(|_| DriverError::InvalidChannel(format!("invalid tag: {t}")))?
        } else {
            0
        };

        let channel_receive_timestamp_offset =
            parse_timestamp_offset(&channel_uri, CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME)?;
        let channel_send_timestamp_offset =
            parse_timestamp_offset(&channel_uri, CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME)?;

        let group_tag = parse_optional_long(&channel_uri, GROUP_TAG_PARAM_NAME)?;
        let nak_delay_ns = channel_uri
            .get(NAK_DELAY_PARAM_NAME)
            .map(|s| parse_duration(NAK_DELAY_PARAM_NAME, s))
            .transpose()?;

        Ok(Self {
            control_mode,
            has_explicit_control,
            has_explicit_endpoint,
            is_multicast,
            has_multicast_ttl,
            multicast_ttl,
            has_tag,
            tag,
            remote_data,
            local_data,
            remote_control,
            local_control,
            original_uri_string: channel_uri_string.to_string(),
            canonical_form,
            channel_uri,
            socket_rcvbuf_length,
            socket_sndbuf_length,
            receiver_window_length,
            channel_receive_timestamp_offset,
            channel_send_timestamp_offset,
            group_tag,
            nak_delay_ns,
        })
    }

    pub fn is_multi_destination(&self) -> bool {
        self.control_mode.is_multi_destination()
    }

    pub fn has_group_semantics(&self) -> bool {
        self.is_multicast || self.is_multi_destination()
    }

    pub fn is_manual_control_mode(&self) -> bool {
        self.control_mode == ControlMode::Manual
    }

    pub fn is_dynamic_control_mode(&self) -> bool {
        self.control_mode == ControlMode::Dynamic
    }

    pub fn is_response_control_mode(&self) -> bool {
        self.control_mode == ControlMode::Response
    }
}

pub fn canonicalise(
    local_param_value: Option<&str>,
    local_data: SocketAddr,
    remote_param_value: Option<&str>,
    remote_data: SocketAddr,
) -> String {
    let mut s = String::with_capacity(48);
    s.push_str("UDP-");

    if let Some(local_val) = local_param_value {
        s.push_str(local_val);
    } else {
        s.push_str(&format!("{}:{}", local_data.ip(), local_data.port()));
    }

    s.push('-');

    if let Some(remote_val) = remote_param_value {
        s.push_str(remote_val);
    } else {
        s.push_str(&format!("{}:{}", remote_data.ip(), remote_data.port()));
    }

    s
}

fn parse_control_mode(channel_uri: &ChannelUri) -> ControlMode {
    if let Some(val) = channel_uri.get(MDC_CONTROL_MODE_PARAM_NAME) {
        ControlMode::parse(val).unwrap_or(ControlMode::None)
    } else {
        ControlMode::None
    }
}

fn parse_buffer_length(channel_uri: &ChannelUri, param_name: &str) -> Result<usize> {
    if let Some(val) = channel_uri.get(param_name) {
        parse_size(param_name, val)
    } else {
        Ok(0)
    }
}

fn parse_timestamp_offset(channel_uri: &ChannelUri, param_name: &str) -> Result<i32> {
    if let Some(val) = channel_uri.get(param_name) {
        if val == RESERVED_OFFSET_STR {
            Ok(RESERVED_VALUE_MESSAGE_OFFSET)
        } else {
            val.parse::<i32>().map_err(|_| {
                DriverError::InvalidChannel(format!(
                    "timestamp offset must be a valid integer or '{RESERVED_OFFSET_STR}'"
                ))
            })
        }
    } else {
        Ok(NULL_VALUE)
    }
}

fn parse_optional_long(channel_uri: &ChannelUri, param_name: &str) -> Result<Option<i64>> {
    if let Some(val) = channel_uri.get(param_name) {
        val.parse::<i64>().map(Some).map_err(|_| {
            DriverError::InvalidChannel(format!("'{param_name}' must be a valid long"))
        })
    } else {
        Ok(None)
    }
}

fn get_interface(channel_uri: &ChannelUri) -> Result<UnresolvedInterface> {
    if let Some(ifc_val) = channel_uri.get(INTERFACE_PARAM_NAME) {
        UnresolvedInterface::parse(ifc_val)
    } else {
        Ok(UnresolvedInterface::Search(
            crate::media::interface_search_address::InterfaceSearchAddress::wildcard(),
        ))
    }
}

fn get_multicast_control_address(endpoint_address: SocketAddr) -> Result<SocketAddr> {
    match endpoint_address.ip() {
        IpAddr::V4(ipv4) => {
            let octets = ipv4.octets();
            let last = octets[3];
            if last % 2 == 0 {
                return Err(DriverError::InvalidChannel(
                    "multicast data address must be odd".to_string(),
                ));
            }
            let new_ipv4 = Ipv4Addr::new(octets[0], octets[1], octets[2], last + 1);
            Ok(SocketAddr::new(
                IpAddr::V4(new_ipv4),
                endpoint_address.port(),
            ))
        }
        IpAddr::V6(ipv6) => {
            let mut segments = ipv6.segments();
            let last = segments[7];
            if last % 2 == 0 {
                return Err(DriverError::InvalidChannel(
                    "multicast data address must be odd".to_string(),
                ));
            }
            segments[7] += 1;
            let new_ipv6 = Ipv6Addr::from(segments);
            Ok(SocketAddr::new(
                IpAddr::V6(new_ipv6),
                endpoint_address.port(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn should_handle_explicit_local_address_and_port_format() {
        let channel =
            UdpChannel::parse("aeron:udp?interface=localhost:40123|endpoint=localhost:40124")
                .unwrap();

        assert_eq!(
            channel.local_data,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40123)
        );
        assert_eq!(
            channel.local_control,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40123)
        );
        assert_eq!(
            channel.remote_data,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40124)
        );
        assert_eq!(
            channel.remote_control,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40124)
        );
    }

    #[test]
    fn should_handle_explicit_local_control_address_and_port_format_ipv4() {
        let channel =
            UdpChannel::parse("aeron:udp?control=localhost:40124|control-mode=dynamic").unwrap();

        assert_eq!(
            channel.local_data,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40124)
        );
        assert_eq!(
            channel.local_control,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40124)
        );
        assert_eq!(
            channel.remote_data,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
        );
        assert_eq!(
            channel.remote_control,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
        );
    }

    #[test]
    fn should_not_allow_dynamic_control_mode_without_explicit_control() {
        let err = UdpChannel::parse("aeron:udp?control-mode=dynamic").unwrap_err();
        assert!(
            err.to_string()
                .contains("explicit control expected with dynamic control mode")
        );
    }

    #[test]
    fn should_handle_implied_local_address_and_port_format() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:40124").unwrap();

        assert_eq!(
            channel.local_data,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
        );
        assert_eq!(
            channel.local_control,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
        );
        assert_eq!(
            channel.remote_data,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40124)
        );
        assert_eq!(
            channel.remote_control,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40124)
        );
    }

    #[test]
    fn should_throw_exception_for_incorrect_scheme() {
        assert!(UdpChannel::parse("unknownudp://localhost:40124").is_err());
    }

    #[test]
    fn should_throw_exception_for_missing_address_with_aeron_uri() {
        assert!(UdpChannel::parse("aeron:udp").is_err());
    }

    #[test]
    fn should_throw_exception_on_even_multicast_address() {
        assert!(UdpChannel::parse("aeron:udp?endpoint=224.10.9.8:40124").is_err());
    }

    #[test]
    fn should_parse_valid_multicast_address() {
        let channel =
            UdpChannel::parse("aeron:udp?interface=localhost|endpoint=224.10.9.9:40124").unwrap();

        assert_eq!(
            channel.local_control,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
        );
        assert_eq!(
            channel.remote_control,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 10, 9, 10)), 40124)
        );
        assert_eq!(
            channel.local_data,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
        );
        assert_eq!(
            channel.remote_data,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 10, 9, 9)), 40124)
        );
    }
}
