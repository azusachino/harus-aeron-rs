//! Property-based tests for aeron-driver configuration, publication parameters, and media transport parsing.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use aeron_driver::media::named_interface::NamedInterface;
use aeron_driver::media::udp_channel::canonicalise;
use aeron_driver::media::wildcard_port_manager::WildcardPortManager;
use aeron_driver::publication_params::{
    parse_duration, parse_size, producer_window_length, validate_mtu_length,
};
use proptest::prelude::*;

proptest! {
    // 1. parse_size roundtrip for numeric values with k/m/g suffixes
    #[test]
    fn prop_parse_size_suffixes(val in 1usize..1000usize) {
        let raw_str = format!("{val}");
        prop_assert_eq!(parse_size("test", &raw_str).unwrap(), val);

        let k_str = format!("{val}k");
        prop_assert_eq!(parse_size("test", &k_str).unwrap(), val * 1024);

        let m_str = format!("{val}m");
        prop_assert_eq!(parse_size("test", &m_str).unwrap(), val * 1024 * 1024);
    }

    // 2. parse_duration roundtrip for numeric values with ns/us/ms/s suffixes
    #[test]
    fn prop_parse_duration_suffixes(val in 1u64..1000u64) {
        let raw_str = format!("{val}");
        prop_assert_eq!(parse_duration("test", &raw_str).unwrap(), val);

        let ns_str = format!("{val}ns");
        prop_assert_eq!(parse_duration("test", &ns_str).unwrap(), val);

        let us_str = format!("{val}us");
        prop_assert_eq!(parse_duration("test", &us_str).unwrap(), val * 1000);

        let ms_str = format!("{val}ms");
        prop_assert_eq!(parse_duration("test", &ms_str).unwrap(), val * 1_000_000);

        let s_str = format!("{val}s");
        prop_assert_eq!(parse_duration("test", &s_str).unwrap(), val * 1_000_000_000);
    }

    // 3. producer_window_length never exceeds half term_length
    #[test]
    fn prop_producer_window_length(
        term_length in (64usize..1048576usize).prop_map(|v| v * 1024),
        config_window in 0usize..10000000usize,
    ) {
        let window = producer_window_length(term_length, config_window);
        let half_term = term_length >> 1;
        prop_assert!(window <= half_term);
        if config_window != 0 {
            prop_assert_eq!(window, config_window.min(half_term));
        } else {
            prop_assert_eq!(window, half_term);
        }
    }

    // 4. validate_mtu_length enforces multiples of 32 between 32 and 65504
    #[test]
    fn prop_validate_mtu_length(valid_multiplier in 1usize..2047usize) {
        let valid_mtu = valid_multiplier * 32;
        prop_assert!(validate_mtu_length(valid_mtu).is_ok());
    }

    // 5. canonicalise formatting produces UDP-local-remote
    #[test]
    fn prop_canonicalise_format(
        local_port in 1000u16..60000u16,
        remote_port in 1000u16..60000u16,
    ) {
        let local_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), local_port);
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), remote_port);

        let canonical = canonicalise(None, local_addr, None, remote_addr);
        prop_assert!(canonical.starts_with("UDP-127.0.0.1:"));
        prop_assert!(canonical.contains("-127.0.0.1:"));
    }

    // 6. NamedInterface parse roundtrip for valid names and ports
    #[test]
    fn prop_named_interface_roundtrip(
        port in 0u16..=65535u16,
    ) {
        let formatted = format!("{{eth0}}:{port}");
        let parsed = NamedInterface::parse(&formatted).unwrap();
        prop_assert_eq!(parsed.name(), "eth0");
        prop_assert_eq!(parsed.port(), port);
    }

    // 7. WildcardPortManager parse_port_range property for valid range
    #[test]
    fn prop_wildcard_port_range(
        low in 1000u16..30000u16,
        delta in 0u16..1000u16,
    ) {
        let high = low + delta;
        let formatted = format!("{low} {high}");
        let (p_low, p_high) = WildcardPortManager::parse_port_range(&formatted).unwrap();
        prop_assert_eq!(p_low, low);
        prop_assert_eq!(p_high, high);
    }
}
