//! Property-based tests for aeron-driver configuration, publication parameters, media transport parsing, and conductor allocations.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use aeron_client::cnc_file::{CncFile, DEFAULT_TO_CLIENTS_LENGTH, DEFAULT_TO_DRIVER_LENGTH};
use aeron_client::driver_proxy::DriverProxy;
use aeron_driver::conductor::DriverConductor;
use aeron_driver::configuration::Configuration;
use aeron_driver::media::control_mode::ControlMode;
use aeron_driver::media::named_interface::NamedInterface;
use aeron_driver::media::transport_poller::DataTransportPoller;
use aeron_driver::media::udp_channel::{UdpChannel, canonicalise};
use aeron_driver::media::udp_channel_transport::UdpChannelTransport;
use aeron_driver::media::wildcard_port_manager::WildcardPortManager;
use aeron_driver::publication_params::{
    parse_duration, parse_size, producer_window_length, validate_mtu_length,
};
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;
use proptest::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};

static CNC_SEQ: AtomicU64 = AtomicU64::new(0);

/// Build a conductor bound to a fresh, uniquely-named CnC file plus a client proxy over its
/// to-driver buffer.
fn fresh_conductor() -> (DriverConductor, DriverProxy<'static>, std::path::PathBuf) {
    let seq = CNC_SEQ.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "aeron-driver-prop-{}-{}.dat",
        std::process::id(),
        seq
    ));
    let _ = std::fs::remove_file(&path);
    let cnc = CncFile::create_new(
        &path,
        DEFAULT_TO_DRIVER_LENGTH,
        DEFAULT_TO_CLIENTS_LENGTH,
        10_000_000_000,
    )
    .unwrap();
    let proxy = DriverProxy::new(ManyToOneRingBuffer::new(cnc.to_driver_buffer()).unwrap(), 1);
    let conductor = DriverConductor::new(Configuration::default(), cnc).unwrap();
    (conductor, proxy, path)
}

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

    // 8. ControlMode parsing property
    #[test]
    fn prop_control_mode_parse(
        mode_str in prop_oneof!["dynamic", "manual", "response", "invalid_mode"]
    ) {
        let parsed = ControlMode::parse(&mode_str);
        match mode_str.as_str() {
            "dynamic" => prop_assert_eq!(parsed, Some(ControlMode::Dynamic)),
            "manual" => prop_assert_eq!(parsed, Some(ControlMode::Manual)),
            "response" => prop_assert_eq!(parsed, Some(ControlMode::Response)),
            _ => prop_assert_eq!(parsed, None),
        }
    }

    // 9. UdpChannelTransport datagram delivery property
    #[test]
    fn prop_udp_transport_datagram_delivery(
        msg_val in 1u32..10000u32,
    ) {
        let channel1 = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let channel2 = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

        let t1 = UdpChannelTransport::bind(channel1, bind_addr, None, 0, 0).unwrap();
        let t2 = UdpChannelTransport::bind(channel2, bind_addr, None, 0, 0).unwrap();
        let t2_addr = t2.local_addr().unwrap();

        let payload = msg_val.to_le_bytes();
        t1.send_to(&payload, t2_addr).unwrap();

        let mut poller = DataTransportPoller::new();
        poller.register(t2);

        let mut read_bytes = 0;
        let mut read_val = 0u32;
        let mut bytes = 0;

        for _ in 0..100 {
            let b = poller.poll_transports(|data, _src| {
                read_bytes += data.len();
                if data.len() == 4 {
                    read_val = u32::from_le_bytes(data.try_into().unwrap());
                }
            }).unwrap();
            bytes += b;
            if bytes >= 4 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        prop_assert_eq!(bytes, 4);
        prop_assert_eq!(read_bytes, 4);
        prop_assert_eq!(read_val, msg_val);
    }

    // 10. DriverConductor allocates strictly-increasing, contiguous registration ids
    #[test]
    fn prop_conductor_registration_ids(
        count in 1i32..100i32,
    ) {
        let (mut conductor, proxy, path) = fresh_conductor();

        for i in 0..count {
            proxy.add_publication("aeron:ipc", i).unwrap();
        }
        conductor.do_work(1_000).unwrap();

        prop_assert_eq!(conductor.publications().len(), count as usize);
        let mut keys: Vec<i64> = conductor.publications().keys().copied().collect();
        keys.sort_unstable();
        for (idx, key) in keys.iter().enumerate() {
            prop_assert_eq!(*key, idx as i64 + 1);
        }

        let _ = std::fs::remove_file(&path);
    }

    // 11. DriverConductor add and remove cleans up state completely
    #[test]
    fn prop_conductor_add_remove_cleanup(
        count in 1i32..50i32,
    ) {
        let (mut conductor, proxy, path) = fresh_conductor();

        for i in 0..count {
            proxy.add_subscription("aeron:ipc", i).unwrap();
        }
        conductor.do_work(1_000).unwrap();
        prop_assert_eq!(conductor.subscriptions().len(), count as usize);

        let ids: Vec<i64> = conductor.subscriptions().keys().copied().collect();
        for id in ids {
            proxy.remove_subscription(id).unwrap();
        }
        conductor.do_work(2_000).unwrap();
        prop_assert_eq!(conductor.subscriptions().len(), 0);

        let _ = std::fs::remove_file(&path);
    }
}
