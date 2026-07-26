//! Network address matching and interface-subnet selection helpers.
//!
//! Mirrors the address-selection behavior of Aeron's `NetworkUtil` without
//! depending on platform-specific network-interface handles.

use std::net::IpAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterfaceAddress {
    pub address: IpAddr,
    pub prefix_length: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkInterface {
    pub name: String,
    pub addresses: Vec<InterfaceAddress>,
    pub is_loopback: bool,
}

impl NetworkInterface {
    pub fn new(name: impl Into<String>, addresses: Vec<InterfaceAddress>) -> Self {
        Self {
            name: name.into(),
            addresses,
            is_loopback: false,
        }
    }
}

pub fn filter_by_subnet(
    interfaces: &[NetworkInterface],
    address: IpAddr,
    subnet_prefix: u8,
) -> Vec<NetworkInterface> {
    let mut matches = interfaces
        .iter()
        .filter_map(|interface| {
            interface
                .addresses
                .iter()
                .find(|candidate| {
                    is_match_with_prefix(
                        &ip_bytes(candidate.address),
                        &ip_bytes(address),
                        subnet_prefix,
                    )
                })
                .map(|candidate| (candidate.prefix_length, interface.clone()))
        })
        .collect::<Vec<_>>();

    matches.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    matches
        .into_iter()
        .map(|(_, interface)| interface)
        .collect()
}

pub fn is_match_with_prefix(candidate: &[u8], expected: &[u8], prefix_length: u8) -> bool {
    if candidate.len() != expected.len() {
        return false;
    }

    let max_prefix = candidate.len() * 8;
    if usize::from(prefix_length) > max_prefix {
        return false;
    }

    let whole_bytes = usize::from(prefix_length) / 8;
    if candidate[..whole_bytes] != expected[..whole_bytes] {
        return false;
    }

    let remaining_bits = prefix_length % 8;
    remaining_bits == 0
        || (candidate[whole_bytes] & (0xff << (8 - remaining_bits)))
            == (expected[whole_bytes] & (0xff << (8 - remaining_bits)))
}

pub fn prefix_length_to_ipv4_mask(prefix_length: u8) -> u32 {
    match prefix_length {
        0 => 0,
        1..=32 => u32::MAX << (32 - prefix_length),
        _ => 0,
    }
}

fn ip_bytes(address: IpAddr) -> Vec<u8> {
    match address {
        IpAddr::V4(address) => address.octets().to_vec(),
        IpAddr::V6(address) => address.octets().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn ipv4(address: [u8; 4], prefix_length: u8) -> InterfaceAddress {
        InterfaceAddress {
            address: IpAddr::V4(Ipv4Addr::from(address)),
            prefix_length,
        }
    }

    fn ipv6(address: [u16; 8], prefix_length: u8) -> InterfaceAddress {
        InterfaceAddress {
            address: IpAddr::V6(Ipv6Addr::new(
                address[0], address[1], address[2], address[3], address[4], address[5], address[6],
                address[7],
            )),
            prefix_length,
        }
    }

    #[test]
    fn should_not_match_if_lengths_are_different() {
        assert!(!is_match_with_prefix(&[], &[0, 0, 0], 0));
        assert!(!is_match_with_prefix(&[0], &[0, 0], 0));
        assert!(!is_match_with_prefix(&[0; 5], &[0; 16], 0));
    }

    #[test]
    fn should_match_aligned_and_unaligned_prefixes() {
        assert!(is_match_with_prefix(b"abcd", b"abcd", 32));
        assert!(is_match_with_prefix(b"abcd", b"abce", 24));
        assert!(!is_match_with_prefix(b"abcd", b"abdd", 24));
        assert!(is_match_with_prefix(
            &[0xaa, 0xff, 0, 0],
            &[0xaa, 0xfe, 0, 0],
            15
        ));
        assert!(!is_match_with_prefix(
            &[0xaa, 0xff, 0, 0],
            &[0xaa, 0xff, 0x80, 0],
            17
        ));
    }

    #[test]
    fn should_filter_ipv4_interfaces_by_subnet_and_match_length() {
        let interfaces = vec![
            NetworkInterface::new("10", vec![ipv4([10, 0, 0, 2], 8)]),
            NetworkInterface::new("8", vec![ipv4([192, 0, 0, 0], 8)]),
            NetworkInterface::new("24", vec![ipv4([192, 168, 1, 1], 24)]),
            NetworkInterface::new("16", vec![ipv4([192, 168, 0, 0], 16)]),
        ];

        let result = filter_by_subnet(&interfaces, IpAddr::V4(Ipv4Addr::new(192, 0, 0, 0)), 8);
        assert_eq!(
            result
                .iter()
                .map(|interface| interface.name.as_str())
                .collect::<Vec<_>>(),
            ["24", "16", "8"]
        );
    }

    #[test]
    fn should_filter_ipv6_interfaces() {
        let interfaces = vec![
            NetworkInterface::new("80", vec![ipv6([0xfe80, 0, 0, 1, 2, 0, 0, 1], 80)]),
            NetworkInterface::new("64", vec![ipv6([0xfe80, 0, 0, 1, 0, 0, 0, 1], 64)]),
        ];

        let result = filter_by_subnet(
            &interfaces,
            IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 1, 2, 0, 0, 0)),
            80,
        );
        assert_eq!(result, vec![interfaces[0].clone()]);
    }

    #[test]
    fn should_convert_ipv4_prefix_to_mask() {
        assert_eq!(prefix_length_to_ipv4_mask(0), 0x00000000);
        assert_eq!(prefix_length_to_ipv4_mask(1), 0x80000000);
        assert_eq!(prefix_length_to_ipv4_mask(24), 0xffffff00);
        assert_eq!(prefix_length_to_ipv4_mask(32), 0xffffffff);
    }
}
