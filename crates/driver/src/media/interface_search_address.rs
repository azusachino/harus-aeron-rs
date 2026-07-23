//! InterfaceSearchAddress and UnresolvedInterface parser.
//!
//! Mirrors `io.aeron.driver.media.InterfaceSearchAddress.java` and `UnresolvedInterface.java`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::error::{DriverError, Result};
use crate::media::named_interface::NamedInterface;
use crate::media::socket_address_parser::{DefaultNameResolver, NameResolver};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnresolvedInterface {
    Search(InterfaceSearchAddress),
    Named(NamedInterface),
}

impl UnresolvedInterface {
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.is_empty() {
            return Err(DriverError::IllegalArgument(
                "interface is null or empty".to_string(),
            ));
        }

        if s.starts_with('{') {
            NamedInterface::parse(s).map(UnresolvedInterface::Named)
        } else {
            InterfaceSearchAddress::parse(s).map(UnresolvedInterface::Search)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterfaceSearchAddress {
    pub address: SocketAddr,
    pub subnet_prefix: u32,
}

impl InterfaceSearchAddress {
    pub fn new(address: SocketAddr, subnet_prefix: u32) -> Self {
        Self {
            address,
            subnet_prefix,
        }
    }

    pub fn wildcard() -> Self {
        Self {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            subnet_prefix: 0,
        }
    }

    pub fn parse(address_and_port: &str) -> Result<Self> {
        let s = address_and_port.trim();
        if s.is_empty() {
            return Err(DriverError::IllegalArgument(
                "search address string is null or empty".to_string(),
            ));
        }

        let mut slash_index = None;
        let mut colon_index = None;
        let mut right_bracket_index = None;

        for (i, c) in s.chars().enumerate() {
            match c {
                '/' => slash_index = Some(i),
                ':' => colon_index = Some(i),
                ']' => right_bracket_index = Some(i),
                _ => {}
            }
        }

        let mut address_end = s.len();
        if let Some(slash) = slash_index {
            address_end = slash;
        }
        if let Some(colon) = colon_index {
            if right_bracket_index.is_none_or(|bracket| colon > bracket) {
                address_end = colon;
            }
        }

        let address_str = &s[..address_end];
        let host_address = DefaultNameResolver.resolve(address_str, "interface", false)?;

        let port = match (colon_index, right_bracket_index, slash_index) {
            (Some(colon), bracket, slash) if bracket.is_none_or(|b| colon > b) => {
                if colon + 1 == s.len() || slash == Some(colon + 1) {
                    return Err(DriverError::IllegalArgument(format!("invalid port: {s}")));
                }
                let port_end = slash.unwrap_or(s.len());
                let port_str = &s[colon + 1..port_end];
                port_str
                    .parse::<u16>()
                    .map_err(|_| DriverError::IllegalArgument(format!("invalid port: {s}")))?
            }
            _ => 0,
        };

        let default_subnet_prefix = match host_address {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };

        let subnet_prefix = if let Some(slash) = slash_index {
            if slash + 1 == s.len() {
                return Err(DriverError::IllegalArgument(format!("invalid subnet: {s}")));
            }
            let subnet_str = &s[slash + 1..];
            subnet_str
                .parse::<u32>()
                .map_err(|_| DriverError::IllegalArgument(format!("invalid subnet: {s}")))?
        } else {
            default_subnet_prefix
        };

        Ok(Self {
            address: SocketAddr::new(host_address, port),
            subnet_prefix,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn should_parse_valid_interface_specifications() {
        assert!(matches!(
            UnresolvedInterface::parse("localhost").unwrap(),
            UnresolvedInterface::Search(_)
        ));
        assert!(matches!(
            UnresolvedInterface::parse("10.0.0.1").unwrap(),
            UnresolvedInterface::Search(_)
        ));
        assert!(matches!(
            UnresolvedInterface::parse("[2001:db8::1]").unwrap(),
            UnresolvedInterface::Search(_)
        ));
        assert!(matches!(
            UnresolvedInterface::parse("{lo}").unwrap(),
            UnresolvedInterface::Named(_)
        ));
    }

    #[test]
    fn should_throw_on_invalid_interface_specifications() {
        assert!(UnresolvedInterface::parse("").is_err());
        assert!(UnresolvedInterface::parse("   ").is_err());
    }

    #[test]
    fn should_parse_interface_search_address_variants() {
        let isa1 = InterfaceSearchAddress::parse("10.0.0.1").unwrap();
        assert_eq!(
            isa1.address,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 0)
        );
        assert_eq!(isa1.subnet_prefix, 32);

        let isa2 = InterfaceSearchAddress::parse("10.0.0.1:8080/24").unwrap();
        assert_eq!(
            isa2.address,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080)
        );
        assert_eq!(isa2.subnet_prefix, 24);

        let isa3 = InterfaceSearchAddress::parse("[2001:db8::1]:9000/64").unwrap();
        assert_eq!(
            isa3.address,
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
                9000
            )
        );
        assert_eq!(isa3.subnet_prefix, 64);
    }
}
