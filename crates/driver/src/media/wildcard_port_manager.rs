//! Wildcard port manager for UDP channel transports within a specific port range.
//!
//! Mirrors `io.aeron.driver.media.WildcardPortManager.java` and `PortManager.java`.

use std::collections::HashSet;
use std::net::SocketAddr;

use crate::error::{DriverError, Result};

pub const EMPTY_PORT_RANGE: (u16, u16) = (0, 0);

pub trait PortManager {
    fn get_managed_port(
        &mut self,
        has_explicit_control: bool,
        bind_address: SocketAddr,
    ) -> Result<SocketAddr>;

    fn free_managed_port(&mut self, bind_address: SocketAddr);
}

#[derive(Debug, Clone)]
pub struct WildcardPortManager {
    port_set: HashSet<u16>,
    low_port: u16,
    high_port: u16,
    is_os_wildcard: bool,
    is_sender: bool,
    next_port: u16,
}

impl WildcardPortManager {
    pub fn new(port_range: (u16, u16), is_sender: bool) -> Self {
        let (low_port, high_port) = port_range;
        let is_os_wildcard = low_port == 0 && high_port == 0;
        Self {
            port_set: HashSet::new(),
            low_port,
            high_port,
            is_os_wildcard,
            is_sender,
            next_port: low_port,
        }
    }

    pub fn parse_port_range(value: &str) -> Result<(u16, u16)> {
        let value = value.trim();
        if value.is_empty() {
            return Ok(EMPTY_PORT_RANGE);
        }

        let parts: Vec<&str> = value.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(DriverError::IllegalArgument(format!(
                "port range \"{value}\" incorrect format"
            )));
        }

        let low = parts[0]
            .parse::<u16>()
            .map_err(|_| DriverError::IllegalArgument(format!("invalid port value {value}")))?;
        let high = parts[1]
            .parse::<u16>()
            .map_err(|_| DriverError::IllegalArgument(format!("invalid port value {value}")))?;

        if low > high {
            return Err(DriverError::IllegalArgument(format!(
                "low port value must be lower than high port value: {value}"
            )));
        }

        Ok((low, high))
    }

    fn find_open_port(&self) -> u16 {
        for port in self.next_port..=self.high_port {
            if !self.port_set.contains(&port) {
                return port;
            }
        }

        for port in self.low_port..self.next_port {
            if !self.port_set.contains(&port) {
                return port;
            }
        }

        0
    }

    fn allocate_open_port(&mut self) -> Result<u16> {
        let port = self.find_open_port();
        if port == 0 {
            return Err(DriverError::IllegalState(format!(
                "no available ports in range {} {}",
                self.low_port, self.high_port
            )));
        }

        self.next_port = if port >= self.high_port {
            self.low_port
        } else {
            port + 1
        };

        self.port_set.insert(port);
        Ok(port)
    }
}

impl PortManager for WildcardPortManager {
    fn get_managed_port(
        &mut self,
        has_explicit_control: bool,
        bind_address: SocketAddr,
    ) -> Result<SocketAddr> {
        let mut address = bind_address;

        if bind_address.port() != 0 {
            self.port_set.insert(bind_address.port());
        } else if !self.is_os_wildcard && (!self.is_sender || has_explicit_control) {
            let allocated = self.allocate_open_port()?;
            address.set_port(allocated);
        }

        Ok(address)
    }

    fn free_managed_port(&mut self, bind_address: SocketAddr) {
        if bind_address.port() != 0 {
            self.port_set.remove(&bind_address.port());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn localhost(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[test]
    fn should_allocate_consecutive_ports_in_range() {
        let mut manager = WildcardPortManager::new((20000, 20003), false);
        let bind_addr = localhost(0);

        assert_eq!(
            manager.get_managed_port(false, bind_addr).unwrap(),
            localhost(20000)
        );
        assert_eq!(
            manager.get_managed_port(false, bind_addr).unwrap(),
            localhost(20001)
        );
        assert_eq!(
            manager.get_managed_port(false, bind_addr).unwrap(),
            localhost(20002)
        );
    }

    #[test]
    fn should_pass_through_0_with_null_ranges() {
        let bind_addr = localhost(0);
        let mut manager = WildcardPortManager::new(EMPTY_PORT_RANGE, false);

        assert_eq!(
            manager.get_managed_port(false, bind_addr).unwrap(),
            localhost(0)
        );
    }

    #[test]
    fn should_pass_through_0_with_sender_pub_without_control_address() {
        let bind_addr = localhost(0);
        let mut manager = WildcardPortManager::new((20000, 20003), true);

        assert_eq!(
            manager.get_managed_port(false, bind_addr).unwrap(),
            localhost(0)
        );
    }

    #[test]
    fn should_pass_through_with_explicit_bind_address_outside_range() {
        let bind_addr = localhost(1000);
        let mut manager = WildcardPortManager::new((20000, 20003), true);

        assert_eq!(
            manager.get_managed_port(false, bind_addr).unwrap(),
            localhost(1000)
        );
    }

    #[test]
    fn should_pass_through_with_explicit_bind_address_inside_range() {
        let bind_addr = localhost(20003);
        let mut manager = WildcardPortManager::new((20000, 20003), true);

        assert_eq!(
            manager.get_managed_port(false, bind_addr).unwrap(),
            localhost(20003)
        );
    }

    #[test]
    fn should_allocate_for_pub_with_explicit_control_address() {
        let bind_addr = localhost(0);
        let mut manager = WildcardPortManager::new((20000, 20003), true);

        assert_eq!(
            manager.get_managed_port(true, bind_addr).unwrap(),
            localhost(20000)
        );
    }

    #[test]
    fn should_throw_on_exhaustion() {
        let bind_addr = localhost(0);
        let mut manager = WildcardPortManager::new((20000, 20003), false);

        assert!(manager.get_managed_port(false, bind_addr).is_ok());
        assert!(manager.get_managed_port(false, bind_addr).is_ok());
        assert!(manager.get_managed_port(false, bind_addr).is_ok());
        assert!(manager.get_managed_port(false, bind_addr).is_ok());
        assert!(manager.get_managed_port(false, bind_addr).is_err());
    }

    #[test]
    fn should_allocate_on_cycling_through_range() {
        let bind_addr = localhost(0);
        let mut manager = WildcardPortManager::new((20000, 20003), false);

        assert!(manager.get_managed_port(false, bind_addr).is_ok());
        assert!(manager.get_managed_port(false, bind_addr).is_ok());
        assert!(manager.get_managed_port(false, bind_addr).is_ok());
        assert!(manager.get_managed_port(false, bind_addr).is_ok());

        manager.free_managed_port(localhost(20000));
        assert_eq!(
            manager.get_managed_port(true, bind_addr).unwrap(),
            localhost(20000)
        );
    }

    #[test]
    fn should_allocate_skipping_in_use_port() {
        let bind_addr = localhost(0);
        let mut manager = WildcardPortManager::new((20000, 20003), false);

        assert!(manager.get_managed_port(false, bind_addr).is_ok());
        assert!(manager.get_managed_port(false, localhost(20001)).is_ok());
        assert_eq!(
            manager.get_managed_port(true, bind_addr).unwrap(),
            localhost(20002)
        );
    }

    #[test]
    fn should_allocate_skipping_in_use_on_cycle() {
        let bind_addr = localhost(0);
        let mut manager = WildcardPortManager::new((20000, 20003), false);

        assert!(manager.get_managed_port(false, localhost(20000)).is_ok());
        assert!(manager.get_managed_port(false, localhost(20001)).is_ok());
        assert!(manager.get_managed_port(false, localhost(20002)).is_ok());

        manager.free_managed_port(localhost(20002));
        assert!(manager.get_managed_port(false, localhost(20003)).is_ok());
        assert_eq!(
            manager.get_managed_port(true, bind_addr).unwrap(),
            localhost(20002)
        );
    }

    #[test]
    fn test_parse_port_range() {
        assert_eq!(
            WildcardPortManager::parse_port_range("20000 20003").unwrap(),
            (20000, 20003)
        );
        assert_eq!(
            WildcardPortManager::parse_port_range("").unwrap(),
            EMPTY_PORT_RANGE
        );
        assert!(WildcardPortManager::parse_port_range("20000").is_err());
        assert!(WildcardPortManager::parse_port_range("20003 20000").is_err());
    }
}
