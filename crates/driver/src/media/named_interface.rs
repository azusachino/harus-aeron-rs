//! Parser for named network interfaces formatted as `{name}` or `{name}:port`.
//!
//! Mirrors `io.aeron.driver.media.NamedInterface.java`.

use crate::error::{DriverError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedInterface {
    name: String,
    port: u16,
}

impl NamedInterface {
    pub fn new(name: impl Into<String>, port: u16) -> Self {
        Self {
            name: name.into(),
            port,
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.is_empty() || !s.starts_with('{') {
            return Err(DriverError::IllegalArgument(format!(
                "invalid named interface: {s}"
            )));
        }

        let last_brace = s.rfind('}').ok_or_else(|| {
            DriverError::IllegalArgument(format!(
                "invalid named interface, missing closing brace: {s}"
            ))
        })?;

        let name = &s[1..last_brace];
        if name.trim().is_empty() {
            return Err(DriverError::IllegalArgument(format!(
                "invalid named interface, empty name: {s}"
            )));
        }

        let remainder = &s[last_brace + 1..];
        let port = if remainder.is_empty() {
            0
        } else if let Some(port_str) = remainder.strip_prefix(':') {
            port_str.parse::<u16>().map_err(|_| {
                DriverError::IllegalArgument(format!("invalid named interface port: {s}"))
            })?
        } else {
            return Err(DriverError::IllegalArgument(format!(
                "invalid named interface format after brace: {s}"
            )));
        };

        Ok(Self {
            name: name.to_string(),
            port,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_throw_if_parse_input_is_invalid() {
        let invalid_inputs = [
            "",
            " ",
            "eth0",
            "eth0:0",
            "{eth0",
            "{eth0}:",
            "{eth0}0",
            "{eth0}:-1000",
            "{eth0};1000",
            "{eth0}:1000:2000",
            "{eth0}:65536",
            "{}",
        ];

        for &invalid in &invalid_inputs {
            assert!(
                NamedInterface::parse(invalid).is_err(),
                "expected parse to fail for: '{invalid}'"
            );
        }
    }

    #[test]
    fn should_parse_valid_inputs() {
        let valid_inputs = [
            ("{eth0}", "eth0", 0),
            ("{eth0}:0", "eth0", 0),
            ("{eth0}:1234", "eth0", 1234),
            ("{eth0:0}:0", "eth0:0", 0),
            ("{eth0:1}:2000", "eth0:1", 2000),
            ("{eth0.100}:80", "eth0.100", 80),
            ("{x}:5678", "x", 5678),
            ("{lo}:65535", "lo", 65535),
            ("{Ethernet 1}", "Ethernet 1", 0),
            ("{ethernet_32768}", "ethernet_32768", 0),
            ("{foo{bar-baz-12345}}:9999", "foo{bar-baz-12345}", 9999),
        ];

        for (str_input, name, port) in valid_inputs {
            let named = NamedInterface::parse(str_input).unwrap();
            assert_eq!(name, named.name());
            assert_eq!(port, named.port());
        }
    }
}
