//! Parser for socket addresses (`host:port` / `[ipv6]:port`) and multicast detection.
//!
//! Mirrors `io.aeron.driver.media.SocketAddressParser.java`.

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

use crate::error::{DriverError, Result};

pub trait NameResolver {
    fn resolve(&self, name: &str, uri_param_name: &str, is_re_resolution: bool) -> Result<IpAddr>;
    fn lookup(&self, name: &str, uri_param_name: &str, is_re_lookup: bool) -> String;

    /// Complete resolver construction after the driver has created its shared counters.
    fn init(&self) {}

    /// Perform periodic resolver work and return the amount of work completed.
    fn do_work(&self, _now_ms: i64) -> usize {
        0
    }

    /// Name used in diagnostics and runtime status.
    fn name(&self) -> String {
        std::any::type_name::<Self>().to_string()
    }

    /// Release resolver-owned resources during driver shutdown.
    fn close(&mut self) {}
}

pub struct DefaultNameResolver;

impl NameResolver for DefaultNameResolver {
    fn resolve(
        &self,
        name: &str,
        _uri_param_name: &str,
        _is_re_resolution: bool,
    ) -> Result<IpAddr> {
        let clean_name = if let Some(idx) = name.find('%') {
            &name[..idx]
        } else {
            name
        };

        if let Ok(ip) = clean_name.parse::<IpAddr>() {
            return Ok(ip);
        }

        let host_port = format!("{name}:0");
        if let Ok(addrs) = host_port.to_socket_addrs() {
            // getaddrinfo's ordering is resolver/OS-dependent (e.g. some hosts return ::1 for
            // "localhost" ahead of 127.0.0.1); prefer an IPv4 result when one exists so
            // resolution is deterministic across platforms, falling back to IPv6 otherwise.
            let mut first = None;
            for addr in addrs {
                if addr.is_ipv4() {
                    return Ok(addr.ip());
                }
                first.get_or_insert(addr);
            }
            if let Some(addr) = first {
                return Ok(addr.ip());
            }
        }

        Err(DriverError::IllegalArgument(format!(
            "unknown host: {name}"
        )))
    }

    fn lookup(&self, name: &str, _uri_param_name: &str, _is_re_lookup: bool) -> String {
        name.to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParseResult {
    host: String,
    port: u16,
}

pub struct SocketAddressParser;

impl SocketAddressParser {
    pub fn parse(
        value: &str,
        uri_param_name: &str,
        is_re_resolution: bool,
        resolver: &impl NameResolver,
    ) -> Result<SocketAddr> {
        let value = value.trim();
        if value.is_empty() {
            return Err(DriverError::IllegalArgument(format!(
                "invalid {uri_param_name}: empty address"
            )));
        }

        let name_and_port = resolver.lookup(value, uri_param_name, is_re_resolution);

        let result = if let Ok(res) = Self::try_parse_ipv4(&name_and_port) {
            res
        } else if let Ok(res) = Self::try_parse_ipv6(&name_and_port) {
            res
        } else {
            return Err(DriverError::IllegalArgument(format!(
                "invalid format: {name_and_port}"
            )));
        };

        let ip = resolver.resolve(&result.host, uri_param_name, is_re_resolution)?;
        Ok(SocketAddr::new(ip, result.port))
    }

    pub fn is_multicast_address(host_and_port: &str) -> Result<bool> {
        let value = host_and_port.trim();
        if value.is_empty() {
            return Err(DriverError::IllegalArgument("empty address".to_string()));
        }

        if let Ok(res) = Self::try_parse_ipv4(value) {
            let host = res.host;
            if let Some(dot_idx) = host.find('.') {
                if let Ok(first_byte) = host[..dot_idx].parse::<u32>() {
                    return Ok((0xE0..=0xEF).contains(&first_byte));
                }
            }
            Ok(false)
        } else if let Ok(res) = Self::try_parse_ipv6(value) {
            let first_two = &res.host[..res.host.len().min(2)];
            Ok(first_two.eq_ignore_ascii_case("ff"))
        } else {
            Err(DriverError::IllegalArgument(format!(
                "invalid format: {host_and_port}"
            )))
        }
    }

    fn try_parse_ipv4(str: &str) -> Result<ParseResult> {
        let mut separator_index = None;
        let bytes = str.as_bytes();

        for (i, &b) in bytes.iter().enumerate() {
            if b == b':' {
                if separator_index.is_some() {
                    return Err(DriverError::IllegalArgument(format!(
                        "multiple colons: {str}"
                    )));
                }
                separator_index = Some(i);
            } else if separator_index.is_some() && !b.is_ascii_digit() {
                return Err(DriverError::IllegalArgument(format!(
                    "non-digit in port: {str}"
                )));
            }
        }

        if let Some(sep) = separator_index {
            if sep + 1 < str.len() {
                let hostname = &str[..sep];
                let port_str = &str[sep + 1..];
                if let Ok(port) = port_str.parse::<u16>() {
                    return Ok(ParseResult {
                        host: hostname.to_string(),
                        port,
                    });
                }
            }
        }

        Err(DriverError::IllegalArgument(format!(
            "address:port is required for ipv4: {str}"
        )))
    }

    fn try_parse_ipv6(str: &str) -> Result<ParseResult> {
        if !str.starts_with('[') {
            return Err(DriverError::IllegalArgument(format!(
                "ipv6 must start with [: {str}"
            )));
        }

        let close_bracket = str
            .find(']')
            .ok_or_else(|| DriverError::IllegalArgument(format!("ipv6 missing ]: {str}")))?;

        let inner = &str[1..close_bracket];
        let scope_index = inner.find('%');
        let host_part = if let Some(idx) = scope_index {
            &inner[..idx]
        } else {
            inner
        };

        // Validate hex digits or colons
        for c in host_part.chars() {
            if c != ':' && !c.is_ascii_hexdigit() {
                return Err(DriverError::IllegalArgument(format!(
                    "invalid character in ipv6 host: {str}"
                )));
            }
        }

        if let Some(scope_str) = scope_index.map(|idx| &inner[idx + 1..]) {
            for c in scope_str.chars() {
                if c != '_' && c != '.' && c != '~' && c != '-' && !c.is_ascii_alphanumeric() {
                    return Err(DriverError::IllegalArgument(format!(
                        "invalid character in ipv6 scope: {str}"
                    )));
                }
            }
        }

        let remainder = &str[close_bracket + 1..];
        if !remainder.starts_with(':') || remainder.len() <= 1 {
            return Err(DriverError::IllegalArgument(format!(
                "[address]:port is required for ipv6: {str}"
            )));
        }

        let port = remainder[1..]
            .parse::<u16>()
            .map_err(|_| DriverError::IllegalArgument(format!("invalid port in ipv6: {str}")))?;

        Ok(ParseResult {
            host: host_part.to_string(),
            port,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    // Cases cross-checked against:
    // vendor/aeron/aeron-driver/src/test/java/io/aeron/driver/media/SocketAddressParserTest.java

    const LOOKUP_RESOLVER_NAME: &str = "StreamX";
    const LOOKUP_RESOLVER_HOSTNAME: &str = "192.168.1.20";
    const LOOKUP_RESOLVER_PORT: u16 = 55;

    struct TestLookupResolver;

    impl NameResolver for TestLookupResolver {
        fn resolve(
            &self,
            name: &str,
            uri_param_name: &str,
            is_re_resolution: bool,
        ) -> Result<IpAddr> {
            DefaultNameResolver.resolve(name, uri_param_name, is_re_resolution)
        }

        fn lookup(&self, name: &str, _uri_param_name: &str, _is_re_lookup: bool) -> String {
            if name == LOOKUP_RESOLVER_NAME {
                format!("{LOOKUP_RESOLVER_HOSTNAME}:{LOOKUP_RESOLVER_PORT}")
            } else {
                name.to_string()
            }
        }
    }

    #[test]
    fn should_parse_ipv4_address_and_port() {
        let addr =
            SocketAddressParser::parse("192.168.1.20:55", "endpoint", false, &DefaultNameResolver)
                .unwrap();
        assert_eq!(
            addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)), 55)
        );
    }

    #[test]
    fn should_parse_host_address_and_port() {
        let addr =
            SocketAddressParser::parse("localhost:55", "endpoint", false, &DefaultNameResolver)
                .unwrap();
        assert_eq!(addr.port(), 55);
    }

    #[test]
    fn default_name_resolver_resolves_localhost() {
        assert_eq!(
            DefaultNameResolver
                .resolve("localhost", "control", true)
                .unwrap(),
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        );
    }

    #[test]
    fn default_name_resolver_rejects_unknown_host() {
        assert!(
            DefaultNameResolver
                .resolve("host-that-does-not-exist.invalid", "endpoint", false)
                .is_err()
        );
    }

    #[test]
    fn should_reject_on_invalid_port() {
        assert!(
            SocketAddressParser::parse("192.168.1.20:aa", "endpoint", false, &DefaultNameResolver)
                .is_err()
        );
        assert!(
            SocketAddressParser::parse(
                "192.168.1.20::123",
                "endpoint",
                false,
                &DefaultNameResolver
            )
            .is_err()
        );
    }

    #[test]
    fn should_reject_on_missing_port() {
        assert!(
            SocketAddressParser::parse("192.168.1.20", "endpoint", false, &DefaultNameResolver)
                .is_err()
        );
    }

    #[test]
    fn should_reject_on_empty_port() {
        assert!(
            SocketAddressParser::parse("192.168.1.20:", "endpoint", false, &DefaultNameResolver)
                .is_err()
        );
        assert!(
            SocketAddressParser::parse("[::1]:", "endpoint", false, &DefaultNameResolver).is_err()
        );
    }

    #[test]
    fn should_reject_on_invalid_ipv6() {
        assert!(
            SocketAddressParser::parse(
                "[FG07::789:1:0:0:3]:111",
                "endpoint",
                false,
                &DefaultNameResolver
            )
            .is_err()
        );
    }

    #[test]
    fn should_reject_on_invalid_ipv6_scope() {
        assert!(
            SocketAddressParser::parse(
                "[FC07::789:1:0:0:3%^]:111",
                "endpoint",
                false,
                &DefaultNameResolver,
            )
            .is_err()
        );
    }

    #[test]
    fn should_parse_ipv6() {
        let addr1 =
            SocketAddressParser::parse("[::1]:54321", "endpoint", false, &DefaultNameResolver)
                .unwrap();
        assert_eq!(addr1.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(addr1.port(), 54321);

        let addr2 = SocketAddressParser::parse(
            "[FC07::789:1:0:0:3]:54321",
            "endpoint",
            false,
            &DefaultNameResolver,
        )
        .unwrap();
        assert_eq!(addr2.port(), 54321);

        let addr3 = SocketAddressParser::parse(
            "[fc07::789:1:0:0:3]:54321",
            "endpoint",
            false,
            &DefaultNameResolver,
        )
        .unwrap();
        assert_eq!(addr3.port(), 54321);
    }

    #[test]
    fn should_parse_with_scope() {
        let addr = SocketAddressParser::parse(
            "[::1%12~_.-34]:1234",
            "endpoint",
            false,
            &DefaultNameResolver,
        )
        .unwrap();
        assert!(addr.ip().is_ipv6());
        assert_eq!(addr.port(), 1234);
    }

    #[test]
    fn should_parse_and_lookup_resolver_for_name() {
        let addr = SocketAddressParser::parse(
            LOOKUP_RESOLVER_NAME,
            "endpoint",
            false,
            &TestLookupResolver,
        )
        .unwrap();
        assert_eq!(
            addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)), 55)
        );
    }

    #[test]
    fn should_parse_and_pass_through_lookup_for_unknown_name() {
        let addr =
            SocketAddressParser::parse("192.168.1.20:55", "endpoint", false, &TestLookupResolver)
                .unwrap();
        assert_eq!(
            addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)), 55)
        );
    }

    #[test]
    fn should_check_for_multicast_address() {
        let cases = [
            ("127.0.0.1:8080", false),
            ("224.0.0.0:24", true),
            ("224.0.0.255:44444", true),
            ("239.255.255.250:1010", true),
            ("239.1.1.1:1111", true),
            ("233.251.255.255:5555", true),
            ("999.255.255.250:7777", false),
            ("a.b.c.d:7777", false),
            ("192.b.c.d:7777", false),
            ("192.168.0.1:1234", false),
            ("test:1234", false),
            ("[ff02::1]:1234", true),
            ("[Ff05::1:3]:80", true),
            ("[fF9E::108]:5555", true),
            ("[FF01:0:0:0:0:0:0:18C]:5555", true),
            ("[fe80::ce81:b1c:bd2c:69e%utun3]:8080", false),
            ("[fe80::1%lo0]:5555", false),
            ("[ff02:0:0:0:0:2]:7777", true),
        ];

        for (host_port, expected) in cases {
            assert_eq!(
                expected,
                SocketAddressParser::is_multicast_address(host_port).unwrap(),
                "failed for: {host_port}"
            );
        }
    }

    #[test]
    fn should_throw_illegal_argument_if_value_is_invalid() {
        let invalid = ["", "a", "1.2.3", "[x]", "[f0:xx]:5"];
        for s in invalid {
            let error = SocketAddressParser::is_multicast_address(s).unwrap_err();
            assert!(
                error.to_string().contains(s),
                "error did not mention: '{s}'"
            );
        }
    }
}
