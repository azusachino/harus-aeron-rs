# Chapter 6: Driver Media Layer & UDP Channel Parsing

This chapter covers the foundational media driver transport layer: `NamedInterface`, `SocketAddressParser`, `InterfaceSearchAddress`, `WildcardPortManager`, and `UdpChannel`.

## Socket & Interface Parsers

- `crates/driver/src/media/named_interface.rs`: Parses named network interface references formatted as `{name}` or `{name}:port`.
- `crates/driver/src/media/socket_address_parser.rs`: Parses socket addresses (`host:port`, `[ipv6]:port`, `[ipv6%scope]:port`) and detects IPv4/IPv6 multicast addresses.
- `crates/driver/src/media/interface_search_address.rs`: Handles `UnresolvedInterface` demultiplexing and search address subnet matching (`10.0.0.1:8080/24`).

## Wildcard Port Allocation

- `crates/driver/src/media/wildcard_port_manager.rs`: Manages dynamic port range allocations (`low_port..=high_port`), explicit control channel overrides, and recycling freed ports.

## UDP Channel Canonicalization

- `crates/driver/src/media/udp_channel.rs`: Performs URI validation, extracts endpoint/control/interface addresses, determines multicast/MDC control modes (`dynamic`, `manual`, `response`), and formats canonical URI strings (`UDP-local-remote`).

## Verification

Run all tests and workspace checks with:

```bash
make check
```
