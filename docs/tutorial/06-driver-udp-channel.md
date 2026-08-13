# Chapter 6: Driver Media Layer & UDP Channel Parsing

This chapter covers the foundational media driver transport layer: `NamedInterface`, `SocketAddressParser`, `InterfaceSearchAddress`, `WildcardPortManager`, and `UdpChannel`.

Everything upstream of this chapter — the sender's flow control, the receiver's demuxing, the
conductor's endpoint bookkeeping in Chapters 7 and 8 — branches on the fields of one struct:
`UdpChannel` (`crates/driver/src/media/udp_channel.rs`). It's a plain, non-generic struct with no
methods that matter more than its fields: `control_mode` (`ControlMode` — manual/dynamic MDC vs.
plain unicast/multicast), `is_multicast` / `has_multicast_ttl` / `multicast_ttl`, `has_tag` /
`tag`, and four resolved socket addresses — `remote_data`, `local_data`, `remote_control`,
`local_control` — plus the `canonical_form` string used to identify one logical channel across
processes. `UdpChannel::parse` runs exactly once per channel URI and does all the hard work
(resolving `endpoint=`/`control=`/`interface=` against `SocketAddressParser` and
`InterfaceSearchAddress`, picking a control mode, validating multicast TTL); after that, every
later piece of driver code just reads fields off this table instead of re-parsing a URI string.
That's the pattern worth internalizing before the rest of this chapter's parser bullets: parse
once into a table, branch on the table forever after.

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
