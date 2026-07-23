//! UDP unicast/multicast channel transport.
//!
//! Mirrors upstream `io.aeron.driver.media`. Async UDP send/recv over
//! `tokio::net::UdpSocket`, channel URI parsing, and the non-blocking poll loop used by
//! driver agents. Not yet implemented.
pub mod control_mode;
pub mod interface_search_address;
pub mod named_interface;
pub mod send_channel_endpoint;
pub mod socket_address_parser;
pub mod transport_poller;
pub mod udp_channel;
pub mod udp_channel_transport;
pub mod wildcard_port_manager;
