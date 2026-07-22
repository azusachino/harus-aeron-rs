//! UDP unicast/multicast channel transport.
//!
//! Mirrors upstream `io.aeron.driver.media`. Async UDP send/recv over
//! `tokio::net::UdpSocket`, channel URI parsing, and the non-blocking poll loop used by
//! driver agents. Not yet implemented.
