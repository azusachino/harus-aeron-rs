//! Aggregator of network publications on a transport channel for sending data and setup frames.
//!
//! Mirrors `io.aeron.driver.media.SendChannelEndpoint.java`.

use std::net::SocketAddr;

use crate::error::Result;
use crate::media::udp_channel::UdpChannel;
use crate::media::udp_channel_transport::UdpChannelTransport;

#[derive(Debug)]
pub struct SendChannelEndpoint {
    udp_channel: UdpChannel,
    transport: UdpChannelTransport,
}

impl SendChannelEndpoint {
    pub fn new(udp_channel: UdpChannel, transport: UdpChannelTransport) -> Self {
        Self {
            udp_channel,
            transport,
        }
    }

    pub fn send_data(&self, buf: &[u8], target: SocketAddr) -> Result<usize> {
        self.transport.send_to(buf, target)
    }

    pub fn udp_channel(&self) -> &UdpChannel {
        &self.udp_channel
    }

    pub fn transport(&self) -> &UdpChannelTransport {
        &self.transport
    }

    pub fn on_status_message(
        &mut self,
        _session_id: i32,
        _stream_id: i32,
        _term_id: i32,
        _term_offset: i32,
        _receiver_window_length: i32,
    ) {
        // Status message processing callback
    }

    pub fn on_nak_message(
        &mut self,
        _session_id: i32,
        _stream_id: i32,
        _term_id: i32,
        _term_offset: i32,
        _length: i32,
    ) {
        // NAK message processing callback
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn should_create_send_channel_endpoint() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();

        let endpoint = SendChannelEndpoint::new(channel, transport);
        assert_eq!(
            &endpoint.udp_channel().original_uri_string,
            "aeron:udp?endpoint=localhost:0"
        );
    }
}
