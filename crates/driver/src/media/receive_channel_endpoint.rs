//! Aggregator of subscriptions on a transport channel for receiving data and setup frames and sending status/NAK feedback.
//!
//! Mirrors `io.aeron.driver.media.ReceiveChannelEndpoint.java`.

use std::net::SocketAddr;

use crate::error::Result;
use crate::media::udp_channel::UdpChannel;
use crate::media::udp_channel_transport::UdpChannelTransport;

#[derive(Debug)]
pub struct ReceiveChannelEndpoint {
    udp_channel: UdpChannel,
    transport: UdpChannelTransport,
}

impl ReceiveChannelEndpoint {
    pub fn new(udp_channel: UdpChannel, transport: UdpChannelTransport) -> Self {
        Self {
            udp_channel,
            transport,
        }
    }

    pub fn udp_channel(&self) -> &UdpChannel {
        &self.udp_channel
    }

    pub fn transport(&self) -> &UdpChannelTransport {
        &self.transport
    }

    pub fn on_data_packet(
        &mut self,
        _session_id: i32,
        _stream_id: i32,
        _term_id: i32,
        _term_offset: i32,
        _payload: &[u8],
        _src_addr: SocketAddr,
    ) {
        // Data packet dispatch to image buffers
    }

    pub fn on_setup_message(
        &mut self,
        _session_id: i32,
        _stream_id: i32,
        _active_term_id: i32,
        _term_offset: i32,
        _src_addr: SocketAddr,
    ) {
        // Setup message handling
    }

    pub fn send_status_message(
        &self,
        _session_id: i32,
        _stream_id: i32,
        _term_id: i32,
        _term_offset: i32,
        _window_length: i32,
        target: SocketAddr,
    ) -> Result<usize> {
        let sm_buf = [0u8; 32];
        self.transport.send_to(&sm_buf, target)
    }

    pub fn send_nak(
        &self,
        _session_id: i32,
        _stream_id: i32,
        _term_id: i32,
        _term_offset: i32,
        _length: i32,
        target: SocketAddr,
    ) -> Result<usize> {
        let nak_buf = [0u8; 28];
        self.transport.send_to(&nak_buf, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn should_create_receive_channel_endpoint() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();

        let endpoint = ReceiveChannelEndpoint::new(channel, transport);
        assert_eq!(
            &endpoint.udp_channel().original_uri_string,
            "aeron:udp?endpoint=localhost:0"
        );
    }
}
