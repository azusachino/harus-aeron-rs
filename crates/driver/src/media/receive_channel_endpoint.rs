//! Aggregator of subscriptions on a transport channel for receiving data and setup frames and sending status/NAK feedback.
//!
//! Mirrors `io.aeron.driver.media.ReceiveChannelEndpoint.java`.

use std::net::SocketAddr;

use aeron_client::protocol::{
    CURRENT_VERSION, HDR_TYPE_NAK, HDR_TYPE_SM, NAK_HEADER_LENGTH, NakFlyweight,
    STATUS_MESSAGE_HEADER_LENGTH, StatusMessageFlyweight,
};
use agrona::AtomicBuffer;

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

    /// Build and send a real Status Message flow-control frame to `target`.
    pub fn send_status_message(
        &self,
        session_id: i32,
        stream_id: i32,
        term_id: i32,
        term_offset: i32,
        window_length: i32,
        target: SocketAddr,
    ) -> Result<usize> {
        let mut buf = [0u8; STATUS_MESSAGE_HEADER_LENGTH];
        {
            let sm = StatusMessageFlyweight::new(AtomicBuffer::new(&mut buf));
            sm.header()
                .set_frame_length(STATUS_MESSAGE_HEADER_LENGTH as i32)
                .set_version(CURRENT_VERSION)
                .set_flags(0)
                .set_header_type(HDR_TYPE_SM);
            sm.set_session_id(session_id)
                .set_stream_id(stream_id)
                .set_consumption_term_id(term_id)
                .set_consumption_term_offset(term_offset)
                .set_receiver_window_length(window_length)
                .set_receiver_id(0);
        }
        self.transport.send_to(&buf, target)
    }

    /// Build and send a real NAK loss-notification frame to `target`.
    pub fn send_nak(
        &self,
        session_id: i32,
        stream_id: i32,
        term_id: i32,
        term_offset: i32,
        length: i32,
        target: SocketAddr,
    ) -> Result<usize> {
        let mut buf = [0u8; NAK_HEADER_LENGTH];
        {
            let nak = NakFlyweight::new(AtomicBuffer::new(&mut buf));
            nak.header()
                .set_frame_length(NAK_HEADER_LENGTH as i32)
                .set_version(CURRENT_VERSION)
                .set_flags(0)
                .set_header_type(HDR_TYPE_NAK);
            nak.set_session_id(session_id)
                .set_stream_id(stream_id)
                .set_term_id(term_id)
                .set_term_offset(term_offset)
                .set_length(length);
        }
        self.transport.send_to(&buf, target)
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
