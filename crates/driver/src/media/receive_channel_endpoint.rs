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

#[derive(Debug, Clone)]
pub struct SetupMessage {
    pub session_id: i32,
    pub stream_id: i32,
    pub active_term_id: i32,
    pub term_offset: i32,
    pub source: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct DataPacket {
    pub session_id: i32,
    pub stream_id: i32,
    pub term_id: i32,
    pub term_offset: i32,
    pub payload: Vec<u8>,
    pub source: SocketAddr,
}

#[derive(Debug)]
pub struct ReceiveChannelEndpoint {
    udp_channel: UdpChannel,
    transport: UdpChannelTransport,
    setup_messages: Vec<SetupMessage>,
    data_packets: Vec<DataPacket>,
}

impl ReceiveChannelEndpoint {
    pub fn new(udp_channel: UdpChannel, transport: UdpChannelTransport) -> Self {
        Self {
            udp_channel,
            transport,
            setup_messages: Vec::new(),
            data_packets: Vec::new(),
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
        session_id: i32,
        stream_id: i32,
        term_id: i32,
        term_offset: i32,
        payload: &[u8],
        src_addr: SocketAddr,
    ) {
        self.data_packets.push(DataPacket {
            session_id,
            stream_id,
            term_id,
            term_offset,
            payload: payload.to_vec(),
            source: src_addr,
        });
    }

    pub fn on_setup_message(
        &mut self,
        session_id: i32,
        stream_id: i32,
        active_term_id: i32,
        term_offset: i32,
        src_addr: SocketAddr,
    ) {
        self.setup_messages.push(SetupMessage {
            session_id,
            stream_id,
            active_term_id,
            term_offset,
            source: src_addr,
        });
    }

    pub fn take_setup_messages(&mut self) -> Vec<SetupMessage> {
        std::mem::take(&mut self.setup_messages)
    }

    pub fn take_data_packets(&mut self) -> Vec<DataPacket> {
        std::mem::take(&mut self.data_packets)
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

    #[test]
    fn should_dispatch_setup_and_data_callbacks() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
        let mut endpoint = ReceiveChannelEndpoint::new(channel, transport);
        let source = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9001);

        endpoint.on_setup_message(7, 9, 1, 0, source);
        endpoint.on_data_packet(7, 9, 1, 0, b"payload", source);

        let setups = endpoint.take_setup_messages();
        assert_eq!(setups.len(), 1);
        assert_eq!(setups[0].source, source);
        let packets = endpoint.take_data_packets();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].payload, b"payload");
        assert_eq!(packets[0].source, source);
    }
}
