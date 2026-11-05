//! Aggregator of network publications on a transport channel for sending data and setup frames.
//!
//! Mirrors `io.aeron.driver.media.SendChannelEndpoint.java`.

use std::net::SocketAddr;

use aeron_client::protocol::{
    CURRENT_VERSION, HDR_TYPE_NAK, HDR_TYPE_SM, HeaderFlyweight, NakFlyweight,
    StatusMessageFlyweight,
};
use agrona::AtomicBuffer;

use crate::error::Result;
use crate::media::udp_channel::UdpChannel;
use crate::media::udp_channel_transport::UdpChannelTransport;

#[derive(Debug, Clone, Copy)]
pub struct StatusMessage {
    pub session_id: i32,
    pub stream_id: i32,
    pub term_id: i32,
    pub term_offset: i32,
    pub receiver_window_length: i32,
    /// Sending receiver's identity, needed by min/tagged multicast flow control to track one
    /// state slot per receiver. Zero for callers that only exercise the plain 5-field form.
    pub receiver_id: i64,
    /// Raw header flags, including `END_OF_STREAM_FLAG` (min/tagged flow control retires a
    /// receiver once it reports end-of-stream instead of waiting out its full timeout).
    pub flags: u8,
    /// Present only on tagged multicast flow control's status messages.
    pub group_tag: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub struct NakMessage {
    pub session_id: i32,
    pub stream_id: i32,
    pub term_id: i32,
    pub term_offset: i32,
    pub length: i32,
}

#[derive(Debug)]
pub struct SendChannelEndpoint {
    udp_channel: UdpChannel,
    transport: UdpChannelTransport,
    status_messages: Vec<StatusMessage>,
    nak_messages: Vec<NakMessage>,
}

impl SendChannelEndpoint {
    pub fn new(udp_channel: UdpChannel, transport: UdpChannelTransport) -> Self {
        Self {
            udp_channel,
            transport,
            status_messages: Vec::new(),
            nak_messages: Vec::new(),
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

    /// Poll control frames arriving on the publication's UDP socket.
    pub fn poll_control(&mut self) -> Result<usize> {
        let mut buffer = [0u8; 65_507];
        let mut work = 0;
        for _ in 0..64 {
            let Some((length, _src)) = self.transport.try_recv_from(&mut buffer)? else {
                break;
            };
            if length < 8 {
                continue;
            }
            let frame = AtomicBuffer::new(&mut buffer[..length]);
            let header = HeaderFlyweight::new(frame.clone());
            let frame_length = header.frame_length();
            if header.version() != CURRENT_VERSION
                || frame_length < 0
                || frame_length as usize > length
            {
                continue;
            }
            match header.header_type() {
                HDR_TYPE_SM
                    if frame_length as usize
                        == aeron_client::protocol::STATUS_MESSAGE_HEADER_LENGTH
                        || frame_length as usize
                            == aeron_client::protocol::STATUS_MESSAGE_HEADER_LENGTH + 8 =>
                {
                    let sm = StatusMessageFlyweight::new(frame);
                    self.on_status_message_full(
                        sm.session_id(),
                        sm.stream_id(),
                        sm.consumption_term_id(),
                        sm.consumption_term_offset(),
                        sm.receiver_window_length(),
                        sm.receiver_id(),
                        sm.flags(),
                        sm.has_group_tag().then(|| sm.group_tag()),
                    );
                    work += 1;
                }
                HDR_TYPE_NAK
                    if frame_length as usize >= aeron_client::protocol::NAK_HEADER_LENGTH =>
                {
                    let nak = NakFlyweight::new(frame);
                    self.on_nak_message(
                        nak.session_id(),
                        nak.stream_id(),
                        nak.term_id(),
                        nak.term_offset(),
                        nak.length(),
                    );
                    work += 1;
                }
                _ => {}
            }
        }
        Ok(work)
    }

    pub fn take_status_messages(&mut self) -> Vec<StatusMessage> {
        std::mem::take(&mut self.status_messages)
    }

    pub fn take_nak_messages(&mut self) -> Vec<NakMessage> {
        std::mem::take(&mut self.nak_messages)
    }

    pub fn on_status_message(
        &mut self,
        session_id: i32,
        stream_id: i32,
        term_id: i32,
        term_offset: i32,
        receiver_window_length: i32,
    ) {
        self.on_status_message_full(
            session_id,
            stream_id,
            term_id,
            term_offset,
            receiver_window_length,
            0,
            0,
            None,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn on_status_message_full(
        &mut self,
        session_id: i32,
        stream_id: i32,
        term_id: i32,
        term_offset: i32,
        receiver_window_length: i32,
        receiver_id: i64,
        flags: u8,
        group_tag: Option<i64>,
    ) {
        self.status_messages.push(StatusMessage {
            session_id,
            stream_id,
            term_id,
            term_offset,
            receiver_window_length,
            receiver_id,
            flags,
            group_tag,
        });
    }

    pub fn on_nak_message(
        &mut self,
        session_id: i32,
        stream_id: i32,
        term_id: i32,
        term_offset: i32,
        length: i32,
    ) {
        self.nak_messages.push(NakMessage {
            session_id,
            stream_id,
            term_id,
            term_offset,
            length,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn rejects_malformed_control_datagrams_before_queueing() {
        use std::net::UdpSocket;
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
        let address = transport.local_addr().unwrap();
        let peer = UdpSocket::bind(bind_addr).unwrap();
        let mut endpoint = SendChannelEndpoint::new(channel, transport);
        let mut bytes = [0u8; 44];
        let sm = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        sm.header()
            .set_header_type(HDR_TYPE_SM)
            .set_frame_length(44)
            .set_version(1);
        peer.send_to(&bytes, address).unwrap();
        endpoint.poll_control().unwrap();
        assert!(endpoint.take_status_messages().is_empty());
        bytes[4] = 0;
        bytes[..4].copy_from_slice(&37i32.to_le_bytes());
        peer.send_to(&bytes[..37], address).unwrap();
        endpoint.poll_control().unwrap();
        assert!(endpoint.take_status_messages().is_empty());
        bytes[..4].copy_from_slice(&44i32.to_le_bytes());
        peer.send_to(&bytes, address).unwrap();
        let mut messages = Vec::new();
        for _ in 0..20 {
            endpoint.poll_control().unwrap();
            messages.extend(endpoint.take_status_messages());
            if !messages.is_empty() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(messages.len(), 1);
    }

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
