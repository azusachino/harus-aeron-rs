//! Receiver agent duty cycle for polling transport pollers and dispatching incoming frames.
//!
//! Mirrors `io.aeron.driver.Receiver.java`.

use std::collections::HashMap;
use std::net::SocketAddr;

use aeron_client::logbuffer::{FRAME_ALIGNMENT, align};
use aeron_client::protocol::{
    DATA_HEADER_LENGTH, DataHeaderFlyweight, HDR_TYPE_DATA, HDR_TYPE_RTTM, HDR_TYPE_SETUP,
    HeaderFlyweight, RTTM_HEADER_LENGTH, RTTM_REPLY_FLAG, RttMeasurementFlyweight, SetupFlyweight,
};
use agrona::AtomicBuffer;

use crate::error::Result;
use crate::media::receive_channel_endpoint::ReceiveChannelEndpoint;

/// Matches vendored `Configuration.STATUS_MESSAGE_TIMEOUT_DEFAULT_NS`.
const STATUS_MESSAGE_TIMEOUT_NS: i64 = 200_000_000;

pub struct Receiver {
    receive_channel_endpoints: Vec<ReceiveChannelEndpoint>,
    /// Frames drained off the wire this cycle, handed to the conductor with their source.
    received_frames: Vec<ReceivedFrame>,
    /// Next contiguous term position and flow-control context for each source/session/stream.
    /// Endpoint identity is part of the receive dispatcher key. Two channels may reuse the
    /// same session, stream, and source address without sharing sequence state.
    expected_positions: HashMap<(usize, i32, i32, SocketAddr), ExpectedPosition>,
}

#[derive(Debug, Clone, Copy)]
struct ExpectedPosition {
    term_id: i32,
    term_offset: i32,
    term_length: i32,
    receiver_window_length: i32,
    last_status_ns: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct ReceiverStatus {
    pub term_id: i32,
    pub term_offset: i32,
    pub receiver_window_length: i32,
}

/// A received datagram together with the endpoint and source that delivered it.
///
/// Upstream keeps this association in `DataPacketDispatcher`/`PublicationImage`; losing it
/// before conductor dispatch makes two publishers using one stream indistinguishable.
#[derive(Debug, Clone)]
pub struct ReceivedFrame {
    pub bytes: Vec<u8>,
    pub source: SocketAddr,
    pub endpoint_index: usize,
    pub session_id: i32,
    pub stream_id: i32,
    /// Original channel URI of the endpoint which received the datagram.
    pub channel: String,
    pub receiver_status: Option<ReceiverStatus>,
}

impl Default for Receiver {
    fn default() -> Self {
        Self::new()
    }
}

impl Receiver {
    pub fn new() -> Self {
        Self {
            receive_channel_endpoints: Vec::new(),
            received_frames: Vec::new(),
            expected_positions: HashMap::new(),
        }
    }

    pub fn add_receive_channel_endpoint(&mut self, endpoint: ReceiveChannelEndpoint) {
        self.receive_channel_endpoints.push(endpoint);
    }

    /// Remove one endpoint for a subscription that no longer has receiver interest.
    ///
    /// The upstream dispatcher removes the stream interest from the endpoint rather than
    /// allowing packets to continue reaching an inactive image. This compact receiver has one
    /// endpoint per subscription, so removing the matching endpoint is the equivalent boundary.
    pub fn remove_receive_channel_endpoint(&mut self, canonical_form: &str) -> bool {
        if let Some(index) = self
            .receive_channel_endpoints
            .iter()
            .position(|endpoint| endpoint.matches_channel(canonical_form))
        {
            self.receive_channel_endpoints.remove(index);
            // The compact key does not carry an endpoint identity. Reset the sequence cache
            // when receiver interest is removed so a later subscription cannot inherit stale
            // term offsets from the previous image.
            self.expected_positions.clear();
            true
        } else {
            false
        }
    }

    pub fn do_work(&mut self) -> Result<usize> {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos() as i64)
            .unwrap_or(0);
        self.do_work_at(now_ns)
    }

    /// Run one receive duty cycle using the driver's shared clock.
    pub fn do_work_at(&mut self, now_ns: i64) -> Result<usize> {
        // Drain every receive endpoint's socket; the conductor turns SETUP/DATA frames into
        // images and reassembled term data.
        let mut buffer = [0u8; 2048];
        let mut count = 0;
        for endpoint_index in 0..self.receive_channel_endpoints.len() {
            while let Ok((len, src)) = self.receive_channel_endpoints[endpoint_index]
                .transport()
                .recv_from(&mut buffer)
            {
                let frame = buffer[..len].to_vec();
                let receiver_status = self.handle_frame(endpoint_index, &frame, src, now_ns)?;
                self.received_frames.push(ReceivedFrame {
                    bytes: frame,
                    source: src,
                    endpoint_index,
                    session_id: i32::from_le_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]),
                    stream_id: i32::from_le_bytes([buffer[12], buffer[13], buffer[14], buffer[15]]),
                    channel: self.receive_channel_endpoints[endpoint_index]
                        .udp_channel()
                        .original_uri_string
                        .clone(),
                    receiver_status,
                });
                count += 1;
            }
        }
        count += self.send_periodic_status_messages(now_ns)?;
        Ok(count)
    }

    fn handle_frame(
        &mut self,
        endpoint_index: usize,
        bytes: &[u8],
        source: SocketAddr,
        now_ns: i64,
    ) -> Result<Option<ReceiverStatus>> {
        if bytes.len() < DATA_HEADER_LENGTH {
            return Ok(None);
        }

        let mut owned = bytes.to_vec();
        let buffer = AtomicBuffer::new(&mut owned);
        let header = HeaderFlyweight::new(buffer);
        match header.header_type() {
            HDR_TYPE_SETUP => {
                let setup = SetupFlyweight::new(buffer);
                self.receive_channel_endpoints[endpoint_index].on_setup_message(
                    setup.session_id(),
                    setup.stream_id(),
                    setup.active_term_id(),
                    setup.term_offset(),
                    source,
                );
                let window = (setup.term_length() / 2).max(setup.mtu_length());
                self.expected_positions
                    .entry((
                        endpoint_index,
                        setup.session_id(),
                        setup.stream_id(),
                        source,
                    ))
                    .or_insert(ExpectedPosition {
                        term_id: setup.active_term_id(),
                        term_offset: setup.term_offset(),
                        term_length: setup.term_length(),
                        receiver_window_length: window,
                        last_status_ns: now_ns,
                    });
                self.receive_channel_endpoints[endpoint_index].send_status_message(
                    setup.session_id(),
                    setup.stream_id(),
                    setup.active_term_id(),
                    setup.term_offset(),
                    window,
                    source,
                )?;
                if let Some(expected) = self.expected_positions.get_mut(&(
                    endpoint_index,
                    setup.session_id(),
                    setup.stream_id(),
                    source,
                )) {
                    expected.last_status_ns = now_ns;
                }
                Ok(self
                    .expected_positions
                    .get(&(
                        endpoint_index,
                        setup.session_id(),
                        setup.stream_id(),
                        source,
                    ))
                    .map(|expected| ReceiverStatus {
                        term_id: expected.term_id,
                        term_offset: expected.term_offset,
                        receiver_window_length: expected.receiver_window_length,
                    }))
            }
            HDR_TYPE_DATA if bytes.len() >= DATA_HEADER_LENGTH => {
                let status = self.handle_data_frames(endpoint_index, bytes, source, now_ns)?;
                Ok(status)
            }
            HDR_TYPE_RTTM if bytes.len() >= RTTM_HEADER_LENGTH => {
                let rtt = RttMeasurementFlyweight::new(buffer);
                let key = (endpoint_index, rtt.session_id(), rtt.stream_id(), source);
                if rtt.header().flags() & RTTM_REPLY_FLAG == 0
                    && self.expected_positions.contains_key(&key)
                {
                    self.receive_channel_endpoints[endpoint_index].send_rtt_measurement_reply(
                        rtt.session_id(),
                        rtt.stream_id(),
                        rtt.echo_timestamp(),
                        source,
                    )?;
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_data_frames(
        &mut self,
        endpoint_index: usize,
        bytes: &[u8],
        source: SocketAddr,
        now_ns: i64,
    ) -> Result<Option<ReceiverStatus>> {
        let mut packet_offset = 0usize;
        let mut status_key = None;
        while packet_offset + DATA_HEADER_LENGTH <= bytes.len() {
            let frame = &bytes[packet_offset..];
            let mut frame_bytes = frame.to_vec();
            let data = DataHeaderFlyweight::new(AtomicBuffer::new(&mut frame_bytes));
            let frame_length = data.frame_length();
            status_key = Some((data.session_id(), data.stream_id()));
            if frame_length < 0 {
                break;
            }
            if frame_length == 0 {
                self.receive_channel_endpoints[endpoint_index].on_data_packet(
                    data.session_id(),
                    data.stream_id(),
                    data.term_id(),
                    data.term_offset(),
                    &[],
                    source,
                );
                break;
            }
            if (frame_length as usize) < DATA_HEADER_LENGTH {
                break;
            }
            let aligned_length = align(frame_length, FRAME_ALIGNMENT) as usize;
            if aligned_length > frame.len() {
                break;
            }
            let frame_length_usize = frame_length as usize;
            self.receive_channel_endpoints[endpoint_index].on_data_packet(
                data.session_id(),
                data.stream_id(),
                data.term_id(),
                data.term_offset(),
                &frame[DATA_HEADER_LENGTH..frame_length_usize.min(frame.len())],
                source,
            );

            let key = (endpoint_index, data.session_id(), data.stream_id(), source);
            let expected = self
                .expected_positions
                .entry(key)
                .or_insert(ExpectedPosition {
                    term_id: data.term_id(),
                    term_offset: data.term_offset(),
                    term_length: 0,
                    receiver_window_length: 0,
                    last_status_ns: 0,
                });
            let expected_term = expected.term_id;
            let expected_offset = expected.term_offset;
            let term_delta = data.term_id().wrapping_sub(expected_term) as i64;
            if term_delta > 0 || (term_delta == 0 && data.term_offset() > expected_offset) {
                let missing = if term_delta == 0 {
                    data.term_offset() - expected_offset
                } else {
                    let term_length = expected.term_length.max(data.term_offset());
                    (term_length - expected_offset).max(0) + data.term_offset().max(0)
                };
                if missing > 0 {
                    self.receive_channel_endpoints[endpoint_index].send_nak(
                        data.session_id(),
                        data.stream_id(),
                        expected_term,
                        expected_offset,
                        missing,
                        source,
                    )?;
                }
            } else if data.term_id() == expected_term && data.term_offset() == expected_offset {
                let next_offset = expected_offset + align(frame_length, FRAME_ALIGNMENT);
                if expected.term_length > 0 && next_offset >= expected.term_length {
                    expected.term_id = expected_term.wrapping_add(1);
                    expected.term_offset = next_offset - expected.term_length;
                } else {
                    expected.term_offset = next_offset;
                }
                if expected.receiver_window_length > 0 {
                    self.receive_channel_endpoints[endpoint_index].send_status_message(
                        data.session_id(),
                        data.stream_id(),
                        expected.term_id,
                        expected.term_offset,
                        expected.receiver_window_length,
                        source,
                    )?;
                    expected.last_status_ns = now_ns;
                }
            }
            packet_offset += aligned_length;
        }
        Ok(status_key.and_then(|(session_id, stream_id)| {
            self.expected_positions
                .get(&(endpoint_index, session_id, stream_id, source))
                .filter(|expected| expected.receiver_window_length > 0)
                .map(|expected| ReceiverStatus {
                    term_id: expected.term_id,
                    term_offset: expected.term_offset,
                    receiver_window_length: expected.receiver_window_length,
                })
        }))
    }

    fn send_periodic_status_messages(&mut self, now_ns: i64) -> Result<usize> {
        let mut due = Vec::new();
        for (&(endpoint_index, session_id, stream_id, source), expected) in &self.expected_positions
        {
            if expected.receiver_window_length > 0
                && now_ns.saturating_sub(expected.last_status_ns) >= STATUS_MESSAGE_TIMEOUT_NS
            {
                due.push((
                    endpoint_index,
                    session_id,
                    stream_id,
                    expected.term_id,
                    expected.term_offset,
                    expected.receiver_window_length,
                    source,
                ));
            }
        }

        for (
            endpoint_index,
            session_id,
            stream_id,
            term_id,
            term_offset,
            receiver_window_length,
            source,
        ) in &due
        {
            self.receive_channel_endpoints[*endpoint_index].send_status_message(
                *session_id,
                *stream_id,
                *term_id,
                *term_offset,
                *receiver_window_length,
                *source,
            )?;
            if let Some(expected) = self.expected_positions.get_mut(&(
                *endpoint_index,
                *session_id,
                *stream_id,
                *source,
            )) {
                expected.last_status_ns = now_ns;
            }
        }
        Ok(due.len())
    }

    /// Take the frames received since the last call for conductor dispatch.
    pub fn take_received_frames(&mut self) -> Vec<ReceivedFrame> {
        std::mem::take(&mut self.received_frames)
    }

    pub fn receive_channel_endpoints(&self) -> &[ReceiveChannelEndpoint] {
        &self.receive_channel_endpoints
    }

    /// Send a SETUP-eliciting Status Message on the endpoint which received a DATA frame.
    pub fn send_setup_eliciting_status(
        &self,
        endpoint_index: usize,
        session_id: i32,
        stream_id: i32,
        source: SocketAddr,
    ) -> Result<usize> {
        self.receive_channel_endpoints
            .get(endpoint_index)
            .ok_or_else(|| {
                crate::error::DriverError::IllegalState(
                    "setup elicitation endpoint index out of range".to_owned(),
                )
            })?
            .send_setup_eliciting_status_message(session_id, stream_id, source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::udp_channel::UdpChannel;
    use crate::media::udp_channel_transport::UdpChannelTransport;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
    use std::time::Duration;

    #[test]
    fn should_add_receive_channel_endpoint_and_do_work() {
        let mut receiver = Receiver::new();
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
        let endpoint = ReceiveChannelEndpoint::new(channel, transport);

        receiver.add_receive_channel_endpoint(endpoint);
        assert_eq!(receiver.receive_channel_endpoints().len(), 1);
        assert_eq!(receiver.do_work().unwrap(), 0);
    }

    #[test]
    fn should_remove_receive_channel_endpoint_and_reset_sequence_state() {
        let mut receiver = Receiver::new();
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
        receiver.add_receive_channel_endpoint(ReceiveChannelEndpoint::new(channel, transport));

        assert!(receiver.remove_receive_channel_endpoint("aeron:udp?endpoint=localhost:0"));
        assert!(receiver.receive_channel_endpoints().is_empty());
        assert!(!receiver.remove_receive_channel_endpoint("aeron:udp?endpoint=localhost:0"));
    }

    #[test]
    fn should_refresh_status_after_source_timeout() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
        let peer = UdpSocket::bind(bind_addr).unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let source = peer.local_addr().unwrap();

        let mut receiver = Receiver::new();
        receiver.add_receive_channel_endpoint(ReceiveChannelEndpoint::new(channel, transport));
        receiver.expected_positions.insert(
            (0, 7, 11, source),
            ExpectedPosition {
                term_id: 3,
                term_offset: 128,
                term_length: 64 * 1024,
                receiver_window_length: 32 * 1024,
                last_status_ns: 0,
            },
        );

        assert_eq!(
            receiver
                .send_periodic_status_messages(STATUS_MESSAGE_TIMEOUT_NS)
                .unwrap(),
            1
        );
        let mut status = [0u8; 256];
        let (length, _) = peer.recv_from(&mut status).unwrap();
        assert!(length > 0);
        assert_eq!(
            receiver.expected_positions[&(0, 7, 11, source)].last_status_ns,
            200_000_000
        );
        assert_eq!(
            receiver.send_periodic_status_messages(200_000_001).unwrap(),
            0
        );
    }

    #[test]
    fn should_reply_to_rtt_measurement_request_for_active_source() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
        let peer = UdpSocket::bind(bind_addr).unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let source = peer.local_addr().unwrap();

        let mut receiver = Receiver::new();
        receiver.add_receive_channel_endpoint(ReceiveChannelEndpoint::new(channel, transport));
        let endpoint_addr = receiver.receive_channel_endpoints()[0]
            .transport()
            .local_addr()
            .unwrap();
        receiver.expected_positions.insert(
            (0, 7, 11, source),
            ExpectedPosition {
                term_id: 3,
                term_offset: 0,
                term_length: 64 * 1024,
                receiver_window_length: 32 * 1024,
                last_status_ns: 1,
            },
        );

        let mut request = [0u8; aeron_client::protocol::RTTM_HEADER_LENGTH];
        {
            let rtt = RttMeasurementFlyweight::new(AtomicBuffer::new(&mut request));
            rtt.header()
                .set_frame_length(RTTM_HEADER_LENGTH as i32)
                .set_version(aeron_client::protocol::CURRENT_VERSION)
                .set_flags(0)
                .set_header_type(aeron_client::protocol::HDR_TYPE_RTTM);
            rtt.set_session_id(7)
                .set_stream_id(11)
                .set_echo_timestamp(1234)
                .set_reception_delta(0)
                .set_receiver_id(0);
        }
        peer.send_to(&request, endpoint_addr).unwrap();

        let mut work = 0;
        for _ in 0..10 {
            work = receiver.do_work_at(2).unwrap();
            if work > 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(work, 1);
        let mut reply = [0u8; 128];
        let (length, _) = peer.recv_from(&mut reply).unwrap();
        let rtt = RttMeasurementFlyweight::new(AtomicBuffer::new(&mut reply[..length]));
        assert_eq!(
            rtt.header().header_type(),
            aeron_client::protocol::HDR_TYPE_RTTM
        );
        assert_ne!(rtt.header().flags() & RTTM_REPLY_FLAG, 0);
        assert_eq!(rtt.echo_timestamp(), 1234);
    }
}
