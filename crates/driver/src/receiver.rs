//! Receiver agent duty cycle for polling transport pollers and dispatching incoming frames.
//!
//! Mirrors `io.aeron.driver.Receiver.java`.

use std::collections::HashMap;
use std::net::SocketAddr;

use aeron_client::logbuffer::{FRAME_ALIGNMENT, align};
use aeron_client::protocol::{
    DATA_HEADER_LENGTH, DataHeaderFlyweight, HDR_TYPE_DATA, HDR_TYPE_SETUP, HeaderFlyweight,
    SetupFlyweight,
};
use agrona::AtomicBuffer;

use crate::error::Result;
use crate::media::receive_channel_endpoint::ReceiveChannelEndpoint;

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
                let receiver_status = self.handle_frame(endpoint_index, &frame, src)?;
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
        Ok(count)
    }

    fn handle_frame(
        &mut self,
        endpoint_index: usize,
        bytes: &[u8],
        source: SocketAddr,
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
                    });
                self.receive_channel_endpoints[endpoint_index].send_status_message(
                    setup.session_id(),
                    setup.stream_id(),
                    setup.active_term_id(),
                    setup.term_offset(),
                    window,
                    source,
                )?;
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
                let status = self.handle_data_frames(endpoint_index, bytes, source)?;
                Ok(status)
            }
            _ => Ok(None),
        }
    }

    fn handle_data_frames(
        &mut self,
        endpoint_index: usize,
        bytes: &[u8],
        source: SocketAddr,
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
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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
}
