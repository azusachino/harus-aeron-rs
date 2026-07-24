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
    expected_positions: HashMap<(i32, i32, SocketAddr), ExpectedPosition>,
}

#[derive(Debug, Clone, Copy)]
struct ExpectedPosition {
    term_id: i32,
    term_offset: i32,
    receiver_window_length: i32,
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
                self.handle_frame(endpoint_index, &frame, src)?;
                self.received_frames.push(ReceivedFrame {
                    bytes: frame,
                    source: src,
                    endpoint_index,
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
    ) -> Result<()> {
        if bytes.len() < DATA_HEADER_LENGTH {
            return Ok(());
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
                    .entry((setup.session_id(), setup.stream_id(), source))
                    .or_insert(ExpectedPosition {
                        term_id: setup.active_term_id(),
                        term_offset: setup.term_offset(),
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
            }
            HDR_TYPE_DATA if bytes.len() >= DATA_HEADER_LENGTH => {
                self.handle_data_frames(endpoint_index, bytes, source)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_data_frames(
        &mut self,
        endpoint_index: usize,
        bytes: &[u8],
        source: SocketAddr,
    ) -> Result<()> {
        let mut packet_offset = 0usize;
        while packet_offset + DATA_HEADER_LENGTH <= bytes.len() {
            let frame = &bytes[packet_offset..];
            let mut frame_bytes = frame.to_vec();
            let data = DataHeaderFlyweight::new(AtomicBuffer::new(&mut frame_bytes));
            let frame_length = data.frame_length();
            if frame_length <= 0 {
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

            let key = (data.session_id(), data.stream_id(), source);
            let expected = self
                .expected_positions
                .entry(key)
                .or_insert(ExpectedPosition {
                    term_id: data.term_id(),
                    term_offset: data.term_offset(),
                    receiver_window_length: 0,
                });
            let expected_term = expected.term_id;
            let expected_offset = expected.term_offset;
            if data.term_id() > expected_term
                || (data.term_id() == expected_term && data.term_offset() > expected_offset)
            {
                let missing = if data.term_id() == expected_term {
                    data.term_offset() - expected_offset
                } else {
                    data.term_offset().max(0)
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
                expected.term_offset = expected_offset + align(frame_length, FRAME_ALIGNMENT);
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
        Ok(())
    }

    /// Take the frames received since the last call for conductor dispatch.
    pub fn take_received_frames(&mut self) -> Vec<ReceivedFrame> {
        std::mem::take(&mut self.received_frames)
    }

    pub fn receive_channel_endpoints(&self) -> &[ReceiveChannelEndpoint] {
        &self.receive_channel_endpoints
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
}
