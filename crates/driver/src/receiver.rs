//! Receiver agent duty cycle for polling transport pollers and dispatching incoming frames.
//!
//! Mirrors `io.aeron.driver.Receiver.java`.

use crate::error::Result;
use crate::media::receive_channel_endpoint::ReceiveChannelEndpoint;

pub struct Receiver {
    receive_channel_endpoints: Vec<ReceiveChannelEndpoint>,
    /// Frames drained off the wire this cycle, handed to the conductor for image reassembly.
    received_frames: Vec<Vec<u8>>,
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
        for endpoint in &self.receive_channel_endpoints {
            let transport = endpoint.transport();
            while let Ok((len, _src)) = transport.recv_from(&mut buffer) {
                self.received_frames.push(buffer[..len].to_vec());
                count += 1;
            }
        }
        Ok(count)
    }

    /// Take the frames received since the last call for conductor dispatch.
    pub fn take_received_frames(&mut self) -> Vec<Vec<u8>> {
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
