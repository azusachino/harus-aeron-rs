//! Receiver agent duty cycle for polling transport pollers and dispatching incoming frames.
//!
//! Mirrors `io.aeron.driver.Receiver.java`.

use crate::error::Result;
use crate::media::receive_channel_endpoint::ReceiveChannelEndpoint;
use crate::media::transport_poller::DataTransportPoller;

pub struct Receiver {
    data_transport_poller: DataTransportPoller,
    receive_channel_endpoints: Vec<ReceiveChannelEndpoint>,
}

impl Default for Receiver {
    fn default() -> Self {
        Self::new()
    }
}

impl Receiver {
    pub fn new() -> Self {
        Self {
            data_transport_poller: DataTransportPoller::new(),
            receive_channel_endpoints: Vec::new(),
        }
    }

    pub fn add_receive_channel_endpoint(&mut self, endpoint: ReceiveChannelEndpoint) {
        self.receive_channel_endpoints.push(endpoint);
    }

    pub fn do_work(&mut self) -> Result<usize> {
        let bytes_polled = self
            .data_transport_poller
            .poll_transports(|_payload, _src| {
                // Incoming frame dispatch logic
            })?;
        Ok(bytes_polled)
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
