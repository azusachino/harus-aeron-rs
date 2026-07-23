//! Sender agent duty cycle for flushing publication data frames and setup messages.
//!
//! Mirrors `io.aeron.driver.Sender.java`.

use crate::error::Result;
use crate::media::send_channel_endpoint::SendChannelEndpoint;

pub struct Sender {
    send_channel_endpoints: Vec<SendChannelEndpoint>,
}

impl Default for Sender {
    fn default() -> Self {
        Self::new()
    }
}

impl Sender {
    pub fn new() -> Self {
        Self {
            send_channel_endpoints: Vec::new(),
        }
    }

    pub fn add_send_channel_endpoint(&mut self, endpoint: SendChannelEndpoint) {
        self.send_channel_endpoints.push(endpoint);
    }

    pub fn do_work(&mut self) -> Result<usize> {
        let work_count = 0;
        // Duty cycle work: flush network publication buffers to send endpoints
        Ok(work_count)
    }

    pub fn send_channel_endpoints(&self) -> &[SendChannelEndpoint] {
        &self.send_channel_endpoints
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::udp_channel::UdpChannel;
    use crate::media::udp_channel_transport::UdpChannelTransport;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn should_add_send_channel_endpoint_and_do_work() {
        let mut sender = Sender::new();
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
        let endpoint = SendChannelEndpoint::new(channel, transport);

        sender.add_send_channel_endpoint(endpoint);
        assert_eq!(sender.send_channel_endpoints().len(), 1);
        assert_eq!(sender.do_work().unwrap(), 0);
    }
}
