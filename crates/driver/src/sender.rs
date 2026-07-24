//! Sender agent duty cycle for flushing publication data frames and setup messages.
//!
//! Mirrors `io.aeron.driver.Sender.java`.

use crate::error::Result;
use crate::media::send_channel_endpoint::SendChannelEndpoint;
use crate::network_publication::NetworkPublication;

pub struct Sender {
    send_channel_endpoints: Vec<SendChannelEndpoint>,
    network_publications: Vec<NetworkPublication>,
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
            network_publications: Vec::new(),
        }
    }

    pub fn add_send_channel_endpoint(&mut self, endpoint: SendChannelEndpoint) {
        self.send_channel_endpoints.push(endpoint);
    }

    /// Take ownership of a network publication so its data path runs each duty cycle.
    pub fn add_network_publication(&mut self, publication: NetworkPublication) {
        self.network_publications.push(publication);
    }

    pub fn do_work(&mut self, now_ns: i64) -> Result<usize> {
        // Flush each network publication: SETUP announcements, new data frames, heartbeats.
        let mut work = 0;
        for publication in &mut self.network_publications {
            work += publication.poll_control()?;
            work += publication.do_work(now_ns)?;
        }
        Ok(work)
    }

    pub fn send_channel_endpoints(&self) -> &[SendChannelEndpoint] {
        &self.send_channel_endpoints
    }

    pub fn network_publications(&self) -> &[NetworkPublication] {
        &self.network_publications
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
        assert_eq!(sender.do_work(0).unwrap(), 0);
    }
}
