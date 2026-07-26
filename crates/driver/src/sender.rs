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

    /// Remove a publication that the conductor has detached from the driver.
    pub fn remove_network_publication(&mut self, registration_id: i64) -> bool {
        if let Some(index) = self
            .network_publications
            .iter()
            .position(|publication| publication.registration_id() == registration_id)
        {
            self.network_publications.remove(index);
            true
        } else {
            false
        }
    }

    pub fn do_work(&mut self, now_ns: i64) -> Result<usize> {
        // Flush each network publication: SETUP announcements, new data frames, heartbeats.
        let mut work = 0;
        for publication in &mut self.network_publications {
            work += publication.poll_control(now_ns)?;
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
    use aeron_client::log_buffers::LogBuffers;
    use aeron_client::logbuffer::TERM_MIN_LENGTH;
    use agrona::AtomicBuffer;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
    use std::sync::Arc;
    use std::sync::atomic::AtomicI64;
    use std::time::Duration;

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

    #[test]
    fn should_run_a_network_publication_and_remove_it() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
        let endpoint = SendChannelEndpoint::new(channel, transport);
        let peer = UdpSocket::bind(bind_addr).unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let destination = peer.local_addr().unwrap();
        let path = std::env::temp_dir().join(format!("aeron-sender-test-{}", std::process::id()));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, 4096).unwrap();
        let publication = NetworkPublication::new(
            42,
            buffers,
            endpoint,
            destination,
            7,
            11,
            3,
            1408,
            Arc::new(AtomicI64::new(0)),
            16,
        );

        let mut sender = Sender::new();
        sender.add_network_publication(publication);
        assert_eq!(sender.do_work(0).unwrap(), 1);
        let mut frame = [0u8; 256];
        let (length, _) = peer.recv_from(&mut frame).unwrap();
        let setup =
            aeron_client::protocol::SetupFlyweight::new(AtomicBuffer::new(&mut frame[..length]));
        assert_eq!(setup.session_id(), 7);
        assert_eq!(setup.stream_id(), 11);
        assert_eq!(setup.active_term_id(), 3);
        assert!(sender.remove_network_publication(42));
        assert!(sender.network_publications().is_empty());
        let _ = std::fs::remove_file(path);
    }
}
