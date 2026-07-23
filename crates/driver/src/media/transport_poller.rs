//! Transport poller for data and control UDP channel transports.
//!
//! Mirrors `io.aeron.driver.media.DataTransportPoller.java` and `ControlTransportPoller.java`.

use std::net::SocketAddr;

use crate::error::Result;
use crate::media::udp_channel_transport::UdpChannelTransport;

pub struct DataTransportPoller {
    transports: Vec<UdpChannelTransport>,
    buffer: Vec<u8>,
}

impl Default for DataTransportPoller {
    fn default() -> Self {
        Self::new()
    }
}

impl DataTransportPoller {
    pub fn new() -> Self {
        Self {
            transports: Vec::new(),
            buffer: vec![0u8; 65536],
        }
    }

    pub fn register(&mut self, transport: UdpChannelTransport) {
        self.transports.push(transport);
    }

    pub fn poll_transports<F>(&mut self, mut frame_handler: F) -> Result<usize>
    where
        F: FnMut(&[u8], SocketAddr),
    {
        let mut total_bytes = 0;
        for transport in &self.transports {
            while let Ok((bytes_read, src_addr)) = transport.recv_from(&mut self.buffer) {
                if bytes_read > 0 {
                    total_bytes += bytes_read;
                    frame_handler(&self.buffer[..bytes_read], src_addr);
                }
            }
        }
        Ok(total_bytes)
    }
}

pub struct ControlTransportPoller {
    transports: Vec<UdpChannelTransport>,
    buffer: Vec<u8>,
}

impl Default for ControlTransportPoller {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlTransportPoller {
    pub fn new() -> Self {
        Self {
            transports: Vec::new(),
            buffer: vec![0u8; 65536],
        }
    }

    pub fn register(&mut self, transport: UdpChannelTransport) {
        self.transports.push(transport);
    }

    pub fn poll_transports<F>(&mut self, mut frame_handler: F) -> Result<usize>
    where
        F: FnMut(&[u8], SocketAddr),
    {
        let mut total_bytes = 0;
        for transport in &self.transports {
            while let Ok((bytes_read, src_addr)) = transport.recv_from(&mut self.buffer) {
                if bytes_read > 0 {
                    total_bytes += bytes_read;
                    frame_handler(&self.buffer[..bytes_read], src_addr);
                }
            }
        }
        Ok(total_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::udp_channel::UdpChannel;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn should_poll_registered_transport() {
        let channel1 = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let channel2 = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

        let t1 = UdpChannelTransport::bind(channel1, bind_addr, None, 0, 0).unwrap();
        let t2 = UdpChannelTransport::bind(channel2, bind_addr, None, 0, 0).unwrap();
        let t2_addr = t2.local_addr().unwrap();

        t1.send_to(b"hello aeron", t2_addr).unwrap();

        let mut poller = DataTransportPoller::new();
        poller.register(t2);

        let mut received = Vec::new();
        let bytes = poller
            .poll_transports(|data, _src| {
                received.extend_from_slice(data);
            })
            .unwrap();

        assert_eq!(bytes, 11);
        assert_eq!(received, b"hello aeron");
    }
}
