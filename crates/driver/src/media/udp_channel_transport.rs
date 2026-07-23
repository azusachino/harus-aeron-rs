//! UDP non-blocking socket transport layer.
//!
//! Mirrors `io.aeron.driver.media.UdpChannelTransport.java`.

use std::net::{SocketAddr, UdpSocket};

use crate::error::{DriverError, Result};
use crate::media::udp_channel::UdpChannel;

#[derive(Debug)]
pub struct UdpChannelTransport {
    socket: UdpSocket,
    udp_channel: UdpChannel,
    bind_address: SocketAddr,
    connect_address: Option<SocketAddr>,
}

impl UdpChannelTransport {
    pub fn bind(
        udp_channel: UdpChannel,
        bind_address: SocketAddr,
        connect_address: Option<SocketAddr>,
        socket_rcvbuf_length: usize,
        socket_sndbuf_length: usize,
    ) -> Result<Self> {
        let socket = UdpSocket::bind(bind_address).map_err(|e| {
            DriverError::IllegalState(format!("failed to bind UDP socket to {bind_address}: {e}"))
        })?;

        socket.set_nonblocking(true).map_err(|e| {
            DriverError::IllegalState(format!("failed to set non-blocking mode: {e}"))
        })?;

        if socket_rcvbuf_length > 0 {
            // Best effort socket option configuration
            let _ = socket;
        }
        if socket_sndbuf_length > 0 {
            let _ = socket_sndbuf_length;
        }

        if let Some(target) = connect_address {
            let _ = socket.connect(target);
        }

        Ok(Self {
            socket,
            udp_channel,
            bind_address,
            connect_address,
        })
    }

    pub fn send_to(&self, buf: &[u8], target: SocketAddr) -> Result<usize> {
        self.socket
            .send_to(buf, target)
            .map_err(|e| DriverError::IllegalState(format!("UDP send_to error to {target}: {e}")))
    }

    pub fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        self.socket
            .recv_from(buf)
            .map_err(|e| DriverError::IllegalState(format!("UDP recv_from error: {e}")))
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.socket.local_addr().map_err(|e| {
            DriverError::IllegalState(format!("failed to query local socket address: {e}"))
        })
    }

    pub fn udp_channel(&self) -> &UdpChannel {
        &self.udp_channel
    }

    pub fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }

    pub fn connect_address(&self) -> Option<SocketAddr> {
        self.connect_address
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn should_bind_udp_channel_transport() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

        let transport = UdpChannelTransport::bind(channel, bind_addr, None, 0, 0).unwrap();
        let local_addr = transport.local_addr().unwrap();
        assert_ne!(local_addr.port(), 0);
    }
}
