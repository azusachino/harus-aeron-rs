//! Multi-stream registration and receiver-feedback (status message / NAK) integration tests.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use aeron_client::driver_proxy::DriverProxy;
use aeron_driver::configuration::Configuration;
use aeron_driver::driver::MediaDriver;
use aeron_driver::media::receive_channel_endpoint::ReceiveChannelEndpoint;
use aeron_driver::media::send_channel_endpoint::SendChannelEndpoint;
use aeron_driver::media::udp_channel::UdpChannel;
use aeron_driver::media::udp_channel_transport::UdpChannelTransport;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

#[test]
fn conductor_registers_multiple_streams_from_one_client() {
    let mut driver = MediaDriver::launch(Configuration::default()).expect("launch");
    let ring = ManyToOneRingBuffer::new(driver.cnc().to_driver_buffer()).unwrap();
    let proxy = DriverProxy::new(ring, 7);

    for stream_id in [1001, 1002, 1003, 1004] {
        proxy.add_publication("aeron:ipc", stream_id).unwrap();
        proxy.add_subscription("aeron:ipc", stream_id).unwrap();
    }

    let work = driver.do_work().unwrap();
    assert_eq!(work, 8);
    assert_eq!(driver.conductor().publications().len(), 4);
    assert_eq!(driver.conductor().subscriptions().len(), 4);
}

#[test]
fn receiver_endpoint_sends_status_message_and_nak_frames() {
    let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let send_transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
    let mut send_endpoint = SendChannelEndpoint::new(channel.clone(), send_transport);

    let recv_transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
    let recv_endpoint = ReceiveChannelEndpoint::new(channel, recv_transport);
    let recv_addr = recv_endpoint.transport().local_addr().unwrap();

    send_endpoint.on_status_message(1001, 100, 1, 0, 65536);
    send_endpoint.on_nak_message(1001, 100, 1, 0, 1400);

    let statuses = send_endpoint.take_status_messages();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].receiver_window_length, 65536);
    let naks = send_endpoint.take_nak_messages();
    assert_eq!(naks.len(), 1);
    assert_eq!(naks[0].length, 1400);

    assert!(
        recv_endpoint
            .send_status_message(1001, 100, 1, 0, 65536, recv_addr)
            .is_ok()
    );
    assert!(
        recv_endpoint
            .send_nak(1001, 100, 1, 0, 1400, recv_addr)
            .is_ok()
    );
}
