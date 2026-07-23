//! Flow control and multi-stream integration tests.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use aeron_driver::configuration::Configuration;
use aeron_driver::driver::MediaDriver;
use aeron_driver::media::receive_channel_endpoint::ReceiveChannelEndpoint;
use aeron_driver::media::send_channel_endpoint::SendChannelEndpoint;
use aeron_driver::media::udp_channel::UdpChannel;
use aeron_driver::media::udp_channel_transport::UdpChannelTransport;

#[test]
fn should_support_multi_stream_registrations_on_conductor() {
    let config = Configuration::default();
    let mut driver = MediaDriver::launch(config).expect("failed to launch MediaDriver");

    let mut pub_ids = Vec::new();
    let mut sub_ids = Vec::new();

    for stream_id in [1001, 1002, 1003, 1004] {
        let pub_id = driver
            .conductor_mut()
            .on_add_publication(stream_id, "aeron:ipc")
            .unwrap();
        let sub_id = driver
            .conductor_mut()
            .on_add_subscription(stream_id, "aeron:ipc")
            .unwrap();
        pub_ids.push(pub_id);
        sub_ids.push(sub_id);
    }

    assert_eq!(driver.conductor().publications().len(), 4);
    assert_eq!(driver.conductor().subscriptions().len(), 4);

    for (pub_id, sub_id) in pub_ids.into_iter().zip(sub_ids) {
        driver
            .conductor_mut()
            .on_remove_publication(pub_id)
            .unwrap();
        driver
            .conductor_mut()
            .on_remove_subscription(sub_id)
            .unwrap();
    }

    assert_eq!(driver.conductor().publications().len(), 0);
    assert_eq!(driver.conductor().subscriptions().len(), 0);
}

#[test]
fn should_handle_flow_control_status_message_and_nak_events() {
    let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let send_transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
    let mut send_endpoint = SendChannelEndpoint::new(channel.clone(), send_transport);

    let recv_transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
    let recv_endpoint = ReceiveChannelEndpoint::new(channel, recv_transport);
    let recv_addr = recv_endpoint.transport().local_addr().unwrap();

    // Trigger status message processing
    send_endpoint.on_status_message(1001, 100, 1, 0, 65536);
    send_endpoint.on_nak_message(1001, 100, 1, 0, 1400);

    // Send receiver feedback frames
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
