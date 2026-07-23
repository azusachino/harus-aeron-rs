//! Integration system tests for MediaDriver pub/sub roundtrip messaging over IPC and UDP channels.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use aeron_driver::configuration::Configuration;
use aeron_driver::driver::MediaDriver;
use aeron_driver::media::send_channel_endpoint::SendChannelEndpoint;
use aeron_driver::media::udp_channel::UdpChannel;
use aeron_driver::media::udp_channel_transport::UdpChannelTransport;

#[test]
fn should_spin_up_driver_and_register_ipc_pub_sub() {
    let config = Configuration::default();
    let mut driver = MediaDriver::launch(config).expect("failed to launch MediaDriver");

    let pub_id = driver
        .conductor_mut()
        .on_add_publication(1001, "aeron:ipc")
        .expect("failed to add publication");

    let sub_id = driver
        .conductor_mut()
        .on_add_subscription(1001, "aeron:ipc")
        .expect("failed to add subscription");

    assert_eq!(driver.conductor().publications().len(), 1);
    assert_eq!(driver.conductor().subscriptions().len(), 1);

    let work = driver.do_work().expect("failed driver duty cycle work");
    assert_eq!(work, 0);

    driver
        .conductor_mut()
        .on_remove_publication(pub_id)
        .unwrap();
    driver
        .conductor_mut()
        .on_remove_subscription(sub_id)
        .unwrap();

    assert_eq!(driver.conductor().publications().len(), 0);
    assert_eq!(driver.conductor().subscriptions().len(), 0);
}

#[test]
fn should_spin_up_driver_and_register_udp_endpoints() {
    let config = Configuration::default();
    let mut driver = MediaDriver::launch(config).expect("failed to launch MediaDriver");

    let channel_str = "aeron:udp?endpoint=localhost:45678";
    let channel = UdpChannel::parse(channel_str).expect("failed to parse UdpChannel");
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0)
        .expect("failed to bind UdpChannelTransport");

    let send_endpoint = SendChannelEndpoint::new(channel, transport);
    driver.sender_mut().add_send_channel_endpoint(send_endpoint);

    assert_eq!(driver.sender().send_channel_endpoints().len(), 1);

    let work = driver.do_work().expect("failed driver duty cycle work");
    assert_eq!(work, 0);
}
