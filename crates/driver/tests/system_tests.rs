//! Integration tests: a client maps the driver's CnC file and drives real ADD/REMOVE
//! commands through the to-driver ring buffer; the conductor registers them and replies on
//! the to-clients broadcast.

use aeron_client::driver_events::{ON_OPERATION_SUCCESS, OperationSucceededFlyweight};
use aeron_client::driver_proxy::DriverProxy;
use aeron_driver::configuration::Configuration;
use aeron_driver::driver::MediaDriver;
use agrona::BroadcastReceiver;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

fn client_proxy(driver: &MediaDriver, client_id: i64) -> DriverProxy<'static> {
    let ring = ManyToOneRingBuffer::new(driver.cnc().to_driver_buffer()).unwrap();
    DriverProxy::new(ring, client_id)
}

#[test]
fn client_registers_ipc_pub_and_sub_through_the_cnc() {
    let mut driver = MediaDriver::launch(Configuration::default()).expect("launch");
    let proxy = client_proxy(&driver, 1);

    proxy.add_publication("aeron:ipc", 1001).unwrap();
    proxy.add_subscription("aeron:ipc", 1001).unwrap();

    let work = driver.do_work().expect("duty cycle");
    assert_eq!(work, 2);
    assert_eq!(driver.conductor().publications().len(), 1);
    assert_eq!(driver.conductor().subscriptions().len(), 1);
}

#[test]
fn removing_a_publication_acks_with_operation_success() {
    let mut driver = MediaDriver::launch(Configuration::default()).expect("launch");
    let proxy = client_proxy(&driver, 1);

    proxy.add_publication("aeron:ipc", 1001).unwrap();
    driver.do_work().unwrap();
    let registration_id = *driver.conductor().publications().keys().next().unwrap();

    // Subscribe to driver events before issuing the remove.
    let mut events = BroadcastReceiver::new(driver.cnc().to_clients_buffer()).unwrap();
    let remove_correlation = proxy.remove_publication(registration_id, false).unwrap();
    driver.do_work().unwrap();

    assert_eq!(driver.conductor().publications().len(), 0);

    let mut seen_type = 0i32;
    let mut seen_correlation = 0i64;
    let got = events
        .receive_next(|msg_type, buf, offset, _len| {
            seen_type = msg_type;
            seen_correlation = OperationSucceededFlyweight::wrap(*buf, offset).correlation_id();
        })
        .unwrap();

    assert!(got);
    assert_eq!(seen_type, ON_OPERATION_SUCCESS);
    assert_eq!(seen_correlation, remove_correlation);
}
