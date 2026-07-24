//! Integration tests: a client maps the driver's CnC file and drives real ADD/REMOVE
//! commands through the to-driver ring buffer; the conductor registers them and replies on
//! the to-clients broadcast.

use aeron_client::conductor::ClientConductor;
use aeron_client::driver_events::{ON_OPERATION_SUCCESS, OperationSucceededFlyweight};
use aeron_client::driver_proxy::DriverProxy;
use aeron_client::logbuffer::TERM_MIN_LENGTH;
use aeron_driver::configuration::Configuration;
use aeron_driver::driver::MediaDriver;
use agrona::AtomicBuffer;
use agrona::BroadcastReceiver;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

fn client_proxy(driver: &MediaDriver, client_id: i64) -> DriverProxy<'static> {
    let ring = ManyToOneRingBuffer::new(driver.cnc().to_driver_buffer()).unwrap();
    DriverProxy::new(ring, client_id)
}

/// A client conductor bound to the driver's CnC to-driver ring and to-clients broadcast.
fn client_conductor(driver: &MediaDriver, client_id: i64) -> ClientConductor<'static> {
    ClientConductor::with_event_buffer(
        client_proxy(driver, client_id),
        driver.cnc().to_clients_buffer(),
    )
    .unwrap()
}

/// Small-term IPC config so the shared log files map cheaply in tests.
fn small_ipc_config() -> Configuration {
    Configuration {
        ipc_term_buffer_length: TERM_MIN_LENGTH as usize,
        ..Configuration::default()
    }
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

#[test]
fn ipc_publication_to_subscription_roundtrip_through_the_driver() {
    const STREAM_ID: i32 = 1001;
    let mut driver = MediaDriver::launch(small_ipc_config()).expect("launch");

    // Two independent client instances over one driver: a publisher and a subscriber.
    let mut publisher = client_conductor(&driver, 1);
    let mut subscriber = client_conductor(&driver, 2);

    let pub_reg = publisher.add_publication("aeron:ipc", STREAM_ID).unwrap();
    let sub_reg = subscriber.add_subscription("aeron:ipc", STREAM_ID).unwrap();

    // Driver registers both, allocates the shared IPC log, and connects the image.
    driver.do_work().expect("driver duty cycle");

    // Share the driver's position counters with the in-process clients (stands in for the
    // shared CnC counters region), then let each client resolve its ready/available events.
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
        subscriber.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");
    subscriber.do_work().expect("subscriber services events");

    // Publish a message into the shared log.
    let mut payload = *b"hello-ipc-roundtrip";
    let payload_len = payload.len();
    let src = AtomicBuffer::new(&mut payload);
    let publication = publisher.publication(pub_reg).expect("publication ready");
    let position = publication.offer(&src, 0, payload_len).unwrap();
    assert!(position > 0, "offer back-pressured or failed: {position}");

    // Poll it back out through the subscriber's image over the same shared log.
    let mut received: Vec<u8> = Vec::new();
    let subscription = subscriber.subscription_mut(sub_reg).expect("subscription");
    let fragments = subscription.poll(
        |buffer, offset, length, _header| {
            let mut bytes = vec![0u8; length];
            buffer.get_bytes(offset, &mut bytes);
            received = bytes;
        },
        10,
    );

    assert_eq!(fragments, 1, "exactly one fragment delivered");
    assert_eq!(received, b"hello-ipc-roundtrip");
}
