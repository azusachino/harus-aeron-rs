//! Integration tests: a client maps the driver's CnC file and drives real ADD/REMOVE
//! commands through the to-driver ring buffer; the conductor registers them and replies on
//! the to-clients broadcast.

use std::net::UdpSocket;
use std::time::Duration;

use aeron_client::conductor::ClientConductor;
use aeron_client::driver_events::{ON_OPERATION_SUCCESS, OperationSucceededFlyweight};
use aeron_client::driver_proxy::DriverProxy;
use aeron_client::logbuffer::TERM_MIN_LENGTH;
use aeron_client::protocol::{DATA_HEADER_LENGTH, HDR_TYPE_DATA, HDR_TYPE_SETUP, HeaderFlyweight};
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

/// Small-term network config so publication log files map cheaply in tests.
fn small_udp_config() -> Configuration {
    Configuration {
        publication_term_buffer_length: TERM_MIN_LENGTH as usize,
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

#[test]
fn network_publication_sender_transmits_setup_and_data_frames() {
    const STREAM_ID: i32 = 2002;

    // A raw socket stands in for a remote receiver's channel endpoint.
    let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
    receiver
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let endpoint_port = receiver.local_addr().unwrap().port();
    let channel = format!("aeron:udp?endpoint=127.0.0.1:{endpoint_port}");

    let mut driver = MediaDriver::launch(small_udp_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);
    let pub_reg = publisher.add_publication(&channel, STREAM_ID).unwrap();

    // Driver registers the publication, binds the send endpoint, and the Sender announces the
    // stream with a SETUP frame.
    driver.do_work().expect("register + first send");

    // Share the publication-limit counter, resolve PUBLICATION_READY, then publish a message.
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");

    let mut payload = *b"hello-udp";
    let payload_len = payload.len();
    let src = AtomicBuffer::new(&mut payload);
    let publication = publisher.publication(pub_reg).expect("publication ready");
    assert!(publication.offer(&src, 0, payload_len).unwrap() > 0);

    // Next duty cycle flushes the framed data out of the shared log onto the wire.
    driver.do_work().expect("flush data");

    // The receiver socket sees a SETUP announcement followed by the DATA frame.
    let mut saw_setup = false;
    let mut data_payload: Option<Vec<u8>> = None;
    let mut buf = [0u8; 2048];
    for _ in 0..4 {
        let n = match receiver.recv(&mut buf) {
            Ok(n) => n,
            Err(_) => break,
        };
        let header = HeaderFlyweight::new(AtomicBuffer::new(&mut buf[..n]));
        let frame_type = header.header_type();
        let frame_length = header.frame_length() as usize;
        if frame_type == HDR_TYPE_SETUP {
            saw_setup = true;
        } else if frame_type == HDR_TYPE_DATA && frame_length > DATA_HEADER_LENGTH {
            // Slice by the frame's own length, not the MTU-padded datagram size.
            data_payload = Some(buf[DATA_HEADER_LENGTH..frame_length].to_vec());
            break;
        }
    }

    assert!(saw_setup, "sender announced the stream with a SETUP frame");
    assert_eq!(data_payload.as_deref(), Some(&b"hello-udp"[..]));
}
