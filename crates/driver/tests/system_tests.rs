//! Integration tests: a client maps the driver's CnC file and drives real ADD/REMOVE
//! commands through the to-driver ring buffer; the conductor registers them and replies on
//! the to-clients broadcast.

use std::net::UdpSocket;
use std::time::Duration;

use aeron_client::conductor::ClientConductor;
use aeron_client::driver_events::{ON_OPERATION_SUCCESS, OperationSucceededFlyweight};
use aeron_client::driver_proxy::DriverProxy;
use aeron_client::logbuffer::TERM_MIN_LENGTH;
use aeron_client::protocol::{
    DATA_HEADER_LENGTH, DataHeaderFlyweight, HDR_TYPE_DATA, HDR_TYPE_SETUP, HeaderFlyweight,
    NAK_HEADER_LENGTH, NakFlyweight, STATUS_MESSAGE_HEADER_LENGTH, SetupFlyweight,
    StatusMessageFlyweight,
};
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
fn network_spy_subscription_reads_the_publication_log() {
    const STREAM_ID: i32 = 1101;
    let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}",
        receiver.local_addr().unwrap().port()
    );
    let spy_channel = format!("aeron-spy:{channel}");
    let mut driver = MediaDriver::launch(small_udp_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);
    let mut spy = client_conductor(&driver, 2);

    let pub_reg = publisher.add_publication(&channel, STREAM_ID).unwrap();
    let spy_reg = spy.add_subscription(&spy_channel, STREAM_ID).unwrap();
    driver.do_work().expect("register publication and spy");

    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
        spy.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");
    spy.do_work().expect("spy services events");

    let mut payload = *b"hello-network-spy";
    let payload_len = payload.len();
    let src = AtomicBuffer::new(&mut payload);
    let publication = publisher.publication(pub_reg).expect("publication ready");
    assert!(publication.offer(&src, 0, payload_len).unwrap() > 0);

    let mut received = Vec::new();
    let subscription = spy.subscription_mut(spy_reg).expect("spy ready");
    assert_eq!(
        subscription.poll(
            |buffer, offset, length, _header| {
                received.resize(length, 0);
                buffer.get_bytes(offset, &mut received);
            },
            10,
        ),
        1
    );
    assert_eq!(received, b"hello-network-spy");
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
    let mut sender_address = None;
    let mut data_frame_length = 0;
    let mut data_session_id = 0;
    let mut data_term_id = 0;
    let mut data_term_offset = 0;
    let mut buf = [0u8; 2048];
    for _ in 0..4 {
        let (n, source) = match receiver.recv_from(&mut buf) {
            Ok(n) => n,
            Err(_) => break,
        };
        let header = HeaderFlyweight::new(AtomicBuffer::new(&mut buf[..n]));
        let frame_type = header.header_type();
        let frame_length = header.frame_length() as usize;
        if frame_type == HDR_TYPE_SETUP {
            saw_setup = true;
            let setup = SetupFlyweight::new(AtomicBuffer::new(&mut buf[..n]));
            let mut status_bytes = [0u8; STATUS_MESSAGE_HEADER_LENGTH];
            let status = StatusMessageFlyweight::new(AtomicBuffer::new(&mut status_bytes));
            status
                .header()
                .set_frame_length(STATUS_MESSAGE_HEADER_LENGTH as i32)
                .set_version(aeron_client::protocol::CURRENT_VERSION)
                .set_flags(0)
                .set_header_type(aeron_client::protocol::HDR_TYPE_SM);
            status
                .set_session_id(setup.session_id())
                .set_stream_id(setup.stream_id())
                .set_consumption_term_id(setup.initial_term_id())
                .set_consumption_term_offset(0)
                .set_receiver_window_length(TERM_MIN_LENGTH)
                .set_receiver_id(0);
            receiver.send_to(&status_bytes, source).unwrap();
            for _ in 0..20 {
                driver.do_work().expect("process status and flush data");
                if driver.sender().network_publications()[0].receiver_limit() > 0 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        } else if frame_type == HDR_TYPE_DATA && frame_length > DATA_HEADER_LENGTH {
            // Slice by the frame's own length, not the MTU-padded datagram size.
            data_payload = Some(buf[DATA_HEADER_LENGTH..frame_length].to_vec());
            sender_address = Some(source);
            data_frame_length = frame_length;
            let data = DataHeaderFlyweight::new(AtomicBuffer::new(&mut buf[..n]));
            data_session_id = data.session_id();
            data_term_id = data.term_id();
            data_term_offset = data.term_offset();
            break;
        }
    }

    assert!(saw_setup, "sender announced the stream with a SETUP frame");
    assert_eq!(data_payload.as_deref(), Some(&b"hello-udp"[..]));

    // Request the frame again and prove the sender retransmits from the mapped term log.
    let mut nak_bytes = [0u8; NAK_HEADER_LENGTH];
    let nak = NakFlyweight::new(AtomicBuffer::new(&mut nak_bytes));
    nak.header()
        .set_frame_length(NAK_HEADER_LENGTH as i32)
        .set_version(aeron_client::protocol::CURRENT_VERSION)
        .set_flags(0)
        .set_header_type(aeron_client::protocol::HDR_TYPE_NAK);
    nak.set_session_id(data_session_id)
        .set_stream_id(STREAM_ID)
        .set_term_id(data_term_id)
        .set_term_offset(data_term_offset)
        .set_length(data_frame_length as i32);
    receiver
        .send_to(&nak_bytes, sender_address.expect("data source"))
        .unwrap();
    for _ in 0..20 {
        driver.do_work().expect("process NAK and retransmit");
        std::thread::sleep(Duration::from_millis(1));
    }

    let mut retransmitted = None;
    for _ in 0..4 {
        let n = match receiver.recv(&mut buf) {
            Ok(n) => n,
            Err(_) => break,
        };
        let (frame_type, frame_length) = {
            let header = HeaderFlyweight::new(AtomicBuffer::new(&mut buf[..n]));
            (header.header_type(), header.frame_length() as usize)
        };
        if frame_type == HDR_TYPE_DATA && frame_length > DATA_HEADER_LENGTH {
            retransmitted = Some(buf[DATA_HEADER_LENGTH..frame_length].to_vec());
            break;
        }
    }
    assert_eq!(retransmitted.as_deref(), Some(&b"hello-udp"[..]));
}

#[test]
fn udp_publication_to_subscription_roundtrip_through_two_drivers() {
    const STREAM_ID: i32 = 3003;

    let endpoint_socket = UdpSocket::bind("127.0.0.1:0").expect("reserve endpoint");
    let endpoint_port = endpoint_socket.local_addr().unwrap().port();
    drop(endpoint_socket);
    let channel = format!("aeron:udp?endpoint=127.0.0.1:{endpoint_port}");

    let mut publisher_driver = MediaDriver::launch(small_udp_config()).expect("publisher launch");
    let mut subscriber_driver = MediaDriver::launch(small_udp_config()).expect("subscriber launch");
    let mut subscriber = client_conductor(&subscriber_driver, 2);
    let sub_reg = subscriber.add_subscription(&channel, STREAM_ID).unwrap();

    // Bind the receiving endpoint before the publication sends its first SETUP.
    subscriber_driver.do_work().expect("register subscription");
    subscriber.do_work().expect("resolve subscription");

    let mut publisher = client_conductor(&publisher_driver, 1);
    let pub_reg = publisher.add_publication(&channel, STREAM_ID).unwrap();
    publisher_driver
        .do_work()
        .expect("register publication and send setup");
    for (id, counter) in publisher_driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("resolve publication");

    // The subscriber receives SETUP, creates its image log, and emits the initial SM.
    subscriber_driver.do_work().expect("receive setup");
    for (id, counter) in subscriber_driver.conductor().counters() {
        subscriber.register_counter(*id, counter.clone());
    }
    subscriber.do_work().expect("resolve available image");

    let mut payload = *b"hello-udp-roundtrip";
    let payload_len = payload.len();
    let src = AtomicBuffer::new(&mut payload);
    let publication = publisher.publication(pub_reg).expect("publication ready");
    assert!(publication.offer(&src, 0, payload_len).unwrap() > 0);

    let mut received = None;
    for _ in 0..100 {
        publisher_driver.do_work().expect("publisher duty cycle");
        subscriber_driver.do_work().expect("subscriber duty cycle");
        for (id, counter) in subscriber_driver.conductor().counters() {
            subscriber.register_counter(*id, counter.clone());
        }
        subscriber.do_work().expect("subscriber events");
        if let Some(subscription) = subscriber.subscription_mut(sub_reg) {
            let mut fragment = Vec::new();
            if subscription.poll(
                |buffer, offset, length, _header| {
                    fragment.resize(length, 0);
                    buffer.get_bytes(offset, &mut fragment);
                },
                10,
            ) > 0
            {
                received = Some(fragment);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(received.as_deref(), Some(&b"hello-udp-roundtrip"[..]));
}

#[test]
fn udp_receiver_status_advances_beyond_initial_window() {
    const STREAM_ID: i32 = 3004;
    const MESSAGE_COUNT: usize = 40;
    const PAYLOAD_LENGTH: usize = 1024;

    let endpoint_socket = UdpSocket::bind("127.0.0.1:0").expect("reserve endpoint");
    let endpoint_port = endpoint_socket.local_addr().unwrap().port();
    drop(endpoint_socket);
    let channel = format!("aeron:udp?endpoint=127.0.0.1:{endpoint_port}");

    let mut publisher_driver = MediaDriver::launch(small_udp_config()).expect("publisher launch");
    let mut subscriber_driver = MediaDriver::launch(small_udp_config()).expect("subscriber launch");
    let mut subscriber = client_conductor(&subscriber_driver, 2);
    let sub_reg = subscriber.add_subscription(&channel, STREAM_ID).unwrap();
    subscriber_driver
        .do_work()
        .expect("bind subscriber endpoint");
    subscriber.do_work().expect("resolve subscription");

    let mut publisher = client_conductor(&publisher_driver, 1);
    let pub_reg = publisher.add_publication(&channel, STREAM_ID).unwrap();
    publisher_driver.do_work().expect("send setup");
    for (id, counter) in publisher_driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("resolve publication");
    subscriber_driver.do_work().expect("create image");
    for (id, counter) in subscriber_driver.conductor().counters() {
        subscriber.register_counter(*id, counter.clone());
    }
    subscriber.do_work().expect("resolve image");

    let mut offered = 0usize;
    let mut received = 0usize;
    for _ in 0..500 {
        if offered < MESSAGE_COUNT {
            let mut payload = vec![offered as u8; PAYLOAD_LENGTH];
            let position = publisher
                .publication(pub_reg)
                .expect("publication ready")
                .offer(&AtomicBuffer::new(&mut payload), 0, PAYLOAD_LENGTH)
                .unwrap();
            if position > 0 {
                offered += 1;
            }
        }
        publisher_driver.do_work().expect("publish data");
        subscriber_driver
            .do_work()
            .expect("receive data and advance SM");
        for (id, counter) in subscriber_driver.conductor().counters() {
            subscriber.register_counter(*id, counter.clone());
        }
        subscriber.do_work().expect("service subscriber events");
        if let Some(subscription) = subscriber.subscription_mut(sub_reg) {
            received += subscription.poll(|_, _, _, _| {}, MESSAGE_COUNT);
        }
        if received == MESSAGE_COUNT && offered == MESSAGE_COUNT {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        received,
        MESSAGE_COUNT,
        "offered={offered}, sender_position={}, receiver_limit={}",
        publisher_driver.sender().network_publications()[0].sender_position(),
        publisher_driver.sender().network_publications()[0].receiver_limit()
    );
}

#[test]
fn udp_receiver_gap_generates_nak_and_sender_retransmits() {
    const STREAM_ID: i32 = 4004;
    const FIRST_PAYLOAD: &[u8] = &[0x11; 800];
    const SECOND_PAYLOAD: &[u8] = &[0x22; 800];

    // The relay lets the test deliver SETUP and the second DATA frame to the real receiver
    // while intentionally withholding the first DATA frame. Control traffic is forwarded in
    // the other direction, so the NAK observed here is generated by Receiver, not injected by
    // the test.
    let relay = UdpSocket::bind("127.0.0.1:0").expect("bind relay");
    relay.set_nonblocking(true).unwrap();
    let relay_address = relay.local_addr().unwrap();

    let subscriber_socket = UdpSocket::bind("127.0.0.1:0").expect("reserve subscriber port");
    let subscriber_port = subscriber_socket.local_addr().unwrap().port();
    drop(subscriber_socket);
    let subscriber_address = format!("127.0.0.1:{subscriber_port}");
    let subscriber_channel = format!("aeron:udp?endpoint={subscriber_address}");
    let publisher_channel = format!("aeron:udp?endpoint={relay_address}");

    let mut publisher_driver = MediaDriver::launch(small_udp_config()).expect("publisher launch");
    let mut subscriber_driver = MediaDriver::launch(small_udp_config()).expect("subscriber launch");
    let mut publisher = client_conductor(&publisher_driver, 1);
    let mut subscriber = client_conductor(&subscriber_driver, 2);
    let pub_reg = publisher
        .add_publication(&publisher_channel, STREAM_ID)
        .unwrap();
    let sub_reg = subscriber
        .add_subscription(&subscriber_channel, STREAM_ID)
        .unwrap();

    subscriber_driver
        .do_work()
        .expect("bind subscriber endpoint");
    subscriber.do_work().expect("resolve subscription");
    publisher_driver
        .do_work()
        .expect("register publication and send setup");
    for (id, counter) in publisher_driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("resolve publication");

    let mut datagram = [0u8; 2048];
    let publisher_address = loop {
        publisher_driver.do_work().expect("send setup");
        if let Ok((length, source)) = relay.recv_from(&mut datagram) {
            let header = HeaderFlyweight::new(AtomicBuffer::new(&mut datagram[..length]));
            if header.header_type() == HDR_TYPE_SETUP {
                let setup = datagram[..length].to_vec();
                relay
                    .send_to(&setup, subscriber_address.as_str())
                    .expect("forward setup");
                break source;
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    };

    subscriber_driver
        .do_work()
        .expect("create publication image");
    for (id, counter) in subscriber_driver.conductor().counters() {
        subscriber.register_counter(*id, counter.clone());
    }
    subscriber.do_work().expect("resolve available image");

    // Forward the receiver's initial SM back to the publication's actual UDP source.
    let mut control = [0u8; 256];
    let status = loop {
        if let Ok((length, _source)) = relay.recv_from(&mut control) {
            let header = HeaderFlyweight::new(AtomicBuffer::new(&mut control[..length]));
            if header.header_type() == aeron_client::protocol::HDR_TYPE_SM {
                break control[..length].to_vec();
            }
        }
        subscriber_driver
            .do_work()
            .expect("send initial status message");
        std::thread::sleep(Duration::from_millis(1));
    };
    relay
        .send_to(&status, publisher_address)
        .expect("forward status");

    for _ in 0..20 {
        publisher_driver.do_work().expect("process status");
        if publisher_driver.sender().network_publications()[0].receiver_limit() > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    let mut first = FIRST_PAYLOAD.to_vec();
    let first_len = first.len();
    let first_position = publisher
        .publication(pub_reg)
        .expect("publication ready")
        .offer(&AtomicBuffer::new(&mut first), 0, first_len)
        .unwrap();
    assert!(first_position > 0);
    let mut second = SECOND_PAYLOAD.to_vec();
    let second_len = second.len();
    let second_position = publisher
        .publication(pub_reg)
        .expect("publication ready")
        .offer(&AtomicBuffer::new(&mut second), 0, second_len)
        .unwrap();
    assert!(second_position > first_position);

    let mut data_frames = Vec::new();
    for _ in 0..40 {
        publisher_driver.do_work().expect("send data frames");
        while let Ok((length, source)) = relay.recv_from(&mut datagram) {
            let header = HeaderFlyweight::new(AtomicBuffer::new(&mut datagram[..length]));
            if header.header_type() == HDR_TYPE_DATA && header.frame_length() > 0 {
                let data = DataHeaderFlyweight::new(AtomicBuffer::new(&mut datagram[..length]));
                data_frames.push((data.term_offset(), datagram[..length].to_vec(), source));
            }
        }
        if data_frames.len() >= 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    data_frames.sort_by_key(|(term_offset, _, _)| *term_offset);
    assert_eq!(data_frames.len(), 2, "two DATA datagrams should be emitted");

    // Deliver only the later frame. Receiver must detect the gap and send a NAK to the relay.
    relay
        .send_to(&data_frames[1].1, subscriber_address.as_str())
        .expect("forward out-of-order data");
    subscriber_driver.do_work().expect("detect data gap");

    let nak = loop {
        if let Ok((length, _source)) = relay.recv_from(&mut control) {
            let header = HeaderFlyweight::new(AtomicBuffer::new(&mut control[..length]));
            if header.header_type() == aeron_client::protocol::HDR_TYPE_NAK {
                break control[..length].to_vec();
            }
        }
        subscriber_driver.do_work().expect("poll receiver gap");
        std::thread::sleep(Duration::from_millis(1));
    };
    let mut nak_for_read = nak.clone();
    let nak_header = NakFlyweight::new(AtomicBuffer::new(&mut nak_for_read));
    assert_eq!(nak_header.term_offset(), data_frames[0].0);
    relay
        .send_to(&nak, publisher_address)
        .expect("forward receiver NAK");

    // The Sender consumes that NAK and retransmits the missing first frame. Forward it to the
    // real receiver, which can now rebuild and deliver both messages in order.
    let mut retransmitted = None;
    for _ in 0..40 {
        publisher_driver.do_work().expect("process receiver NAK");
        while let Ok((length, source)) = relay.recv_from(&mut datagram) {
            let header = HeaderFlyweight::new(AtomicBuffer::new(&mut datagram[..length]));
            if header.header_type() == HDR_TYPE_DATA && header.frame_length() > 0 {
                let data = DataHeaderFlyweight::new(AtomicBuffer::new(&mut datagram[..length]));
                if data.term_offset() == data_frames[0].0 {
                    retransmitted = Some(datagram[..length].to_vec());
                    let _ = source;
                    break;
                }
            }
        }
        if retransmitted.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let retransmitted = retransmitted.expect("sender retransmitted the missing frame");
    relay
        .send_to(&retransmitted, subscriber_address.as_str())
        .expect("forward retransmitted data");

    let mut received = Vec::new();
    for _ in 0..100 {
        subscriber_driver
            .do_work()
            .expect("rebuild recovered stream");
        for (id, counter) in subscriber_driver.conductor().counters() {
            subscriber.register_counter(*id, counter.clone());
        }
        subscriber.do_work().expect("service image events");
        if let Some(subscription) = subscriber.subscription_mut(sub_reg) {
            subscription.poll(
                |buffer, offset, length, _header| {
                    let mut fragment = vec![0u8; length];
                    buffer.get_bytes(offset, &mut fragment);
                    received.push(fragment);
                },
                10,
            );
        }
        if received.len() >= 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        received,
        vec![FIRST_PAYLOAD.to_vec(), SECOND_PAYLOAD.to_vec()]
    );
}
