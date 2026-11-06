//! Integration tests: a client maps the driver's CnC file and drives real ADD/REMOVE
//! commands through the to-driver ring buffer; the conductor registers them and replies on
//! the to-clients broadcast.

use std::net::UdpSocket;
use std::time::Duration;

use aeron_client::conductor::ClientConductor;
use aeron_client::driver_events::{
    ErrorResponseFlyweight, ON_ERROR, ON_OPERATION_SUCCESS, OperationSucceededFlyweight,
};
use aeron_client::driver_proxy::DriverProxy;
use aeron_client::fragment_assembler::{ControlledFragmentAssembler, FragmentAssembler};
use aeron_client::logbuffer::{ControlledPollAction, TERM_MIN_LENGTH};
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

/// Small-term, small-MTU IPC config so a modest payload spans multiple fragments.
fn small_mtu_ipc_config() -> Configuration {
    Configuration {
        ipc_term_buffer_length: TERM_MIN_LENGTH as usize,
        ipc_mtu_length: 64,
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
            seen_correlation =
                OperationSucceededFlyweight::wrap(buf.clone(), offset).correlation_id();
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
    let mut config = small_udp_config();
    config.spies_simulate_connection = true;
    let mut driver = MediaDriver::launch(config).expect("launch");
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
    assert_eq!(
        publication.offer(&src, 0, payload_len).unwrap(),
        aeron_client::publication::NOT_CONNECTED
    );

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
                .set_receiver_window_length(TERM_MIN_LENGTH / 2)
                .set_receiver_id(0);
            receiver.send_to(&status_bytes, source).unwrap();
            for _ in 0..20 {
                driver.do_work().expect("process status and flush data");
                if driver.sender().network_publications()[0].receiver_limit() > 0 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            break;
        }
    }

    assert!(saw_setup, "sender announced the stream with a SETUP frame");
    assert!(publication.offer(&src, 0, payload_len).unwrap() > 0);
    driver.do_work().expect("flush data");
    for _ in 0..4 {
        let (n, source) = match receiver.recv_from(&mut buf) {
            Ok(value) => value,
            Err(_) => break,
        };
        let header = HeaderFlyweight::new(AtomicBuffer::new(&mut buf[..n]));
        let frame_length = header.frame_length() as usize;
        if header.header_type() == HDR_TYPE_DATA && frame_length > DATA_HEADER_LENGTH {
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
fn udp_packet_is_rejected_before_any_frame_is_delivered() {
    let reservation = UdpSocket::bind("127.0.0.1:0").unwrap();
    let endpoint = reservation.local_addr().unwrap();
    drop(reservation);
    let channel = format!("aeron:udp?endpoint={endpoint}");
    let mut driver = MediaDriver::launch(small_udp_config()).unwrap();
    let mut client = client_conductor(&driver, 12);
    let registration = client.add_subscription(&channel, 77).unwrap();
    driver.do_work().unwrap();
    client.do_work().unwrap();
    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut setup = [0u8; 40];
    let fw = SetupFlyweight::new(AtomicBuffer::new(&mut setup));
    fw.header()
        .set_frame_length(40)
        .set_version(0)
        .set_header_type(HDR_TYPE_SETUP);
    fw.set_session_id(7)
        .set_stream_id(77)
        .set_initial_term_id(3)
        .set_active_term_id(3)
        .set_term_offset(0)
        .set_term_length(65536)
        .set_mtu_length(1408);
    sender.send_to(&setup, endpoint).unwrap();
    for _ in 0..20 {
        driver.do_work().unwrap();
        for (id, counter) in driver.conductor().counters() {
            client.register_counter(*id, counter.clone());
        }
        client.do_work().unwrap();
        if client.subscription_mut(registration).unwrap().image_count() > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let mut packet = [0u8; 128];
    for offset in [0, 64] {
        let fw = DataHeaderFlyweight::new(AtomicBuffer::new(&mut packet[offset..offset + 64]));
        fw.set_frame_length(33)
            .set_version(0)
            .set_flags(0xc0)
            .set_header_type(HDR_TYPE_DATA)
            .set_session_id(7)
            .set_stream_id(77)
            .set_term_id(3)
            .set_term_offset(offset as i32);
        packet[offset + 32] = (offset / 64 + 1) as u8;
    }
    packet[64 + 4] = 1; // invalid second frame version
    sender.send_to(&packet, endpoint).unwrap();
    for _ in 0..5 {
        driver.do_work().unwrap();
    }
    let mut seen = Vec::new();
    let sub = client.subscription_mut(registration).unwrap();
    assert_eq!(
        sub.poll(|buffer, offset, _, _| seen.push(buffer.get_u8(offset)), 10),
        0
    );
    assert!(seen.is_empty());
    packet[64 + 4] = 0;
    sender.send_to(&packet, endpoint).unwrap();
    for _ in 0..20 {
        driver.do_work().unwrap();
        if sub.poll(|buffer, offset, _, _| seen.push(buffer.get_u8(offset)), 10) == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(seen, [1, 2]);

    // Aeron transmits only the header of a final padding frame; its logical span is 128.
    let mut pad = [0u8; 32];
    let fw = DataHeaderFlyweight::new(AtomicBuffer::new(&mut pad));
    fw.set_frame_length(128)
        .set_version(0)
        .set_header_type(aeron_client::protocol::HDR_TYPE_PAD)
        .set_session_id(7)
        .set_stream_id(77)
        .set_term_id(3)
        .set_term_offset(128);
    sender.send_to(&pad, endpoint).unwrap();
    let mut after_pad = [0u8; 64];
    let fw = DataHeaderFlyweight::new(AtomicBuffer::new(&mut after_pad));
    fw.set_frame_length(33)
        .set_version(0)
        .set_flags(0xc0)
        .set_header_type(HDR_TYPE_DATA)
        .set_session_id(7)
        .set_stream_id(77)
        .set_term_id(3)
        .set_term_offset(256);
    after_pad[32] = 3;
    sender.send_to(&after_pad, endpoint).unwrap();
    for _ in 0..20 {
        driver.do_work().unwrap();
        sub.poll(|buffer, offset, _, _| seen.push(buffer.get_u8(offset)), 10);
        if seen.len() == 3 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(seen, [1, 2, 3]);
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
    for _ in 0..20 {
        publisher_driver
            .do_work()
            .expect("publisher receives initial status");
        if publisher_driver.sender().network_publications()[0].receiver_limit() > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }

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

#[test]
fn udp_receiver_recovers_from_two_independent_gaps_in_one_stream() {
    const STREAM_ID: i32 = 4005;
    const PAYLOADS: [&[u8]; 4] = [&[0x11; 700], &[0x22; 700], &[0x33; 700], &[0x44; 700]];

    // Same relay technique as the single-gap test, but this time two separate frames are
    // withheld (the 1st and 3rd of four), each one only revealed as missing once a later frame
    // that did arrive shows the receiver it skipped ahead. Real Receiver gap detection, not an
    // injected NAK.
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

    let mut last_position = 0;
    for payload in PAYLOADS {
        let mut bytes = payload.to_vec();
        let len = bytes.len();
        let position = publisher
            .publication(pub_reg)
            .expect("publication ready")
            .offer(&AtomicBuffer::new(&mut bytes), 0, len)
            .unwrap();
        assert!(position > last_position);
        last_position = position;
    }

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
        if data_frames.len() >= PAYLOADS.len() {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    data_frames.sort_by_key(|(term_offset, _, _)| *term_offset);
    assert_eq!(
        data_frames.len(),
        PAYLOADS.len(),
        "four DATA datagrams should be emitted"
    );

    // Withhold frames 0 and 2 (two separate gaps). The real loss detector only tracks the
    // earliest outstanding gap at a time (it advances the rebuild position gap by gap), so each
    // gap is resolved -- NAK, retransmit, rebuild -- before the next one is even revealed to the
    // receiver by delivering the frame that exposes it.
    for &(missing_index, revealing_index) in &[(0usize, 1usize), (2usize, 3usize)] {
        relay
            .send_to(&data_frames[revealing_index].1, subscriber_address.as_str())
            .expect("forward out-of-order data");
        subscriber_driver.do_work().expect("detect gap");

        let missing_offset = data_frames[missing_index].0;
        let nak = loop {
            if let Ok((length, _source)) = relay.recv_from(&mut control) {
                let header = HeaderFlyweight::new(AtomicBuffer::new(&mut control[..length]));
                if header.header_type() == aeron_client::protocol::HDR_TYPE_NAK {
                    let candidate = NakFlyweight::new(AtomicBuffer::new(&mut control[..length]));
                    if candidate.term_offset() == missing_offset {
                        break control[..length].to_vec();
                    }
                }
            }
            subscriber_driver.do_work().expect("poll receiver gap");
            std::thread::sleep(Duration::from_millis(1));
        };
        relay
            .send_to(&nak, publisher_address)
            .expect("forward receiver NAK");

        let retransmitted = loop {
            publisher_driver.do_work().expect("process receiver NAK");
            let mut found = None;
            while let Ok((length, _source)) = relay.recv_from(&mut datagram) {
                let header = HeaderFlyweight::new(AtomicBuffer::new(&mut datagram[..length]));
                if header.header_type() == HDR_TYPE_DATA && header.frame_length() > 0 {
                    let data = DataHeaderFlyweight::new(AtomicBuffer::new(&mut datagram[..length]));
                    if data.term_offset() == missing_offset {
                        found = Some(datagram[..length].to_vec());
                    }
                }
            }
            if let Some(frame) = found {
                break frame;
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        relay
            .send_to(&retransmitted, subscriber_address.as_str())
            .expect("forward retransmitted data");
        subscriber_driver
            .do_work()
            .expect("rebuild past resolved gap");
    }

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
        if received.len() >= PAYLOADS.len() {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        received,
        PAYLOADS.iter().map(|p| p.to_vec()).collect::<Vec<_>>(),
        "all four messages must be delivered, in order, after both gaps recover"
    );
}

#[test]
fn manual_add_destination_casts_data_to_a_second_receiver() {
    const STREAM_ID: i32 = 8008;

    // Two independent receiver sockets stand in for two remote subscribers: one reachable via
    // the publication's original channel endpoint, one added later as a manual MDC destination.
    let first_receiver = UdpSocket::bind("127.0.0.1:0").expect("bind first receiver");
    let second_receiver = UdpSocket::bind("127.0.0.1:0").expect("bind second receiver");
    for socket in [&first_receiver, &second_receiver] {
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
    }
    let first_port = first_receiver.local_addr().unwrap().port();
    let second_port = second_receiver.local_addr().unwrap().port();
    let channel = format!("aeron:udp?endpoint=127.0.0.1:{first_port}");
    let second_destination = format!("aeron:udp?endpoint=127.0.0.1:{second_port}");

    let mut driver = MediaDriver::launch(small_udp_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);
    let pub_reg = publisher.add_publication(&channel, STREAM_ID).unwrap();
    driver
        .do_work()
        .expect("register publication and send setup");

    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");

    // send_data() stays gated until a receiver's Status Message is seen, exactly like the
    // single-destination case: capture the SETUP the original endpoint announced and answer it,
    // so the publication's flow-control window actually opens before we add a second cast target.
    let mut buf = [0u8; 2048];
    let (n, source) = first_receiver.recv_from(&mut buf).expect("receive setup");
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
        .set_receiver_window_length(TERM_MIN_LENGTH / 2)
        .set_receiver_id(0);
    first_receiver.send_to(&status_bytes, source).unwrap();
    for _ in 0..20 {
        driver.do_work().expect("process status message");
        if driver.sender().network_publications()[0].receiver_limit() > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        driver.sender().network_publications()[0].receiver_limit() > 0,
        "flow-control window must open before adding a second destination"
    );

    // ADD_DESTINATION goes through the same to-driver ring as every other client command; the
    // driver applies it to the Sender's live NetworkPublication on the next duty cycle.
    publisher
        .driver_proxy()
        .add_destination(pub_reg, &second_destination)
        .expect("add manual MDC destination");
    driver.do_work().expect("apply destination update");
    assert_eq!(
        driver.sender().network_publications()[0]
            .destinations()
            .len(),
        2,
        "publication should now cast to both receivers"
    );

    let mut payload = *b"hello-mdc";
    let payload_len = payload.len();
    let src = AtomicBuffer::new(&mut payload);
    let publication = publisher.publication(pub_reg).expect("publication ready");
    assert!(publication.offer(&src, 0, payload_len).unwrap() > 0);
    for _ in 0..20 {
        driver.do_work().expect("flush data to both destinations");
        std::thread::sleep(Duration::from_millis(1));
    }

    for receiver in [&first_receiver, &second_receiver] {
        let mut buf = [0u8; 2048];
        let mut data_payload = None;
        for _ in 0..8 {
            let n = match receiver.recv(&mut buf) {
                Ok(n) => n,
                Err(_) => break,
            };
            let (frame_type, frame_length) = {
                let header = HeaderFlyweight::new(AtomicBuffer::new(&mut buf[..n]));
                (header.header_type(), header.frame_length() as usize)
            };
            if frame_type == HDR_TYPE_DATA && frame_length > DATA_HEADER_LENGTH {
                data_payload = Some(buf[DATA_HEADER_LENGTH..frame_length].to_vec());
                break;
            }
        }
        assert_eq!(
            data_payload.as_deref(),
            Some(&b"hello-mdc"[..]),
            "each destination should independently receive the DATA frame"
        );
    }
}

#[test]
fn manual_mdc_destination_remove_stops_traffic_and_readd_resumes() {
    const STREAM_ID: i32 = 8018;

    let first_receiver = UdpSocket::bind("127.0.0.1:0").expect("bind first receiver");
    let second_receiver = UdpSocket::bind("127.0.0.1:0").expect("bind second receiver");
    for socket in [&first_receiver, &second_receiver] {
        socket
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
    }
    let first_port = first_receiver.local_addr().unwrap().port();
    let second_port = second_receiver.local_addr().unwrap().port();
    let channel = format!("aeron:udp?endpoint=127.0.0.1:{first_port}");
    let second_destination = format!("aeron:udp?endpoint=127.0.0.1:{second_port}");

    let mut driver = MediaDriver::launch(small_udp_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);
    let pub_reg = publisher.add_publication(&channel, STREAM_ID).unwrap();
    driver
        .do_work()
        .expect("register publication and send setup");

    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");

    // Capture SETUP on first_receiver and reply with SM so flow-control opens
    let mut buf = [0u8; 2048];
    let (n, source) = first_receiver.recv_from(&mut buf).expect("receive setup");
    let setup = SetupFlyweight::new(AtomicBuffer::new(&mut buf[..n]));
    let session_id = setup.session_id();
    let initial_term_id = setup.initial_term_id();

    let mut status_bytes = [0u8; STATUS_MESSAGE_HEADER_LENGTH];
    let status = StatusMessageFlyweight::new(AtomicBuffer::new(&mut status_bytes));
    status
        .header()
        .set_frame_length(STATUS_MESSAGE_HEADER_LENGTH as i32)
        .set_version(aeron_client::protocol::CURRENT_VERSION)
        .set_flags(0)
        .set_header_type(aeron_client::protocol::HDR_TYPE_SM);
    status
        .set_session_id(session_id)
        .set_stream_id(STREAM_ID)
        .set_consumption_term_id(initial_term_id)
        .set_consumption_term_offset(0)
        .set_receiver_window_length(TERM_MIN_LENGTH / 2)
        .set_receiver_id(1);
    first_receiver.send_to(&status_bytes, source).unwrap();

    for _ in 0..20 {
        driver.do_work().expect("process status message");
        if driver.sender().network_publications()[0].receiver_limit() > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(driver.sender().network_publications()[0].receiver_limit() > 0);

    // 1. Add second destination -> 2 destinations
    publisher
        .driver_proxy()
        .add_destination(pub_reg, &second_destination)
        .expect("add manual MDC destination");
    driver.do_work().expect("apply destination update");
    assert_eq!(
        driver.sender().network_publications()[0]
            .destinations()
            .len(),
        2
    );

    // Helper to receive data payload on a socket
    let recv_data = |socket: &UdpSocket| -> Option<Vec<u8>> {
        let mut b = [0u8; 2048];
        for _ in 0..10 {
            if let Ok(len) = socket.recv(&mut b) {
                let (frame_type, frame_len) = {
                    let header = HeaderFlyweight::new(AtomicBuffer::new(&mut b[..len]));
                    (header.header_type(), header.frame_length() as usize)
                };
                if frame_type == HDR_TYPE_DATA && frame_len > DATA_HEADER_LENGTH {
                    return Some(b[DATA_HEADER_LENGTH..frame_len].to_vec());
                }
            } else {
                break;
            }
        }
        None
    };

    // Offer message 1 -> both receive
    let pub_ref = publisher.publication(pub_reg).expect("publication ready");
    let mut payload1 = *b"msg-1-both";
    let len1 = payload1.len();
    let src1 = AtomicBuffer::new(&mut payload1);
    assert!(pub_ref.offer(&src1, 0, len1).unwrap() > 0);
    for _ in 0..20 {
        driver.do_work().expect("flush data");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        recv_data(&first_receiver).as_deref(),
        Some(&b"msg-1-both"[..])
    );
    assert_eq!(
        recv_data(&second_receiver).as_deref(),
        Some(&b"msg-1-both"[..])
    );

    // 2. Remove second destination -> 1 destination
    publisher
        .driver_proxy()
        .remove_destination(pub_reg, &second_destination)
        .expect("remove manual MDC destination");
    driver.do_work().expect("apply remove destination update");
    assert_eq!(
        driver.sender().network_publications()[0]
            .destinations()
            .len(),
        1
    );

    // Drain any old buffered packets on sockets
    while first_receiver.recv(&mut buf).is_ok() {}
    while second_receiver.recv(&mut buf).is_ok() {}

    // Offer message 2 -> only first receives, second receives nothing
    let mut payload2 = *b"msg-2-first-only";
    let len2 = payload2.len();
    let src2 = AtomicBuffer::new(&mut payload2);
    assert!(pub_ref.offer(&src2, 0, len2).unwrap() > 0);
    for _ in 0..20 {
        driver.do_work().expect("flush data");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        recv_data(&first_receiver).as_deref(),
        Some(&b"msg-2-first-only"[..])
    );
    assert_eq!(
        recv_data(&second_receiver),
        None,
        "removed destination must not receive data frames"
    );

    // 3. Re-add second destination -> 2 destinations
    publisher
        .driver_proxy()
        .add_destination(pub_reg, &second_destination)
        .expect("re-add manual MDC destination");
    driver.do_work().expect("apply re-add destination update");
    assert_eq!(
        driver.sender().network_publications()[0]
            .destinations()
            .len(),
        2
    );

    // Drain old packets
    while first_receiver.recv(&mut buf).is_ok() {}
    while second_receiver.recv(&mut buf).is_ok() {}

    // Offer message 3 -> both receive again!
    let mut payload3 = *b"msg-3-both-again";
    let len3 = payload3.len();
    let src3 = AtomicBuffer::new(&mut payload3);
    assert!(pub_ref.offer(&src3, 0, len3).unwrap() > 0);
    for _ in 0..20 {
        driver.do_work().expect("flush data");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        recv_data(&first_receiver).as_deref(),
        Some(&b"msg-3-both-again"[..])
    );
    assert_eq!(
        recv_data(&second_receiver).as_deref(),
        Some(&b"msg-3-both-again"[..])
    );
}

#[test]
fn manual_mdc_publication_initializes_with_empty_destinations_when_port_is_zero() {
    const STREAM_ID: i32 = 8019;
    let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
    receiver
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let port = receiver.local_addr().unwrap().port();
    let dest_channel = format!("aeron:udp?endpoint=127.0.0.1:{port}");

    let mut driver = MediaDriver::launch(small_udp_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);
    let pub_reg = publisher
        .add_publication("aeron:udp?control-mode=manual", STREAM_ID)
        .unwrap();
    driver.do_work().expect("register publication");
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");

    // Initially destination port is 0, so publication has NO destinations
    assert!(
        driver.sender().network_publications()[0]
            .destinations()
            .is_empty()
    );

    // Add destination
    publisher
        .driver_proxy()
        .add_destination(pub_reg, &dest_channel)
        .expect("add destination");
    driver.do_work().expect("apply destination");
    assert_eq!(
        driver.sender().network_publications()[0]
            .destinations()
            .len(),
        1
    );

    // Receiver captures SETUP and sends SM
    let mut buf = [0u8; 2048];
    let (n, source) = receiver.recv_from(&mut buf).expect("receive setup");
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
        .set_stream_id(STREAM_ID)
        .set_consumption_term_id(setup.initial_term_id())
        .set_consumption_term_offset(0)
        .set_receiver_window_length(TERM_MIN_LENGTH / 2)
        .set_receiver_id(1);
    receiver.send_to(&status_bytes, source).unwrap();

    for _ in 0..20 {
        driver.do_work().expect("process status message");
        if driver.sender().network_publications()[0].receiver_limit() > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(driver.sender().network_publications()[0].receiver_limit() > 0);

    // Offer data
    let pub_ref = publisher.publication(pub_reg).expect("publication ready");
    let mut payload = *b"hello-manual-mdc";
    let len = payload.len();
    let src = AtomicBuffer::new(&mut payload);
    assert!(pub_ref.offer(&src, 0, len).unwrap() > 0);
    for _ in 0..20 {
        driver.do_work().expect("flush data");
        std::thread::sleep(Duration::from_millis(1));
    }

    let mut data_payload = None;
    for _ in 0..10 {
        if let Ok(len) = receiver.recv(&mut buf) {
            let (frame_type, frame_len) = {
                let header = HeaderFlyweight::new(AtomicBuffer::new(&mut buf[..len]));
                (header.header_type(), header.frame_length() as usize)
            };
            if frame_type == HDR_TYPE_DATA && frame_len > DATA_HEADER_LENGTH {
                data_payload = Some(buf[DATA_HEADER_LENGTH..frame_len].to_vec());
                break;
            }
        } else {
            break;
        }
    }
    assert_eq!(data_payload.as_deref(), Some(&b"hello-manual-mdc"[..]));

    // Remove destination -> destinations becomes empty again
    publisher
        .driver_proxy()
        .remove_destination(pub_reg, &dest_channel)
        .expect("remove destination");
    driver.do_work().expect("apply destination removal");
    assert!(
        driver.sender().network_publications()[0]
            .destinations()
            .is_empty()
    );
}

#[test]
fn supports_closing_and_reopening_publication_with_same_channel() {
    const STREAM_ID: i32 = 8020;
    let mut driver = MediaDriver::launch(small_udp_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);

    let uris = [
        "aeron:ipc",
        "aeron:udp?endpoint=127.0.0.1:20121",
        "aeron:udp?control-mode=manual",
    ];

    for uri in &uris {
        for _ in 0..5 {
            let pub_reg = publisher.add_publication(uri, STREAM_ID).unwrap();
            driver.do_work().expect("register publication");
            for (id, counter) in driver.conductor().counters() {
                publisher.register_counter(*id, counter.clone());
            }
            publisher.do_work().expect("publisher ready");

            assert!(publisher.publication(pub_reg).is_some());

            publisher.remove_publication(pub_reg).unwrap();
            driver.do_work().expect("remove publication");
            publisher.do_work().expect("publisher process removal");

            assert!(publisher.publication(pub_reg).is_none());
        }
    }
}

#[test]
fn subscription_receive_destination_lifecycle_and_isolation() {
    const STREAM_ID: i32 = 8021;
    let mut driver = MediaDriver::launch(small_udp_config()).expect("launch");
    let mut client1 = client_conductor(&driver, 1);
    let mut client2 = client_conductor(&driver, 2);

    let sub_reg1 = client1
        .add_subscription("aeron:udp?control-mode=manual", STREAM_ID)
        .unwrap();
    let sub_reg2 = client2
        .add_subscription("aeron:udp?control-mode=manual", STREAM_ID)
        .unwrap();
    driver.do_work().expect("register subscriptions");

    let dest_channel = "aeron:udp?endpoint=127.0.0.1:20551";

    // 1. Client 1 adds destination endpoint
    client1
        .driver_proxy()
        .add_receive_destination(sub_reg1, dest_channel)
        .unwrap();
    driver.do_work().expect("add receive destination for sub1");
    assert!(
        driver
            .conductor()
            .receive_destination_aliases()
            .contains_key(dest_channel)
    );

    // 2. Client 1 removes destination
    client1
        .driver_proxy()
        .remove_receive_destination(sub_reg1, dest_channel)
        .unwrap();
    driver
        .do_work()
        .expect("remove receive destination for sub1");
    assert!(
        !driver
            .conductor()
            .receive_destination_aliases()
            .contains_key(dest_channel)
    );

    // 3. Client 2 adds destination now
    client2
        .driver_proxy()
        .add_receive_destination(sub_reg2, dest_channel)
        .unwrap();
    driver.do_work().expect("add receive destination for sub2");
    assert!(
        driver
            .conductor()
            .receive_destination_aliases()
            .contains_key(dest_channel)
    );

    // 4. Remove subscription 2 -> automatically cleans up receive destination aliases
    client2.remove_subscription(sub_reg2).unwrap();
    driver.do_work().expect("remove sub2");
    assert!(
        !driver
            .conductor()
            .receive_destination_aliases()
            .contains_key(dest_channel)
    );
}

#[test]
fn fragmented_ipc_message_reassembles_into_one_callback() {
    const STREAM_ID: i32 = 6006;
    let mut driver = MediaDriver::launch(small_mtu_ipc_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);
    let mut subscriber = client_conductor(&driver, 2);

    let pub_reg = publisher.add_publication("aeron:ipc", STREAM_ID).unwrap();
    let sub_reg = subscriber.add_subscription("aeron:ipc", STREAM_ID).unwrap();
    driver.do_work().expect("driver duty cycle");

    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
        subscriber.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");
    subscriber.do_work().expect("subscriber services events");

    // Payload well beyond the 32-byte-per-frame MTU limit configured above, forcing offer() to
    // split it into BEGIN/middle/END fragments across separate term frames.
    let mut payload: Vec<u8> = (0..100u8).collect();
    let payload_len = payload.len();
    let src = AtomicBuffer::new(&mut payload);
    let publication = publisher.publication(pub_reg).expect("publication ready");
    let max_payload = publication.max_payload_length();
    assert!(
        max_payload < payload_len,
        "test payload must exceed a single fragment: max={max_payload}"
    );
    assert!(publication.offer(&src, 0, payload_len).unwrap() > 0);

    let mut assembler = FragmentAssembler::default();
    let mut deliveries = 0;
    let mut reassembled = Vec::new();
    let subscription = subscriber.subscription_mut(sub_reg).expect("subscription");
    let raw_fragments_seen = subscription.poll(
        |buffer, offset, length, header| {
            assembler.on_fragment(buffer, offset, length, header, |buf, off, len, _hdr| {
                deliveries += 1;
                let mut bytes = vec![0u8; len];
                buf.get_bytes(off, &mut bytes);
                reassembled = bytes;
            });
        },
        10,
    );

    assert!(
        raw_fragments_seen > 1,
        "message should arrive as multiple raw fragments: saw {raw_fragments_seen}"
    );
    assert_eq!(
        deliveries, 1,
        "assembler must deliver exactly one reassembled message"
    );
    assert_eq!(reassembled, (0..100u8).collect::<Vec<u8>>());
}

#[test]
fn repeated_publication_and_subscription_churn_keeps_driver_state_bounded() {
    const STREAM_ID: i32 = 7007;
    let mut driver = MediaDriver::launch(small_ipc_config()).expect("launch");
    let proxy = client_proxy(&driver, 1);

    // Publication images and their registrations must be fully reclaimed on removal, not just
    // stop being polled -- otherwise the driver's resource maps grow without bound across the
    // lifetime of a long-running process that repeatedly opens and closes streams.
    for _ in 0..5 {
        let pub_id = proxy.add_publication("aeron:ipc", STREAM_ID).unwrap();
        let sub_id = proxy.add_subscription("aeron:ipc", STREAM_ID).unwrap();
        driver.do_work().unwrap();
        assert_eq!(driver.conductor().publications().len(), 1);
        assert_eq!(driver.conductor().subscriptions().len(), 1);

        proxy.remove_publication(pub_id, false).unwrap();
        proxy.remove_subscription(sub_id).unwrap();
        driver.do_work().unwrap();
        assert_eq!(
            driver.conductor().publications().len(),
            0,
            "publication map must not accumulate stale entries across churn"
        );
        assert_eq!(
            driver.conductor().subscriptions().len(),
            0,
            "subscription map must not accumulate stale entries across churn"
        );
    }
}

#[test]
fn subscriber_reconnects_after_remove_and_readd_and_receives_new_data() {
    const STREAM_ID: i32 = 9009;
    let mut driver = MediaDriver::launch(small_ipc_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);
    let mut subscriber = client_conductor(&driver, 2);

    let pub_reg = publisher.add_publication("aeron:ipc", STREAM_ID).unwrap();
    let sub_reg = subscriber.add_subscription("aeron:ipc", STREAM_ID).unwrap();
    driver.do_work().expect("driver duty cycle");
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
        subscriber.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");
    subscriber.do_work().expect("subscriber services events");

    let offer = |publisher: &mut ClientConductor<'static>, text: &'static [u8]| {
        let mut payload = text.to_vec();
        let payload_len = payload.len();
        let src = AtomicBuffer::new(&mut payload);
        let publication = publisher.publication(pub_reg).expect("publication ready");
        assert!(publication.offer(&src, 0, payload_len).unwrap() > 0);
    };
    let poll_one = |subscriber: &mut ClientConductor<'static>, sub_reg: i64| -> Vec<u8> {
        let mut received = Vec::new();
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
        received
    };

    offer(&mut publisher, b"before-reconnect");
    assert_eq!(poll_one(&mut subscriber, sub_reg), b"before-reconnect");

    // Drop the subscription entirely (not just idle) and re-register it on the same channel and
    // stream while the publication stays live -- this is what a client reconnecting after a
    // restart or a dropped image looks like.
    subscriber.remove_subscription(sub_reg).unwrap();
    driver.do_work().expect("remove subscription");
    assert_eq!(
        driver.conductor().subscriptions().len(),
        0,
        "old subscription must be fully deregistered before reconnecting"
    );

    let reconnected_sub_reg = subscriber.add_subscription("aeron:ipc", STREAM_ID).unwrap();
    driver.do_work().expect("re-register subscription");
    for (id, counter) in driver.conductor().counters() {
        subscriber.register_counter(*id, counter.clone());
    }
    subscriber
        .do_work()
        .expect("resolve reconnected subscription");

    offer(&mut publisher, b"after-reconnect");
    assert_eq!(
        poll_one(&mut subscriber, reconnected_sub_reg),
        b"after-reconnect",
        "reconnected subscription must receive data published after it rejoined"
    );
}

#[test]
fn silent_client_is_reaped_after_the_liveness_timeout() {
    let mut driver = MediaDriver::launch(Configuration {
        ipc_term_buffer_length: TERM_MIN_LENGTH as usize,
        client_liveness_timeout_ns: 10_000_000, // 10ms, so the test does not sit idle for long.
        ..Configuration::default()
    })
    .expect("launch");
    let proxy = client_proxy(&driver, 1);

    proxy.add_publication("aeron:ipc", 1201).unwrap();
    proxy.add_subscription("aeron:ipc", 1201).unwrap();
    driver
        .do_work()
        .expect("register publication and subscription");
    assert_eq!(driver.conductor().publications().len(), 1);
    assert_eq!(driver.conductor().subscriptions().len(), 1);

    // Never send another keepalive or command; the driver must reap this client's registrations
    // once real time carries it past the configured liveness timeout, exactly as it would for a
    // process that crashed without a clean CLIENT_CLOSE.
    let mut reaped = false;
    for _ in 0..200 {
        driver.do_work().expect("sweep timed-out clients");
        if driver.conductor().publications().is_empty()
            && driver.conductor().subscriptions().is_empty()
        {
            reaped = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        reaped,
        "driver must free a silent client's publication and subscription after its liveness timeout"
    );
}

#[test]
fn one_client_publishes_and_subscribes_two_independent_streams_concurrently() {
    const STREAM_A: i32 = 1301;
    const STREAM_B: i32 = 1302;
    let mut driver = MediaDriver::launch(small_ipc_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);
    let mut subscriber = client_conductor(&driver, 2);

    let pub_a = publisher.add_publication("aeron:ipc", STREAM_A).unwrap();
    let pub_b = publisher.add_publication("aeron:ipc", STREAM_B).unwrap();
    let sub_a = subscriber.add_subscription("aeron:ipc", STREAM_A).unwrap();
    let sub_b = subscriber.add_subscription("aeron:ipc", STREAM_B).unwrap();
    driver.do_work().expect("register both streams");

    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
        subscriber.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");
    subscriber.do_work().expect("subscriber services events");

    let offer = |conductor: &mut ClientConductor<'static>, reg: i64, text: &'static [u8]| {
        let mut payload = text.to_vec();
        let payload_len = payload.len();
        let src = AtomicBuffer::new(&mut payload);
        let publication = conductor.publication(reg).expect("publication ready");
        assert!(publication.offer(&src, 0, payload_len).unwrap() > 0);
    };
    let poll_one = |conductor: &mut ClientConductor<'static>, reg: i64| -> Vec<u8> {
        let mut received = Vec::new();
        let subscription = conductor.subscription_mut(reg).expect("subscription");
        let fragments = subscription.poll(
            |buffer, offset, length, _header| {
                let mut bytes = vec![0u8; length];
                buffer.get_bytes(offset, &mut bytes);
                received = bytes;
            },
            10,
        );
        assert_eq!(fragments, 1, "exactly one fragment delivered");
        received
    };

    offer(&mut publisher, pub_a, b"stream-a-payload");
    offer(&mut publisher, pub_b, b"stream-b-payload");

    // Each stream's own subscription must see only its own data, not the other stream's, even
    // though both were registered by the same client and published back to back.
    assert_eq!(poll_one(&mut subscriber, sub_a), b"stream-a-payload");
    assert_eq!(poll_one(&mut subscriber, sub_b), b"stream-b-payload");
}

fn last_error_correlation(events: &mut BroadcastReceiver) -> Option<i64> {
    let mut correlation = None;
    while events
        .receive_next(|msg_type, buf, offset, _len| {
            if msg_type == ON_ERROR {
                correlation = Some(
                    ErrorResponseFlyweight::wrap(buf.clone(), offset)
                        .offending_command_correlation_id(),
                );
            }
        })
        .unwrap()
    {
        if correlation.is_some() {
            break;
        }
    }
    correlation
}

#[test]
fn publication_with_conflicting_explicit_session_id_is_rejected() {
    let mut driver = MediaDriver::launch(Configuration::default()).expect("launch");
    let proxy = client_proxy(&driver, 1);
    let mut events = BroadcastReceiver::new(driver.cnc().to_clients_buffer()).unwrap();

    // An explicit session publication already owns this channel+stream; a second registration
    // naming the same session id must be refused rather than colliding with the existing log.
    proxy
        .add_publication("aeron:ipc?session-id=4096", 7001)
        .unwrap();
    driver.do_work().expect("register first publication");
    assert_eq!(driver.conductor().publications().len(), 1);

    let conflicting_correlation = proxy
        .add_publication("aeron:ipc?session-id=4096", 7001)
        .unwrap();
    driver.do_work().expect("reject conflicting publication");

    assert_eq!(
        driver.conductor().publications().len(),
        1,
        "the conflicting registration must not be added"
    );
    assert_eq!(
        last_error_correlation(&mut events),
        Some(conflicting_correlation),
        "driver must ON_ERROR the conflicting registration's own correlation id"
    );
}

#[test]
fn subscription_with_conflicting_reliability_is_rejected() {
    let mut driver = MediaDriver::launch(Configuration::default()).expect("launch");
    let proxy = client_proxy(&driver, 1);
    let mut events = BroadcastReceiver::new(driver.cnc().to_clients_buffer()).unwrap();
    let channel = "aeron:udp?endpoint=127.0.0.1:0";

    proxy.add_subscription(channel, 7002).unwrap();
    driver.do_work().expect("register first subscription");
    assert_eq!(driver.conductor().subscriptions().len(), 1);

    // A second wildcard-session subscription on the same channel+stream that disagrees on
    // reliability would leave the driver unable to give the image a single consistent loss
    // policy, so it must be refused rather than silently coexisting.
    let conflicting_correlation = proxy
        .add_subscription(&format!("{channel}|reliable=false"), 7002)
        .unwrap();
    driver.do_work().expect("reject conflicting subscription");

    assert_eq!(
        driver.conductor().subscriptions().len(),
        1,
        "the conflicting registration must not be added"
    );
    assert_eq!(
        last_error_correlation(&mut events),
        Some(conflicting_correlation),
        "driver must ON_ERROR the conflicting registration's own correlation id"
    );
}

#[test]
fn session_specific_subscriptions_conflict_by_session_not_by_channel() {
    let mut driver = MediaDriver::launch(Configuration::default()).expect("launch");
    let proxy = client_proxy(&driver, 1);
    let mut events = BroadcastReceiver::new(driver.cnc().to_clients_buffer()).unwrap();
    let channel = "aeron:udp?endpoint=127.0.0.1:0";

    proxy
        .add_subscription(&format!("{channel}|session-id=1024"), 7003)
        .unwrap();
    driver.do_work().expect("register session 1024");
    assert_eq!(driver.conductor().subscriptions().len(), 1);

    // Same channel, same session id, disagreeing reliability: must be rejected -- two
    // subscriptions to one session's image cannot each demand a different loss policy.
    let conflicting_correlation = proxy
        .add_subscription(&format!("{channel}|session-id=1024|reliable=false"), 7003)
        .unwrap();
    driver.do_work().expect("reject same-session conflict");
    assert_eq!(driver.conductor().subscriptions().len(), 1);
    assert_eq!(
        last_error_correlation(&mut events),
        Some(conflicting_correlation),
        "driver must ON_ERROR a same-session reliability conflict"
    );

    // Same channel, a *different* explicit session id: conflict detection is keyed on session
    // identity, not just channel+stream, so this must be allowed to coexist.
    proxy
        .add_subscription(&format!("{channel}|session-id=2048|reliable=false"), 7003)
        .unwrap();
    driver.do_work().expect("register the other session");
    assert_eq!(
        driver.conductor().subscriptions().len(),
        2,
        "a different session id must not be treated as conflicting"
    );
}

#[test]
fn repeated_driver_errors_are_deduplicated_in_the_cnc_error_log() {
    let mut driver = MediaDriver::launch(Configuration::default()).expect("launch");
    let proxy = client_proxy(&driver, 1);
    let channel = "aeron:udp?endpoint=127.0.0.1:0";
    const STREAM_ID: i32 = 7004;

    proxy.add_subscription(channel, STREAM_ID).unwrap();
    driver.do_work().expect("register first subscription");

    // Trigger the exact same conflict twice: the driver's own error log must summarise these as
    // one distinct record with observation_count == 2, not two separate log entries, matching
    // vendor DistinctErrorLog semantics.
    proxy
        .add_subscription(&format!("{channel}|reliable=false"), STREAM_ID)
        .unwrap();
    driver
        .do_work()
        .expect("reject first conflicting registration");
    proxy
        .add_subscription(&format!("{channel}|reliable=false"), STREAM_ID)
        .unwrap();
    driver
        .do_work()
        .expect("reject second conflicting registration");

    let error_log_buffer = driver.conductor().cnc().error_log_buffer();
    let mut records: Vec<(i32, i64, i64, Vec<u8>)> = Vec::new();
    let entries = aeron_client::error_log::read_error_log(
        &error_log_buffer,
        0,
        |count, first, last, desc| {
            records.push((count, first, last, desc.to_vec()));
        },
    );

    assert_eq!(
        entries, 1,
        "the two identical conflicts must collapse into a single distinct-error record"
    );
    assert_eq!(
        records[0].0, 2,
        "observation_count must reflect both occurrences"
    );
    assert!(
        records[0].1 <= records[0].2,
        "first_observation_timestamp <= last"
    );
    let description = String::from_utf8(records[0].3.clone()).unwrap();
    assert!(
        description.contains("conflicting reliable and unreliable subscriptions"),
        "unexpected description: {description}"
    );
}

#[test]
fn min_flow_control_gates_the_sender_on_the_slowest_receiver() {
    const STREAM_ID: i32 = 9002;
    let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
    receiver
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}|fc=min",
        receiver.local_addr().unwrap().port()
    );

    let mut driver = MediaDriver::launch(small_udp_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);
    let pub_reg = publisher.add_publication(&channel, STREAM_ID).unwrap();
    driver
        .do_work()
        .expect("register publication and send setup");
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");
    let _ = publisher.publication(pub_reg).expect("publication ready");

    // Capture the real SETUP so both fake receivers build correctly-scoped Status Messages.
    let mut buf = [0u8; 2048];
    let (n, source) = receiver.recv_from(&mut buf).expect("receive setup");
    let (session_id, initial_term_id) = {
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut buf[..n]));
        (setup.session_id(), setup.initial_term_id())
    };

    let send_sm = |receiver_id: i64, window: i32| {
        let mut bytes = [0u8; STATUS_MESSAGE_HEADER_LENGTH];
        let sm = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        sm.header()
            .set_frame_length(STATUS_MESSAGE_HEADER_LENGTH as i32)
            .set_version(aeron_client::protocol::CURRENT_VERSION)
            .set_flags(0)
            .set_header_type(aeron_client::protocol::HDR_TYPE_SM);
        sm.set_session_id(session_id)
            .set_stream_id(STREAM_ID)
            .set_consumption_term_id(initial_term_id)
            .set_consumption_term_offset(0)
            .set_receiver_window_length(window)
            .set_receiver_id(receiver_id);
        receiver.send_to(&bytes, source).unwrap();
    };

    // Two independent receivers reporting from the same position but very different windows: a
    // slow one that has almost no room, and a fast one with room for a whole term. The slow
    // receiver must be admitted first -- MinMulticastFlowControl only admits a newly-seen
    // receiver whose window isn't wildly behind the current tracked minimum (an anti-flap
    // safeguard against one rogue receiver tanking the group), so reporting the fast receiver
    // first would raise that admission threshold and cause the slow one to be rejected instead
    // of tracked.
    send_sm(2, 1024);
    send_sm(1, TERM_MIN_LENGTH);

    for _ in 0..20 {
        driver.do_work().expect("process both status messages");
        std::thread::sleep(Duration::from_millis(1));
    }

    let limit = driver.sender().network_publications()[0].receiver_limit();
    assert_eq!(
        limit, 1024,
        "fc=min must cap the sender at the slowest receiver's window, not the fastest"
    );
}

#[test]
fn status_message_position_bounds_protect_sender_limit_under_live_traffic() {
    const STREAM_ID: i32 = 9003;
    let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
    receiver
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}",
        receiver.local_addr().unwrap().port()
    );

    let mut driver = MediaDriver::launch(small_udp_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);
    let pub_reg = publisher.add_publication(&channel, STREAM_ID).unwrap();
    driver
        .do_work()
        .expect("register publication and send setup");
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");
    let _ = publisher.publication(pub_reg).expect("publication ready");

    // Capture the initial SETUP frame from the sender
    let mut buf = [0u8; 2048];
    let (n, source) = receiver.recv_from(&mut buf).expect("receive setup");
    let (session_id, initial_term_id) = {
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut buf[..n]));
        (setup.session_id(), setup.initial_term_id())
    };

    let send_sm = |term_id: i32, term_offset: i32, window: i32, flags: u8| {
        let mut bytes = [0u8; STATUS_MESSAGE_HEADER_LENGTH];
        let sm = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        sm.header()
            .set_frame_length(STATUS_MESSAGE_HEADER_LENGTH as i32)
            .set_version(aeron_client::protocol::CURRENT_VERSION)
            .set_flags(flags)
            .set_header_type(aeron_client::protocol::HDR_TYPE_SM);
        sm.set_session_id(session_id)
            .set_stream_id(STREAM_ID)
            .set_consumption_term_id(term_id)
            .set_consumption_term_offset(term_offset)
            .set_receiver_window_length(window)
            .set_receiver_id(1);
        receiver.send_to(&bytes, source).unwrap();
    };

    // 1. Send out-of-bounds SM: 5 terms ahead
    send_sm(initial_term_id + 5, 0, 4096, 0);
    for _ in 0..10 {
        driver.do_work().expect("drain control frames");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        driver.sender().network_publications()[0].receiver_limit(),
        0,
        "out-of-bounds SM must be rejected and cannot move receiver limit"
    );

    // 2. Send legitimate in-bounds SM: initial term, offset 0, window 4096
    send_sm(initial_term_id, 0, 4096, 0);
    for _ in 0..10 {
        driver.do_work().expect("process valid SM");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        driver.sender().network_publications()[0].receiver_limit(),
        4096,
        "valid in-bounds SM advances receiver limit"
    );

    // 3. Send out-of-bounds SM with SEND_SETUP_FLAG (eliciting setup):
    send_sm(
        initial_term_id + 99,
        0,
        4096,
        aeron_client::protocol::SEND_SETUP_FLAG,
    );
    for _ in 0..10 {
        driver.do_work().expect("process setup-eliciting SM");
        std::thread::sleep(Duration::from_millis(1));
    }
    // Receiver limit should remain 4096 (unaffected by out-of-bounds setup request)
    assert_eq!(
        driver.sender().network_publications()[0].receiver_limit(),
        4096
    );
    // Driver should re-transmit SETUP frame
    let (setup_len, _) = receiver
        .recv_from(&mut buf)
        .expect("receive re-sent setup frame");
    let re_setup = SetupFlyweight::new(AtomicBuffer::new(&mut buf[..setup_len]));
    assert_eq!(re_setup.session_id(), session_id);
    assert_eq!(re_setup.initial_term_id(), initial_term_id);
}

#[test]
fn min_flow_control_expires_timed_out_slow_receiver_and_advances_sender_limit() {
    const STREAM_ID: i32 = 9004;
    let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
    receiver
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    // Configure fc=min with a 50ms receiver timeout so timeout can be triggered reliably in test
    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}|fc=min,t:50ms",
        receiver.local_addr().unwrap().port()
    );

    let mut driver = MediaDriver::launch(small_udp_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);
    let pub_reg = publisher.add_publication(&channel, STREAM_ID).unwrap();
    driver
        .do_work()
        .expect("register publication and send setup");
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");
    let pub_client = publisher.publication(pub_reg).expect("publication ready");

    // Initially, no status messages have been received: publication is not connected
    assert!(!pub_client.is_connected());

    // Capture the initial SETUP frame
    let mut buf = [0u8; 2048];
    let (n, source) = receiver.recv_from(&mut buf).expect("receive setup");
    let (session_id, initial_term_id) = {
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut buf[..n]));
        (setup.session_id(), setup.initial_term_id())
    };

    let send_sm = |receiver_id: i64, window: i32| {
        let mut bytes = [0u8; STATUS_MESSAGE_HEADER_LENGTH];
        let sm = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        sm.header()
            .set_frame_length(STATUS_MESSAGE_HEADER_LENGTH as i32)
            .set_version(aeron_client::protocol::CURRENT_VERSION)
            .set_flags(0)
            .set_header_type(aeron_client::protocol::HDR_TYPE_SM);
        sm.set_session_id(session_id)
            .set_stream_id(STREAM_ID)
            .set_consumption_term_id(initial_term_id)
            .set_consumption_term_offset(0)
            .set_receiver_window_length(window)
            .set_receiver_id(receiver_id);
        receiver.send_to(&bytes, source).unwrap();
    };

    // Report slow receiver (window = 1024) first, then fast receiver (window = TERM_MIN_LENGTH / 2)
    let fast_window = TERM_MIN_LENGTH / 2;
    send_sm(2, 1024);
    send_sm(1, fast_window);

    for _ in 0..20 {
        driver.do_work().expect("process both status messages");
        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        driver.sender().network_publications()[0].receiver_limit(),
        1024,
        "fc=min must initially cap the sender at the slowest receiver's window"
    );
    assert!(
        pub_client.is_connected(),
        "publication must be marked connected after receiving SMs"
    );

    // Sleep past the 50ms receiver timeout (70ms)
    std::thread::sleep(Duration::from_millis(70));

    // Receiver 1 sends a heartbeat SM to keep itself active
    send_sm(1, fast_window);

    // Run driver duty cycles so on_idle detects receiver 2 has timed out
    for _ in 0..20 {
        driver.do_work().expect("driver work");
        std::thread::sleep(Duration::from_millis(1));
    }

    let actual_limit = driver.sender().network_publications()[0].receiver_limit();
    assert_eq!(
        actual_limit, fast_window as i64,
        "after receiver 2 times out, sender limit must unblock to receiver 1's window"
    );
    assert!(
        pub_client.is_connected(),
        "publication remains connected via active receiver 1"
    );
}

#[test]
fn network_publication_connection_status_tracks_receiver_liveness_and_required_group_size() {
    const STREAM_ID: i32 = 9005;
    let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
    receiver
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    // Requires at least 1 receiver and a 40ms timeout
    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}|fc=min,g:/1,t:40ms",
        receiver.local_addr().unwrap().port()
    );

    let mut driver = MediaDriver::launch(small_udp_config()).expect("launch");
    let mut publisher = client_conductor(&driver, 1);
    let pub_reg = publisher.add_publication(&channel, STREAM_ID).unwrap();
    driver
        .do_work()
        .expect("register publication and send setup");
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().expect("publisher services events");
    let pub_client = publisher.publication(pub_reg).expect("publication ready");

    // Before any status message: not connected
    assert!(!pub_client.is_connected());

    let mut buf = [0u8; 2048];
    let (n, source) = receiver.recv_from(&mut buf).expect("receive setup");
    let (session_id, initial_term_id) = {
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut buf[..n]));
        (setup.session_id(), setup.initial_term_id())
    };

    let send_sm = |flags: u8| {
        let mut bytes = [0u8; STATUS_MESSAGE_HEADER_LENGTH];
        let sm = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        sm.header()
            .set_frame_length(STATUS_MESSAGE_HEADER_LENGTH as i32)
            .set_version(aeron_client::protocol::CURRENT_VERSION)
            .set_flags(flags)
            .set_header_type(aeron_client::protocol::HDR_TYPE_SM);
        sm.set_session_id(session_id)
            .set_stream_id(STREAM_ID)
            .set_consumption_term_id(initial_term_id)
            .set_consumption_term_offset(0)
            .set_receiver_window_length(4096)
            .set_receiver_id(101);
        receiver.send_to(&bytes, source).unwrap();
    };

    // 1. Send active SM: connected becomes true
    send_sm(0);
    for _ in 0..15 {
        driver.do_work().expect("driver work");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(pub_client.is_connected(), "connected once SM is processed");

    // 2. Sleep past timeout (60ms > 40ms) with no SMs: receiver times out, connected becomes false
    std::thread::sleep(Duration::from_millis(60));
    for _ in 0..15 {
        driver.do_work().expect("driver work");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        !pub_client.is_connected(),
        "disconnected once receiver times out"
    );

    // 3. Receiver sends new SM: publication reconnects
    send_sm(0);
    for _ in 0..15 {
        driver.do_work().expect("driver work");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(pub_client.is_connected(), "reconnected on new SM");

    // 4. Receiver sends EOS flag: publication immediately disconnects without waiting for timeout
    send_sm(aeron_client::protocol::END_OF_STREAM_FLAG);
    for _ in 0..15 {
        driver.do_work().expect("driver work");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        !pub_client.is_connected(),
        "disconnected immediately on EOS"
    );
}

#[test]
fn image_should_complete_drain_state_even_if_there_are_non_active_untethered_subscriptions() {
    let mut config = small_ipc_config();
    config.untethered_window_limit_timeout_ns = 20_000_000; // 20ms
    config.untethered_linger_timeout_ns = 50_000_000; // 50ms
    config.untethered_resting_timeout_ns = 0;
    let mut driver = MediaDriver::launch(config).expect("launch");

    const STREAM_ID: i32 = 1001;
    let mut publisher = client_conductor(&driver, 1);
    let mut subscriber = client_conductor(&driver, 2);

    let pub_channel = "aeron:ipc?term-length=64k|eos=true|untethered-window-limit-timeout=20ms|untethered-linger-timeout=50ms|untethered-resting-timeout=0s";
    let sub_channel = "aeron:ipc?rejoin=true|tether=true";
    let untethered_channel = "aeron:ipc?rejoin=true|tether=false";

    let pub_reg = publisher.add_publication(pub_channel, STREAM_ID).unwrap();
    let tethered_sub_reg = subscriber.add_subscription(sub_channel, STREAM_ID).unwrap();
    let untethered_sub_reg = subscriber
        .add_subscription(untethered_channel, STREAM_ID)
        .unwrap();

    driver.do_work().unwrap();

    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
        subscriber.register_counter(*id, counter.clone());
    }

    publisher.do_work().unwrap();
    subscriber.do_work().unwrap();

    let tethered_sub = subscriber.subscription_mut(tethered_sub_reg).unwrap();
    assert_eq!(tethered_sub.image_count(), 1);

    let untethered_sub = subscriber.subscription_mut(untethered_sub_reg).unwrap();
    assert_eq!(untethered_sub.image_count(), 1);
    let untethered_image = untethered_sub.image_at_index(0).unwrap();
    let untethered_sub_pos_counter_id = untethered_image.subscriber_position_id();

    // Verify counter is allocated in CnC
    let meta_offset =
        untethered_sub_pos_counter_id as usize * aeron_client::cnc_file::COUNTER_METADATA_LENGTH;
    assert_eq!(
        driver.cnc().counters_metadata_buffer().get_i32(meta_offset),
        aeron_client::cnc_file::COUNTER_RECORD_ALLOCATED
    );

    // Offer messages up to 32k. Tethered subscriber polls; untethered subscriber does NOT poll.
    let mut payload = vec![0u8; 1024 - aeron_client::protocol::DATA_HEADER_LENGTH];
    let payload_len = payload.len();
    let src = AtomicBuffer::new(&mut payload);
    let publication = publisher.publication(pub_reg).unwrap();

    let target_window_length = 32 * 1024;
    while publication.position() < target_window_length {
        let pos = publication.offer(&src, 0, payload_len).unwrap();
        if pos > 0 {
            let tethered_sub = subscriber.subscription_mut(tethered_sub_reg).unwrap();
            tethered_sub.poll(|_, _, _, _| {}, 10);
        }
    }

    // Advance driver clock past untethered window limit timeout (20ms).
    // Conductor marks untethered subscriber as Linger and sends unavailable image event.
    let base_time_ns = 1_000_000_000i64;
    driver.do_work_at(base_time_ns).unwrap();
    driver.do_work_at(base_time_ns + 30_000_000).unwrap(); // +30ms > 20ms
    subscriber.do_work().unwrap();

    // Tethered subscriber consumes everything up to publication position.
    let eos_pos = publication.position();
    let tethered_sub = subscriber.subscription_mut(tethered_sub_reg).unwrap();
    while tethered_sub.image_at_index(0).unwrap().position() < eos_pos {
        tethered_sub.poll(|_, _, _, _| {}, 10);
    }

    // Close publication -> publication enters DRAINING.
    publisher.remove_publication(pub_reg).unwrap();
    driver.do_work_at(base_time_ns + 35_000_000).unwrap();

    // Advance time past untethered linger timeout (50ms).
    // In DRAINING, allowRejoin is false: untethered subscriber in Linger is closed.
    driver.do_work_at(base_time_ns + 90_000_000).unwrap(); // +90ms > 30ms + 50ms

    // Assert that the untethered subscriber position counter is closed!
    assert_ne!(
        driver.cnc().counters_metadata_buffer().get_i32(meta_offset),
        aeron_client::cnc_file::COUNTER_RECORD_ALLOCATED,
        "untethered subscriber position counter must be closed when image completes drain state"
    );
    assert!(
        !driver
            .conductor()
            .counters()
            .contains_key(&untethered_sub_pos_counter_id),
        "untethered subscriber position counter must be removed from conductor counters"
    );
}

#[test]
fn buffer_claim_should_receive_published_message_with_interleaved_abort_ipc() {
    let mut driver = MediaDriver::launch(small_ipc_config()).expect("launch");
    const STREAM_ID: i32 = 4001;
    let mut publisher = client_conductor(&driver, 1);
    let mut subscriber = client_conductor(&driver, 2);

    let pub_reg = publisher.add_publication("aeron:ipc", STREAM_ID).unwrap();
    let sub_reg = subscriber.add_subscription("aeron:ipc", STREAM_ID).unwrap();

    driver.do_work().unwrap();
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
        subscriber.register_counter(*id, counter.clone());
    }
    publisher.do_work().unwrap();
    subscriber.do_work().unwrap();

    let publication = publisher.publication(pub_reg).unwrap();

    // 1. Publish message 1
    let mut msg1 = *b"msg-one";
    let len1 = msg1.len();
    let src1 = AtomicBuffer::new(&mut msg1);
    assert!(publication.offer(&src1, 0, len1).unwrap() > 0);

    // 2. Try claim message 2
    let (pos, claim) = publication.try_claim(200).unwrap();
    assert!(pos > 0);
    let claim = claim.expect("claim should succeed");

    // 3. Publish message 3
    let mut msg3 = *b"msg-three";
    let len3 = msg3.len();
    let src3 = AtomicBuffer::new(&mut msg3);
    assert!(publication.offer(&src3, 0, len3).unwrap() > 0);

    // 4. Abort message 2
    claim.abort();

    // 5. Poll subscriber
    let subscription = subscriber.subscription_mut(sub_reg).unwrap();
    let mut received = Vec::new();
    let mut fragments = 0;
    for _ in 0..10 {
        fragments += subscription.poll(
            |buf, offset, len, _hdr| {
                let mut data = vec![0u8; len];
                buf.get_bytes(offset, &mut data);
                received.push(data);
            },
            10,
        );
        if fragments >= 2 {
            break;
        }
    }
    assert_eq!(fragments, 2);
    assert_eq!(received.len(), 2);
    assert_eq!(received[0], b"msg-one");
    assert_eq!(received[1], b"msg-three");
}

#[test]
fn buffer_claim_should_receive_published_message_with_interleaved_abort_udp() {
    let reservation = UdpSocket::bind("127.0.0.1:0").unwrap();
    let endpoint = reservation.local_addr().unwrap();
    drop(reservation);
    let channel = format!("aeron:udp?endpoint={endpoint}");

    let mut publisher_driver = MediaDriver::launch(small_udp_config()).expect("publisher launch");
    let mut subscriber_driver = MediaDriver::launch(small_udp_config()).expect("subscriber launch");
    const STREAM_ID: i32 = 4002;

    let mut subscriber = client_conductor(&subscriber_driver, 2);
    let sub_reg = subscriber.add_subscription(&channel, STREAM_ID).unwrap();
    subscriber_driver.do_work().unwrap();
    subscriber.do_work().unwrap();

    let mut publisher = client_conductor(&publisher_driver, 1);
    let pub_reg = publisher.add_publication(&channel, STREAM_ID).unwrap();
    publisher_driver.do_work().unwrap();
    for (id, counter) in publisher_driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().unwrap();

    subscriber_driver.do_work().unwrap();
    for (id, counter) in subscriber_driver.conductor().counters() {
        subscriber.register_counter(*id, counter.clone());
    }
    subscriber.do_work().unwrap();

    for _ in 0..20 {
        publisher_driver.do_work().unwrap();
        if publisher_driver.sender().network_publications()[0].receiver_limit() > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    let publication = publisher.publication(pub_reg).unwrap();

    // 1. Offer message 1
    let mut msg1 = *b"udp-msg-one";
    let len1 = msg1.len();
    let src1 = AtomicBuffer::new(&mut msg1);
    assert!(publication.offer(&src1, 0, len1).unwrap() > 0);

    // 2. Try claim message 2
    let (pos, claim) = publication.try_claim(200).unwrap();
    assert!(pos > 0);
    let claim = claim.expect("claim should succeed");

    // 3. Offer message 3
    let mut msg3 = *b"udp-msg-three";
    let len3 = msg3.len();
    let src3 = AtomicBuffer::new(&mut msg3);
    assert!(publication.offer(&src3, 0, len3).unwrap() > 0);

    // 4. Abort claim
    claim.abort();

    let mut received = Vec::new();
    for _ in 0..100 {
        publisher_driver.do_work().unwrap();
        subscriber_driver.do_work().unwrap();
        for (id, counter) in subscriber_driver.conductor().counters() {
            subscriber.register_counter(*id, counter.clone());
        }
        subscriber.do_work().unwrap();
        if let Some(sub) = subscriber.subscription_mut(sub_reg) {
            sub.poll(
                |buf, offset, len, _hdr| {
                    let mut data = vec![0u8; len];
                    buf.get_bytes(offset, &mut data);
                    received.push(data);
                },
                10,
            );
            if received.len() >= 2 {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(received.len(), 2);
    assert_eq!(received[0], b"udp-msg-one");
    assert_eq!(received[1], b"udp-msg-three");
}

#[test]
fn buffer_claim_should_transfer_reserved_value_ipc() {
    let mut driver = MediaDriver::launch(small_ipc_config()).expect("launch");
    const STREAM_ID: i32 = 4003;
    let mut publisher = client_conductor(&driver, 1);
    let mut subscriber = client_conductor(&driver, 2);

    let pub_reg = publisher.add_publication("aeron:ipc", STREAM_ID).unwrap();
    let sub_reg = subscriber.add_subscription("aeron:ipc", STREAM_ID).unwrap();

    driver.do_work().unwrap();
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
        subscriber.register_counter(*id, counter.clone());
    }
    publisher.do_work().unwrap();
    subscriber.do_work().unwrap();

    let publication = publisher.publication(pub_reg).unwrap();
    let (pos, claim) = publication.try_claim(64).unwrap();
    assert!(pos > 0);
    let claim = claim.expect("claim succeeds");
    let reserved_val = 0x1234_5678_9ABC_DEF0i64;
    claim.set_reserved_value(reserved_val);
    let mut payload = [42u8; 64];
    let src = AtomicBuffer::new(&mut payload);
    claim.put_bytes(&src, 0, 64);
    claim.commit();

    let subscription = subscriber.subscription_mut(sub_reg).unwrap();
    let mut seen_reserved = 0i64;
    let mut seen_length = 0usize;
    let read = subscription.poll(
        |_buf, _offset, len, hdr| {
            seen_length = len;
            seen_reserved = hdr.reserved_value();
        },
        10,
    );
    assert_eq!(read, 1);
    assert_eq!(seen_length, 64);
    assert_eq!(seen_reserved, reserved_val);
}

#[test]
fn buffer_claim_should_transfer_reserved_value_udp() {
    let reservation = UdpSocket::bind("127.0.0.1:0").unwrap();
    let endpoint = reservation.local_addr().unwrap();
    drop(reservation);
    let channel = format!("aeron:udp?endpoint={endpoint}");

    let mut publisher_driver = MediaDriver::launch(small_udp_config()).expect("publisher launch");
    let mut subscriber_driver = MediaDriver::launch(small_udp_config()).expect("subscriber launch");
    const STREAM_ID: i32 = 4004;

    let mut subscriber = client_conductor(&subscriber_driver, 2);
    let sub_reg = subscriber.add_subscription(&channel, STREAM_ID).unwrap();
    subscriber_driver.do_work().unwrap();
    subscriber.do_work().unwrap();

    let mut publisher = client_conductor(&publisher_driver, 1);
    let pub_reg = publisher.add_publication(&channel, STREAM_ID).unwrap();
    publisher_driver.do_work().unwrap();
    for (id, counter) in publisher_driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
    }
    publisher.do_work().unwrap();

    subscriber_driver.do_work().unwrap();
    for (id, counter) in subscriber_driver.conductor().counters() {
        subscriber.register_counter(*id, counter.clone());
    }
    subscriber.do_work().unwrap();

    for _ in 0..20 {
        publisher_driver.do_work().unwrap();
        if publisher_driver.sender().network_publications()[0].receiver_limit() > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    let publication = publisher.publication(pub_reg).unwrap();
    let (pos, claim) = publication.try_claim(64).unwrap();
    assert!(pos > 0);
    let claim = claim.expect("claim succeeds");
    let reserved_val = 0x0FED_CBA9_8765_4321i64;
    claim.set_reserved_value(reserved_val);
    let mut payload = [77u8; 64];
    let src = AtomicBuffer::new(&mut payload);
    claim.put_bytes(&src, 0, 64);
    claim.commit();

    let mut seen_reserved = 0i64;
    let mut seen_length = 0usize;
    for _ in 0..100 {
        publisher_driver.do_work().unwrap();
        subscriber_driver.do_work().unwrap();
        for (id, counter) in subscriber_driver.conductor().counters() {
            subscriber.register_counter(*id, counter.clone());
        }
        subscriber.do_work().unwrap();
        if let Some(sub) = subscriber.subscription_mut(sub_reg) {
            let read = sub.poll(
                |_buf, _offset, len, hdr| {
                    seen_length = len;
                    seen_reserved = hdr.reserved_value();
                },
                10,
            );
            if read > 0 {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(seen_length, 64);
    assert_eq!(seen_reserved, reserved_val);
}

#[test]
fn controlled_message_poll_with_abort_break_and_continue() {
    let mut driver = MediaDriver::launch(small_ipc_config()).expect("launch");
    const STREAM_ID: i32 = 4005;
    let mut publisher = client_conductor(&driver, 1);
    let mut subscriber = client_conductor(&driver, 2);

    let pub_reg = publisher.add_publication("aeron:ipc", STREAM_ID).unwrap();
    let sub_reg = subscriber.add_subscription("aeron:ipc", STREAM_ID).unwrap();

    driver.do_work().unwrap();
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
        subscriber.register_counter(*id, counter.clone());
    }
    publisher.do_work().unwrap();
    subscriber.do_work().unwrap();

    let publication = publisher.publication(pub_reg).unwrap();
    let msgs: [&[u8]; 4] = [b"msg-0", b"msg-1", b"msg-2", b"msg-3"];
    for msg in &msgs {
        let mut bytes = msg.to_vec();
        let src = AtomicBuffer::new(&mut bytes);
        assert!(publication.offer(&src, 0, msg.len()).unwrap() > 0);
    }

    let subscription = subscriber.subscription_mut(sub_reg).unwrap();
    let mut collected = Vec::new();
    let mut callback_count = 0;

    let mut total_read = 0;
    while total_read < 4 {
        let read = subscription.controlled_poll(
            |buf, offset, len, _hdr| {
                callback_count += 1;
                let action = match callback_count {
                    3 => ControlledPollAction::Abort,
                    5 => ControlledPollAction::Break,
                    _ => ControlledPollAction::Continue,
                };
                if action != ControlledPollAction::Abort {
                    let mut data = vec![0u8; len];
                    buf.get_bytes(offset, &mut data);
                    collected.push(data);
                }
                action
            },
            10,
        );
        total_read += read;
    }

    assert_eq!(collected.len(), 4);
    assert_eq!(collected[0], b"msg-0");
    assert_eq!(collected[1], b"msg-1");
    assert_eq!(collected[2], b"msg-2");
    assert_eq!(collected[3], b"msg-3");
    // Message 2 was aborted on callback 3 and retried on callback 4; callback 5 was msg 3 returning Break
    assert_eq!(callback_count, 5);
}

#[test]
fn controlled_fragment_assembler_with_interleaved_abort_and_retry() {
    let mut driver = MediaDriver::launch(small_mtu_ipc_config()).expect("launch");
    const STREAM_ID: i32 = 4006;
    let mut publisher = client_conductor(&driver, 1);
    let mut subscriber = client_conductor(&driver, 2);

    let pub_reg = publisher.add_publication("aeron:ipc", STREAM_ID).unwrap();
    let sub_reg = subscriber.add_subscription("aeron:ipc", STREAM_ID).unwrap();

    driver.do_work().unwrap();
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
        subscriber.register_counter(*id, counter.clone());
    }
    publisher.do_work().unwrap();
    subscriber.do_work().unwrap();

    let publication = publisher.publication(pub_reg).unwrap();

    // Message 1: 16 bytes (unfragmented, <= 64 MTU - header)
    let msg1 = vec![1u8; 16];
    let mut msg1_bytes = msg1.clone();
    let src1 = AtomicBuffer::new(&mut msg1_bytes);
    assert!(publication.offer(&src1, 0, 16).unwrap() > 0);

    // Message 2: 128 bytes (fragmented across multiple frames)
    let msg2 = vec![2u8; 128];
    let mut msg2_bytes = msg2.clone();
    let src2 = AtomicBuffer::new(&mut msg2_bytes);
    assert!(publication.offer(&src2, 0, 128).unwrap() > 0);

    // Message 3: 16 bytes (unfragmented)
    let msg3 = vec![3u8; 16];
    let mut msg3_bytes = msg3.clone();
    let src3 = AtomicBuffer::new(&mut msg3_bytes);
    assert!(publication.offer(&src3, 0, 16).unwrap() > 0);

    let mut assembler = ControlledFragmentAssembler::default();
    let subscription = subscriber.subscription_mut(sub_reg).unwrap();

    let mut delivered = Vec::new();
    let mut attempts = 0;

    // Drain fragments through ControlledFragmentAssembler
    while delivered.len() < 3 {
        subscription.controlled_poll(
            |buf, offset, len, hdr| {
                assembler.on_fragment(buf, offset, len, hdr, |abuf, aoff, alen, _ahdr| {
                    attempts += 1;
                    // Abort on first delivery of msg 1 and first delivery of msg 2
                    if attempts == 1 || attempts == 3 {
                        ControlledPollAction::Abort
                    } else {
                        let mut data = vec![0u8; alen];
                        abuf.get_bytes(aoff, &mut data);
                        delivered.push(data);
                        ControlledPollAction::Continue
                    }
                })
            },
            10,
        );
    }

    assert_eq!(delivered.len(), 3);
    assert_eq!(delivered[0], msg1);
    assert_eq!(delivered[1], msg2);
    assert_eq!(delivered[2], msg3);
    assert_eq!(attempts, 5); // 1 abort + 1 ok for msg1, 1 abort + 1 ok for msg2, 1 ok for msg3
}

#[test]
fn concurrent_publication_term_rotation_race_with_offer_and_try_claim() {
    let mut driver = MediaDriver::launch(small_ipc_config()).expect("launch");
    const STREAM_ID: i32 = 4007;
    let mut publisher = client_conductor(&driver, 1);
    let mut subscriber = client_conductor(&driver, 2);

    let pub_reg = publisher.add_publication("aeron:ipc", STREAM_ID).unwrap();
    let sub_reg = subscriber.add_subscription("aeron:ipc", STREAM_ID).unwrap();

    driver.do_work().unwrap();
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
        subscriber.register_counter(*id, counter.clone());
    }
    publisher.do_work().unwrap();
    subscriber.do_work().unwrap();

    let publication = publisher.publication(pub_reg).unwrap();

    const NUM_THREADS: usize = 3;
    const MSGS_PER_THREAD: usize = 150;
    const MSG_SIZE: usize = 512;
    let total_expected = NUM_THREADS * MSGS_PER_THREAD;

    std::thread::scope(|s| {
        // Consumer thread: services driver duty cycle and drains subscriber
        let consumer_handle = s.spawn(move || {
            let mut counts_by_thread = [0usize; NUM_THREADS];
            let mut total_received = 0;
            while total_received < total_expected {
                driver.do_work().unwrap();
                let subscription = subscriber.subscription_mut(sub_reg).unwrap();
                let read = subscription.poll(
                    |buf, offset, len, _hdr| {
                        assert_eq!(len, MSG_SIZE);
                        let t_idx = buf.get_u8(offset) as usize;
                        let seq = buf.get_i32(offset + 1) as usize;
                        assert_eq!(seq, counts_by_thread[t_idx]);
                        counts_by_thread[t_idx] += 1;
                        total_received += 1;
                    },
                    50,
                );
                if read == 0 {
                    std::thread::yield_now();
                }
            }
            counts_by_thread
        });

        // Publisher threads concurrently writing messages that rotate terms
        for t_idx in 0..NUM_THREADS {
            s.spawn(move || {
                for i in 0..MSGS_PER_THREAD {
                    let mut payload = vec![0u8; MSG_SIZE];
                    payload[0] = t_idx as u8;
                    payload[1..5].copy_from_slice(&(i as u32).to_le_bytes());

                    if t_idx == 0 {
                        // Use try_claim
                        let mut published = false;
                        while !published {
                            match publication.try_claim(MSG_SIZE) {
                                Ok((pos, Some(claim))) if pos > 0 => {
                                    let src = AtomicBuffer::new(&mut payload);
                                    claim.put_bytes(&src, 0, MSG_SIZE);
                                    claim.commit();
                                    published = true;
                                }
                                _ => std::thread::yield_now(),
                            }
                        }
                    } else {
                        // Use offer
                        let mut published = false;
                        while !published {
                            let src = AtomicBuffer::new(&mut payload);
                            match publication.offer(&src, 0, MSG_SIZE) {
                                Ok(pos) if pos > 0 => published = true,
                                _ => std::thread::yield_now(),
                            }
                        }
                    }
                }
            });
        }

        let counts_by_thread = consumer_handle.join().unwrap();
        for count in counts_by_thread {
            assert_eq!(count, MSGS_PER_THREAD);
        }
    });
}

#[test]
fn exclusive_publication_try_claim_commit_and_abort() {
    let mut driver = MediaDriver::launch(small_ipc_config()).expect("launch");
    const STREAM_ID: i32 = 4008;
    let mut publisher = client_conductor(&driver, 1);
    let mut subscriber = client_conductor(&driver, 2);

    let pub_reg = publisher
        .add_exclusive_publication("aeron:ipc", STREAM_ID)
        .unwrap();
    let sub_reg = subscriber.add_subscription("aeron:ipc", STREAM_ID).unwrap();

    driver.do_work().unwrap();
    for (id, counter) in driver.conductor().counters() {
        publisher.register_counter(*id, counter.clone());
        subscriber.register_counter(*id, counter.clone());
    }
    publisher.do_work().unwrap();
    subscriber.do_work().unwrap();

    let publication = publisher.exclusive_publication_mut(pub_reg).unwrap();

    // 1. Offer message 1
    let mut msg1 = *b"exclusive-msg-1";
    let len1 = msg1.len();
    let src1 = AtomicBuffer::new(&mut msg1);
    assert!(publication.offer(&src1, 0, len1).unwrap() > 0);

    // 2. Try claim message 2 and abort
    let (pos2, claim2) = publication.try_claim(256).unwrap();
    assert!(pos2 > 0);
    claim2.unwrap().abort();

    // 3. Try claim message 3 and commit
    let (pos3, claim3) = publication.try_claim(128).unwrap();
    assert!(pos3 > 0);
    let claim3 = claim3.unwrap();
    let mut msg3 = *b"exclusive-claimed-msg-3";
    let len3 = msg3.len();
    let src3 = AtomicBuffer::new(&mut msg3);
    claim3.put_bytes(&src3, 0, len3);
    claim3.commit();

    let subscription = subscriber.subscription_mut(sub_reg).unwrap();
    let mut received = Vec::new();
    let read = subscription.poll(
        |buf, offset, len, _hdr| {
            let mut data = vec![0u8; len];
            buf.get_bytes(offset, &mut data);
            received.push(data);
        },
        10,
    );
    assert_eq!(read, 2);
    assert_eq!(received.len(), 2);
    assert_eq!(received[0], b"exclusive-msg-1");
    assert_eq!(
        received[1][..b"exclusive-claimed-msg-3".len()],
        *b"exclusive-claimed-msg-3"
    );
}
