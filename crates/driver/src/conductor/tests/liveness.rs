use super::super::*;
use super::fixtures::*;
use aeron_client::driver_events::{
    CounterUpdateFlyweight, ON_AVAILABLE_IMAGE, ON_CLIENT_TIMEOUT, ON_COUNTER_READY,
    ON_UNAVAILABLE_COUNTER, ON_UNAVAILABLE_IMAGE,
};
use agrona::BroadcastReceiver;

#[test]
fn sweeps_clients_that_stop_sending_keepalives() {
    let (cnc, path) = temp_cnc("liveness");
    let dir = temp_dir("liveness");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();

    let client = proxy(&cnc);
    client.add_publication("aeron:ipc", 1001).unwrap();
    conductor.do_work(1_000).unwrap();
    assert_eq!(conductor.active_client_count(), 1);
    assert_eq!(conductor.publications().len(), 1);

    // Advance well past the liveness timeout with no keepalive.
    let swept = conductor.do_work(1_000 + LIVENESS_TIMEOUT_NS + 1);
    assert_eq!(swept.unwrap(), 1, "one client timed out");
    assert_eq!(conductor.active_client_count(), 0);
    assert_eq!(
        conductor.publications().len(),
        0,
        "timed-out client's resources removed"
    );

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn reports_client_timeout_before_removing_client_resources() {
    let (cnc, path) = temp_cnc("client-timeout-event");
    let dir = temp_dir("client-timeout-event");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);

    client.client_keepalive().unwrap();
    let counter_registration = client.add_counter(88, &[9, 8], "timeout counter").unwrap();
    conductor.do_work(1_000).unwrap();
    assert_eq!(conductor.active_client_count(), 1);
    let mut counter_id = -1;
    assert!(
        events
            .receive_next(|msg_type, buf, offset, _len| {
                assert_eq!(msg_type, ON_COUNTER_READY);
                counter_id = CounterUpdateFlyweight::wrap(buf.clone(), offset).counter_id();
            })
            .unwrap()
    );
    assert!(conductor.counters().contains_key(&counter_id));

    assert_eq!(
        conductor.do_work(1_000 + LIVENESS_TIMEOUT_NS + 1).unwrap(),
        1
    );
    assert_eq!(conductor.active_client_count(), 0);
    assert!(!conductor.counters().contains_key(&counter_id));

    let mut seen_client_id = 0;
    assert!(
        events
            .receive_next(|msg_type, buf, offset, _len| {
                assert_eq!(msg_type, ON_CLIENT_TIMEOUT);
                seen_client_id = ClientTimeoutFlyweight::wrap(buf.clone(), offset).client_id();
            })
            .unwrap()
    );
    assert_eq!(seen_client_id, CLIENT_ID);
    let mut unavailable_registration = 0;
    let mut unavailable_id = -1;
    assert!(
        events
            .receive_next(|msg_type, buf, offset, _len| {
                assert_eq!(msg_type, ON_UNAVAILABLE_COUNTER);
                let update = CounterUpdateFlyweight::wrap(buf.clone(), offset);
                unavailable_registration = update.correlation_id();
                unavailable_id = update.counter_id();
            })
            .unwrap()
    );
    assert_eq!(unavailable_registration, counter_registration);
    assert_eq!(unavailable_id, counter_id);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn expires_network_image_and_emits_unavailable_image() {
    let (cnc, path) = temp_cnc("image-liveness");
    let dir = temp_dir("image-liveness");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);
    let subscription_registration = client
        .add_subscription("aeron:udp?endpoint=127.0.0.1:0", 7)
        .unwrap();
    conductor.do_work(1_000).unwrap();
    let _ = conductor.take_new_receive_endpoints();

    let source = SocketAddr::from(([127, 0, 0, 1], 9002));
    let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
    let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
    setup
        .header()
        .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
        .set_version(CURRENT_VERSION)
        .set_header_type(HDR_TYPE_SETUP);
    setup
        .set_session_id(7)
        .set_stream_id(7)
        .set_initial_term_id(0)
        .set_active_term_id(0)
        .set_term_offset(0)
        .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
        .set_mtu_length(1408)
        .set_ttl(0);
    conductor
        .on_received_frame(&setup_bytes, source, 1_000)
        .unwrap();

    let mut available_correlation = 0i64;
    while events
        .receive_next(|msg_type, buf, offset, _| {
            if msg_type == ON_AVAILABLE_IMAGE {
                available_correlation = buf.get_i64(offset);
            }
        })
        .unwrap()
    {}
    assert_ne!(available_correlation, 0);

    client.client_keepalive().unwrap();
    conductor.do_work(5_000_000_000).unwrap();
    // Past the image-liveness timeout: the receiver deactivates and releases the silent image,
    // moving it to DRAINING — but no UNAVAILABLE_IMAGE is emitted until it enters LINGER.
    conductor
        .do_work(IMAGE_LIVENESS_TIMEOUT_NS + 2_000)
        .unwrap();
    // Past the DRAINING release window (5 status-message timeouts) the image enters LINGER and
    // notifies subscribers. The client keepalive keeps the subscription alive throughout.
    conductor
        .do_work(IMAGE_LIVENESS_TIMEOUT_NS + STATUS_MESSAGE_TIMEOUT_NS * 5 + 3_000)
        .unwrap();

    let mut unavailable = None;
    while events
        .receive_next(|msg_type, buf, offset, _| {
            if msg_type == ON_UNAVAILABLE_IMAGE {
                unavailable = Some((buf.get_i64(offset), buf.get_i64(offset + 8)));
            }
        })
        .unwrap()
    {}
    assert_eq!(
        unavailable,
        Some((available_correlation, subscription_registration))
    );

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}
