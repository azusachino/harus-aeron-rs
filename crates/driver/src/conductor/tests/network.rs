use super::super::*;
use super::fixtures::*;
use aeron_client::driver_events::ON_AVAILABLE_IMAGE;
use agrona::AtomicBuffer;
use agrona::BroadcastReceiver;

#[test]
fn removing_udp_subscription_detaches_image_and_receive_channel() {
    let (cnc, path) = temp_cnc("remove-udp-subscription");
    let dir = temp_dir("remove-udp-subscription");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    let channel = "aeron:udp?endpoint=127.0.0.1:0";
    let registration_id = client.add_subscription(channel, 7).unwrap();
    conductor.do_work(1_000).unwrap();
    let mut endpoints = conductor.take_new_receive_endpoints();
    assert_eq!(endpoints.len(), 1);
    let endpoint_key = endpoints[0].canonical_form().to_owned();
    endpoints.pop();

    let source = SocketAddr::from(([127, 0, 0, 1], 9003));
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
        .on_received_frame(&setup_bytes, source, 2_000)
        .unwrap();
    assert_eq!(conductor.images.len(), 1);

    client.remove_subscription(registration_id).unwrap();
    conductor.do_work(3_000).unwrap();

    assert!(conductor.images.is_empty());
    assert!(conductor.counters().is_empty());
    assert_eq!(
        conductor.take_removed_receive_channels(),
        vec![endpoint_key]
    );

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn shares_udp_receive_endpoint_until_last_subscription_is_removed() {
    let (cnc, path) = temp_cnc("shared-udp-endpoint");
    let dir = temp_dir("shared-udp-endpoint");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    let channel = "aeron:udp?endpoint=127.0.0.1:0";

    let first = client.add_subscription(channel, 7).unwrap();
    conductor.do_work(1_000).unwrap();
    let endpoints = conductor.take_new_receive_endpoints();
    assert_eq!(endpoints.len(), 1);
    let endpoint_key = endpoints[0].canonical_form().to_owned();

    let second = client.add_subscription(channel, 8).unwrap();
    conductor.do_work(2_000).unwrap();
    assert!(conductor.take_new_receive_endpoints().is_empty());

    client.remove_subscription(first).unwrap();
    conductor.do_work(3_000).unwrap();
    assert!(conductor.take_removed_receive_channels().is_empty());

    client.remove_subscription(second).unwrap();
    conductor.do_work(4_000).unwrap();
    assert_eq!(
        conductor.take_removed_receive_channels(),
        vec![endpoint_key]
    );

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn shares_udp_receive_endpoint_across_session_and_reliability_uri_options() {
    let (cnc, path) = temp_cnc("shared-udp-options");
    let dir = temp_dir("shared-udp-options");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);

    let first = client
        .add_subscription("aeron:udp?endpoint=127.0.0.1:0|session-id=1024", 7)
        .unwrap();
    conductor.do_work(1_000).unwrap();
    let endpoints = conductor.take_new_receive_endpoints();
    assert_eq!(endpoints.len(), 1);
    let endpoint_key = endpoints[0].canonical_form().to_owned();

    let second = client
        .add_subscription(
            "aeron:udp?endpoint=127.0.0.1:0|session-id=1025|reliable=false",
            7,
        )
        .unwrap();
    conductor.do_work(2_000).unwrap();
    assert!(conductor.take_new_receive_endpoints().is_empty());

    client.remove_subscription(first).unwrap();
    conductor.do_work(3_000).unwrap();
    assert!(conductor.take_removed_receive_channels().is_empty());

    client.remove_subscription(second).unwrap();
    conductor.do_work(4_000).unwrap();
    assert_eq!(
        conductor.take_removed_receive_channels(),
        vec![endpoint_key]
    );

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn routes_identical_streams_by_receive_channel_identity() {
    let (cnc, path) = temp_cnc("channel-isolation");
    let dir = temp_dir("channel-isolation");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    let first_channel = "aeron:udp?endpoint=127.0.0.1:0";
    let second_channel = "aeron:udp?endpoint=localhost:0";
    let first = client.add_subscription(first_channel, 7).unwrap();
    let second = client.add_subscription(second_channel, 7).unwrap();
    conductor.do_work(1_000).unwrap();
    let _ = conductor.take_new_receive_endpoints();

    let source = SocketAddr::from(([127, 0, 0, 1], 9010));
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
        .on_received_frame_for_channel(&setup_bytes, source, 2_000, Some(first_channel))
        .unwrap();
    let image = conductor.images.values().next().unwrap();
    assert_eq!(image.image_links().count(), 1);
    assert!(image.has_subscription(first));
    assert!(!image.has_subscription(second));

    drop(conductor);
    let _ = std::fs::remove_file(path);
}

#[test]
fn tracks_data_before_setup_and_only_creates_image_after_setup() {
    let (cnc, path) = temp_cnc("dispatcher-session-state");
    let dir = temp_dir("dispatcher-session-state");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    let channel = "aeron:udp?endpoint=127.0.0.1:0";
    client.add_subscription(channel, 7).unwrap();
    conductor.do_work(1_000).unwrap();
    let _ = conductor.take_new_receive_endpoints();

    let source = SocketAddr::from(([127, 0, 0, 1], 9011));
    let mut data_bytes = vec![0u8; DATA_HEADER_LENGTH];
    let data = DataHeaderFlyweight::new(AtomicBuffer::new(&mut data_bytes));
    data.set_frame_length(DATA_HEADER_LENGTH as i32)
        .set_version(CURRENT_VERSION)
        .set_header_type(HDR_TYPE_DATA)
        .set_flags(0)
        .set_session_id(7)
        .set_stream_id(7)
        .set_term_id(0)
        .set_term_offset(0);

    conductor
        .on_received_frame_for_channel(&data_bytes, source, 2_000, Some(channel))
        .unwrap();
    assert!(conductor.images.is_empty());
    assert_eq!(
        conductor
            .session_states
            .get(&(channel.to_owned(), 7, 7, source)),
        Some(&(NetworkSessionState::PendingSetupFrame, 2_000))
    );
    assert!(conductor.take_setup_elicitation(channel, 7, 7, source, 2_000));
    assert!(!conductor.take_setup_elicitation(channel, 7, 7, source, 2_000 + 999_999_999,));
    assert!(conductor.take_setup_elicitation(channel, 7, 7, source, 2_000 + 1_000_000_000,));

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
        .on_received_frame_for_channel(&setup_bytes, source, 3_000, Some(channel))
        .unwrap();

    assert_eq!(conductor.images.len(), 1);
    assert_eq!(
        conductor
            .session_states
            .get(&(channel.to_owned(), 7, 7, source)),
        Some(&(NetworkSessionState::Active, 3_000))
    );

    drop(conductor);
    let _ = std::fs::remove_file(path);
}

#[test]
fn records_no_interest_until_a_matching_subscription_clears_it() {
    let (cnc, path) = temp_cnc("dispatcher-no-interest");
    let dir = temp_dir("dispatcher-no-interest");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let channel = "aeron:udp?endpoint=127.0.0.1:0";
    let source = SocketAddr::from(([127, 0, 0, 1], 9012));

    let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
    let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
    setup
        .header()
        .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
        .set_version(CURRENT_VERSION)
        .set_header_type(HDR_TYPE_SETUP);
    setup
        .set_session_id(8)
        .set_stream_id(8)
        .set_initial_term_id(0)
        .set_active_term_id(0)
        .set_term_offset(0)
        .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
        .set_mtu_length(1408)
        .set_ttl(0);

    conductor
        .on_received_frame_for_channel(&setup_bytes, source, 1_000, Some(channel))
        .unwrap();
    let key = (channel.to_owned(), 8, 8, source);
    assert!(conductor.images.is_empty());
    assert_eq!(
        conductor.session_states.get(&key),
        Some(&(NetworkSessionState::NoInterest, 1_000))
    );

    let client = proxy(&cnc);
    client.add_subscription(channel, 8).unwrap();
    conductor.do_work(2_000).unwrap();
    conductor
        .on_received_frame_for_channel(&setup_bytes, source, 3_000, Some(channel))
        .unwrap();
    assert_eq!(conductor.images.len(), 1);
    assert_eq!(
        conductor.session_states.get(&key),
        Some(&(NetworkSessionState::Active, 3_000))
    );

    drop(conductor);
    let _ = std::fs::remove_file(path);
}

#[test]
fn applies_session_specific_subscription_interest() {
    let (cnc, path) = temp_cnc("dispatcher-session-filter");
    let dir = temp_dir("dispatcher-session-filter");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    let channel = "aeron:udp?endpoint=127.0.0.1:0|session-id=7";
    client.add_subscription(channel, 9).unwrap();
    conductor.do_work(1_000).unwrap();
    let _ = conductor.take_new_receive_endpoints();
    let source = SocketAddr::from(([127, 0, 0, 1], 9013));

    for session_id in [8, 7] {
        let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
        setup
            .header()
            .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
            .set_version(CURRENT_VERSION)
            .set_header_type(HDR_TYPE_SETUP);
        setup
            .set_session_id(session_id)
            .set_stream_id(9)
            .set_initial_term_id(0)
            .set_active_term_id(0)
            .set_term_offset(0)
            .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
            .set_mtu_length(1408)
            .set_ttl(0);
        conductor
            .on_received_frame_for_channel(
                &setup_bytes,
                source,
                2_000 + session_id as i64,
                Some(channel),
            )
            .unwrap();
    }

    assert_eq!(conductor.images.len(), 1);
    assert!(
        conductor
            .images
            .keys()
            .any(|(_, session_id, _, _)| *session_id == 7)
    );
    assert!(
        conductor
            .session_states
            .get(&(channel.to_owned(), 8, 9, source))
            .is_some_and(|(state, _)| *state == NetworkSessionState::NoInterest)
    );

    drop(conductor);
    let _ = std::fs::remove_file(path);
}

#[test]
fn enforces_stream_session_limit() {
    let (cnc, path) = temp_cnc("dispatcher-session-limit");
    let dir = temp_dir("dispatcher-session-limit");
    let mut config = small_config();
    config.stream_session_limit = 1;
    let mut conductor = DriverConductor::new(config, cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    let channel = "aeron:udp?endpoint=127.0.0.1:0";
    client.add_subscription(channel, 10).unwrap();
    conductor.do_work(1_000).unwrap();
    let _ = conductor.take_new_receive_endpoints();
    let source = SocketAddr::from(([127, 0, 0, 1], 9014));

    for session_id in [10, 11] {
        let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
        setup
            .header()
            .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
            .set_version(CURRENT_VERSION)
            .set_header_type(HDR_TYPE_SETUP);
        setup
            .set_session_id(session_id)
            .set_stream_id(10)
            .set_initial_term_id(0)
            .set_active_term_id(0)
            .set_term_offset(0)
            .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
            .set_mtu_length(1408)
            .set_ttl(0);
        conductor
            .on_received_frame_for_channel(
                &setup_bytes,
                source,
                2_000 + session_id as i64,
                Some(channel),
            )
            .unwrap();
    }

    assert_eq!(conductor.images.len(), 1);
    assert!(
        conductor
            .session_states
            .get(&(channel.to_owned(), 11, 10, source))
            .is_some_and(|(state, _)| *state == NetworkSessionState::NoInterest)
    );

    drop(conductor);
    let _ = std::fs::remove_file(path);
}

#[test]
fn receive_image_starts_at_setup_active_position() {
    let (cnc, path) = temp_cnc("udp-image-active-position");
    let dir = temp_dir("udp-image-active-position");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    client
        .add_subscription("aeron:udp?endpoint=127.0.0.1:0", 7)
        .unwrap();
    conductor.do_work(1_000).unwrap();
    let _ = conductor.take_new_receive_endpoints();

    let source = SocketAddr::from(([127, 0, 0, 1], 9005));
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
        .set_term_offset(1024)
        .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
        .set_mtu_length(1408)
        .set_ttl(0);

    conductor
        .on_received_frame(&setup_bytes, source, 2_000)
        .unwrap();
    let image = conductor.images.values().next().unwrap();
    assert_eq!(image.hwm_position(), 1024);
    assert_eq!(image.rebuild_position(), 1024);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn removing_udp_publication_detaches_sender_resource() {
    let (cnc, path) = temp_cnc("remove-udp-publication");
    let dir = temp_dir("remove-udp-publication");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    let registration_id = client
        .add_publication("aeron:udp?endpoint=127.0.0.1:0", 7)
        .unwrap();
    conductor.do_work(1_000).unwrap();
    assert_eq!(conductor.take_new_network_publications().len(), 1);

    client.remove_publication(registration_id, false).unwrap();
    conductor.do_work(2_000).unwrap();

    assert_eq!(
        conductor.take_removed_network_publications(),
        vec![registration_id]
    );
    assert!(conductor.publications().is_empty());

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn links_late_udp_subscription_to_existing_image() {
    let (cnc, path) = temp_cnc("udp-image-fanout");
    let dir = temp_dir("udp-image-fanout");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);
    let channel = "aeron:udp?endpoint=127.0.0.1:0";
    let first_subscription = client.add_subscription(channel, 7).unwrap();
    conductor.do_work(1_000).unwrap();
    let _ = conductor.take_new_receive_endpoints();

    let source = SocketAddr::from(([127, 0, 0, 1], 9004));
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
        .on_received_frame(&setup_bytes, source, 2_000)
        .unwrap();
    let mut initial_available = 0;
    while events
        .receive_next(|msg_type, _, _, _| {
            if msg_type == ON_AVAILABLE_IMAGE {
                initial_available += 1;
            }
        })
        .unwrap()
    {}
    assert_eq!(initial_available, 1);

    let second_subscription = client.add_subscription(channel, 7).unwrap();
    conductor.do_work(3_000).unwrap();
    let _ = conductor.take_new_receive_endpoints();

    let mut late_available = 0;
    while events
        .receive_next(|msg_type, _, _, _| {
            if msg_type == ON_AVAILABLE_IMAGE {
                late_available += 1;
            }
        })
        .unwrap()
    {}
    assert_eq!(late_available, 1);
    assert_eq!(conductor.images.len(), 1);
    assert_eq!(
        conductor
            .images
            .values()
            .next()
            .map(|image| image.image_links().count()),
        Some(2)
    );

    client.remove_subscription(second_subscription).unwrap();
    conductor.do_work(5_000).unwrap();
    assert_eq!(
        conductor
            .images
            .values()
            .next()
            .map(|image| image.image_links().count()),
        Some(1)
    );
    client.remove_subscription(first_subscription).unwrap();
    conductor.do_work(6_000).unwrap();
    assert!(conductor.images.is_empty());

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}
