use super::super::*;
use super::fixtures::*;
use aeron_client::driver_events::{
    ON_AVAILABLE_IMAGE, ON_SUBSCRIPTION_READY, ON_UNAVAILABLE_IMAGE,
};
use agrona::BroadcastReceiver;

#[test]
fn positions_ipc_publication_at_replay_term_triple() {
    let (cnc, path) = temp_cnc("publication-replay");
    let dir = temp_dir("publication-replay");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    let registration_id = client
        .add_publication("aeron:ipc?initial-term-id=7|term-id=11|term-offset=64", 7)
        .unwrap();

    conductor.do_work(1_000).unwrap();

    let entry = conductor.publications().get(&registration_id).unwrap();
    assert_eq!(entry.initial_term_id, 7);
    let metadata = conductor
        .log_buffers
        .get(&registration_id)
        .unwrap()
        .meta_data_buffer();
    assert_eq!(aeron_client::logbuffer::initial_term_id(&metadata), 7);
    assert_eq!(aeron_client::logbuffer::active_term_count(&metadata), 4);
    let active_partition = aeron_client::logbuffer::index_by_term_count(4) as usize;
    let tail = aeron_client::logbuffer::raw_tail(&metadata, active_partition);
    assert_eq!(aeron_client::logbuffer::term_id(tail), 11);
    assert_eq!(aeron_client::logbuffer::term_offset(tail, 64 * 1024), 64);
    assert_eq!(
        aeron_client::logbuffer::compute_position(11, 64, 16, 7),
        4 * 64 * 1024 + 64
    );

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn links_spy_subscription_to_an_existing_ipc_publication() {
    let (cnc, path) = temp_cnc("spy-subscription");
    let dir = temp_dir("spy-subscription");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);

    client.add_subscription("aeron-spy:aeron:ipc", 7).unwrap();
    conductor.do_work(1_000).unwrap();
    let publication = client.add_publication("aeron:ipc", 7).unwrap();
    conductor.do_work(2_000).unwrap();

    let mut event_types = Vec::new();
    while events
        .receive_next(|message_type, _, _, _| event_types.push(message_type))
        .unwrap()
    {}
    assert!(event_types.contains(&ON_SUBSCRIPTION_READY));
    assert!(event_types.contains(&ON_AVAILABLE_IMAGE));

    client.remove_publication(publication, false).unwrap();
    conductor.do_work(3_000).unwrap();
    let mut removed_spy_image = false;
    while events
        .receive_next(|message_type, _, _, _| {
            removed_spy_image |= message_type == ON_UNAVAILABLE_IMAGE;
        })
        .unwrap()
    {}
    assert!(removed_spy_image);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ipc_publication_allocates_shared_log_and_emits_publication_ready() {
    let (cnc, path) = temp_cnc("ipcpub");
    let dir = temp_dir("ipcpub");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();

    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);
    let correlation_id = client.add_publication("aeron:ipc", 1001).unwrap();
    conductor.do_work(1_000).unwrap();

    // The publication carries a real, mappable log file.
    let entry = conductor.publications().values().next().unwrap().clone();
    assert!(entry.is_ipc);
    let log_file_name = entry.log_file_name.clone().expect("ipc log allocated");
    let mapped = LogBuffers::map_existing(&log_file_name).expect("shared log mappable");
    assert_eq!(mapped.term_length(), small_config().ipc_term_buffer_length);

    // And PUBLICATION_READY is delivered to the client with the same log file name.
    let mut seen_type = 0i32;
    let mut seen_name = String::new();
    let mut seen_session = 0i32;
    let got = events
        .receive_next(|msg_type, buf, offset, _len| {
            seen_type = msg_type;
            let fw = PublicationBuffersReadyFlyweight::wrap(buf.clone(), offset);
            seen_session = fw.session_id();
            seen_name = fw.log_file_name().unwrap();
        })
        .unwrap();
    assert!(got);
    assert_eq!(seen_type, ON_PUBLICATION_READY);
    assert_eq!(seen_name, log_file_name);
    assert_eq!(seen_session, entry.session_id);
    assert_eq!(correlation_id, entry.correlation_id);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn resolves_tagged_session_id_against_active_publication() {
    let (cnc, path) = temp_cnc("tagged-session");
    let dir = temp_dir("tagged-session");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);

    let tagged = client
        .add_publication("aeron:ipc?tags=100,200", 1011)
        .unwrap();
    conductor.do_work(1_000).unwrap();
    let tagged_entry = conductor.publications().get(&tagged).unwrap().clone();
    assert_eq!(tagged_entry.entity_tag, 200);

    let resolved = client
        .add_publication("aeron:ipc?session-id=tag:200", 1011)
        .unwrap();
    conductor.do_work(2_000).unwrap();
    let resolved_entry = conductor.publications().get(&resolved).unwrap();
    assert_eq!(resolved_entry.session_id, tagged_entry.session_id);
    assert_eq!(resolved_entry.term_length, tagged_entry.term_length);
    assert_eq!(resolved_entry.mtu_length, tagged_entry.mtu_length);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn untethered_ipc_subscription_leaves_flow_control_and_rejoins() {
    let (cnc, path) = temp_cnc("untethered-ipc");
    let dir = temp_dir("untethered-ipc");
    let mut config = small_config();
    config.untethered_window_limit_timeout_ns = 100;
    config.untethered_linger_timeout_ns = 100;
    config.untethered_resting_timeout_ns = 100;
    let mut conductor = DriverConductor::new(config, cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);

    let publication = client.add_publication("aeron:ipc", 1012).unwrap();
    let subscription = client
        .add_subscription("aeron:ipc?tether=false", 1012)
        .unwrap();
    conductor.do_work(1_000).unwrap();

    let publication_limit_id = conductor
        .publications()
        .get(&publication)
        .unwrap()
        .publication_limit_counter_id;
    assert_eq!(conductor.ipc_subscriptions.len(), 1);
    assert_eq!(
        conductor
            .counters()
            .get(&publication_limit_id)
            .unwrap()
            .load(Ordering::Acquire),
        (small_config().ipc_term_buffer_length / 2) as i64
    );

    let publication_log = conductor.log_buffers.get(&publication).unwrap();
    let metadata = publication_log.meta_data_buffer();
    let initial_term_id = conductor
        .publications()
        .get(&publication)
        .unwrap()
        .initial_term_id;
    aeron_client::logbuffer::set_raw_tail(
        &metadata,
        0,
        aeron_client::logbuffer::pack_tail(initial_term_id.wrapping_add(1), 0),
    );

    conductor.do_work(1_101).unwrap();
    assert_eq!(
        conductor
            .ipc_subscriptions
            .get(&(publication, subscription))
            .unwrap()
            .state,
        IpcSubscriptionStateKind::Linger
    );
    assert_eq!(
        conductor
            .counters()
            .get(&publication_limit_id)
            .unwrap()
            .load(Ordering::Acquire),
        0
    );
    let mut unavailable = false;
    while events
        .receive_next(|msg_type, buf, offset, _| {
            if msg_type == ON_UNAVAILABLE_IMAGE && buf.get_i64(offset + 8) == subscription {
                unavailable = true;
            }
        })
        .unwrap()
    {}
    assert!(unavailable);

    conductor.do_work(1_201).unwrap();
    assert_eq!(
        conductor
            .ipc_subscriptions
            .get(&(publication, subscription))
            .unwrap()
            .state,
        IpcSubscriptionStateKind::Resting
    );
    conductor.do_work(1_301).unwrap();
    assert_eq!(
        conductor
            .ipc_subscriptions
            .get(&(publication, subscription))
            .unwrap()
            .state,
        IpcSubscriptionStateKind::Active
    );
    assert!(
        conductor
            .counters()
            .get(&publication_limit_id)
            .unwrap()
            .load(Ordering::Acquire)
            > 0
    );

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ipc_publication_limit_starts_closed_and_opens_for_a_subscription() {
    let (cnc, path) = temp_cnc("ipc-limit");
    let dir = temp_dir("ipc-limit");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);

    let publication = client.add_publication("aeron:ipc", 1010).unwrap();
    conductor.do_work(1_000).unwrap();
    let counter_id = conductor
        .publications()
        .get(&publication)
        .unwrap()
        .publication_limit_counter_id;
    assert_eq!(
        conductor
            .counters()
            .get(&counter_id)
            .unwrap()
            .load(Ordering::Acquire),
        0
    );

    client.add_subscription("aeron:ipc", 1010).unwrap();
    conductor.do_work(2_000).unwrap();
    assert!(
        conductor
            .counters()
            .get(&counter_id)
            .unwrap()
            .load(Ordering::Acquire)
            > 0
    );

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ipc_publication_and_subscription_connect_with_available_image() {
    let (cnc, path) = temp_cnc("ipcimage");
    let dir = temp_dir("ipcimage");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();

    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);
    // Subscription first, publication second — the image links on the later add.
    let sub_correlation = client.add_subscription("aeron:ipc", 7).unwrap();
    client.add_publication("aeron:ipc", 7).unwrap();
    conductor.do_work(1_000).unwrap();

    let pub_log = conductor
        .publications()
        .values()
        .next()
        .unwrap()
        .log_file_name
        .clone()
        .unwrap();

    // Drain events, capturing the AVAILABLE_IMAGE.
    let mut image_name = None;
    let mut image_sub_reg = 0i64;
    let mut image_source = String::new();
    while events
        .receive_next(|msg_type, buf, offset, _len| {
            if msg_type == ON_AVAILABLE_IMAGE {
                let fw = ImageBuffersReadyFlyweight::wrap(buf.clone(), offset);
                image_name = Some(fw.log_file_name().unwrap());
                image_sub_reg = fw.subscription_registration_id();
                image_source = fw.source_identity().unwrap();
            }
        })
        .unwrap()
    {}

    assert_eq!(image_name.as_deref(), Some(pub_log.as_str()));
    assert_eq!(image_sub_reg, sub_correlation);
    assert_eq!(image_source, "aeron:ipc");

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}
