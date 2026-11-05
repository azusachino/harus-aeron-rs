use super::*;
use aeron_client::cnc_file::{
    DEFAULT_COUNTER_METADATA_LENGTH, DEFAULT_COUNTER_VALUES_LENGTH, DEFAULT_ERROR_LOG_LENGTH,
    DEFAULT_TO_CLIENTS_LENGTH, DEFAULT_TO_DRIVER_LENGTH,
};
use aeron_client::driver_events::{
    CounterUpdateFlyweight, ErrorResponseFlyweight, NextAvailableSessionIdFlyweight,
    ON_AVAILABLE_IMAGE, ON_CLIENT_TIMEOUT, ON_COUNTER_READY, ON_ERROR,
    ON_NEXT_AVAILABLE_SESSION_ID, ON_SUBSCRIPTION_READY, ON_UNAVAILABLE_COUNTER,
    ON_UNAVAILABLE_IMAGE,
};
use aeron_client::driver_proxy::DriverProxy;
use agrona::BroadcastReceiver;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

const CLIENT_ID: i64 = 42;
const LIVENESS_TIMEOUT_NS: i64 = 10_000_000_000;

/// A tiny IPC term length keeps test log files fast to map (64 KiB terms, not 64 MiB).
fn small_config() -> Configuration {
    Configuration {
        ipc_term_buffer_length: aeron_client::logbuffer::TERM_MIN_LENGTH as usize,
        ..Configuration::default()
    }
}

/// A per-test scratch directory for the driver's log files, removed on drop.
struct TempDir(std::path::PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn temp_dir(tag: &str) -> TempDir {
    let path = std::env::temp_dir().join(format!(
        "aeron-conductor-dir-{}-{}-{:?}",
        tag,
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    TempDir(path)
}

fn temp_cnc(tag: &str) -> (CncFile, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "aeron-conductor-{}-{}-{:?}.dat",
        tag,
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_file(&path);
    let cnc = CncFile::create_new_with_regions(
        &path,
        DEFAULT_TO_DRIVER_LENGTH,
        DEFAULT_TO_CLIENTS_LENGTH,
        DEFAULT_COUNTER_METADATA_LENGTH,
        DEFAULT_COUNTER_VALUES_LENGTH,
        DEFAULT_ERROR_LOG_LENGTH,
        LIVENESS_TIMEOUT_NS,
    )
    .unwrap();
    (cnc, path)
}

#[test]
fn malformed_variable_length_commands_are_rejected_without_panicking() {
    let mut bytes = vec![0u8; 32];
    let buffer = AtomicBuffer::new(&mut bytes);
    buffer.put_i64(0, CLIENT_ID);
    buffer.put_i64(8, 91);
    buffer.put_i32(aeron_client::command::PUB_CHANNEL_LENGTH_OFFSET, 128);

    let command = decode_command(
        aeron_client::driver_proxy::ADD_PUBLICATION,
        &buffer,
        0,
        aeron_client::command::PUB_CHANNEL_OFFSET,
    );
    assert!(matches!(
        command,
        Some(Command::Unknown {
            correlation_id: 91,
            message_type: aeron_client::driver_proxy::ADD_PUBLICATION,
        })
    ));
}

#[test]
fn decodes_java_counter_with_unaligned_key_length() {
    // Aeron 1.54.0 CounterVector.java output, including one padding byte.
    let mut bytes = [
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12,
        0x11, 0x24, 0x23, 0x22, 0x21, 0x03, 0, 0, 0, 0x31, 0x32, 0x33, 0, 0x03, 0, 0, 0, 0x61,
        0x62, 0x63,
    ];
    let buffer = AtomicBuffer::new(&mut bytes);
    assert!(matches!(
        decode_command(aeron_client::driver_proxy::ADD_COUNTER, &buffer, 0, 35),
        Some(Command::AddCounter { client_id: 0x0102030405060708,
            correlation_id: 0x1112131415161718, type_id: 0x21222324,
            key, label }) if key == b"123" && label == "abc"
    ));
    assert!(matches!(
        decode_command(aeron_client::driver_proxy::ADD_COUNTER, &buffer, 0, 34),
        Some(Command::Unknown { .. })
    ));
}

#[test]
fn malformed_counter_lengths_are_rejected_without_panicking() {
    let mut bytes = vec![0u8; 32];
    let buffer = AtomicBuffer::new(&mut bytes);
    buffer.put_i64(8, 92);
    buffer.put_i32(aeron_client::command::KEY_LENGTH_OFFSET, -1);

    let command = decode_command(
        aeron_client::driver_proxy::ADD_COUNTER,
        &buffer,
        0,
        aeron_client::command::KEY_BUFFER_OFFSET,
    );
    assert!(matches!(
        command,
        Some(Command::Unknown {
            correlation_id: 92,
            message_type: aeron_client::driver_proxy::ADD_COUNTER,
        })
    ));
}

fn proxy(cnc: &CncFile) -> DriverProxy<'static> {
    let ring = ManyToOneRingBuffer::new(cnc.to_driver_buffer()).unwrap();
    DriverProxy::new(ring, CLIENT_ID)
}

#[test]
fn registers_publication_and_subscription_from_commands() {
    let (cnc, path) = temp_cnc("register");
    let dir = temp_dir("register");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();

    let client = proxy(&cnc);
    client.add_publication("aeron:ipc", 1001).unwrap();
    client.add_subscription("aeron:ipc", 1001).unwrap();

    let work = conductor.do_work(1_000).unwrap();
    assert_eq!(work, 2, "two commands processed");
    assert_eq!(conductor.publications().len(), 1);
    assert_eq!(conductor.subscriptions().len(), 1);
    assert_eq!(conductor.active_client_count(), 1);

    let pubs: Vec<_> = conductor.publications().values().collect();
    assert_eq!(pubs[0].stream_id, 1001);
    assert_eq!(pubs[0].channel, "aeron:ipc");
    assert_eq!(pubs[0].client_id, CLIENT_ID);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn adds_and_removes_receive_destination_alias_through_commands() {
    let (cnc, path) = temp_cnc("receive-destination");
    let dir = temp_dir("receive-destination");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    let subscription = client
        .add_subscription("aeron:udp?endpoint=127.0.0.1:0", 1001)
        .unwrap();
    conductor.do_work(1_000).unwrap();
    assert_eq!(conductor.take_new_receive_endpoints().len(), 1);

    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let destination = socket.local_addr().unwrap();
    drop(socket);
    let channel = format!("aeron:udp?endpoint={destination}");
    client
        .add_receive_destination(subscription, &channel)
        .unwrap();
    conductor.do_work(2_000).unwrap();
    assert_eq!(conductor.take_new_receive_endpoints().len(), 1);
    assert_eq!(
        conductor.resolve_receive_channel(&channel),
        "aeron:udp?endpoint=127.0.0.1:0"
    );

    client
        .remove_receive_destination(subscription, &channel)
        .unwrap();
    conductor.do_work(3_000).unwrap();
    assert_eq!(conductor.take_removed_receive_channels().len(), 1);
    assert_eq!(conductor.resolve_receive_channel(&channel), channel);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn honors_explicit_publication_session_id_through_the_cnc() {
    let (cnc, path) = temp_cnc("publication-session");
    let dir = temp_dir("publication-session");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    client
        .add_publication("aeron:ipc?session-id=4096", 7)
        .unwrap();

    conductor.do_work(1_000).unwrap();

    assert_eq!(conductor.publications().len(), 1);
    assert_eq!(
        conductor.publications().values().next().unwrap().session_id,
        4096
    );

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn applies_publication_term_and_mtu_parameters_at_registration() {
    let (cnc, path) = temp_cnc("publication-params");
    let dir = temp_dir("publication-params");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    client
        .add_publication("aeron:ipc?term-length=64k|mtu=4k", 7)
        .unwrap();

    conductor.do_work(1_000).unwrap();

    let entry = conductor.publications().values().next().unwrap();
    let buffers = LogBuffers::map_existing(entry.log_file_name.as_ref().unwrap()).unwrap();
    assert_eq!(buffers.term_length(), 64 * 1024);
    assert_eq!(
        aeron_client::logbuffer::mtu_length(&buffers.meta_data_buffer()),
        4 * 1024
    );

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

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
fn rejects_a_publication_with_a_conflicting_explicit_session_id() {
    let (cnc, path) = temp_cnc("publication-session-conflict");
    let dir = temp_dir("publication-session-conflict");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);
    client
        .add_publication("aeron:ipc?session-id=4096", 7)
        .unwrap();
    conductor.do_work(1_000).unwrap();
    let correlation_id = client
        .add_publication("aeron:ipc?session-id=4096", 7)
        .unwrap();

    conductor.do_work(2_000).unwrap();

    assert_eq!(conductor.publications().len(), 1);
    let mut error_correlation = None;
    while events
        .receive_next(|msg_type, buf, offset, _| {
            if msg_type == ON_ERROR {
                error_correlation = Some(
                    ErrorResponseFlyweight::wrap(buf.clone(), offset)
                        .offending_command_correlation_id(),
                );
            }
        })
        .unwrap()
    {
        if error_correlation.is_some() {
            break;
        }
    }
    assert_eq!(error_correlation, Some(correlation_id));

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn publishes_subscription_ready_event_to_clients() {
    let (cnc, path) = temp_cnc("subready");
    let dir = temp_dir("subready");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();

    // Client subscribes to events first (broadcast delivers only after listening).
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();

    let client = proxy(&cnc);
    let correlation_id = client.add_subscription("aeron:ipc", 7).unwrap();
    conductor.do_work(1_000).unwrap();

    let mut seen_type = 0i32;
    let mut seen_correlation = 0i64;
    let got = events
        .receive_next(|msg_type, buf, offset, _len| {
            seen_type = msg_type;
            seen_correlation =
                SubscriptionReadyFlyweight::wrap(buf.clone(), offset).correlation_id();
        })
        .unwrap();

    assert!(got);
    assert_eq!(seen_type, ON_SUBSCRIPTION_READY);
    assert_eq!(seen_correlation, correlation_id);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn adds_and_removes_client_counters_through_the_cnc() {
    let (cnc, path) = temp_cnc("client-counter");
    let dir = temp_dir("client-counter");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);
    let registration_id = client.add_counter(77, &[1, 2, 3], "test counter").unwrap();

    assert_eq!(conductor.do_work(1_000).unwrap(), 1);
    let mut ready_correlation = 0;
    let mut counter_id = -1;
    assert!(
        events
            .receive_next(|msg_type, buf, offset, _len| {
                assert_eq!(msg_type, ON_COUNTER_READY);
                let update = CounterUpdateFlyweight::wrap(buf.clone(), offset);
                ready_correlation = update.correlation_id();
                counter_id = update.counter_id();
            })
            .unwrap()
    );
    assert_eq!(ready_correlation, registration_id);
    assert!(conductor.counters().contains_key(&counter_id));
    let metadata = conductor.cnc().counters_metadata_buffer();
    let metadata_offset = counter_id as usize * aeron_client::cnc_file::COUNTER_METADATA_LENGTH;
    assert_eq!(
        metadata.get_i32(metadata_offset),
        aeron_client::cnc_file::COUNTER_RECORD_ALLOCATED
    );
    assert_eq!(
        metadata.get_i32(metadata_offset + aeron_client::cnc_file::COUNTER_TYPE_ID_OFFSET),
        77
    );

    let remove_correlation = client.remove_counter(registration_id).unwrap();
    assert_eq!(conductor.do_work(2_000).unwrap(), 1);
    let mut unavailable_correlation = 0;
    let mut unavailable_id = -1;
    assert!(
        events
            .receive_next(|msg_type, buf, offset, _len| {
                assert_eq!(msg_type, ON_UNAVAILABLE_COUNTER);
                let update = CounterUpdateFlyweight::wrap(buf.clone(), offset);
                unavailable_correlation = update.correlation_id();
                unavailable_id = update.counter_id();
            })
            .unwrap()
    );
    assert_eq!(unavailable_correlation, registration_id);
    assert_eq!(unavailable_id, counter_id);
    assert_ne!(remove_correlation, unavailable_correlation);
    assert!(!conductor.counters().contains_key(&counter_id));
    assert_eq!(metadata.get_i32(metadata_offset), 0);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn returns_next_available_session_ids_through_the_cnc() {
    let (cnc, path) = temp_cnc("next-session-id");
    let dir = temp_dir("next-session-id");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);

    let first_correlation = client.get_next_available_session_id(7).unwrap();
    let second_correlation = client.get_next_available_session_id(8).unwrap();
    assert_eq!(conductor.do_work(1_000).unwrap(), 2);

    let mut responses = Vec::new();
    while events
        .receive_next(|msg_type, buf, offset, _len| {
            assert_eq!(msg_type, ON_NEXT_AVAILABLE_SESSION_ID);
            let response = NextAvailableSessionIdFlyweight::wrap(buf.clone(), offset);
            responses.push((response.correlation_id(), response.next_session_id()));
        })
        .unwrap()
    {}
    assert_eq!(
        responses,
        vec![(first_correlation, 1), (second_correlation, 2)]
    );

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

fn receive_event_type(events: &mut BroadcastReceiver<'_>) -> i32 {
    let mut seen_type = 0;
    assert!(
        events
            .receive_next(|msg_type, _, _, _| seen_type = msg_type)
            .unwrap()
    );
    seen_type
}

#[test]
fn rejects_conflicting_reliable_and_unreliable_wildcard_subscriptions() {
    let (cnc, path) = temp_cnc("reliability-conflict");
    let dir = temp_dir("reliability-conflict");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);
    let channel = "aeron:udp?endpoint=127.0.0.1:0";

    client.add_subscription(channel, 7).unwrap();
    assert_eq!(conductor.do_work(1_000).unwrap(), 1);
    assert_eq!(receive_event_type(&mut events), ON_SUBSCRIPTION_READY);
    client
        .add_subscription("aeron:udp?endpoint=127.0.0.1:0|reliable=false", 7)
        .unwrap();
    assert_eq!(conductor.do_work(2_000).unwrap(), 2);
    assert_eq!(conductor.subscriptions().len(), 1);
    assert_eq!(receive_event_type(&mut events), ON_ERROR);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn rejects_conflicting_reliability_for_the_same_session_specific_subscription() {
    let (cnc, path) = temp_cnc("session-reliability-conflict");
    let dir = temp_dir("session-reliability-conflict");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);

    client
        .add_subscription("aeron:udp?endpoint=127.0.0.1:0|session-id=1024", 7)
        .unwrap();
    assert_eq!(conductor.do_work(1_000).unwrap(), 1);
    assert_eq!(receive_event_type(&mut events), ON_SUBSCRIPTION_READY);

    client
        .add_subscription(
            "aeron:udp?endpoint=127.0.0.1:0|session-id=1024|reliable=false",
            7,
        )
        .unwrap();
    assert_eq!(conductor.do_work(2_000).unwrap(), 2);
    assert_eq!(conductor.subscriptions().len(), 1);
    assert_eq!(receive_event_type(&mut events), ON_ERROR);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn allows_conflicting_reliability_for_different_session_specific_subscriptions() {
    let (cnc, path) = temp_cnc("session-reliability");
    let dir = temp_dir("session-reliability");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);

    client
        .add_subscription("aeron:udp?endpoint=127.0.0.1:0|session-id=1024", 7)
        .unwrap();
    client
        .add_subscription(
            "aeron:udp?endpoint=127.0.0.1:0|session-id=1025|reliable=false",
            7,
        )
        .unwrap();
    assert_eq!(conductor.do_work(1_000).unwrap(), 2);
    assert_eq!(conductor.subscriptions().len(), 2);
    assert_eq!(receive_event_type(&mut events), ON_SUBSCRIPTION_READY);
    assert_eq!(receive_event_type(&mut events), ON_SUBSCRIPTION_READY);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn allows_wildcard_and_different_session_reliability_subscriptions() {
    let (cnc, path) = temp_cnc("wildcard-session-reliability");
    let dir = temp_dir("wildcard-session-reliability");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);

    client
        .add_subscription(
            "aeron:udp?endpoint=127.0.0.1:0|session-id=1024|reliable=false",
            7,
        )
        .unwrap();
    client
        .add_subscription("aeron:udp?endpoint=127.0.0.1:0", 7)
        .unwrap();
    client
        .add_subscription(
            "aeron:udp?endpoint=127.0.0.1:0|session-id=1025|reliable=false",
            7,
        )
        .unwrap();
    assert_eq!(conductor.do_work(1_000).unwrap(), 3);
    assert_eq!(conductor.subscriptions().len(), 3);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn reports_unknown_client_commands_to_the_originating_correlation() {
    let (cnc, path) = temp_cnc("unknown-command");
    let dir = temp_dir("unknown-command");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();

    let mut command_bytes = [0u8; aeron_client::command::CORRELATED_MESSAGE_HEADER_LENGTH];
    let command = aeron_client::command::CorrelatedMessageFlyweight::new(
        AtomicBuffer::new(&mut command_bytes),
        0,
    );
    command.set_client_id(CLIENT_ID);
    command.set_correlation_id(77);
    let ring = ManyToOneRingBuffer::new(cnc.to_driver_buffer()).unwrap();
    let command_length = command_bytes.len();
    ring.write(
        932_467_234,
        &AtomicBuffer::new(&mut command_bytes),
        0,
        command_length,
    )
    .unwrap();

    conductor.do_work(1_000).unwrap();
    assert_eq!(conductor.error_count(), 1);
    assert_eq!(
        cnc.counters_values_buffer().get_i64(
            SYSTEM_COUNTER_ID_DRIVER_EXCEPTIONS as usize
                * aeron_client::cnc_file::COUNTER_VALUE_LENGTH,
        ),
        1
    );

    let mut seen_type = 0;
    let mut seen_correlation = 0;
    let mut seen_code = 0;
    let mut seen_message = String::new();
    assert!(
        events
            .receive_next(|msg_type, buf, offset, _len| {
                seen_type = msg_type;
                let error = ErrorResponseFlyweight::wrap(buf.clone(), offset);
                seen_correlation = error.offending_command_correlation_id();
                seen_code = error.error_code();
                seen_message = error.error_message().unwrap();
            })
            .unwrap()
    );
    assert_eq!(seen_type, ON_ERROR);
    assert_eq!(seen_correlation, 77);
    assert_eq!(seen_code, 6);
    assert_eq!(seen_message, "ERROR - command typeId=932467234");

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
fn reports_invalid_channel_and_continues_processing_commands() {
    let (cnc, path) = temp_cnc("invalid-channel");
    let dir = temp_dir("invalid-channel");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let client = proxy(&cnc);

    let invalid_correlation = client.add_publication("aeron:unknown", 7).unwrap();
    assert_eq!(conductor.do_work(1_000).unwrap(), 2);
    assert!(conductor.publications().is_empty());

    let mut seen_type = 0;
    let mut seen_correlation = 0;
    let mut seen_code = 0;
    let mut seen_message = String::new();
    assert!(
        events
            .receive_next(|msg_type, buf, offset, _len| {
                seen_type = msg_type;
                let error = ErrorResponseFlyweight::wrap(buf.clone(), offset);
                seen_correlation = error.offending_command_correlation_id();
                seen_code = error.error_code();
                seen_message = error.error_message().unwrap();
            })
            .unwrap()
    );
    assert_eq!(seen_type, ON_ERROR);
    assert_eq!(seen_correlation, invalid_correlation);
    assert_eq!(seen_code, 1);
    assert!(seen_message.contains("unsupported media"));

    let valid_correlation = client.add_subscription("aeron:ipc", 7).unwrap();
    assert_eq!(conductor.do_work(2_000).unwrap(), 1);
    assert_eq!(conductor.subscriptions().len(), 1);
    let mut ready_correlation = 0;
    assert!(
        events
            .receive_next(|msg_type, buf, offset, _len| {
                assert_eq!(msg_type, ON_SUBSCRIPTION_READY);
                ready_correlation =
                    SubscriptionReadyFlyweight::wrap(buf.clone(), offset).correlation_id();
            })
            .unwrap()
    );
    assert_eq!(ready_correlation, valid_correlation);

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn classifies_command_failures_like_aeron_error_codes() {
    assert_eq!(error_code(&DriverError::InvalidChannel("bad".into())), 1);
    assert_eq!(
        error_code(&DriverError::IllegalState("No space left on device".into())),
        12
    );
    assert_eq!(
        error_code(&DriverError::IllegalArgument(
            "There is not enough space on the disk".into()
        )),
        12
    );
    assert_eq!(error_code(&DriverError::IllegalState("other".into())), 11);
}

#[test]
fn remove_publishes_operation_success_and_drops_registration() {
    let (cnc, path) = temp_cnc("remove");
    let dir = temp_dir("remove");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();

    let client = proxy(&cnc);
    let sub_correlation = client.add_subscription("aeron:ipc", 7).unwrap();
    conductor.do_work(1_000).unwrap();
    let registration_id = *conductor.subscriptions().keys().next().unwrap();

    let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
    let remove_correlation = client.remove_subscription(registration_id).unwrap();
    conductor.do_work(2_000).unwrap();

    assert_eq!(conductor.subscriptions().len(), 0);
    assert_ne!(sub_correlation, remove_correlation);

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

    drop(conductor);
    let _ = std::fs::remove_file(&path);
}

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
fn releases_ipc_subscriber_position_counter_when_subscription_is_removed() {
    let (cnc, path) = temp_cnc("ipc-counter-release");
    let dir = temp_dir("ipc-counter-release");
    let mut conductor = DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
    let client = proxy(&cnc);
    let publication = client.add_publication("aeron:ipc", 7).unwrap();
    let subscription = client.add_subscription("aeron:ipc", 7).unwrap();
    conductor.do_work(1_000).unwrap();
    assert_eq!(conductor.counters().len(), 2);

    client.remove_subscription(subscription).unwrap();
    conductor.do_work(2_000).unwrap();
    assert_eq!(conductor.counters().len(), 1);

    client.remove_publication(publication, false).unwrap();
    conductor.do_work(3_000).unwrap();
    assert!(conductor.counters().is_empty());

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
