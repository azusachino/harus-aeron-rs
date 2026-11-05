use super::super::*;
use super::fixtures::*;
use aeron_client::driver_events::{
    CounterUpdateFlyweight, ErrorResponseFlyweight, NextAvailableSessionIdFlyweight,
    ON_COUNTER_READY, ON_ERROR, ON_NEXT_AVAILABLE_SESSION_ID, ON_SUBSCRIPTION_READY,
    ON_UNAVAILABLE_COUNTER,
};
use agrona::AtomicBuffer;
use agrona::BroadcastReceiver;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

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
