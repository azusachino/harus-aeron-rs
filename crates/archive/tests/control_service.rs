use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use aeron_archive::codecs::{
    ConnectRequest, ControlResponse, ControlResponseCode, ExtendRecordingRequestV2,
    FindLastMatchingRecordingRequest, ListRecordingRequest, ListRecordingSubscriptionsRequest,
    ListRecordingsForUriRequest, ListRecordingsRequest, MaxRecordedPositionRequest,
    PROTOCOL_SEMANTIC_VERSION, PurgeRecordingRequest, RecordingPositionRequest, ReplayRequest,
    SourceLocation, StartPositionRequest, StartRecordingRequest, StartRecordingRequestV2,
    StopPositionRequest, StopRecordingSubscriptionRequest, StopReplayRequest,
    TruncateRecordingRequest,
};
use aeron_archive::recording_descriptor::RecordingDescriptor;
use aeron_archive::recording_subscription_descriptor::RecordingSubscriptionDescriptor;
use aeron_archive::service::{ArchiveContext, ArchiveService};
use aeron_client::conductor::ClientConductor;
use aeron_client::driver_proxy::DriverProxy;
use aeron_client::logbuffer::TERM_MIN_LENGTH;
use aeron_driver::configuration::Configuration;
use aeron_driver::driver::MediaDriver;
use agrona::AtomicBuffer;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

const CONTROL_STREAM: i32 = 10;
const RESPONSE_STREAM: i32 = 701;
const RESTART_RESPONSE_STREAM: i32 = 704;
const LIVE_STREAM: i32 = 702;
const REPLAY_STREAM: i32 = 703;
const BOUNDED_REPLAY_STREAM: i32 = 705;
const INVALID_REPLAY_STREAM: i32 = 706;

fn client(driver: &MediaDriver, client_id: i64) -> ClientConductor<'static> {
    let ring = ManyToOneRingBuffer::new(driver.cnc().to_driver_buffer()).expect("driver ring");
    ClientConductor::with_event_buffer(
        DriverProxy::new(ring, client_id),
        driver.cnc().to_clients_buffer(),
    )
    .expect("client")
}

fn offer(client: &ClientConductor<'static>, publication_id: i64, bytes: &mut [u8]) -> bool {
    let length = bytes.len();
    let buffer = AtomicBuffer::new(bytes);
    client
        .publication(publication_id)
        .is_some_and(|publication| {
            publication
                .offer(&buffer, 0, length)
                .is_ok_and(|value| value > 0)
        })
}

fn cycle(
    driver: &mut MediaDriver,
    archive: &mut ArchiveService,
    client: &mut ClientConductor<'static>,
) {
    driver.do_work().expect("driver work");
    for (id, counter) in driver.conductor().counters() {
        client.register_counter(*id, counter.clone());
    }
    archive.do_work().expect("archive work");
    client.do_work().expect("client work");
}

fn await_resources(
    driver: &mut MediaDriver,
    archive: &mut ArchiveService,
    client: &mut ClientConductor<'static>,
    publication_id: i64,
    subscription_id: i64,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        cycle(driver, archive, client);
        if client.publication(publication_id).is_some()
            && client.subscription(subscription_id).is_some()
        {
            return;
        }
    }
    panic!("Aeron resources did not become ready");
}

fn await_response(
    driver: &mut MediaDriver,
    archive: &mut ArchiveService,
    client: &mut ClientConductor<'static>,
    subscription_id: i64,
    correlation_id: i64,
) -> ControlResponse {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        cycle(driver, archive, client);
        let mut response = None;
        if let Some(subscription) = client.subscription_mut(subscription_id) {
            subscription.poll(
                |buffer, offset, length, _| {
                    let mut bytes = vec![0; length];
                    buffer.get_bytes(offset, &mut bytes);
                    if let Ok(candidate) = ControlResponse::decode(&bytes) {
                        if candidate.correlation_id == correlation_id {
                            response = Some(candidate);
                        }
                    }
                },
                10,
            );
        }
        if let Some(response) = response {
            return response;
        }
    }
    panic!("response {correlation_id} did not arrive");
}

fn await_listing(
    driver: &mut MediaDriver,
    archive: &mut ArchiveService,
    client: &mut ClientConductor<'static>,
    subscription_id: i64,
    correlation_id: i64,
) -> (Vec<RecordingDescriptor>, Option<ControlResponse>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut descriptors = Vec::new();
    while Instant::now() < deadline {
        cycle(driver, archive, client);
        let mut terminal = None;
        if let Some(subscription) = client.subscription_mut(subscription_id) {
            subscription.poll(
                |buffer, offset, length, _| {
                    let mut bytes = vec![0; length];
                    buffer.get_bytes(offset, &mut bytes);
                    if let Ok(descriptor) = RecordingDescriptor::decode(&bytes) {
                        if descriptor.correlation_id == correlation_id
                            && !descriptors.iter().any(|existing: &RecordingDescriptor| {
                                existing.recording_id == descriptor.recording_id
                            })
                        {
                            descriptors.push(descriptor);
                        }
                    } else if let Ok(response) = ControlResponse::decode(&bytes) {
                        if response.correlation_id == correlation_id {
                            terminal = Some(response);
                        }
                    }
                },
                20,
            );
        }
        if terminal.is_some() {
            return (descriptors, terminal);
        }
        if !descriptors.is_empty() {
            return (descriptors, None);
        }
    }
    panic!("listing {correlation_id} did not arrive");
}

fn await_recordings(
    driver: &mut MediaDriver,
    archive: &mut ArchiveService,
    client: &mut ClientConductor<'static>,
    subscription_id: i64,
    correlation_id: i64,
    expected_count: usize,
) -> Vec<RecordingDescriptor> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut descriptors = Vec::new();
    while Instant::now() < deadline {
        cycle(driver, archive, client);
        if let Some(subscription) = client.subscription_mut(subscription_id) {
            subscription.poll(
                |buffer, offset, length, _| {
                    let mut bytes = vec![0; length];
                    buffer.get_bytes(offset, &mut bytes);
                    if let Ok(descriptor) = RecordingDescriptor::decode(&bytes)
                        && descriptor.correlation_id == correlation_id
                        && !descriptors.iter().any(|existing: &RecordingDescriptor| {
                            existing.recording_id == descriptor.recording_id
                        })
                    {
                        descriptors.push(descriptor);
                    }
                },
                20,
            );
        }
        if descriptors.len() == expected_count {
            return descriptors;
        }
    }
    panic!(
        "expected {expected_count} recordings for listing {correlation_id}, got {}",
        descriptors.len()
    );
}

fn await_subscription_listing(
    driver: &mut MediaDriver,
    archive: &mut ArchiveService,
    client: &mut ClientConductor<'static>,
    subscription_id: i64,
    correlation_id: i64,
) -> (
    Vec<RecordingSubscriptionDescriptor>,
    Option<ControlResponse>,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut descriptors = Vec::new();
    while Instant::now() < deadline {
        cycle(driver, archive, client);
        let mut terminal = None;
        if let Some(subscription) = client.subscription_mut(subscription_id) {
            subscription.poll(
                |buffer, offset, length, _| {
                    let mut bytes = vec![0; length];
                    buffer.get_bytes(offset, &mut bytes);
                    if let Ok(descriptor) = RecordingSubscriptionDescriptor::decode(&bytes) {
                        if descriptor.correlation_id == correlation_id
                            && !descriptors.iter().any(
                                |existing: &RecordingSubscriptionDescriptor| {
                                    existing.subscription_id == descriptor.subscription_id
                                },
                            )
                        {
                            descriptors.push(descriptor);
                        }
                    } else if let Ok(response) = ControlResponse::decode(&bytes) {
                        if response.correlation_id == correlation_id {
                            terminal = Some(response);
                        }
                    }
                },
                20,
            );
        }
        if terminal.is_some() || !descriptors.is_empty() {
            return (descriptors, terminal);
        }
    }
    panic!("recording subscription listing {correlation_id} did not arrive");
}

fn connect(
    driver: &mut MediaDriver,
    archive: &mut ArchiveService,
    client: &mut ClientConductor<'static>,
    control_publication: i64,
    response_subscription: i64,
    correlation_id: i64,
    response_stream_id: i32,
) -> i64 {
    let mut request = ConnectRequest {
        correlation_id,
        response_stream_id,
        version: PROTOCOL_SEMANTIC_VERSION,
        response_channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("connect encode");
    while !offer(client, control_publication, &mut request) {
        cycle(driver, archive, client);
    }
    await_response(
        driver,
        archive,
        client,
        response_subscription,
        correlation_id,
    )
    .control_session_id
}

fn archive_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "aeron-archive-control-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ))
}

#[test]
fn standalone_archive_process_starts_stops_and_restarts() {
    let archive_dir = archive_dir();
    let _ = std::fs::remove_dir_all(&archive_dir);
    let mut driver = MediaDriver::start(Configuration::default()).expect("running driver");

    for _ in 0..2 {
        let child = Command::new(env!("CARGO_BIN_EXE_aeron-archive"))
            .arg(driver.aeron_dir())
            .arg(&archive_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("start archive process");
        let output = child.wait_with_output().expect("stop archive process");
        assert!(
            output.status.success(),
            "archive stderr was inherited; status={}",
            output.status
        );
        assert_eq!(
            String::from_utf8(output.stdout).expect("utf8 path").trim(),
            archive_dir.to_string_lossy()
        );
    }

    driver.stop().expect("stop driver");
    std::fs::remove_dir_all(archive_dir).expect("cleanup");
}

#[test]
fn extends_truncates_and_purges_a_recording_over_the_control_plane() {
    let archive_dir = archive_dir();
    let _ = std::fs::remove_dir_all(&archive_dir);
    let mut driver = MediaDriver::launch(Configuration {
        ipc_term_buffer_length: TERM_MIN_LENGTH as usize,
        ..Configuration::default()
    })
    .expect("driver");
    let mut archive = ArchiveService::launch(ArchiveContext::new(driver.aeron_dir(), &archive_dir))
        .expect("archive");
    let mut external = client(&driver, 8_002);
    let control_publication = external
        .add_publication("aeron:ipc", CONTROL_STREAM)
        .expect("control publication");
    let response_subscription = external
        .add_subscription("aeron:ipc", RESPONSE_STREAM + 10)
        .expect("response subscription");
    await_resources(
        &mut driver,
        &mut archive,
        &mut external,
        control_publication,
        response_subscription,
    );
    let session_id = connect(
        &mut driver,
        &mut archive,
        &mut external,
        control_publication,
        response_subscription,
        101,
        RESPONSE_STREAM + 10,
    );
    let live_publication = external
        .add_publication("aeron:ipc", LIVE_STREAM + 10)
        .expect("live publication");

    let mut start = StartRecordingRequestV2 {
        control_session_id: session_id,
        correlation_id: 102,
        stream_id: LIVE_STREAM + 10,
        source_location: SourceLocation::Local,
        auto_stop: true,
        channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("start encode");
    while !offer(&external, control_publication, &mut start) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let _first_subscription_id = await_response(
        &mut driver,
        &mut archive,
        &mut external,
        response_subscription,
        102,
    )
    .relevant_id;
    for _ in 0..20 {
        cycle(&mut driver, &mut archive, &mut external);
    }

    let initial_payloads = [b"before-extend-1".to_vec(), b"before-extend-2".to_vec()];
    for payload in &initial_payloads {
        let mut bytes = payload.clone();
        while !offer(&external, live_publication, &mut bytes) {
            cycle(&mut driver, &mut archive, &mut external);
        }
    }
    for _ in 0..20 {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert!(
        external
            .remove_publication(live_publication)
            .expect("remove initial publication")
    );
    for _ in 0..50 {
        cycle(&mut driver, &mut archive, &mut external);
    }

    let mut list = ListRecordingsRequest {
        control_session_id: session_id,
        correlation_id: 103,
        from_recording_id: 0,
        record_count: 1,
    }
    .encode()
    .expect("list encode");
    while !offer(&external, control_publication, &mut list) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let (descriptors, terminal) = await_listing(
        &mut driver,
        &mut archive,
        &mut external,
        response_subscription,
        103,
    );
    assert!(terminal.is_none());
    let recording_id = descriptors[0].recording_id;
    let pre_extend_stop = descriptors[0].stop_position;
    let initial_term_id = descriptors[0].initial_term_id;
    let term_length = descriptors[0].term_buffer_length;
    let term_id = initial_term_id.wrapping_add((pre_extend_stop / i64::from(term_length)) as i32);
    let term_offset = pre_extend_stop % i64::from(term_length);
    let extension_channel = format!(
        "aeron:ipc?initial-term-id={initial_term_id}|term-id={term_id}|term-offset={term_offset}|term-length={term_length}|mtu=1408"
    );
    let extension_publication = external
        .add_publication(&extension_channel, LIVE_STREAM + 10)
        .expect("extension publication");
    for _ in 0..20 {
        cycle(&mut driver, &mut archive, &mut external);
    }

    let mut extend = ExtendRecordingRequestV2 {
        control_session_id: session_id,
        correlation_id: 104,
        recording_id,
        stream_id: LIVE_STREAM + 10,
        source_location: SourceLocation::Local,
        auto_stop: true,
        channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("extend encode");
    while !offer(&external, control_publication, &mut extend) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let _extension_subscription_id = await_response(
        &mut driver,
        &mut archive,
        &mut external,
        response_subscription,
        104,
    )
    .relevant_id;
    for _ in 0..20 {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let extension_payloads = [b"after-extend-1".to_vec(), b"after-extend-2".to_vec()];
    for payload in &extension_payloads {
        let mut bytes = payload.clone();
        while !offer(&external, extension_publication, &mut bytes) {
            cycle(&mut driver, &mut archive, &mut external);
        }
    }
    for _ in 0..20 {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert!(
        external
            .remove_publication(extension_publication)
            .expect("remove extension publication")
    );
    for _ in 0..50 {
        cycle(&mut driver, &mut archive, &mut external);
    }

    let mut stop_position = StopPositionRequest {
        control_session_id: session_id,
        correlation_id: 105,
        recording_id,
    }
    .encode()
    .expect("stop position encode");
    while !offer(&external, control_publication, &mut stop_position) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            105,
        )
        .relevant_id
            > pre_extend_stop
    );

    let mut invalid_truncate = TruncateRecordingRequest {
        control_session_id: session_id,
        correlation_id: 115,
        recording_id,
        position: pre_extend_stop + 1,
    }
    .encode()
    .expect("invalid truncate encode");
    while !offer(&external, control_publication, &mut invalid_truncate) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            115,
        )
        .code,
        ControlResponseCode::Error
    );

    let mut truncate = TruncateRecordingRequest {
        control_session_id: session_id,
        correlation_id: 108,
        recording_id,
        position: pre_extend_stop,
    }
    .encode()
    .expect("truncate encode");
    while !offer(&external, control_publication, &mut truncate) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            108,
        )
        .code,
        ControlResponseCode::Ok
    );

    drop(archive);
    let mut restarted_context = ArchiveContext::new(driver.aeron_dir(), &archive_dir);
    restarted_context.client_id += 1;
    let mut archive = ArchiveService::launch(restarted_context).expect("restart archive");
    let restarted_response_subscription = external
        .add_subscription("aeron:ipc", RESPONSE_STREAM + 11)
        .expect("restart response subscription");
    for _ in 0..20 {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let session_id = connect(
        &mut driver,
        &mut archive,
        &mut external,
        control_publication,
        restarted_response_subscription,
        109,
        RESPONSE_STREAM + 11,
    );
    let replay_subscription = external
        .add_subscription("aeron:ipc", REPLAY_STREAM + 10)
        .expect("replay subscription");
    let mut replay = ReplayRequest {
        control_session_id: session_id,
        correlation_id: 110,
        recording_id,
        position: -1,
        length: -1,
        replay_stream_id: REPLAY_STREAM + 10,
        file_io_max_length: 0,
        replay_token: 0,
        replay_channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("replay encode");
    while !offer(&external, control_publication, &mut replay) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let replay_session_id = await_response(
        &mut driver,
        &mut archive,
        &mut external,
        restarted_response_subscription,
        110,
    )
    .relevant_id;
    assert!(replay_session_id > 0);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut replayed = Vec::new();
    while Instant::now() < deadline && replayed.len() < initial_payloads.len() {
        cycle(&mut driver, &mut archive, &mut external);
        if let Some(subscription) = external.subscription_mut(replay_subscription) {
            subscription.poll(
                |buffer, offset, length, _| {
                    let mut payload = vec![0; length];
                    buffer.get_bytes(offset, &mut payload);
                    replayed.push(payload);
                },
                10,
            );
        }
    }
    assert_eq!(replayed, initial_payloads);
    let mut stop_replay = StopReplayRequest {
        control_session_id: session_id,
        correlation_id: 113,
        replay_session_id,
    }
    .encode()
    .expect("stop replay encode");
    while !offer(&external, control_publication, &mut stop_replay) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            restarted_response_subscription,
            113,
        )
        .code,
        ControlResponseCode::Ok
    );

    let mut purge = PurgeRecordingRequest {
        control_session_id: session_id,
        correlation_id: 111,
        recording_id,
    }
    .encode()
    .expect("purge encode");
    while !offer(&external, control_publication, &mut purge) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let purge_response = await_response(
        &mut driver,
        &mut archive,
        &mut external,
        restarted_response_subscription,
        111,
    );
    assert_eq!(
        purge_response.code,
        ControlResponseCode::Ok,
        "{purge_response:?}"
    );

    drop(archive);
    let mut purged_context = ArchiveContext::new(driver.aeron_dir(), &archive_dir);
    purged_context.client_id += 2;
    let mut archive = ArchiveService::launch(purged_context).expect("restart purged archive");
    let purged_response_subscription = external
        .add_subscription("aeron:ipc", RESPONSE_STREAM + 12)
        .expect("purged response subscription");
    for _ in 0..20 {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let session_id = connect(
        &mut driver,
        &mut archive,
        &mut external,
        control_publication,
        purged_response_subscription,
        114,
        RESPONSE_STREAM + 12,
    );
    let mut list_purged = ListRecordingRequest {
        control_session_id: session_id,
        correlation_id: 112,
        recording_id,
    }
    .encode()
    .expect("list purged encode");
    while !offer(&external, control_publication, &mut list_purged) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            purged_response_subscription,
            112,
        )
        .code,
        ControlResponseCode::RecordingUnknown
    );
    let mut purge_again = PurgeRecordingRequest {
        control_session_id: session_id,
        correlation_id: 116,
        recording_id,
    }
    .encode()
    .expect("purge unknown encode");
    while !offer(&external, control_publication, &mut purge_again) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            purged_response_subscription,
            116,
        )
        .code,
        ControlResponseCode::RecordingUnknown
    );
    assert!(
        std::fs::read_dir(&archive_dir)
            .expect("archive directory")
            .all(|entry| !entry
                .expect("archive entry")
                .file_name()
                .to_string_lossy()
                .starts_with(&format!("{recording_id}-")))
    );

    std::fs::remove_dir_all(archive_dir).expect("cleanup");
}

#[test]
fn controls_record_restart_list_and_replay_over_real_aeron_channels() {
    let archive_dir = archive_dir();
    let _ = std::fs::remove_dir_all(&archive_dir);
    let mut driver = MediaDriver::launch(Configuration {
        ipc_term_buffer_length: TERM_MIN_LENGTH as usize,
        ..Configuration::default()
    })
    .expect("driver");

    let context = ArchiveContext::new(driver.aeron_dir(), &archive_dir);
    let mut archive = ArchiveService::launch(context).expect("archive");
    let mut external = client(&driver, 8_001);
    let control_publication = external
        .add_publication("aeron:ipc", CONTROL_STREAM)
        .expect("control publication");
    let response_subscription = external
        .add_subscription("aeron:ipc", RESPONSE_STREAM)
        .expect("response subscription");
    await_resources(
        &mut driver,
        &mut archive,
        &mut external,
        control_publication,
        response_subscription,
    );
    let session_id = connect(
        &mut driver,
        &mut archive,
        &mut external,
        control_publication,
        response_subscription,
        1,
        RESPONSE_STREAM,
    );

    let live_publication = external
        .add_publication("aeron:ipc", LIVE_STREAM)
        .expect("live publication");
    let mut start = StartRecordingRequest {
        control_session_id: session_id,
        correlation_id: 2,
        stream_id: LIVE_STREAM,
        source_location: SourceLocation::Local,
        channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("start encode");
    while !offer(&external, control_publication, &mut start) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let recording_subscription_id = await_response(
        &mut driver,
        &mut archive,
        &mut external,
        response_subscription,
        2,
    )
    .relevant_id;

    let mut list_subscriptions = ListRecordingSubscriptionsRequest {
        control_session_id: session_id,
        correlation_id: 13,
        pseudo_index: 0,
        subscription_count: 2,
        apply_stream_id: true,
        stream_id: LIVE_STREAM,
        channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("list recording subscriptions encode");
    while !offer(&external, control_publication, &mut list_subscriptions) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let (subscriptions, terminal) = await_subscription_listing(
        &mut driver,
        &mut archive,
        &mut external,
        response_subscription,
        13,
    );
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].subscription_id, recording_subscription_id);
    assert_eq!(subscriptions[0].stream_id, LIVE_STREAM);
    assert_eq!(subscriptions[0].stripped_channel, "aeron:ipc");
    assert_eq!(
        terminal.expect("subscription exhaustion").code,
        ControlResponseCode::SubscriptionUnknown
    );

    let payloads = [
        b"managed-archive-control-plane".to_vec(),
        b"bounded-second".to_vec(),
        b"stoppable-third".to_vec(),
    ];
    for payload in &payloads {
        let mut payload_bytes = payload.clone();
        while !offer(&external, live_publication, &mut payload_bytes) {
            cycle(&mut driver, &mut archive, &mut external);
        }
    }
    for _ in 0..20 {
        cycle(&mut driver, &mut archive, &mut external);
    }

    let mut list_active = ListRecordingsRequest {
        control_session_id: session_id,
        correlation_id: 17,
        from_recording_id: 0,
        record_count: 1,
    }
    .encode()
    .expect("list active recording encode");
    while !offer(&external, control_publication, &mut list_active) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let (active_descriptors, terminal) = await_listing(
        &mut driver,
        &mut archive,
        &mut external,
        response_subscription,
        17,
    );
    assert!(terminal.is_none());
    let active_descriptor = &active_descriptors[0];
    let recording_id = active_descriptor.recording_id;

    let mut start_position = StartPositionRequest {
        control_session_id: session_id,
        correlation_id: 18,
        recording_id,
    }
    .encode()
    .expect("start position encode");
    while !offer(&external, control_publication, &mut start_position) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            18,
        )
        .relevant_id,
        active_descriptor.start_position
    );

    let mut stop_position = StopPositionRequest {
        control_session_id: session_id,
        correlation_id: 19,
        recording_id,
    }
    .encode()
    .expect("active stop position encode");
    while !offer(&external, control_publication, &mut stop_position) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            19,
        )
        .relevant_id,
        -1
    );

    let mut unknown_recording_position = RecordingPositionRequest {
        control_session_id: session_id,
        correlation_id: 26,
        recording_id: recording_id + 1,
    }
    .encode()
    .expect("unknown recording position encode");
    while !offer(
        &external,
        control_publication,
        &mut unknown_recording_position,
    ) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            26,
        )
        .code,
        ControlResponseCode::RecordingUnknown
    );

    let mut max_position = MaxRecordedPositionRequest {
        control_session_id: session_id,
        correlation_id: 20,
        recording_id,
    }
    .encode()
    .expect("max recorded position encode");
    while !offer(&external, control_publication, &mut max_position) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            20,
        )
        .relevant_id
            > active_descriptor.start_position
    );

    let mut recording_position = RecordingPositionRequest {
        control_session_id: session_id,
        correlation_id: 24,
        recording_id,
    }
    .encode()
    .expect("recording position encode");
    while !offer(&external, control_publication, &mut recording_position) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            24,
        )
        .relevant_id
            > active_descriptor.start_position
    );

    let mut find_last = FindLastMatchingRecordingRequest {
        control_session_id: session_id,
        correlation_id: 21,
        min_recording_id: recording_id,
        session_id: active_descriptor.session_id,
        stream_id: LIVE_STREAM,
        channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("find last matching recording encode");
    while !offer(&external, control_publication, &mut find_last) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            21,
        )
        .relevant_id,
        recording_id
    );

    let mut stop = StopRecordingSubscriptionRequest {
        control_session_id: session_id,
        correlation_id: 3,
        subscription_id: recording_subscription_id,
    }
    .encode()
    .expect("stop encode");
    while !offer(&external, control_publication, &mut stop) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            3,
        )
        .code,
        ControlResponseCode::Ok
    );

    let mut stop_again = StopRecordingSubscriptionRequest {
        control_session_id: session_id,
        correlation_id: 15,
        subscription_id: recording_subscription_id,
    }
    .encode()
    .expect("stop recording subscription again encode");
    while !offer(&external, control_publication, &mut stop_again) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            15,
        )
        .code,
        ControlResponseCode::SubscriptionUnknown
    );

    let mut list_stopped_subscription = ListRecordingSubscriptionsRequest {
        control_session_id: session_id,
        correlation_id: 16,
        pseudo_index: 0,
        subscription_count: 1,
        apply_stream_id: false,
        stream_id: 0,
        channel: String::new(),
    }
    .encode()
    .expect("list stopped subscription encode");
    while !offer(
        &external,
        control_publication,
        &mut list_stopped_subscription,
    ) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            16,
        )
        .code,
        ControlResponseCode::SubscriptionUnknown
    );

    let mut list_recordings = ListRecordingsRequest {
        control_session_id: session_id,
        correlation_id: 14,
        from_recording_id: 0,
        record_count: 1,
    }
    .encode()
    .expect("list recordings encode");
    while !offer(&external, control_publication, &mut list_recordings) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let (recordings, terminal) = await_listing(
        &mut driver,
        &mut archive,
        &mut external,
        response_subscription,
        14,
    );
    assert!(terminal.is_none());
    assert_eq!(recordings.len(), 1);
    assert_eq!(recordings[0].recording_id, recording_id);

    let mut stopped_recording_position = RecordingPositionRequest {
        control_session_id: session_id,
        correlation_id: 25,
        recording_id,
    }
    .encode()
    .expect("stopped recording position encode");
    while !offer(
        &external,
        control_publication,
        &mut stopped_recording_position,
    ) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            25,
        )
        .relevant_id,
        -1
    );

    let mut stopped_max_position = MaxRecordedPositionRequest {
        control_session_id: session_id,
        correlation_id: 22,
        recording_id,
    }
    .encode()
    .expect("stopped max position encode");
    while !offer(&external, control_publication, &mut stopped_max_position) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            22,
        )
        .relevant_id,
        recordings[0].stop_position
    );

    drop(archive);
    let mut restarted_context = ArchiveContext::new(driver.aeron_dir(), &archive_dir);
    restarted_context.client_id += 1;
    let mut archive = ArchiveService::launch(restarted_context).expect("restart archive");
    let restart_response_subscription = external
        .add_subscription("aeron:ipc", RESTART_RESPONSE_STREAM)
        .expect("restart response subscription");
    for _ in 0..20 {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let session_id = connect(
        &mut driver,
        &mut archive,
        &mut external,
        control_publication,
        restart_response_subscription,
        4,
        RESTART_RESPONSE_STREAM,
    );
    let mut list = ListRecordingRequest {
        control_session_id: session_id,
        correlation_id: 5,
        recording_id,
    }
    .encode()
    .expect("list encode");
    while !offer(&external, control_publication, &mut list) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let (descriptors, terminal) = await_listing(
        &mut driver,
        &mut archive,
        &mut external,
        restart_response_subscription,
        5,
    );
    assert!(terminal.is_none());
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0].recording_id, recording_id);
    assert_eq!(descriptors[0].control_session_id, session_id);
    assert_eq!(descriptors[0].correlation_id, 5);
    assert_eq!(descriptors[0].stream_id, LIVE_STREAM);
    assert_eq!(descriptors[0].original_channel, "aeron:ipc");
    assert!(descriptors[0].stop_position > 0);

    let mut persisted_stop_position = StopPositionRequest {
        control_session_id: session_id,
        correlation_id: 23,
        recording_id,
    }
    .encode()
    .expect("persisted stop position encode");
    while !offer(&external, control_publication, &mut persisted_stop_position) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            restart_response_subscription,
            23,
        )
        .relevant_id,
        descriptors[0].stop_position
    );

    let mut list_range = ListRecordingsRequest {
        control_session_id: session_id,
        correlation_id: 7,
        from_recording_id: recording_id,
        record_count: 2,
    }
    .encode()
    .expect("list range encode");
    while !offer(&external, control_publication, &mut list_range) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let (descriptors, terminal) = await_listing(
        &mut driver,
        &mut archive,
        &mut external,
        restart_response_subscription,
        7,
    );
    assert_eq!(descriptors.len(), 1);
    let terminal = terminal.expect("range exhaustion response");
    assert_eq!(terminal.code, ControlResponseCode::RecordingUnknown);
    assert_eq!(terminal.relevant_id, recording_id + 1);

    let mut list_uri = ListRecordingsForUriRequest {
        control_session_id: session_id,
        correlation_id: 8,
        from_recording_id: recording_id,
        record_count: 1,
        stream_id: LIVE_STREAM,
        channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("list uri encode");
    while !offer(&external, control_publication, &mut list_uri) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let (descriptors, terminal) = await_listing(
        &mut driver,
        &mut archive,
        &mut external,
        restart_response_subscription,
        8,
    );
    assert!(terminal.is_none());
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0].recording_id, recording_id);

    let replay_subscription = external
        .add_subscription("aeron:ipc", REPLAY_STREAM)
        .expect("replay subscription");
    let mut replay = ReplayRequest {
        control_session_id: session_id,
        correlation_id: 6,
        recording_id,
        position: 0,
        length: i64::MAX,
        replay_stream_id: REPLAY_STREAM,
        file_io_max_length: 0,
        replay_token: 0,
        replay_channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("replay encode");
    while !offer(&external, control_publication, &mut replay) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let replay_session_id = await_response(
        &mut driver,
        &mut archive,
        &mut external,
        restart_response_subscription,
        6,
    )
    .relevant_id;
    let mut stop_replay = StopReplayRequest {
        control_session_id: session_id,
        correlation_id: 9,
        replay_session_id,
    }
    .encode()
    .expect("stop replay encode");
    while !offer(&external, control_publication, &mut stop_replay) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let stop_response = await_response(
        &mut driver,
        &mut archive,
        &mut external,
        restart_response_subscription,
        9,
    );
    assert_eq!(stop_response.code, ControlResponseCode::Ok);
    assert_eq!(stop_response.relevant_id, 0);

    let mut replayed = Vec::new();
    if let Some(subscription) = external.subscription_mut(replay_subscription) {
        subscription.poll(
            |buffer, offset, length, _| {
                let mut payload = vec![0; length];
                buffer.get_bytes(offset, &mut payload);
                replayed.push(payload);
            },
            10,
        );
    }
    assert_eq!(replayed, vec![payloads[0].clone()]);

    let bounded_subscription = external
        .add_subscription("aeron:ipc", BOUNDED_REPLAY_STREAM)
        .expect("bounded replay subscription");
    let mut bounded_replay = ReplayRequest {
        control_session_id: session_id,
        correlation_id: 10,
        recording_id,
        position: 64,
        length: 64,
        replay_stream_id: BOUNDED_REPLAY_STREAM,
        file_io_max_length: 0,
        replay_token: 0,
        replay_channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("bounded replay encode");
    while !offer(&external, control_publication, &mut bounded_replay) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let bounded_session_id = await_response(
        &mut driver,
        &mut archive,
        &mut external,
        restart_response_subscription,
        10,
    )
    .relevant_id;
    assert_ne!(bounded_session_id, replay_session_id);
    for _ in 0..5 {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let mut bounded_payloads = Vec::new();
    if let Some(subscription) = external.subscription_mut(bounded_subscription) {
        subscription.poll(
            |buffer, offset, length, _| {
                let mut payload = vec![0; length];
                buffer.get_bytes(offset, &mut payload);
                bounded_payloads.push(payload);
            },
            10,
        );
    }
    assert_eq!(bounded_payloads, vec![payloads[1].clone()]);

    let mut stop_completed_replay = StopReplayRequest {
        control_session_id: session_id,
        correlation_id: 11,
        replay_session_id: bounded_session_id,
    }
    .encode()
    .expect("stop completed replay encode");
    while !offer(&external, control_publication, &mut stop_completed_replay) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            restart_response_subscription,
            11,
        )
        .code,
        ControlResponseCode::Ok
    );

    let _invalid_subscription = external
        .add_subscription("aeron:ipc", INVALID_REPLAY_STREAM)
        .expect("invalid replay subscription");
    let mut invalid_replay = ReplayRequest {
        control_session_id: session_id,
        correlation_id: 12,
        recording_id,
        position: 1,
        length: 64,
        replay_stream_id: INVALID_REPLAY_STREAM,
        file_io_max_length: 0,
        replay_token: 0,
        replay_channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("invalid replay encode");
    while !offer(&external, control_publication, &mut invalid_replay) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            restart_response_subscription,
            12,
        )
        .code,
        ControlResponseCode::Error
    );

    std::fs::remove_dir_all(archive_dir).expect("cleanup");
}

#[test]
fn records_each_joined_image_under_one_shared_subscription() {
    const SHARED_STREAM: i32 = 707;
    const SHARED_RESPONSE_STREAM: i32 = 708;
    let archive_dir = archive_dir();
    let _ = std::fs::remove_dir_all(&archive_dir);
    let mut driver = MediaDriver::launch(Configuration {
        ipc_term_buffer_length: TERM_MIN_LENGTH as usize,
        ..Configuration::default()
    })
    .expect("driver");
    let context = ArchiveContext::new(driver.aeron_dir(), &archive_dir);
    let mut archive = ArchiveService::launch(context).expect("archive");
    let mut external = client(&driver, 8_101);
    let control_publication = external
        .add_publication("aeron:ipc", CONTROL_STREAM)
        .expect("control publication");
    let response_subscription = external
        .add_subscription("aeron:ipc", SHARED_RESPONSE_STREAM)
        .expect("response subscription");
    await_resources(
        &mut driver,
        &mut archive,
        &mut external,
        control_publication,
        response_subscription,
    );
    let session_id = connect(
        &mut driver,
        &mut archive,
        &mut external,
        control_publication,
        response_subscription,
        101,
        SHARED_RESPONSE_STREAM,
    );

    let first_publication = external
        .add_publication("aeron:ipc", SHARED_STREAM)
        .expect("first publication");
    let second_publication = external
        .add_publication("aeron:ipc", SHARED_STREAM)
        .expect("second publication");
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline
        && (external.publication(first_publication).is_none()
            || external.publication(second_publication).is_none())
    {
        cycle(&mut driver, &mut archive, &mut external);
    }

    let mut start = StartRecordingRequest {
        control_session_id: session_id,
        correlation_id: 102,
        stream_id: SHARED_STREAM,
        source_location: SourceLocation::Local,
        channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("start encode");
    while !offer(&external, control_publication, &mut start) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let recording_subscription_id = await_response(
        &mut driver,
        &mut archive,
        &mut external,
        response_subscription,
        102,
    )
    .relevant_id;

    let mut first_payload = b"first-image".to_vec();
    while !offer(&external, first_publication, &mut first_payload) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let mut second_payload = b"second-image".to_vec();
    while !offer(&external, second_publication, &mut second_payload) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    for _ in 0..40 {
        cycle(&mut driver, &mut archive, &mut external);
    }

    let mut list = ListRecordingsRequest {
        control_session_id: session_id,
        correlation_id: 103,
        from_recording_id: 0,
        record_count: 2,
    }
    .encode()
    .expect("list recordings encode");
    while !offer(&external, control_publication, &mut list) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let active_recordings = await_recordings(
        &mut driver,
        &mut archive,
        &mut external,
        response_subscription,
        103,
        2,
    );
    assert!(
        active_recordings
            .iter()
            .all(|descriptor| descriptor.stop_position == -1)
    );
    assert_ne!(
        active_recordings[0].session_id, active_recordings[1].session_id,
        "joined images must own distinct recording sessions"
    );

    let mut stop = StopRecordingSubscriptionRequest {
        control_session_id: session_id,
        correlation_id: 104,
        subscription_id: recording_subscription_id,
    }
    .encode()
    .expect("stop encode");
    while !offer(&external, control_publication, &mut stop) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert_eq!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            104,
        )
        .code,
        ControlResponseCode::Ok
    );

    let mut list_stopped = ListRecordingsRequest {
        control_session_id: session_id,
        correlation_id: 105,
        from_recording_id: 0,
        record_count: 2,
    }
    .encode()
    .expect("stopped list recordings encode");
    while !offer(&external, control_publication, &mut list_stopped) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    let stopped_recordings = await_recordings(
        &mut driver,
        &mut archive,
        &mut external,
        response_subscription,
        105,
        2,
    );
    assert!(
        stopped_recordings
            .iter()
            .all(|descriptor| descriptor.stop_position > descriptor.start_position)
    );

    std::fs::remove_dir_all(archive_dir).expect("cleanup");
}
