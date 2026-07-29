use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use aeron_archive::codecs::{
    ConnectRequest, ControlResponse, ControlResponseCode, ListRecordingRequest,
    ListRecordingsForUriRequest, ListRecordingsRequest, PROTOCOL_SEMANTIC_VERSION, ReplayRequest,
    SourceLocation, StartRecordingRequest, StopRecordingRequest,
};
use aeron_archive::recording_descriptor::RecordingDescriptor;
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
    let recording_id = await_response(
        &mut driver,
        &mut archive,
        &mut external,
        response_subscription,
        2,
    )
    .relevant_id;

    let payload = b"managed-archive-control-plane".to_vec();
    let mut payload_bytes = payload.clone();
    while !offer(&external, live_publication, &mut payload_bytes) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    for _ in 0..20 {
        cycle(&mut driver, &mut archive, &mut external);
    }

    let mut stop = StopRecordingRequest {
        control_session_id: session_id,
        correlation_id: 3,
        stream_id: LIVE_STREAM,
        channel: "aeron:ipc".into(),
    }
    .encode()
    .expect("stop encode");
    while !offer(&external, control_publication, &mut stop) {
        cycle(&mut driver, &mut archive, &mut external);
    }
    assert!(
        await_response(
            &mut driver,
            &mut archive,
            &mut external,
            response_subscription,
            3,
        )
        .relevant_id
            > 0
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
    let _ = await_response(
        &mut driver,
        &mut archive,
        &mut external,
        restart_response_subscription,
        6,
    );
    let mut replayed = Vec::new();
    if let Some(subscription) = external.subscription_mut(replay_subscription) {
        subscription.poll(
            |buffer, offset, length, _| {
                replayed.resize(length, 0);
                buffer.get_bytes(offset, &mut replayed);
            },
            10,
        );
    }
    assert_eq!(replayed, payload);

    std::fs::remove_dir_all(archive_dir).expect("cleanup");
}
