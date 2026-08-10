use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use aeron_archive::runtime::ArchiveRuntime;
use aeron_client::conductor::ClientConductor;
use aeron_client::driver_proxy::DriverProxy;
use aeron_client::image::Image;
use aeron_client::log_buffers::LogBuffers;
use aeron_client::logbuffer::{
    LOG_DEFAULT_FRAME_HEADER_OFFSET, TERM_MIN_LENGTH, initialise_tail_with_term_id,
    set_active_term_count, set_initial_term_id, set_is_connected, set_mtu_length,
};
use aeron_client::protocol::{DataHeaderFlyweight, HDR_TYPE_DATA};
use aeron_client::publication::ConcurrentPublication;
use aeron_client::subscription::Subscription;
use aeron_driver::configuration::Configuration;
use aeron_driver::driver::MediaDriver;
use agrona::AtomicBuffer;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

fn client(driver: &MediaDriver, client_id: i64) -> ClientConductor<'static> {
    let ring = ManyToOneRingBuffer::new(driver.cnc().to_driver_buffer()).expect("driver ring");
    ClientConductor::with_event_buffer(
        DriverProxy::new(ring, client_id),
        driver.cnc().to_clients_buffer(),
    )
    .expect("client")
}

fn service(driver: &mut MediaDriver, clients: &mut [&mut ClientConductor<'static>]) {
    driver.do_work().expect("driver work");
    for (id, counter) in driver.conductor().counters() {
        for client in clients.iter_mut() {
            client.register_counter(*id, counter.clone());
        }
    }
    for client in clients {
        client.do_work().expect("client work");
    }
}

fn archive_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "aeron-archive-runtime-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ))
}

fn test_log_buffers(session_id: i32) -> LogBuffers {
    let log = LogBuffers::from_zeroed(TERM_MIN_LENGTH, 4096).expect("log");
    let metadata = log.meta_data_buffer();
    set_is_connected(&metadata, true);
    set_mtu_length(&metadata, 4096);
    set_initial_term_id(&metadata, 1);
    initialise_tail_with_term_id(&metadata, 0, 1);
    set_active_term_count(&metadata, 0);
    let mut header_bytes = [0; aeron_client::logbuffer::HEADER_LENGTH as usize];
    let header = DataHeaderFlyweight::new(AtomicBuffer::new(&mut header_bytes));
    header.set_header_type(HDR_TYPE_DATA);
    header.set_session_id(session_id);
    header.set_stream_id(9100);
    metadata.put_bytes(LOG_DEFAULT_FRAME_HEADER_OFFSET, &header_bytes);
    log
}

#[test]
fn records_each_image_into_its_own_session_under_one_subscription() {
    const STREAM_ID: i32 = 9100;
    let archive_dir = archive_dir();
    let _ = std::fs::remove_dir_all(&archive_dir);

    let first_log = test_log_buffers(41);
    let second_log = test_log_buffers(42);
    let first_publication = ConcurrentPublication::new(
        "aeron:ipc".into(),
        STREAM_ID,
        41,
        1,
        1,
        1,
        Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 2) as i64)),
        first_log.clone(),
    );
    let second_publication = ConcurrentPublication::new(
        "aeron:ipc".into(),
        STREAM_ID,
        42,
        1,
        2,
        2,
        Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 2) as i64)),
        second_log.clone(),
    );
    let mut subscription = Subscription::new(7, STREAM_ID, "aeron:ipc".into());
    subscription.add_image(Image::new(
        7,
        41,
        1,
        0,
        Arc::new(AtomicI64::new(0)),
        first_log,
        "first".into(),
        1001,
    ));
    subscription.add_image(Image::new(
        7,
        42,
        1,
        0,
        Arc::new(AtomicI64::new(0)),
        second_log,
        "second".into(),
        1002,
    ));

    let mut archive = ArchiveRuntime::open(&archive_dir).expect("archive open");
    let first_recording = archive
        .start_recording(&subscription, 1001)
        .expect("first recording");
    let second_recording = archive
        .start_recording(&subscription, 1002)
        .expect("second recording");
    assert_ne!(first_recording, second_recording);

    let mut first_payload = b"first-image".to_vec();
    let first_payload_length = first_payload.len();
    let first_buffer = AtomicBuffer::new(&mut first_payload);
    assert!(
        first_publication
            .offer(&first_buffer, 0, first_payload_length)
            .expect("first offer")
            > 0
    );
    let mut second_payload = b"second-image".to_vec();
    let second_payload_length = second_payload.len();
    let second_buffer = AtomicBuffer::new(&mut second_payload);
    assert!(
        second_publication
            .offer(&second_buffer, 0, second_payload_length)
            .expect("second offer")
            > 0
    );

    assert_eq!(
        archive
            .poll_recording(first_recording, &mut subscription, 1001, 10)
            .expect("first poll"),
        1
    );
    assert_eq!(
        archive
            .poll_recording(second_recording, &mut subscription, 1002, 10)
            .expect("second poll"),
        1
    );
    let first_stop = archive.stop_recording(first_recording).expect("first stop");
    let second_stop = archive
        .stop_recording(second_recording)
        .expect("second stop");
    assert!(first_stop > 0);
    assert!(second_stop > 0);

    let first_descriptor = archive
        .catalog()
        .get_recording(first_recording)
        .expect("first descriptor");
    let second_descriptor = archive
        .catalog()
        .get_recording(second_recording)
        .expect("second descriptor");
    assert_eq!(first_descriptor.session_id, 41);
    assert_eq!(second_descriptor.session_id, 42);

    std::fs::remove_dir_all(archive_dir).expect("cleanup");
}

#[test]
fn records_persists_restarts_and_replays_through_real_driver_clients() {
    const LIVE_STREAM: i32 = 9101;
    const REPLAY_STREAM: i32 = 9102;
    const ACTIVE_REPLAY_STREAM: i32 = 9103;
    let archive_dir = archive_dir();
    let _ = std::fs::remove_dir_all(&archive_dir);

    let mut driver = MediaDriver::launch(Configuration {
        ipc_term_buffer_length: TERM_MIN_LENGTH as usize,
        ..Configuration::default()
    })
    .expect("driver");
    let mut publisher = client(&driver, 1);
    let mut archive_client = client(&driver, 2);
    let publication_id = publisher
        .add_publication("aeron:ipc", LIVE_STREAM)
        .expect("publication");
    let recording_subscription_id = archive_client
        .add_subscription("aeron:ipc", LIVE_STREAM)
        .expect("recording subscription");
    service(&mut driver, &mut [&mut publisher, &mut archive_client]);

    let mut archive = ArchiveRuntime::open(&archive_dir).expect("archive open");
    let recording_image_correlation_id = archive_client
        .subscription(recording_subscription_id)
        .expect("recording subscription")
        .images()
        .first()
        .expect("recording image")
        .correlation_id();
    let recording_id = archive
        .start_recording(
            archive_client
                .subscription(recording_subscription_id)
                .expect("recording image"),
            recording_image_correlation_id,
        )
        .expect("start recording");

    let active_replay_publication_id = publisher
        .add_publication("aeron:ipc", ACTIVE_REPLAY_STREAM)
        .expect("active replay publication");
    let active_replay_subscription_id = archive_client
        .add_subscription("aeron:ipc", ACTIVE_REPLAY_STREAM)
        .expect("active replay subscription");
    service(&mut driver, &mut [&mut publisher, &mut archive_client]);

    let payloads: [&[u8]; 3] = [b"first", b"second-message", b"third"];
    for payload in payloads {
        let mut bytes = payload.to_vec();
        let length = bytes.len();
        let buffer = AtomicBuffer::new(&mut bytes);
        assert!(
            publisher
                .publication(publication_id)
                .expect("publication ready")
                .offer(&buffer, 0, length)
                .expect("offer")
                > 0
        );
    }
    assert_eq!(
        archive
            .poll_recording(
                recording_id,
                archive_client
                    .subscription_mut(recording_subscription_id)
                    .expect("recording subscription"),
                recording_image_correlation_id,
                10,
            )
            .expect("record"),
        payloads.len()
    );

    let mut active_replay = archive
        .start_replay(recording_id, -1, -1)
        .expect("replay active recording snapshot");
    assert_eq!(
        active_replay.limit(),
        archive
            .recording_position(recording_id)
            .expect("active recording position")
    );
    assert_eq!(
        active_replay
            .poll(
                publisher
                    .publication(active_replay_publication_id)
                    .expect("active replay publication ready"),
                10,
            )
            .expect("poll active replay"),
        payloads.len()
    );
    let mut active_replayed = Vec::new();
    assert_eq!(
        archive_client
            .subscription_mut(active_replay_subscription_id)
            .expect("active replay subscription ready")
            .poll(
                |buffer, offset, length, _| {
                    let mut bytes = vec![0; length];
                    buffer.get_bytes(offset, &mut bytes);
                    active_replayed.push(bytes);
                },
                10,
            ),
        payloads.len()
    );
    assert_eq!(
        active_replayed,
        payloads
            .iter()
            .map(|payload| payload.to_vec())
            .collect::<Vec<_>>()
    );

    let stop_position = archive.stop_recording(recording_id).expect("stop");
    assert!(stop_position > 0);
    drop(archive);

    let archive = ArchiveRuntime::open(&archive_dir).expect("restart archive");
    let descriptor = archive
        .catalog()
        .get_recording(recording_id)
        .expect("persisted descriptor");
    assert_eq!(descriptor.stop_position, stop_position);
    assert!(archive_dir.join(descriptor.segment_file_name(0)).exists());

    let mut replay_publisher = client(&driver, 3);
    let mut replay_subscriber = client(&driver, 4);
    let replay_publication_id = replay_publisher
        .add_publication("aeron:ipc", REPLAY_STREAM)
        .expect("replay publication");
    let replay_subscription_id = replay_subscriber
        .add_subscription("aeron:ipc", REPLAY_STREAM)
        .expect("replay subscription");
    service(
        &mut driver,
        &mut [&mut replay_publisher, &mut replay_subscriber],
    );
    assert_eq!(
        archive
            .replay(
                recording_id,
                replay_publisher
                    .publication(replay_publication_id)
                    .expect("replay publication ready"),
            )
            .expect("replay"),
        payloads.len()
    );

    let mut replayed = Vec::new();
    assert_eq!(
        replay_subscriber
            .subscription_mut(replay_subscription_id)
            .expect("replay subscription ready")
            .poll(
                |buffer, offset, length, _| {
                    let mut bytes = vec![0; length];
                    buffer.get_bytes(offset, &mut bytes);
                    replayed.push(bytes);
                },
                10,
            ),
        payloads.len()
    );
    assert_eq!(
        replayed,
        payloads
            .iter()
            .map(|payload| payload.to_vec())
            .collect::<Vec<_>>()
    );

    std::fs::remove_dir_all(archive_dir).expect("cleanup");
}
