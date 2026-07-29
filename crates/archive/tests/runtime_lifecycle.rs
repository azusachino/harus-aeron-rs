use std::path::PathBuf;

use aeron_archive::runtime::ArchiveRuntime;
use aeron_client::conductor::ClientConductor;
use aeron_client::driver_proxy::DriverProxy;
use aeron_client::logbuffer::TERM_MIN_LENGTH;
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

#[test]
fn records_persists_restarts_and_replays_through_real_driver_clients() {
    const LIVE_STREAM: i32 = 9101;
    const REPLAY_STREAM: i32 = 9102;
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
    let recording_id = archive
        .start_recording(
            archive_client
                .subscription(recording_subscription_id)
                .expect("recording image"),
        )
        .expect("start recording");

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
                10,
            )
            .expect("record"),
        payloads.len()
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
