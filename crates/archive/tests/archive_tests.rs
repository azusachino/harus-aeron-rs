//! Integration tests for aeron-archive catalog, SBE codecs, and conductor lifecycle.

use aeron_archive::catalog::Catalog;
use aeron_archive::codecs::{
    ControlRequestHeader, ControlResponseCode, ControlResponseHeader, MessageHeader, SBE_SCHEMA_ID,
    SBE_SCHEMA_VERSION,
};
use aeron_archive::conductor::ArchiveConductor;
use aeron_archive::control_session::ControlSession;
use aeron_archive::recording_descriptor::RecordingDescriptor;
use agrona::atomic_buffer::AtomicBuffer;

#[test]
fn test_sbe_message_header_roundtrip() {
    let mut backing = [0u8; 32];
    let buffer = AtomicBuffer::new(&mut backing);

    let header = MessageHeader {
        block_length: 24,
        template_id: 1,
        schema_id: SBE_SCHEMA_ID,
        version: SBE_SCHEMA_VERSION,
    };
    header.encode(&buffer, 0);

    let decoded = MessageHeader::decode(&buffer, 0);
    assert_eq!(decoded.block_length, 24);
    assert_eq!(decoded.template_id, 1);
    assert_eq!(decoded.schema_id, SBE_SCHEMA_ID);
    assert_eq!(decoded.version, SBE_SCHEMA_VERSION);
}

#[test]
fn test_control_headers_encode_decode() {
    let mut backing = [0u8; 64];
    let buffer = AtomicBuffer::new(&mut backing);

    let req = ControlRequestHeader {
        control_session_id: 100,
        correlation_id: 200,
        template_id: 3,
        schema_id: SBE_SCHEMA_ID as i16,
        version: SBE_SCHEMA_VERSION as i16,
    };
    req.encode(&buffer, 0);
    let decoded_req = ControlRequestHeader::decode(&buffer, 0);
    assert_eq!(decoded_req, req);

    let resp = ControlResponseHeader {
        control_session_id: 100,
        correlation_id: 200,
        relevant_id: 1,
        code: ControlResponseCode::Ok as i32,
        _pad: 0,
    };
    resp.encode(&buffer, 24);
    let decoded_resp = ControlResponseHeader::decode(&buffer, 24);
    assert_eq!(decoded_resp, resp);
}

#[test]
fn test_catalog_file_persistence_roundtrip() {
    let temp_dir = std::env::temp_dir();
    let catalog_path = temp_dir.join(format!(
        "test-catalog-{}-{:?}.dat",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_file(&catalog_path);

    let mut catalog = Catalog::new();
    let rec_id1 = catalog.allocate_recording_id();
    let desc1 = RecordingDescriptor::new(
        1,
        100,
        rec_id1,
        0,
        1024,
        1000,
        2000,
        0,
        128 * 1024 * 1024,
        64 * 1024,
        1408,
        10,
        1001,
        "aeron:ipc".into(),
        "aeron:ipc".into(),
        "source1".into(),
    );
    catalog.add_recording(desc1.clone());

    let rec_id2 = catalog.allocate_recording_id();
    let desc2 = RecordingDescriptor::new(
        1,
        101,
        rec_id2,
        0,
        2048,
        1000,
        3000,
        0,
        128 * 1024 * 1024,
        64 * 1024,
        1408,
        10,
        1002,
        "aeron:udp?endpoint=127.0.0.1:40123".into(),
        "aeron:udp?endpoint=127.0.0.1:40123".into(),
        "source2".into(),
    );
    catalog.add_recording(desc2.clone());

    catalog.save_to_file(&catalog_path).expect("save catalog");

    let loaded = Catalog::load_from_file(&catalog_path).expect("load catalog");
    assert_eq!(loaded.recordings().len(), 2);
    assert_eq!(loaded.get_recording(rec_id1), Some(&desc1));
    assert_eq!(loaded.get_recording(rec_id2), Some(&desc2));

    let _ = std::fs::remove_file(catalog_path);
}

#[test]
fn test_archive_conductor_full_lifecycle() {
    let mut conductor = ArchiveConductor::new();

    // 1. Client connects
    let session_id = conductor.on_connect();
    assert_eq!(session_id, 1);

    // 2. Start recording
    let rec_id = conductor.on_start_recording(2001, "aeron:ipc", "client_source");
    assert_eq!(rec_id, 1);

    // 3. Query catalog
    let list = conductor.list_recordings_for_uri(1, 10, "aeron:ipc", 2001);
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].recording_id, rec_id);

    // 4. Start replay
    let replay_id = conductor.on_start_replay(rec_id, 0, 14080);
    assert_eq!(replay_id, 1);

    // 5. Conduct duty cycle
    let work = conductor.do_work().expect("conductor work");
    assert!(work > 0);

    // 6. Stop replay & recording
    assert!(conductor.on_stop_replay(replay_id));
    assert!(conductor.on_stop_recording(rec_id));
}

use aeron_archive::codecs::RecordingSignal;

#[test]
fn test_recording_signal_vendor_discriminants() {
    assert_eq!(RecordingSignal::Start as i32, 0);
    assert_eq!(RecordingSignal::Stop as i32, 1);
    assert_eq!(RecordingSignal::Extend as i32, 2);
    assert_eq!(RecordingSignal::Replicate as i32, 3);
    assert_eq!(RecordingSignal::Merge as i32, 4);
    assert_eq!(RecordingSignal::Sync as i32, 5);
    assert_eq!(RecordingSignal::Delete as i32, 6);
    assert_eq!(RecordingSignal::ReplicateEnd as i32, 7);
}

#[test]
fn test_sbe_schema_version_is_v13() {
    assert_eq!(SBE_SCHEMA_VERSION, 13);
}

#[test]
fn test_stop_recording_updates_catalog_descriptor_persistence() {
    let mut conductor = ArchiveConductor::new();
    let rec_id = conductor.on_start_recording(3001, "aeron:ipc", "client_source");

    // Do some work to advance recording position
    conductor.do_work().unwrap();

    // Stop recording
    assert!(conductor.on_stop_recording(rec_id));

    // Verify catalog descriptor stop position is updated
    let desc = conductor
        .catalog()
        .get_recording(rec_id)
        .expect("descriptor");
    assert!(desc.stop_position > 0);
    assert!(desc.stop_timestamp > 0);

    // Save and reload catalog to verify persistence of updated stop_position
    let temp_dir = std::env::temp_dir();
    let catalog_path = temp_dir.join(format!(
        "test-stop-catalog-{}-{:?}.dat",
        std::process::id(),
        std::thread::current().id()
    ));
    conductor
        .catalog()
        .save_to_file(&catalog_path)
        .expect("save catalog");

    let reloaded = Catalog::load_from_file(&catalog_path).expect("load catalog");
    let reloaded_desc = reloaded.get_recording(rec_id).expect("reloaded descriptor");
    assert_eq!(reloaded_desc.stop_position, desc.stop_position);
    assert_eq!(reloaded_desc.stop_timestamp, desc.stop_timestamp);

    let _ = std::fs::remove_file(catalog_path);
}

#[test]
fn test_uri_listing_fragment_matching_and_limits() {
    let mut conductor = ArchiveConductor::new();
    let rec_id = conductor.on_start_recording(
        4001,
        "aeron:udp?endpoint=127.0.0.1:40123|mtu=1408",
        "source",
    );

    // Fragment matching: query for "udp?endpoint" should match "aeron:udp?endpoint=127.0.0.1:40123|mtu=1408"
    let list_frag = conductor.list_recordings_for_uri(1, 10, "udp?endpoint", 4001);
    assert_eq!(list_frag.len(), 1);
    assert_eq!(list_frag[0].recording_id, rec_id);

    // Zero or negative limits should return empty list
    assert!(conductor.list_recordings(1, 0).is_empty());
    assert!(conductor.list_recordings(1, -5).is_empty());
    assert!(
        conductor
            .list_recordings_for_uri(1, 0, "udp", 4001)
            .is_empty()
    );
    assert!(
        conductor
            .list_recordings_for_uri(1, -1, "udp", 4001)
            .is_empty()
    );
}

#[test]
fn test_control_session_responses() {
    let mut session = ControlSession::new(42);
    session.connect();

    let ok_resp = session.build_ok_response(1234, 99);
    assert_eq!(ok_resp.control_session_id, 42);
    assert_eq!(ok_resp.correlation_id, 1234);
    assert_eq!(ok_resp.relevant_id, 99);
    assert_eq!(ok_resp.code, ControlResponseCode::Ok as i32);

    let err_resp = session.build_error_response(1235, 1);
    assert_eq!(err_resp.code, ControlResponseCode::Error as i32);
}
