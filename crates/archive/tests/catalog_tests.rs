use std::path::{Path, PathBuf};

use aeron_archive::catalog::{Catalog, RecordingState};
use aeron_archive::recording_descriptor::RecordingDescriptor;

fn catalog_path(test_name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "aeron-catalog-test-{test_name}-{}-{:?}.dat",
        std::process::id(),
        std::thread::current().id()
    ))
}

fn descriptor(
    recording_id: i64,
    session_id: i32,
    stream_id: i32,
    channel: &str,
) -> RecordingDescriptor {
    RecordingDescriptor::new(
        1,
        2,
        recording_id,
        0,
        -1,
        3,
        -1,
        4,
        8 * 1024,
        4 * 1024,
        1024,
        session_id,
        stream_id,
        channel.into(),
        format!("{channel}?tag=ok"),
        "source".into(),
    )
}

fn add_recording(catalog: &mut Catalog, session_id: i32, stream_id: i32, channel: &str) -> i64 {
    let recording_id = catalog.allocate_recording_id();
    catalog.add_recording(descriptor(recording_id, session_id, stream_id, channel));
    recording_id
}

fn save(catalog: &Catalog, path: &Path) {
    let _ = std::fs::remove_file(path);
    catalog.save_to_file(path).expect("save catalog");
}

#[test]
fn state_transitions_filter_valid_recordings_and_ignore_unknown_ids() {
    let mut catalog = Catalog::new();
    let recording_id = add_recording(&mut catalog, 7, 11, "channel");

    assert_eq!(
        catalog.recording_state(recording_id),
        Some(RecordingState::Valid)
    );
    assert!(catalog.change_state(recording_id, RecordingState::Invalid));
    assert!(catalog.get_valid_recording(recording_id).is_none());
    assert_eq!(
        catalog.recording_state(recording_id),
        Some(RecordingState::Invalid)
    );
    assert!(catalog.change_state(recording_id, RecordingState::Valid));
    assert!(catalog.get_valid_recording(recording_id).is_some());
    assert!(!catalog.change_state(99, RecordingState::Invalid));
}

#[test]
fn find_last_matching_returns_the_newest_matching_recording() {
    let mut catalog = Catalog::new();
    let first = add_recording(&mut catalog, 7, 11, "channel");
    let second = add_recording(&mut catalog, 7, 11, "channel");
    let _other = add_recording(&mut catalog, 8, 11, "channel");

    assert_eq!(
        catalog.find_last_matching(first, 7, 11, "channel?tag"),
        Some(second)
    );
    assert_eq!(
        catalog.find_last_matching(second + 1, 7, 11, "channel"),
        None
    );
    assert_eq!(catalog.find_recording_for_stream(7, "channel"), None);
    assert_eq!(
        catalog.find_recording_for_stream(11, "channel"),
        Some(catalog.get_recording(3).unwrap())
    );
}

#[test]
fn stop_and_extend_updates_both_catalog_indexes() {
    let mut catalog = Catalog::new();
    let recording_id = add_recording(&mut catalog, 7, 11, "channel");

    assert!(catalog.update_stop_position(recording_id, 1024, 42));
    assert_eq!(
        catalog.get_recording(recording_id).unwrap().stop_position,
        1024
    );
    assert_eq!(catalog.recordings()[0].stop_timestamp, 42);

    assert!(catalog.extend_recording(recording_id, 9, 10, 12));
    let recording = catalog.get_recording(recording_id).unwrap();
    assert_eq!(recording.control_session_id, 9);
    assert_eq!(recording.correlation_id, 10);
    assert_eq!(recording.session_id, 12);
    assert_eq!(recording.stop_position, -1);
    assert_eq!(catalog.recordings()[0].session_id, 12);
}

#[test]
fn removing_a_recording_removes_it_from_lookup_and_persistence() {
    let path = catalog_path("remove");
    let mut catalog = Catalog::new();
    let first = add_recording(&mut catalog, 1, 1, "first");
    let second = add_recording(&mut catalog, 2, 2, "second");
    assert!(catalog.remove_recording(first));
    assert!(!catalog.remove_recording(first));
    save(&catalog, &path);

    let loaded = Catalog::load_from_file(&path).expect("load catalog");
    assert!(loaded.get_recording(first).is_none());
    assert_eq!(loaded.get_recording(second).unwrap().stream_id, 2);
    let _ = std::fs::remove_file(path);
}
