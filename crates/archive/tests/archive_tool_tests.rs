use std::path::{Path, PathBuf};

use aeron_archive::archive_tool::{
    catalog_capacity, compact, delete_orphaned_segments, mark_recording_invalid,
    mark_recording_valid, verify, verify_and_mark_invalid,
};
use aeron_archive::catalog::{Catalog, RecordingState};
use aeron_archive::recording_descriptor::RecordingDescriptor;

fn archive_dir(test_name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "aeron-archive-tool-{test_name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ))
}

fn descriptor(recording_id: i64, start_position: i64, stop_position: i64) -> RecordingDescriptor {
    RecordingDescriptor::new(
        0,
        0,
        recording_id,
        start_position,
        stop_position,
        0,
        -1,
        0,
        1024,
        256,
        128,
        2,
        2,
        "ch".into(),
        "ch".into(),
        "source".into(),
    )
}

fn save_catalog(path: &Path, descriptors: &[RecordingDescriptor]) {
    let _ = std::fs::remove_dir_all(path);
    std::fs::create_dir_all(path).expect("create archive directory");
    let mut catalog = Catalog::new();
    for descriptor in descriptors {
        let allocated = catalog.allocate_recording_id();
        assert_eq!(allocated, descriptor.recording_id);
        catalog.add_recording(descriptor.clone());
    }
    catalog
        .save_to_file(&path.join("catalog.dat"))
        .expect("save catalog");
}

#[test]
fn state_changes_persist_and_require_the_expected_source_state() {
    let path = archive_dir("state");
    save_catalog(&path, &[descriptor(1, 0, -1)]);

    mark_recording_invalid(&path, 1).expect("mark invalid");
    assert_eq!(
        Catalog::load_from_file(&path.join("catalog.dat"))
            .unwrap()
            .recording_state(1),
        Some(RecordingState::Invalid)
    );
    assert!(mark_recording_invalid(&path, 1).is_err());
    mark_recording_valid(&path, 1).expect("mark valid");

    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn compact_removes_invalid_catalog_entries_and_their_segments() {
    let path = archive_dir("compact");
    save_catalog(&path, &[descriptor(1, 0, -1), descriptor(2, 0, -1)]);
    mark_recording_invalid(&path, 2).expect("mark invalid");
    std::fs::write(path.join("2-0.rec"), []).expect("create invalid segment");
    std::fs::write(path.join("1-0.rec"), []).expect("create valid segment");

    assert_eq!(compact(&path).expect("compact archive"), 1);
    let catalog = Catalog::load_from_file(&path.join("catalog.dat")).unwrap();
    assert!(catalog.get_recording(2).is_none());
    assert!(path.join("1-0.rec").exists());
    assert!(!path.join("2-0.rec").exists());

    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn orphan_cleanup_removes_unknown_recording_segments_and_can_target_an_id() {
    let path = archive_dir("orphans");
    save_catalog(&path, &[descriptor(1, 0, -1)]);
    std::fs::write(path.join("1-0.rec"), []).expect("create known segment");
    std::fs::write(path.join("99-0.rec"), []).expect("create orphan segment");
    std::fs::write(path.join("100-0.rec"), []).expect("create second orphan segment");

    assert_eq!(delete_orphaned_segments(&path, Some(99)).unwrap(), 1);
    assert!(path.join("100-0.rec").exists());
    assert_eq!(delete_orphaned_segments(&path, None).unwrap(), 1);
    assert!(path.join("1-0.rec").exists());

    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn verification_reports_descriptor_invariants_and_can_mark_them_invalid() {
    let path = archive_dir("verify");
    save_catalog(&path, &[descriptor(1, 0, -1), descriptor(2, 3, -1)]);

    let report = verify(&path).expect("verify archive");
    assert_eq!(report.checked_recordings, 2);
    assert_eq!(report.invalid_recordings, vec![2]);

    verify_and_mark_invalid(&path).expect("mark verification failures");
    assert_eq!(
        Catalog::load_from_file(&path.join("catalog.dat"))
            .unwrap()
            .recording_state(2),
        Some(RecordingState::Invalid)
    );

    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn catalog_capacity_reports_the_persisted_catalog_size() {
    let path = archive_dir("capacity");
    save_catalog(&path, &[descriptor(1, 0, -1)]);
    assert_eq!(
        catalog_capacity(&path).unwrap(),
        std::fs::metadata(path.join("catalog.dat")).unwrap().len()
    );
    let _ = std::fs::remove_dir_all(path);
}
