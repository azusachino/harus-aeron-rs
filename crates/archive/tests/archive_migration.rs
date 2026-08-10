//! Archive v2.1.0 to v3.0.0 migration coverage.

#[path = "migration_utils.rs"]
mod migration_utils;

use aeron_archive::archive_mark_file::ArchiveMarkFile;
use aeron_archive::catalog::Catalog;
use aeron_archive::migration::{
    MINIMUM_VERSION, VERSION_2_1_0, migrate_v2_archive, migration_timestamp_file_name,
};
use migration_utils::{
    RecordingDescriptorV2, create_archive_mark_file_in_version2, create_catalog_in_version2,
};

fn directory(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "aeron-archive-migration-test-{}-{}-{}",
        std::process::id(),
        label,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn descriptor(valid: u8, recording_id: i64, source_identity: &str) -> RecordingDescriptorV2 {
    RecordingDescriptorV2 {
        valid,
        control_session_id: 42,
        correlation_id: 21,
        recording_id,
        start_timestamp: 123,
        stop_timestamp: 456,
        start_position: 0,
        stop_position: 1024,
        initial_term_id: 3,
        segment_file_length: 500,
        term_buffer_length: 1024,
        mtu_length: 1408,
        session_id: 8,
        stream_id: 55,
        stripped_channel: "strCh".into(),
        original_channel: "aeron:udp?endpoint=localhost:9090".into(),
        source_identity: source_identity.into(),
    }
}

#[test]
fn migrates_an_empty_v2_archive_and_upgrades_the_mark_file() {
    let directory = directory("empty");
    create_archive_mark_file_in_version2(&directory, 1234).unwrap();
    create_catalog_in_version2(&directory, &[]).unwrap();

    let report = migrate_v2_archive(&directory).unwrap();

    assert_eq!(report.from_version, VERSION_2_1_0);
    assert_eq!(report.to_version, MINIMUM_VERSION);
    assert_eq!(report.recording_count, 0);
    assert_eq!(report.next_recording_id, 0);
    assert_eq!(
        report.timestamp_file.file_name().unwrap().to_string_lossy(),
        migration_timestamp_file_name(VERSION_2_1_0, MINIMUM_VERSION)
    );
    let mark_file = ArchiveMarkFile::open(directory.join("archive-mark.dat")).unwrap();
    assert_eq!(mark_file.header().unwrap().version, MINIMUM_VERSION);
    assert_eq!(mark_file.activity_timestamp(), 1234);
    let catalog = Catalog::load_from_file(&directory.join("catalog.dat")).unwrap();
    assert_eq!(catalog.header().version, MINIMUM_VERSION);
    assert!(catalog.recordings().is_empty());
    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn migrates_recording_gaps_and_invalid_state() {
    let directory = directory("recordings");
    create_archive_mark_file_in_version2(&directory, 1).unwrap();
    create_catalog_in_version2(
        &directory,
        &[
            descriptor(1, 0, "sourceA"),
            descriptor(1, 4, "sourceB"),
            descriptor(0, 56, "sourceC"),
        ],
    )
    .unwrap();

    let report = migrate_v2_archive(&directory).unwrap();
    assert_eq!(report.recording_count, 3);
    assert_eq!(report.next_recording_id, 57);
    let catalog = Catalog::load_from_file(&directory.join("catalog.dat")).unwrap();
    assert_eq!(catalog.recordings().len(), 3);
    assert_eq!(catalog.header().next_recording_id, 57);
    assert_eq!(catalog.get_recording(4).unwrap().source_identity, "sourceB");
    assert!(catalog.get_valid_recording(56).is_none());
    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn refuses_to_repeat_a_migration_with_an_existing_timestamp_file() {
    let directory = directory("repeat");
    create_archive_mark_file_in_version2(&directory, 1).unwrap();
    create_catalog_in_version2(&directory, &[]).unwrap();
    let timestamp = directory.join(migration_timestamp_file_name(
        VERSION_2_1_0,
        MINIMUM_VERSION,
    ));
    std::fs::File::create(timestamp).unwrap();

    let result = migrate_v2_archive(&directory);
    assert!(matches!(
        result,
        Err(aeron_archive::migration::ArchiveMigrationError::Io(error))
            if error.kind() == std::io::ErrorKind::AlreadyExists
    ));
    let _ = std::fs::remove_dir_all(directory);
}
