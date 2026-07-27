//! Property-based tests for aeron-archive catalog and recording descriptors.

use aeron_archive::catalog::Catalog;
use aeron_archive::recording_descriptor::RecordingDescriptor;
use proptest::prelude::*;

proptest! {
    // 1. Catalog recording ID allocation property
    #[test]
    fn prop_catalog_recording_id_sequence(
        count in 1i32..100i32,
    ) {
        let mut catalog = Catalog::new();
        let mut last_id = 0i64;

        for _ in 0..count {
            let rec_id = catalog.allocate_recording_id();
            prop_assert!(rec_id > last_id);
            last_id = rec_id;
        }

        prop_assert_eq!(catalog.header().next_recording_id, (count + 1) as i64);
    }

    // 2. Catalog descriptor store and lookup roundtrip property
    #[test]
    fn prop_catalog_descriptor_roundtrip(
        stream_id in 1000i32..2000i32,
        start_pos in 0i64..10000i64,
    ) {
        let mut catalog = Catalog::new();
        let rec_id = catalog.allocate_recording_id();

        let desc = RecordingDescriptor::new(
            1,
            100,
            rec_id,
            start_pos,
            start_pos + 1024,
            1000,
            2000,
            0,
            128 * 1024 * 1024,
            64 * 1024,
            1408,
            10,
            stream_id,
            "aeron:ipc".into(),
            "aeron:ipc".into(),
            "source_test".into(),
        );

        catalog.add_recording(desc.clone());
        let fetched = catalog.get_recording(rec_id).unwrap();

        prop_assert_eq!(fetched.recording_id, rec_id);
        prop_assert_eq!(fetched.stream_id, stream_id);
        prop_assert_eq!(fetched.start_position, start_pos);
    }
}
