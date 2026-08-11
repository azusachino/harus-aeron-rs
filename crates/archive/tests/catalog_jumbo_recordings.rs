use aeron_archive::catalog::{Catalog, RecordingState};
use aeron_archive::list_recordings_for_uri_session::ListRecordingsForUriSession;
use aeron_archive::list_recordings_session::ListRecordingsEvent;
use aeron_archive::recording_descriptor::RecordingDescriptor;

const RECORDING_COUNT: i64 = 192;

fn descriptor(recording_id: i64, index: i64) -> RecordingDescriptor {
    let variant = index % 3;
    let stripped_channel = match variant {
        0 => format!("ch1{}", "x".repeat(2_000)),
        1 => "ch2".to_string(),
        _ => "ch3".to_string(),
    };
    let original_channel = match variant {
        0 => "ch1?tag=OK".to_string(),
        1 => format!("ch2?tag={}", "O".repeat(2_500)),
        _ => "ch3?tag=OK|endpoint=localhost:8089".to_string(),
    };
    let source_identity = match variant {
        2 => format!("src{}", "3".repeat(1_999)),
        _ => format!("src{}", variant + 1),
    };

    RecordingDescriptor::new(
        1,
        2,
        recording_id,
        index * 100,
        -1,
        index * 100,
        -1,
        0,
        4 * 8 * 1024 * 1024,
        8 * 1024 * 1024,
        4 * 4 * 1024,
        variant as i32,
        (variant + 1) as i32,
        stripped_channel,
        original_channel,
        source_identity,
    )
}

fn invalid_ranges(catalog: &Catalog) {
    for &(start, end) in &[(1, 3), (21, 30), (101, 111), (188, 192)] {
        for recording_id in start..=end {
            assert!(catalog.change_state(recording_id, RecordingState::Invalid));
        }
    }
}

#[test]
fn lists_long_descriptors_across_catalog_gaps_and_scan_batches() {
    let mut catalog = Catalog::new();
    for index in 0..RECORDING_COUNT {
        catalog.add_recording(descriptor(index + 1, index));
    }
    invalid_ranges(&catalog);

    let expected = catalog
        .recordings()
        .iter()
        .filter(|descriptor| {
            descriptor.stream_id == 3
                && descriptor.original_channel.contains("ch3?tag=OK")
                && catalog
                    .get_valid_recording(descriptor.recording_id)
                    .is_some()
        })
        .map(|descriptor| descriptor.recording_id)
        .collect::<Vec<_>>();

    let mut session =
        ListRecordingsForUriSession::new(17, 1, RECORDING_COUNT as i32, "ch3?tag=OK", 3, &catalog);
    let mut listed = Vec::new();
    while !session.is_done() {
        listed.extend(session.do_work(|_, _| true));
    }

    let listed_ids = listed
        .into_iter()
        .filter_map(|event| match event {
            ListRecordingsEvent::Descriptor { descriptor, .. } => Some(descriptor.recording_id),
            ListRecordingsEvent::RecordingUnknown { .. } => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(listed_ids, expected);
    assert!(listed_ids.windows(2).all(|ids| ids[0] < ids[1]));
    assert!(
        catalog
            .get_recording(*listed_ids.first().expect("matching recording"))
            .is_some_and(|descriptor| descriptor.source_identity.len() > 1_000)
    );
}
