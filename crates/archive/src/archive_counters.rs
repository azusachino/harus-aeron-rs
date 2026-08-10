//! Archive counters keyed by the Archive's client identifier.
//!
//! This is the Rust equivalent of upstream `ArchiveCounters`: the key is the
//! eight-byte archive ID and the label carries a human-readable suffix.

use agrona::{AgronaError, CountersManager};

/// Counter type for the position reached by an active recording.
pub const ARCHIVE_RECORDING_POSITION_TYPE_ID: i32 = 100;
/// Counter type for the number of errors observed by an Archive.
pub const ARCHIVE_ERROR_COUNT_TYPE_ID: i32 = 101;
/// Suffix appended to every Archive counter label.
pub const ARCHIVE_ID_LABEL_SUFFIX: &str = " - archiveId=";
/// Value returned when a matching counter cannot be found.
pub const NULL_VALUE: i32 = -1;

/// Allocate an Archive counter whose key is the supplied archive ID.
pub fn allocate(
    counters: &CountersManager<'_>,
    type_id: i32,
    name: &str,
    archive_id: i64,
) -> Result<i32, AgronaError> {
    let key = archive_id.to_le_bytes();
    let label = format!("{name}{ARCHIVE_ID_LABEL_SUFFIX}{archive_id}");
    counters.allocate(type_id, &key, &label)
}

/// Allocate the Archive error counter.
pub fn allocate_error_counter(
    counters: &CountersManager<'_>,
    archive_id: i64,
) -> Result<i32, AgronaError> {
    allocate(
        counters,
        ARCHIVE_ERROR_COUNT_TYPE_ID,
        "Archive Errors",
        archive_id,
    )
}

/// Find the first allocated counter matching a type and archive ID.
pub fn find(counters: &CountersManager<'_>, type_id: i32, archive_id: i64) -> i32 {
    let expected_key = archive_id.to_le_bytes();
    let mut found = NULL_VALUE;

    counters.for_each(|counter_id, counter_type_id, key, _| {
        if found == NULL_VALUE && counter_type_id == type_id && key.starts_with(&expected_key) {
            found = counter_id;
        }
    });

    found
}

/// Return the number of bytes required for an Archive ID label suffix.
pub fn length_of_archive_id_label(archive_id: i64) -> usize {
    ARCHIVE_ID_LABEL_SUFFIX.len() + archive_id.to_string().len()
}

/// Append an Archive ID label suffix and return its length.
pub fn append_archive_id_label(buffer: &mut [u8], offset: usize, archive_id: i64) -> usize {
    let suffix = format!("{ARCHIVE_ID_LABEL_SUFFIX}{archive_id}");
    buffer[offset..offset + suffix.len()].copy_from_slice(suffix.as_bytes());
    suffix.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agrona::AtomicBuffer;
    use agrona::counters::{COUNTER_LENGTH, METADATA_LENGTH};

    fn with_manager<F>(capacity: usize, test: F)
    where
        F: FnOnce(&CountersManager<'_>),
    {
        let mut metadata = vec![0; METADATA_LENGTH * capacity];
        let mut values = vec![0; COUNTER_LENGTH * capacity];
        let manager = CountersManager::new(
            AtomicBuffer::new(&mut metadata),
            AtomicBuffer::new(&mut values),
        )
        .expect("counter buffers should be valid");
        test(&manager);
    }

    fn allocated_counter(manager: &CountersManager<'_>, id: i32) -> (i32, Vec<u8>, String) {
        let mut result = None;
        manager.for_each(|counter_id, type_id, key, label| {
            if counter_id == id {
                result = Some((type_id, key.to_vec(), label.to_owned()));
            }
        });
        result.expect("counter should be allocated")
    }

    #[test]
    fn allocate_counter_uses_archive_id_as_key_and_label_suffix() {
        with_manager(2, |manager| {
            let archive_id = -1_832_178_932_131_546;
            let counter_id = allocate(manager, 999, "<test counter>", archive_id).unwrap();
            let (type_id, key, label) = allocated_counter(manager, counter_id);

            assert_eq!(999, type_id);
            assert_eq!(archive_id.to_le_bytes(), key.as_slice());
            assert_eq!("<test counter> - archiveId=-1832178932131546", label);
        });
    }

    #[test]
    fn allocate_error_counter_uses_archive_error_type() {
        with_manager(2, |manager| {
            let archive_id = 24_623_864;
            let counter_id = allocate_error_counter(manager, archive_id).unwrap();
            let (type_id, key, label) = allocated_counter(manager, counter_id);

            assert_eq!(ARCHIVE_ERROR_COUNT_TYPE_ID, type_id);
            assert_eq!(archive_id.to_le_bytes(), key.as_slice());
            assert_eq!("Archive Errors - archiveId=24623864", label);
        });
    }

    #[test]
    fn find_returns_null_value_if_counter_not_found() {
        with_manager(2, |manager| {
            allocate(manager, 42, "test", 61_312_936_129_398_123).unwrap();

            assert_eq!(NULL_VALUE, find(manager, 5, 8));
            assert_eq!(NULL_VALUE, find(manager, 42, -10));
            assert_eq!(NULL_VALUE, find(manager, 42, 61_312_936_129_398_124));
        });
    }

    #[test]
    fn find_returns_first_matching_counter() {
        with_manager(8, |manager| {
            let archive_id = i64::MIN / 13;
            allocate(manager, 7, "test 1", 0).unwrap();
            allocate(manager, 7, "test 2", 42).unwrap();
            allocate(manager, 21, "test 3", archive_id).unwrap();
            let first_match = allocate(manager, 7, "test 4", archive_id).unwrap();
            allocate(manager, 7, "test 5", archive_id).unwrap();

            assert_eq!(first_match, find(manager, 7, archive_id));
        });
    }

    #[test]
    fn length_of_archive_id_label_counts_ascii_digits() {
        for archive_id in [
            i64::MIN,
            i64::MAX,
            0,
            -1,
            56_436_747_823,
            -1_235_127_312_317_278_312,
        ] {
            assert_eq!(
                ARCHIVE_ID_LABEL_SUFFIX.len() + archive_id.to_string().len(),
                length_of_archive_id_label(archive_id)
            );
        }
    }

    #[test]
    fn append_archive_id_label_writes_suffix_at_offset() {
        let offset = 13;
        let archive_id = -23_462_384;
        let mut buffer = [0; 100];

        let length = append_archive_id_label(&mut buffer, offset, archive_id);

        assert_eq!(length_of_archive_id_label(archive_id), length);
        assert_eq!(
            ARCHIVE_ID_LABEL_SUFFIX.as_bytes(),
            &buffer[offset..offset + ARCHIVE_ID_LABEL_SUFFIX.len()]
        );
        assert_eq!(
            archive_id.to_string().as_bytes(),
            &buffer[offset + ARCHIVE_ID_LABEL_SUFFIX.len()..offset + length]
        );
    }
}
