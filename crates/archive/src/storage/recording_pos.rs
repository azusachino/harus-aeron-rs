//! Recording-position counter metadata and lookup.
//!
//! The counter key mirrors upstream `RecordingPos`: recording ID, session ID,
//! source-identity length and bytes, then archive ID.

use agrona::counters::{MAX_KEY_LENGTH, MAX_LABEL_LENGTH};
use agrona::{AgronaError, CountersManager};

use crate::archive_counters::{
    ARCHIVE_ID_LABEL_SUFFIX, ARCHIVE_RECORDING_POSITION_TYPE_ID, NULL_VALUE,
    length_of_archive_id_label,
};

/// Type ID for an active recording-position counter.
pub const RECORDING_POSITION_TYPE_ID: i32 = ARCHIVE_RECORDING_POSITION_TYPE_ID;
/// Sentinel returned when no recording ID or counter is found.
pub const NULL_RECORDING_ID: i64 = -1;
/// Human-readable counter name.
pub const NAME: &str = "rec-pos";

/// Offset of the recording ID in the counter key.
pub const RECORDING_ID_OFFSET: usize = 0;
/// Offset of the session ID in the counter key.
pub const SESSION_ID_OFFSET: usize = RECORDING_ID_OFFSET + 8;
/// Offset of the source-identity length in the counter key.
pub const SOURCE_IDENTITY_LENGTH_OFFSET: usize = SESSION_ID_OFFSET + 4;
/// Offset of source-identity bytes in the counter key.
pub const SOURCE_IDENTITY_OFFSET: usize = SOURCE_IDENTITY_LENGTH_OFFSET + 4;

/// Allocate and populate a recording-position counter.
pub fn allocate(
    counters: &CountersManager<'_>,
    archive_id: i64,
    recording_id: i64,
    session_id: i32,
    stream_id: i32,
    stripped_channel: &str,
    source_identity: &str,
) -> Result<i32, AgronaError> {
    let max_source_identity_length = MAX_KEY_LENGTH - SOURCE_IDENTITY_OFFSET - 8;
    let source_bytes = source_identity.as_bytes();
    let source_length = source_bytes.len().min(max_source_identity_length);
    let archive_id_offset = SOURCE_IDENTITY_OFFSET + source_length;
    let mut key = vec![0; archive_id_offset + 8];
    key[RECORDING_ID_OFFSET..SESSION_ID_OFFSET].copy_from_slice(&recording_id.to_le_bytes());
    key[SESSION_ID_OFFSET..SOURCE_IDENTITY_LENGTH_OFFSET]
        .copy_from_slice(&session_id.to_le_bytes());
    key[SOURCE_IDENTITY_LENGTH_OFFSET..SOURCE_IDENTITY_OFFSET]
        .copy_from_slice(&(source_length as i32).to_le_bytes());
    key[SOURCE_IDENTITY_OFFSET..archive_id_offset].copy_from_slice(&source_bytes[..source_length]);
    key[archive_id_offset..].copy_from_slice(&archive_id.to_le_bytes());

    let prefix = format!("{NAME}: {recording_id} {session_id} {stream_id} ");
    let suffix_length = length_of_archive_id_label(archive_id);
    let available_channel_length = MAX_LABEL_LENGTH.saturating_sub(prefix.len() + suffix_length);
    let mut channel_end = stripped_channel.len().min(available_channel_length);
    while !stripped_channel.is_char_boundary(channel_end) {
        channel_end -= 1;
    }
    let label = format!(
        "{prefix}{}{ARCHIVE_ID_LABEL_SUFFIX}{archive_id}",
        &stripped_channel[..channel_end]
    );

    counters.allocate(RECORDING_POSITION_TYPE_ID, &key, &label)
}

/// Find the first active recording-position counter for a recording and archive.
pub fn find_counter_id_by_recording(
    counters: &CountersManager<'_>,
    recording_id: i64,
    archive_id: i64,
) -> i32 {
    find_counter(counters, |key| {
        let (key_recording_id, _, key_archive_id) = recording_key_fields(key)?;
        (key_recording_id == recording_id
            && (archive_id == NULL_RECORDING_ID || key_archive_id == archive_id))
            .then_some(())
    })
}

/// Find the first active recording-position counter for a session and archive.
pub fn find_counter_id_by_session(
    counters: &CountersManager<'_>,
    session_id: i32,
    archive_id: i64,
) -> i32 {
    find_counter(counters, |key| {
        let (_, key_session_id, key_archive_id) = recording_key_fields(key)?;
        (key_session_id == session_id
            && (archive_id == NULL_RECORDING_ID || key_archive_id == archive_id))
            .then_some(())
    })
}

fn find_counter<F>(counters: &CountersManager<'_>, mut matches: F) -> i32
where
    F: FnMut(&[u8]) -> Option<()>,
{
    let mut found = NULL_VALUE;
    counters.for_each(|counter_id, counter_type_id, key, _| {
        if found == NULL_VALUE
            && counter_type_id == RECORDING_POSITION_TYPE_ID
            && matches(key).is_some()
        {
            found = counter_id;
        }
    });
    found
}

fn recording_key_fields(key: &[u8]) -> Option<(i64, i32, i64)> {
    let recording_id = read_i64(key, RECORDING_ID_OFFSET)?;
    let session_id = read_i32(key, SESSION_ID_OFFSET)?;
    let source_length = usize::try_from(read_i32(key, SOURCE_IDENTITY_LENGTH_OFFSET)?).ok()?;
    let archive_id_offset = SOURCE_IDENTITY_OFFSET.checked_add(source_length)?;
    let archive_id = read_i64(key, archive_id_offset)?;
    Some((recording_id, session_id, archive_id))
}

fn read_i32(bytes: &[u8], offset: usize) -> Option<i32> {
    Some(i32::from_le_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_i64(bytes: &[u8], offset: usize) -> Option<i64> {
    Some(i64::from_le_bytes(
        bytes.get(offset..offset + 8)?.try_into().ok()?,
    ))
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

    fn counter_metadata(manager: &CountersManager<'_>, id: i32) -> (Vec<u8>, String) {
        let mut result = None;
        manager.for_each(|counter_id, _, key, label| {
            if counter_id == id {
                result = Some((key.to_vec(), label.to_owned()));
            }
        });
        result.expect("counter should be allocated")
    }

    #[test]
    fn allocate_max_key_and_label() {
        with_manager(1, |manager| {
            let archive_id = i64::MIN;
            let recording_id = 54_311;
            let session_id = 42;
            let stream_id = -13;
            let stripped_channel = format!("stripped channel{}", ".".repeat(1_000));
            let source_identity = format!("source identity{}", "X".repeat(5_000));
            let counter_id = allocate(
                manager,
                archive_id,
                recording_id,
                session_id,
                stream_id,
                &stripped_channel,
                &source_identity,
            )
            .unwrap();
            let (key, label) = counter_metadata(manager, counter_id);

            assert_eq!(MAX_KEY_LENGTH, SOURCE_IDENTITY_OFFSET + 88 + 8);
            assert_eq!(recording_id, read_i64(&key, RECORDING_ID_OFFSET).unwrap());
            assert_eq!(session_id, read_i32(&key, SESSION_ID_OFFSET).unwrap());
            assert_eq!(88, read_i32(&key, SOURCE_IDENTITY_LENGTH_OFFSET).unwrap());
            assert_eq!(
                &source_identity.as_bytes()[..88],
                &key[SOURCE_IDENTITY_OFFSET..SOURCE_IDENTITY_OFFSET + 88]
            );
            assert_eq!(
                archive_id,
                read_i64(&key, SOURCE_IDENTITY_OFFSET + 88).unwrap()
            );

            let prefix = "rec-pos: 54311 42 -13 ";
            let expected_channel_length =
                MAX_LABEL_LENGTH - prefix.len() - length_of_archive_id_label(archive_id);
            assert_eq!(MAX_LABEL_LENGTH, label.len());
            assert_eq!(&label[..prefix.len()], prefix);
            assert_eq!(
                &label[prefix.len()..prefix.len() + expected_channel_length],
                &stripped_channel[..expected_channel_length]
            );
            assert_eq!(
                &label[prefix.len() + expected_channel_length..],
                " - archiveId=-9223372036854775808"
            );
        });
    }

    #[test]
    fn allocate_keeps_label_after_aligned_key_boundary() {
        with_manager(1, |manager| {
            let counter_id = allocate(manager, 888, 1, 30, 222, "channel", "source").unwrap();
            let (key, label) = counter_metadata(manager, counter_id);

            let key_length = SOURCE_IDENTITY_OFFSET + "source".len() + 8;
            assert_eq!(30, key_length);
            assert_eq!("rec-pos: 1 30 222 channel - archiveId=888", label);
            assert_eq!(32, (key_length + 3) & !3);
            assert_eq!(
                30,
                read_i32(&key, SOURCE_IDENTITY_LENGTH_OFFSET).unwrap() as usize + 24
            );
        });
    }

    #[test]
    fn finds_by_recording_id_and_archive_id() {
        with_manager(4, |manager| {
            allocate(manager, 7, 42, 7, 1, "one", "source").unwrap();
            let matching = allocate(manager, 19, 42, 8, 2, "two", "source").unwrap();

            assert_eq!(matching, find_counter_id_by_recording(manager, 42, 19));
            assert_eq!(
                NULL_VALUE,
                find_counter_id_by_recording(manager, 42, i64::MIN)
            );
            assert_eq!(
                0,
                find_counter_id_by_recording(manager, 42, NULL_RECORDING_ID)
            );
        });
    }

    #[test]
    fn finds_by_session_id_and_archive_id() {
        with_manager(4, |manager| {
            allocate(manager, 19, 41, 777, 1, "one", "src").unwrap();
            allocate(manager, 7, 42, 888, 2, "two", "src").unwrap();
            let matching = allocate(manager, 19, 42, 888, 3, "three", "src").unwrap();

            assert_eq!(matching, find_counter_id_by_session(manager, 888, 19));
            assert_eq!(
                NULL_VALUE,
                find_counter_id_by_session(manager, 888, i64::MIN)
            );
            assert_eq!(
                1,
                find_counter_id_by_session(manager, 888, NULL_RECORDING_ID)
            );
        });
    }

    #[test]
    fn multi_archive_isolation_and_wildcard_lookups() {
        with_manager(8, |manager| {
            let c_arch100 = allocate(manager, 100, 501, 1001, 1, "ch1", "src1").unwrap();
            let c_arch200 = allocate(manager, 200, 501, 1001, 1, "ch1", "src1").unwrap();
            let c_arch300 = allocate(manager, 300, 501, 1001, 1, "ch1", "src1").unwrap();

            // Exact archive ID match
            assert_eq!(c_arch100, find_counter_id_by_recording(manager, 501, 100));
            assert_eq!(c_arch200, find_counter_id_by_recording(manager, 501, 200));
            assert_eq!(c_arch300, find_counter_id_by_recording(manager, 501, 300));

            assert_eq!(c_arch100, find_counter_id_by_session(manager, 1001, 100));
            assert_eq!(c_arch200, find_counter_id_by_session(manager, 1001, 200));
            assert_eq!(c_arch300, find_counter_id_by_session(manager, 1001, 300));

            // Unknown archive ID returns NULL_VALUE
            assert_eq!(NULL_VALUE, find_counter_id_by_recording(manager, 501, 999));
            assert_eq!(NULL_VALUE, find_counter_id_by_session(manager, 1001, 999));

            // Wildcard NULL_RECORDING_ID (-1) returns the first allocated match
            assert_eq!(
                c_arch100,
                find_counter_id_by_recording(manager, 501, NULL_RECORDING_ID)
            );
            assert_eq!(
                c_arch100,
                find_counter_id_by_session(manager, 1001, NULL_RECORDING_ID)
            );
        });
    }
}
