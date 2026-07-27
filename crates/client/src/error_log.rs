//! Distinct error log: a deduplicating, append-only record of driver-side errors written into
//! the CnC file's error-log region.
//!
//! Mirrors `vendor/aeron/aeron-client/src/main/c/concurrent/aeron_distinct_error_log.{h,c}`.
//! Each distinct `(error_code, description)` pair gets one record; repeat occurrences bump that
//! record's `observation_count` and `last_observation_timestamp` in place rather than growing
//! the log, so real Java tooling (`ErrorLogReader`) reading this region sees the same summarised
//! view a Java or C driver would have produced.

use agrona::AtomicBuffer;

/// `length(i32) + observation_count(i32) + last_observation_timestamp(i64) +
/// first_observation_timestamp(i64)`, matching `aeron_error_log_entry_t`.
pub const ERROR_LOG_HEADER_LENGTH: usize = 24;
/// Records are aligned to 8 bytes (`sizeof(int64_t)`), matching `AERON_ERROR_LOG_RECORD_ALIGNMENT`.
pub const ERROR_LOG_RECORD_ALIGNMENT: usize = 8;

const LENGTH_OFFSET: usize = 0;
const OBSERVATION_COUNT_OFFSET: usize = 4;
const LAST_OBSERVATION_TIMESTAMP_OFFSET: usize = 8;
const FIRST_OBSERVATION_TIMESTAMP_OFFSET: usize = 16;
const DESCRIPTION_OFFSET: usize = ERROR_LOG_HEADER_LENGTH;

fn align_up(value: usize, alignment: usize) -> usize {
    (value + (alignment - 1)) & !(alignment - 1)
}

/// True if the log's first record has ever been written (`length != 0`), mirroring
/// `aeron_error_log_exists`.
pub fn error_log_exists(buffer: &AtomicBuffer) -> bool {
    buffer.get_i32_volatile(LENGTH_OFFSET) != 0
}

/// Walk every record with `last_observation_timestamp >= since_timestamp`, invoking `reader`
/// with `(observation_count, first_observation_timestamp, last_observation_timestamp,
/// description_bytes)`. Returns the number of records visited. Mirrors `aeron_error_log_read`.
pub fn read_error_log<F>(buffer: &AtomicBuffer, since_timestamp: i64, mut reader: F) -> usize
where
    F: FnMut(i32, i64, i64, &[u8]),
{
    let capacity = buffer.capacity();
    let mut offset = 0usize;
    let mut entries = 0usize;

    while offset < capacity {
        let length = buffer.get_i32_volatile(offset + LENGTH_OFFSET);
        if length == 0 {
            break;
        }
        let length = length as usize;

        let last_observation_timestamp =
            buffer.get_i64_volatile(offset + LAST_OBSERVATION_TIMESTAMP_OFFSET);
        if last_observation_timestamp >= since_timestamp {
            entries += 1;
            let observation_count = buffer.get_i32(offset + OBSERVATION_COUNT_OFFSET);
            let first_observation_timestamp =
                buffer.get_i64(offset + FIRST_OBSERVATION_TIMESTAMP_OFFSET);
            let description_length = length - ERROR_LOG_HEADER_LENGTH;
            let mut description = vec![0u8; description_length];
            buffer.get_bytes(offset + DESCRIPTION_OFFSET, &mut description);
            reader(
                observation_count,
                first_observation_timestamp,
                last_observation_timestamp,
                &description,
            );
        }

        offset += align_up(length, ERROR_LOG_RECORD_ALIGNMENT);
    }

    entries
}

/// Error returned when a new distinct observation does not fit in the remaining buffer space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsufficientSpace;

/// Write side of the distinct error log. Not thread-safe by itself -- matches the vendor
/// implementation's single-writer-plus-mutex design; the driver conductor is already the sole
/// writer within its single-threaded duty cycle, so no internal locking is needed here.
pub struct DistinctErrorLog<'a> {
    buffer: AtomicBuffer<'a>,
    next_offset: usize,
    // In-memory dedup index: (error_code, description, record_offset). A per-record buffer scan
    // would also work but this mirrors the vendor implementation's O(1)-per-known-error lookup
    // instead of rescanning the log on every record() call.
    observations: Vec<(i32, String, usize)>,
}

impl<'a> DistinctErrorLog<'a> {
    pub fn new(buffer: AtomicBuffer<'a>) -> Self {
        Self {
            buffer,
            next_offset: 0,
            observations: Vec::new(),
        }
    }

    pub fn num_observations(&self) -> usize {
        self.observations.len()
    }

    /// Record one occurrence of `description` at `now_ms`. A repeat of an already-seen
    /// `description` (`error_code` is a caller-side lookup key only, matching the vendor
    /// implementation -- it is never itself persisted) bumps the existing record's count and
    /// last-seen timestamp; a new description allocates a new record. Fails if the buffer has no
    /// room left for a genuinely new description.
    pub fn record(
        &mut self,
        error_code: i32,
        description: &str,
        now_ms: i64,
    ) -> Result<(), InsufficientSpace> {
        if let Some(&(_, _, offset)) = self
            .observations
            .iter()
            .find(|(code, desc, _)| *code == error_code && desc == description)
        {
            self.buffer
                .get_and_add_i32(offset + OBSERVATION_COUNT_OFFSET, 1);
            self.buffer
                .put_i64_ordered(offset + LAST_OBSERVATION_TIMESTAMP_OFFSET, now_ms);
            return Ok(());
        }

        let description_bytes = description.as_bytes();
        let length = ERROR_LOG_HEADER_LENGTH + description_bytes.len();
        let offset = self.next_offset;
        if offset + length > self.buffer.capacity() {
            return Err(InsufficientSpace);
        }

        self.buffer
            .put_bytes(offset + DESCRIPTION_OFFSET, description_bytes);
        self.buffer
            .put_i64(offset + FIRST_OBSERVATION_TIMESTAMP_OFFSET, now_ms);
        self.buffer.put_i32(offset + OBSERVATION_COUNT_OFFSET, 1);
        self.buffer
            .put_i64_ordered(offset + LAST_OBSERVATION_TIMESTAMP_OFFSET, now_ms);
        // Release-store the length last: this is what publishes the record to a concurrent
        // reader (error_log_exists/read_error_log both gate on a non-zero length).
        self.buffer
            .put_i32_ordered(offset + LENGTH_OFFSET, length as i32);

        self.next_offset = align_up(offset + length, ERROR_LOG_RECORD_ALIGNMENT);
        self.observations
            .push((error_code, description.to_string(), offset));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(capacity: usize) -> (Vec<u8>, usize) {
        (vec![0u8; capacity], capacity)
    }

    #[test]
    fn should_fail_to_record_when_insufficient_space() {
        let (mut bytes, _) = log(8);
        let buffer = AtomicBuffer::new(&mut bytes);
        let mut error_log = DistinctErrorLog::new(buffer);

        assert_eq!(
            error_log.record(1, "description", 7),
            Err(InsufficientSpace)
        );
    }

    #[test]
    fn should_record_first_observation() {
        let (mut bytes, _) = log(64 * 1024);
        let buffer = AtomicBuffer::new(&mut bytes);
        let mut error_log = DistinctErrorLog::new(buffer);

        error_log.record(1, "description", 7).unwrap();
        assert_eq!(error_log.num_observations(), 1);

        let check = AtomicBuffer::new(&mut bytes);
        assert_eq!(
            check.get_i64(FIRST_OBSERVATION_TIMESTAMP_OFFSET),
            7,
            "first_observation_timestamp"
        );
        assert!(check.get_i32(LENGTH_OFFSET) as usize > ERROR_LOG_HEADER_LENGTH);
        assert_eq!(check.get_i32(OBSERVATION_COUNT_OFFSET), 1);
        assert_eq!(check.get_i64(LAST_OBSERVATION_TIMESTAMP_OFFSET), 7);
    }

    #[test]
    fn should_summarise_observations() {
        let (mut bytes, _) = log(64 * 1024);
        let buffer = AtomicBuffer::new(&mut bytes);
        let mut error_log = DistinctErrorLog::new(buffer);

        error_log.record(1, "description", 7).unwrap();
        error_log.record(1, "description", 8).unwrap();
        assert_eq!(error_log.num_observations(), 1);

        let check = AtomicBuffer::new(&mut bytes);
        assert_eq!(check.get_i64(FIRST_OBSERVATION_TIMESTAMP_OFFSET), 7);
        assert_eq!(check.get_i32(OBSERVATION_COUNT_OFFSET), 2);
        assert_eq!(check.get_i64(LAST_OBSERVATION_TIMESTAMP_OFFSET), 8);
    }

    #[test]
    fn should_record_two_distinct_observations() {
        let (mut bytes, _) = log(64 * 1024);
        let buffer = AtomicBuffer::new(&mut bytes);
        let mut error_log = DistinctErrorLog::new(buffer);

        error_log.record(1, "description 1", 7).unwrap();
        error_log.record(2, "description 2", 8).unwrap();
        assert_eq!(error_log.num_observations(), 2);

        let check = AtomicBuffer::new(&mut bytes);
        let first_length = check.get_i32(LENGTH_OFFSET) as usize;
        assert!(first_length > ERROR_LOG_HEADER_LENGTH);
        assert_eq!(check.get_i32(OBSERVATION_COUNT_OFFSET), 1);
        assert_eq!(check.get_i64(LAST_OBSERVATION_TIMESTAMP_OFFSET), 7);

        let second_offset = align_up(first_length, ERROR_LOG_RECORD_ALIGNMENT);
        assert!(check.get_i32(second_offset + LENGTH_OFFSET) as usize > ERROR_LOG_HEADER_LENGTH);
        assert_eq!(
            check.get_i64(second_offset + FIRST_OBSERVATION_TIMESTAMP_OFFSET),
            8
        );
        assert_eq!(check.get_i32(second_offset + OBSERVATION_COUNT_OFFSET), 1);
        assert_eq!(
            check.get_i64(second_offset + LAST_OBSERVATION_TIMESTAMP_OFFSET),
            8
        );
    }

    #[test]
    fn should_read_no_errors_from_empty_log() {
        let (mut bytes, _) = log(64 * 1024);
        let buffer = AtomicBuffer::new(&mut bytes);
        let mut called = false;
        let entries = read_error_log(&buffer, 0, |_, _, _, _| called = true);
        assert_eq!(entries, 0);
        assert!(!called);
    }

    #[test]
    fn should_read_first_observation() {
        let (mut bytes, _) = log(64 * 1024);
        {
            let buffer = AtomicBuffer::new(&mut bytes);
            let mut error_log = DistinctErrorLog::new(buffer);
            error_log.record(1, "description 1", 7).unwrap();
        }

        let buffer = AtomicBuffer::new(&mut bytes);
        let mut seen = Vec::new();
        let entries = read_error_log(&buffer, 0, |count, first, last, desc| {
            seen.push((count, first, last, desc.to_vec()))
        });

        assert_eq!(entries, 1);
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, 1);
        assert_eq!(seen[0].1, 7);
        assert_eq!(seen[0].2, 7);
        assert!(!seen[0].3.is_empty());
    }

    #[test]
    fn should_read_summarised_observation() {
        let (mut bytes, _) = log(64 * 1024);
        {
            let buffer = AtomicBuffer::new(&mut bytes);
            let mut error_log = DistinctErrorLog::new(buffer);
            error_log.record(1, "description 1", 7).unwrap();
            error_log.record(1, "description 1", 8).unwrap();
        }

        let buffer = AtomicBuffer::new(&mut bytes);
        let mut seen = Vec::new();
        let entries = read_error_log(&buffer, 0, |count, first, last, desc| {
            seen.push((count, first, last, desc.to_vec()))
        });

        assert_eq!(entries, 1);
        assert_eq!(seen[0].0, 2);
        assert_eq!(seen[0].1, 7);
        assert_eq!(seen[0].2, 8);
    }

    #[test]
    fn should_read_two_distinct_observations() {
        let (mut bytes, _) = log(64 * 1024);
        {
            let buffer = AtomicBuffer::new(&mut bytes);
            let mut error_log = DistinctErrorLog::new(buffer);
            error_log.record(1, "description 1", 7).unwrap();
            error_log.record(2, "description 2", 8).unwrap();
        }

        let buffer = AtomicBuffer::new(&mut bytes);
        let mut seen = Vec::new();
        let entries = read_error_log(&buffer, 0, |count, first, last, desc| {
            seen.push((count, first, last, desc.to_vec()))
        });

        assert_eq!(entries, 2);
        assert_eq!(seen[0], (1, 7, 7, b"description 1".to_vec()));
        assert_eq!(seen[1], (1, 8, 8, b"description 2".to_vec()));
    }

    #[test]
    fn should_only_read_observations_since_timestamp() {
        let (mut bytes, _) = log(64 * 1024);
        {
            let buffer = AtomicBuffer::new(&mut bytes);
            let mut error_log = DistinctErrorLog::new(buffer);
            error_log.record(1, "description 1", 7).unwrap();
            error_log.record(2, "description 2", 8).unwrap();
        }

        let buffer = AtomicBuffer::new(&mut bytes);
        let mut seen = Vec::new();
        let entries = read_error_log(&buffer, 8, |count, first, last, desc| {
            seen.push((count, first, last, desc.to_vec()))
        });

        assert_eq!(entries, 1);
        assert_eq!(seen[0].3, b"description 2");
    }

    #[test]
    fn error_log_exists_reflects_whether_anything_was_recorded() {
        let (mut bytes, _) = log(64 * 1024);
        {
            let buffer = AtomicBuffer::new(&mut bytes);
            assert!(!error_log_exists(&buffer));
        }

        {
            let buffer = AtomicBuffer::new(&mut bytes);
            let mut error_log = DistinctErrorLog::new(buffer);
            error_log.record(1, "description", 7).unwrap();
        }

        let buffer = AtomicBuffer::new(&mut bytes);
        assert!(error_log_exists(&buffer));
    }
}
