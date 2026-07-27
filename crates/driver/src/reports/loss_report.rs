//! Aggregated loss report storage, compatible with Aeron's report layout.

use agrona::AtomicBuffer;

pub const ENTRY_ALIGNMENT: usize = 64;
pub const OBSERVATION_COUNT_OFFSET: usize = 0;
pub const TOTAL_BYTES_LOST_OFFSET: usize = OBSERVATION_COUNT_OFFSET + 8;
pub const FIRST_OBSERVATION_OFFSET: usize = TOTAL_BYTES_LOST_OFFSET + 8;
pub const LAST_OBSERVATION_OFFSET: usize = FIRST_OBSERVATION_OFFSET + 8;
pub const SESSION_ID_OFFSET: usize = LAST_OBSERVATION_OFFSET + 8;
pub const STREAM_ID_OFFSET: usize = SESSION_ID_OFFSET + 4;
pub const CHANNEL_OFFSET: usize = STREAM_ID_OFFSET + 4;

#[inline]
fn align(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

/// A report writer. It is intentionally single-writer, while readers may inspect completed
/// entries concurrently through [`loss_report_reader`](super::loss_report_reader).
pub struct LossReport<'a> {
    buffer: AtomicBuffer<'a>,
    next_record_offset: usize,
}

impl<'a> LossReport<'a> {
    pub fn new(buffer: AtomicBuffer<'a>) -> Self {
        Self {
            buffer,
            next_record_offset: 0,
        }
    }

    pub fn create_entry(
        &mut self,
        initial_bytes_lost: i64,
        timestamp_ms: i64,
        session_id: i32,
        stream_id: i32,
        channel: &str,
        source: &str,
    ) -> Option<ReportEntry<'a>> {
        let channel_length = 4 + channel.len();
        let source_length = 4 + source.len();
        let required_capacity = CHANNEL_OFFSET + align(channel_length, 4) + source_length;
        if required_capacity
            > self
                .buffer
                .capacity()
                .saturating_sub(self.next_record_offset)
        {
            return None;
        }

        let offset = self.next_record_offset;
        self.buffer
            .put_i64(offset + TOTAL_BYTES_LOST_OFFSET, initial_bytes_lost);
        self.buffer
            .put_i64(offset + FIRST_OBSERVATION_OFFSET, timestamp_ms);
        self.buffer
            .put_i64(offset + LAST_OBSERVATION_OFFSET, timestamp_ms);
        self.buffer.put_i32(offset + SESSION_ID_OFFSET, session_id);
        self.buffer.put_i32(offset + STREAM_ID_OFFSET, stream_id);
        self.buffer
            .put_string_utf8(offset + CHANNEL_OFFSET, channel);
        self.buffer
            .put_string_utf8(offset + CHANNEL_OFFSET + align(channel_length, 4), source);
        self.buffer
            .put_i64_ordered(offset + OBSERVATION_COUNT_OFFSET, 1);
        self.next_record_offset += align(required_capacity, ENTRY_ALIGNMENT);

        Some(ReportEntry {
            buffer: self.buffer,
            offset,
        })
    }

    pub fn next_record_offset(&self) -> usize {
        self.next_record_offset
    }
}

#[derive(Clone, Copy)]
pub struct ReportEntry<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}

impl ReportEntry<'_> {
    pub fn record_observation(&self, bytes_lost: i64, timestamp_ms: i64) {
        self.buffer
            .put_i64_ordered(self.offset + LAST_OBSERVATION_OFFSET, timestamp_ms);
        self.buffer
            .get_and_add_i64(self.offset + TOTAL_BYTES_LOST_OFFSET, bytes_lost);
        self.buffer
            .get_and_add_i64(self.offset + OBSERVATION_COUNT_OFFSET, 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reports::loss_report_reader;

    #[test]
    fn creates_and_updates_entries() {
        let mut bytes = vec![0u8; 1024];
        let buffer = AtomicBuffer::new(&mut bytes);
        let mut report = LossReport::new(buffer);
        let entry = report
            .create_entry(32, 7, 3, 1, "aeron:udp://stuff", "127.0.0.1:8888")
            .unwrap();
        entry.record_observation(64, 10);

        let entries = loss_report_reader::read(&buffer);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].observation_count, 2);
        assert_eq!(entries[0].total_bytes_lost, 96);
        assert_eq!(entries[0].last_observation_timestamp, 10);
    }

    #[test]
    fn stops_when_the_report_has_no_space() {
        let mut bytes = vec![0u8; 48];
        let buffer = AtomicBuffer::new(&mut bytes);
        let mut report = LossReport::new(buffer);
        assert!(
            report
                .create_entry(1, 1, 1, 1, "channel", "source")
                .is_none()
        );
    }
}
