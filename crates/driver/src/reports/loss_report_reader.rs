//! Concurrent reader for [`super::loss_report::LossReport`] entries.

use agrona::AtomicBuffer;

use super::loss_report::{
    CHANNEL_OFFSET, ENTRY_ALIGNMENT, FIRST_OBSERVATION_OFFSET, LAST_OBSERVATION_OFFSET,
    OBSERVATION_COUNT_OFFSET, SESSION_ID_OFFSET, STREAM_ID_OFFSET, TOTAL_BYTES_LOST_OFFSET,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LossReportEntry {
    pub observation_count: i64,
    pub total_bytes_lost: i64,
    pub first_observation_timestamp: i64,
    pub last_observation_timestamp: i64,
    pub session_id: i32,
    pub stream_id: i32,
    pub channel: String,
    pub source: String,
}

pub fn read(buffer: &AtomicBuffer<'_>) -> Vec<LossReportEntry> {
    let mut entries = Vec::new();
    let mut offset = 0;
    while offset < buffer.capacity() {
        let observation_count = buffer.get_i64_volatile(offset + OBSERVATION_COUNT_OFFSET);
        if observation_count <= 0 {
            break;
        }

        let channel = buffer.get_string_utf8(offset + CHANNEL_OFFSET);
        let source_offset = CHANNEL_OFFSET + align(4 + channel.len(), 4);
        let source = buffer.get_string_utf8(offset + source_offset);
        let record_length = source_offset + 4 + source.len();
        entries.push(LossReportEntry {
            observation_count,
            total_bytes_lost: buffer.get_i64_volatile(offset + TOTAL_BYTES_LOST_OFFSET),
            first_observation_timestamp: buffer.get_i64(offset + FIRST_OBSERVATION_OFFSET),
            last_observation_timestamp: buffer.get_i64_volatile(offset + LAST_OBSERVATION_OFFSET),
            session_id: buffer.get_i32(offset + SESSION_ID_OFFSET),
            stream_id: buffer.get_i32(offset + STREAM_ID_OFFSET),
            channel,
            source,
        });
        offset += align(record_length, ENTRY_ALIGNMENT);
    }
    entries
}

#[inline]
fn align(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reports::loss_report::LossReport;

    #[test]
    fn reads_multiple_entries_in_order() {
        let mut bytes = vec![0u8; 1024];
        let buffer = AtomicBuffer::new(&mut bytes);
        let mut report = LossReport::new(buffer);
        report.create_entry(32, 7, 3, 1, "channel-one", "source-one");
        report.create_entry(48, 17, 13, 11, "channel-two", "source-two");

        let entries = read(&buffer);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].channel, "channel-one");
        assert_eq!(entries[1].source, "source-two");
    }
}
