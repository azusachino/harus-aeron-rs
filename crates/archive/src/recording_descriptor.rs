//! Recording descriptor flyweight and record structure for Aeron Archive.
//!
//! Mirrors `io.aeron.archive.client.RecordingDescriptorDecoder.java`.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingDescriptor {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub recording_id: i64,
    pub start_position: i64,
    pub stop_position: i64,
    pub start_timestamp: i64,
    pub stop_timestamp: i64,
    pub initial_term_id: i32,
    pub segment_file_length: i32,
    pub term_buffer_length: i32,
    pub mtu_length: i32,
    pub session_id: i32,
    pub stream_id: i32,
    pub stripped_channel: String,
    pub original_channel: String,
    pub source_identity: String,
}

impl RecordingDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        start_position: i64,
        stop_position: i64,
        start_timestamp: i64,
        stop_timestamp: i64,
        initial_term_id: i32,
        segment_file_length: i32,
        term_buffer_length: i32,
        mtu_length: i32,
        session_id: i32,
        stream_id: i32,
        stripped_channel: String,
        original_channel: String,
        source_identity: String,
    ) -> Self {
        Self {
            control_session_id,
            correlation_id,
            recording_id,
            start_position,
            stop_position,
            start_timestamp,
            stop_timestamp,
            initial_term_id,
            segment_file_length,
            term_buffer_length,
            mtu_length,
            session_id,
            stream_id,
            stripped_channel,
            original_channel,
            source_identity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_construct_recording_descriptor() {
        let desc = RecordingDescriptor::new(
            1,
            100,
            1,
            0,
            1024,
            1000,
            2000,
            0,
            128 * 1024 * 1024,
            64 * 1024,
            1408,
            10,
            1001,
            "aeron:ipc".into(),
            "aeron:ipc".into(),
            "test_source".into(),
        );

        assert_eq!(desc.recording_id, 1);
        assert_eq!(desc.stream_id, 1001);
    }
}
