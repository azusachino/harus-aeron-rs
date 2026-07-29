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

    /// Calculate the segment file base position for a given stream position matching Aeron Archive specification.
    pub fn segment_position(&self, position: i64) -> i64 {
        segment_file_base_position(
            self.start_position,
            position,
            self.term_buffer_length,
            self.segment_file_length,
        )
    }

    /// Format the segment file name matching Aeron Archive convention: `<recording_id>-<segment_position>.rec`.
    pub fn segment_file_name(&self, position: i64) -> String {
        let seg_pos = self.segment_position(position);
        format!("{}-{seg_pos}.rec", self.recording_id)
    }
}

/// Compute segment file base position matching Aeron Archive (`AeronArchive.segmentFileBasePosition`).
pub fn segment_file_base_position(
    start_position: i64,
    position: i64,
    term_buffer_length: i32,
    segment_file_length: i32,
) -> i64 {
    if term_buffer_length <= 0 || segment_file_length <= 0 {
        return start_position;
    }
    let start_term_base_position =
        start_position - (start_position & (term_buffer_length as i64 - 1));
    let length_from_base_position = position - start_term_base_position;
    let segments =
        length_from_base_position - (length_from_base_position & (segment_file_length as i64 - 1));
    start_term_base_position + segments
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
        assert_eq!(desc.segment_position(0), 0);
        assert_eq!(desc.segment_position(100), 0);
        assert_eq!(desc.segment_file_name(0), "1-0.rec");
    }

    #[test]
    fn should_calculate_segment_file_base_position_correctly() {
        let term_length = 64 * 1024;
        let segment_length = 128 * 1024 * 1024;

        // When recording starts at position 1000 inside term 0, segment base position must be 0, not 1000
        assert_eq!(
            segment_file_base_position(1000, 1000, term_length, segment_length),
            0
        );

        assert_eq!(
            segment_file_base_position(1000, 5000, term_length, segment_length),
            0
        );

        // When position crosses into second segment
        let pos_in_seg_2 = segment_length as i64 + 100;
        assert_eq!(
            segment_file_base_position(1000, pos_in_seg_2, term_length, segment_length),
            segment_length as i64
        );
    }
}
