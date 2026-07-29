//! Recording descriptor flyweight and record structure for Aeron Archive.
//!
//! Mirrors `io.aeron.archive.client.RecordingDescriptorDecoder.java`.

use crate::codecs::{
    CodecError, MessageHeader, SBE_SCHEMA_ID, SBE_SCHEMA_VERSION, VAR_ASCII_MAX_LENGTH,
};

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
    pub const TEMPLATE_ID: u16 = 22;
    pub const BLOCK_LENGTH: u16 = 80;

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

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        validate_ascii(&self.stripped_channel)?;
        validate_ascii(&self.original_channel)?;
        validate_ascii(&self.source_identity)?;
        let fixed_end = MessageHeader::SIZE + usize::from(Self::BLOCK_LENGTH);
        let length = fixed_end
            + 12
            + self.stripped_channel.len()
            + self.original_channel.len()
            + self.source_identity.len();
        let mut frame = vec![0; length];
        MessageHeader {
            block_length: Self::BLOCK_LENGTH,
            template_id: Self::TEMPLATE_ID,
            schema_id: SBE_SCHEMA_ID,
            version: SBE_SCHEMA_VERSION,
        }
        .encode_into(&mut frame)?;
        put_i64(&mut frame, 8, self.control_session_id);
        put_i64(&mut frame, 16, self.correlation_id);
        put_i64(&mut frame, 24, self.recording_id);
        put_i64(&mut frame, 32, self.start_timestamp);
        put_i64(&mut frame, 40, self.stop_timestamp);
        put_i64(&mut frame, 48, self.start_position);
        put_i64(&mut frame, 56, self.stop_position);
        put_i32(&mut frame, 64, self.initial_term_id);
        put_i32(&mut frame, 68, self.segment_file_length);
        put_i32(&mut frame, 72, self.term_buffer_length);
        put_i32(&mut frame, 76, self.mtu_length);
        put_i32(&mut frame, 80, self.session_id);
        put_i32(&mut frame, 84, self.stream_id);
        let offset = put_ascii(&mut frame, fixed_end, &self.stripped_channel);
        let offset = put_ascii(&mut frame, offset, &self.original_channel);
        put_ascii(&mut frame, offset, &self.source_identity);
        Ok(frame)
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let header = MessageHeader::decode_from(frame)?;
        header.validate(Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        let fixed_end = MessageHeader::SIZE + usize::from(header.block_length);
        require(frame, fixed_end)?;
        let (stripped_channel, offset) = get_ascii(frame, fixed_end)?;
        let (original_channel, offset) = get_ascii(frame, offset)?;
        let (source_identity, _) = get_ascii(frame, offset)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            recording_id: get_i64(frame, 24),
            start_timestamp: get_i64(frame, 32),
            stop_timestamp: get_i64(frame, 40),
            start_position: get_i64(frame, 48),
            stop_position: get_i64(frame, 56),
            initial_term_id: get_i32(frame, 64),
            segment_file_length: get_i32(frame, 68),
            term_buffer_length: get_i32(frame, 72),
            mtu_length: get_i32(frame, 76),
            session_id: get_i32(frame, 80),
            stream_id: get_i32(frame, 84),
            stripped_channel,
            original_channel,
            source_identity,
        })
    }
}

fn validate_ascii(value: &str) -> Result<(), CodecError> {
    if !value.is_ascii() {
        return Err(CodecError::NonAscii);
    }
    if value.len() > VAR_ASCII_MAX_LENGTH || value.len() > u32::MAX as usize {
        return Err(CodecError::VariableFieldTooLong {
            length: value.len(),
        });
    }
    Ok(())
}

fn put_ascii(frame: &mut [u8], offset: usize, value: &str) -> usize {
    let length = value.len();
    frame[offset..offset + 4].copy_from_slice(&(length as u32).to_le_bytes());
    frame[offset + 4..offset + 4 + length].copy_from_slice(value.as_bytes());
    offset + 4 + length
}

fn get_ascii(frame: &[u8], offset: usize) -> Result<(String, usize), CodecError> {
    require(frame, offset + 4)?;
    let length = u32::from_le_bytes(frame[offset..offset + 4].try_into().map_err(|_| {
        CodecError::Truncated {
            needed: offset + 4,
            available: frame.len(),
        }
    })?) as usize;
    if length > VAR_ASCII_MAX_LENGTH {
        return Err(CodecError::VariableFieldTooLong { length });
    }
    let end = offset
        .checked_add(4)
        .and_then(|start| start.checked_add(length))
        .ok_or(CodecError::VariableFieldTooLong { length })?;
    require(frame, end)?;
    let bytes = &frame[offset + 4..end];
    if !bytes.is_ascii() {
        return Err(CodecError::NonAscii);
    }
    Ok((String::from_utf8_lossy(bytes).into_owned(), end))
}

fn require(frame: &[u8], needed: usize) -> Result<(), CodecError> {
    if frame.len() < needed {
        Err(CodecError::Truncated {
            needed,
            available: frame.len(),
        })
    } else {
        Ok(())
    }
}

fn put_i32(frame: &mut [u8], offset: usize, value: i32) {
    frame[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_i64(frame: &mut [u8], offset: usize, value: i64) {
    frame[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn get_i32(frame: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes(frame[offset..offset + 4].try_into().unwrap_or([0; 4]))
}

fn get_i64(frame: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(frame[offset..offset + 8].try_into().unwrap_or([0; 8]))
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
    fn complete_v13_descriptor_roundtrips_and_rejects_every_truncation() {
        let descriptor = RecordingDescriptor::new(
            7,
            43,
            3,
            64,
            1024,
            1000,
            2000,
            11,
            128 * 1024 * 1024,
            64 * 1024,
            1408,
            19,
            1002,
            "aeron:ipc".into(),
            "aeron:ipc?alias=live".into(),
            "source".into(),
        );
        let encoded = descriptor.encode().unwrap();
        assert_eq!(RecordingDescriptor::decode(&encoded).unwrap(), descriptor);
        for length in 0..encoded.len() {
            assert!(
                RecordingDescriptor::decode(&encoded[..length]).is_err(),
                "length {length}"
            );
        }
        assert_eq!(
            to_hex(&encoded),
            "5000160065000d0007000000000000002b000000000000000300000000000000e803000000000000d007000000000000400000000000000000040000000000000b00000000000008000001008005000013000000ea030000090000006165726f6e3a697063140000006165726f6e3a6970633f616c6961733d6c69766506000000736f75726365"
        );
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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
