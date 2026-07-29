//! Aeron Archive v13 recording-subscription descriptor response codec.

use crate::codecs::{
    CodecError, MessageHeader, SBE_SCHEMA_ID, SBE_SCHEMA_VERSION, VAR_ASCII_MAX_LENGTH,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingSubscriptionDescriptor {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub subscription_id: i64,
    pub stream_id: i32,
    pub stripped_channel: String,
}

impl RecordingSubscriptionDescriptor {
    pub const TEMPLATE_ID: u16 = 23;
    pub const BLOCK_LENGTH: u16 = 28;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        if !self.stripped_channel.is_ascii() {
            return Err(CodecError::NonAscii);
        }
        if self.stripped_channel.len() > VAR_ASCII_MAX_LENGTH
            || self.stripped_channel.len() > u32::MAX as usize
        {
            return Err(CodecError::VariableFieldTooLong {
                length: self.stripped_channel.len(),
            });
        }
        let variable_offset = MessageHeader::SIZE + usize::from(Self::BLOCK_LENGTH);
        let mut frame = vec![0; variable_offset + 4 + self.stripped_channel.len()];
        MessageHeader {
            block_length: Self::BLOCK_LENGTH,
            template_id: Self::TEMPLATE_ID,
            schema_id: SBE_SCHEMA_ID,
            version: SBE_SCHEMA_VERSION,
        }
        .encode_into(&mut frame)?;
        put_i64(&mut frame, 8, self.control_session_id);
        put_i64(&mut frame, 16, self.correlation_id);
        put_i64(&mut frame, 24, self.subscription_id);
        put_i32(&mut frame, 32, self.stream_id);
        put_u32(
            &mut frame,
            variable_offset,
            self.stripped_channel.len() as u32,
        );
        frame[variable_offset + 4..].copy_from_slice(self.stripped_channel.as_bytes());
        Ok(frame)
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let header = MessageHeader::decode_from(frame)?;
        header.validate(Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        let variable_offset = MessageHeader::SIZE + usize::from(header.block_length);
        require(frame, variable_offset + 4)?;
        let length = get_u32(frame, variable_offset) as usize;
        if length > VAR_ASCII_MAX_LENGTH {
            return Err(CodecError::VariableFieldTooLong { length });
        }
        let end = variable_offset
            .checked_add(4)
            .and_then(|start| start.checked_add(length))
            .ok_or(CodecError::VariableFieldTooLong { length })?;
        require(frame, end)?;
        let bytes = &frame[variable_offset + 4..end];
        if !bytes.is_ascii() {
            return Err(CodecError::NonAscii);
        }
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            subscription_id: get_i64(frame, 24),
            stream_id: get_i32(frame, 32),
            stripped_channel: String::from_utf8_lossy(bytes).into_owned(),
        })
    }
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

fn put_u32(frame: &mut [u8], offset: usize, value: u32) {
    frame[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_i32(frame: &mut [u8], offset: usize, value: i32) {
    frame[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_i64(frame: &mut [u8], offset: usize, value: i64) {
    frame[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn get_u32(frame: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(frame[offset..offset + 4].try_into().unwrap_or([0; 4]))
}

fn get_i32(frame: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes(frame[offset..offset + 4].try_into().unwrap_or([0; 4]))
}

fn get_i64(frame: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(frame[offset..offset + 8].try_into().unwrap_or([0; 8]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_descriptor_roundtrips_and_rejects_every_truncation() {
        let descriptor = RecordingSubscriptionDescriptor {
            control_session_id: 7,
            correlation_id: 43,
            subscription_id: 99,
            stream_id: 1002,
            stripped_channel: "aeron:ipc".into(),
        };
        let encoded = descriptor.encode().unwrap();
        assert_eq!(
            RecordingSubscriptionDescriptor::decode(&encoded).unwrap(),
            descriptor
        );
        for length in 0..encoded.len() {
            assert!(
                RecordingSubscriptionDescriptor::decode(&encoded[..length]).is_err(),
                "length {length}"
            );
        }
        assert_eq!(
            encoded
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "1c00170065000d0007000000000000002b000000000000006300000000000000ea030000090000006165726f6e3a697063"
        );
    }
}
