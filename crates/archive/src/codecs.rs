//! Archive control protocol SBE / binary codecs.
//!
//! Mirrors `io.aeron.archive.codecs`.

use agrona::atomic_buffer::AtomicBuffer;

pub const SBE_SCHEMA_ID: u16 = 101;
pub const SBE_SCHEMA_VERSION: u16 = 13;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageHeader {
    pub block_length: u16,
    pub template_id: u16,
    pub schema_id: u16,
    pub version: u16,
}

impl MessageHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn encode(&self, buffer: &AtomicBuffer, offset: usize) {
        buffer.put_u16(offset, self.block_length);
        buffer.put_u16(offset + 2, self.template_id);
        buffer.put_u16(offset + 4, self.schema_id);
        buffer.put_u16(offset + 6, self.version);
    }

    pub fn decode(buffer: &AtomicBuffer, offset: usize) -> Self {
        Self {
            block_length: buffer.get_u16(offset),
            template_id: buffer.get_u16(offset + 2),
            schema_id: buffer.get_u16(offset + 4),
            version: buffer.get_u16(offset + 6),
        }
    }
}

const _: () = assert!(std::mem::size_of::<MessageHeader>() == 8);

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlRequestType {
    Connect = 1,
    StartRecording = 2,
    StopRecording = 3,
    Replay = 4,
    ListRecordings = 5,
    CloseSession = 6,
    ListRecordingsForUri = 7,
    ExtendRecording = 8,
    StopReplay = 9,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlResponseCode {
    Ok = 0,
    Error = 1,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordingSignal {
    Start = 0,
    Stop = 1,
    Extend = 2,
    Replicate = 3,
    Merge = 4,
    Sync = 5,
    Delete = 6,
    ReplicateEnd = 7,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlRequestHeader {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub template_id: i32,
    pub schema_id: i16,
    pub version: i16,
}

impl ControlRequestHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn encode(&self, buffer: &AtomicBuffer, offset: usize) {
        buffer.put_i64(offset, self.control_session_id);
        buffer.put_i64(offset + 8, self.correlation_id);
        buffer.put_i32(offset + 16, self.template_id);
        buffer.put_i16(offset + 20, self.schema_id);
        buffer.put_i16(offset + 22, self.version);
    }

    pub fn decode(buffer: &AtomicBuffer, offset: usize) -> Self {
        Self {
            control_session_id: buffer.get_i64(offset),
            correlation_id: buffer.get_i64(offset + 8),
            template_id: buffer.get_i32(offset + 16),
            schema_id: buffer.get_i16(offset + 20),
            version: buffer.get_i16(offset + 22),
        }
    }
}

const _: () = assert!(std::mem::size_of::<ControlRequestHeader>() == 24);

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlResponseHeader {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub relevant_id: i64,
    pub code: i32,
    pub _pad: i32,
}

impl ControlResponseHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn encode(&self, buffer: &AtomicBuffer, offset: usize) {
        buffer.put_i64(offset, self.control_session_id);
        buffer.put_i64(offset + 8, self.correlation_id);
        buffer.put_i64(offset + 16, self.relevant_id);
        buffer.put_i32(offset + 24, self.code);
        buffer.put_i32(offset + 28, self._pad);
    }

    pub fn decode(buffer: &AtomicBuffer, offset: usize) -> Self {
        Self {
            control_session_id: buffer.get_i64(offset),
            correlation_id: buffer.get_i64(offset + 8),
            relevant_id: buffer.get_i64(offset + 16),
            code: buffer.get_i32(offset + 24),
            _pad: buffer.get_i32(offset + 28),
        }
    }
}

const _: () = assert!(std::mem::size_of::<ControlResponseHeader>() == 32);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_validate_codecs_header_sizes() {
        assert_eq!(MessageHeader::SIZE, 8);
        assert_eq!(ControlRequestHeader::SIZE, 24);
        assert_eq!(ControlResponseHeader::SIZE, 32);
    }

    #[test]
    fn should_encode_and_decode_message_header() {
        let mut backing = [0u8; 64];
        let buffer = AtomicBuffer::new(&mut backing);
        let header = MessageHeader {
            block_length: 32,
            template_id: 5,
            schema_id: SBE_SCHEMA_ID,
            version: SBE_SCHEMA_VERSION,
        };
        header.encode(&buffer, 0);

        let decoded = MessageHeader::decode(&buffer, 0);
        assert_eq!(decoded, header);
    }

    #[test]
    fn should_encode_and_decode_control_headers() {
        let mut backing = [0u8; 64];
        let buffer = AtomicBuffer::new(&mut backing);

        let req = ControlRequestHeader {
            control_session_id: 100,
            correlation_id: 200,
            template_id: 2,
            schema_id: SBE_SCHEMA_ID as i16,
            version: SBE_SCHEMA_VERSION as i16,
        };
        req.encode(&buffer, 0);
        assert_eq!(ControlRequestHeader::decode(&buffer, 0), req);

        let resp = ControlResponseHeader {
            control_session_id: 100,
            correlation_id: 200,
            relevant_id: 300,
            code: ControlResponseCode::Ok as i32,
            _pad: 0,
        };
        resp.encode(&buffer, 32);
        assert_eq!(ControlResponseHeader::decode(&buffer, 32), resp);
    }
}
