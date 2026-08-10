//! Bounded codecs for the Aeron Archive v13 SBE control protocol.
//!
//! The wire authority is `vendor/aeron/aeron-archive/src/main/resources/archive/
//! aeron-archive-codecs.xml`. SBE messages consist of an eight-byte message header,
//! a template-specific fixed block, and zero or more length-prefixed variable fields.
//! Versioned request decoders also accept the shorter fixed blocks emitted by v6 clients.

pub const SBE_SCHEMA_ID: u16 = 101;
pub const SBE_SCHEMA_VERSION: u16 = 13;
pub const PROTOCOL_SEMANTIC_VERSION: i32 = (1 << 16) | (12 << 8);
pub const VAR_ASCII_MAX_LENGTH: usize = 1_073_741_824;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CodecError {
    #[error("frame is truncated: need {needed} bytes, have {available}")]
    Truncated { needed: usize, available: usize },
    #[error("wrong SBE schema id: expected {expected}, got {actual}")]
    WrongSchema { expected: u16, actual: u16 },
    #[error("wrong SBE template id: expected {expected}, got {actual}")]
    WrongTemplate { expected: u16, actual: u16 },
    #[error("unsupported SBE block length: need at least {minimum}, got {actual}")]
    BlockLength { minimum: u16, actual: u16 },
    #[error("variable field is too long: {length} bytes")]
    VariableFieldTooLong { length: usize },
    #[error("variable field is not US-ASCII")]
    NonAscii,
    #[error("unknown control response code: {0}")]
    UnknownResponseCode(i32),
    #[error("unknown source location: {0}")]
    UnknownSourceLocation(i32),
    #[error("unknown boolean value: {0}")]
    UnknownBoolean(i32),
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageHeader {
    pub block_length: u16,
    pub template_id: u16,
    pub schema_id: u16,
    pub version: u16,
}

impl MessageHeader {
    pub const SIZE: usize = 8;

    pub fn encode_into(&self, frame: &mut [u8]) -> Result<(), CodecError> {
        require(frame, Self::SIZE)?;
        put_u16(frame, 0, self.block_length);
        put_u16(frame, 2, self.template_id);
        put_u16(frame, 4, self.schema_id);
        put_u16(frame, 6, self.version);
        Ok(())
    }

    pub fn decode_from(frame: &[u8]) -> Result<Self, CodecError> {
        require(frame, Self::SIZE)?;
        Ok(Self {
            block_length: get_u16(frame, 0),
            template_id: get_u16(frame, 2),
            schema_id: get_u16(frame, 4),
            version: get_u16(frame, 6),
        })
    }

    pub(crate) fn validate(
        &self,
        template_id: u16,
        minimum_block_length: u16,
    ) -> Result<(), CodecError> {
        if self.schema_id != SBE_SCHEMA_ID {
            return Err(CodecError::WrongSchema {
                expected: SBE_SCHEMA_ID,
                actual: self.schema_id,
            });
        }
        if self.template_id != template_id {
            return Err(CodecError::WrongTemplate {
                expected: template_id,
                actual: self.template_id,
            });
        }
        if self.block_length < minimum_block_length {
            return Err(CodecError::BlockLength {
                minimum: minimum_block_length,
                actual: self.block_length,
            });
        }
        Ok(())
    }
}

const _: () = assert!(std::mem::size_of::<MessageHeader>() == MessageHeader::SIZE);

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlRequestType {
    Connect = 2,
    CloseSession = 3,
    StartRecording = 4,
    StopRecording = 5,
    Replay = 6,
    StopReplay = 7,
    ListRecordings = 8,
    ListRecordingsForUri = 9,
    ExtendRecording = 11,
    RecordingPosition = 12,
    TruncateRecording = 13,
    StopRecordingSubscription = 14,
    StopPosition = 15,
    FindLastMatchingRecording = 16,
    ListRecordingSubscriptions = 17,
    BoundedReplay = 18,
    ReplicateRequest2 = 66,
    StartPosition = 52,
    StartRecordingV2 = 63,
    ExtendRecordingV2 = 64,
    MaxRecordedPosition = 67,
    PurgeRecording = 104,
    ReplayTokenRequest = 105,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlResponseCode {
    Ok = 0,
    Error = 1,
    RecordingUnknown = 2,
    SubscriptionUnknown = 3,
}

impl TryFrom<i32> for ControlResponseCode {
    type Error = CodecError;

    fn try_from(value: i32) -> Result<Self, CodecError> {
        match value {
            0 => Ok(Self::Ok),
            1 => Ok(Self::Error),
            2 => Ok(Self::RecordingUnknown),
            3 => Ok(Self::SubscriptionUnknown),
            _ => Err(CodecError::UnknownResponseCode(value)),
        }
    }
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceLocation {
    Local = 0,
    Remote = 1,
}

impl TryFrom<i32> for SourceLocation {
    type Error = CodecError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Local),
            1 => Ok(Self::Remote),
            _ => Err(CodecError::UnknownSourceLocation(value)),
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectRequest {
    pub correlation_id: i64,
    pub response_stream_id: i32,
    pub version: i32,
    pub response_channel: String,
}

impl ConnectRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::Connect as u16;
    pub const BLOCK_LENGTH: u16 = 16;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_var_message(
            Self::TEMPLATE_ID,
            Self::BLOCK_LENGTH,
            &self.response_channel,
            |frame| {
                put_i64(frame, 8, self.correlation_id);
                put_i32(frame, 16, self.response_stream_id);
                put_i32(frame, 20, self.version);
            },
        )
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let (header, variable_offset) =
            decode_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            correlation_id: get_i64(frame, 8),
            response_stream_id: get_i32(frame, 16),
            version: if header.version >= 2 {
                get_i32(frame, 20)
            } else {
                0
            },
            response_channel: decode_ascii(frame, variable_offset)?.0,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartRecordingRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub stream_id: i32,
    pub source_location: SourceLocation,
    pub channel: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseSessionRequest {
    pub control_session_id: i64,
}

impl CloseSessionRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::CloseSession as u16;
    pub const BLOCK_LENGTH: u16 = 8;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_fixed_message(Self::TEMPLATE_ID, Self::BLOCK_LENGTH, |frame| {
            put_i64(frame, 8, self.control_session_id)
        })
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        decode_fixed_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopRecordingRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub stream_id: i32,
    pub channel: String,
}

impl StopRecordingRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::StopRecording as u16;
    pub const BLOCK_LENGTH: u16 = 20;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_var_message(
            Self::TEMPLATE_ID,
            Self::BLOCK_LENGTH,
            &self.channel,
            |frame| {
                put_i64(frame, 8, self.control_session_id);
                put_i64(frame, 16, self.correlation_id);
                put_i32(frame, 24, self.stream_id);
            },
        )
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let (_, variable_offset) = decode_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            stream_id: get_i32(frame, 24),
            channel: decode_ascii(frame, variable_offset)?.0,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub recording_id: i64,
    pub position: i64,
    pub length: i64,
    pub replay_stream_id: i32,
    pub file_io_max_length: i32,
    pub replay_token: i64,
    pub replay_channel: String,
}

impl ReplayRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::Replay as u16;
    pub const BLOCK_LENGTH_V6: u16 = 44;
    pub const BLOCK_LENGTH: u16 = 56;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_var_message(
            Self::TEMPLATE_ID,
            Self::BLOCK_LENGTH,
            &self.replay_channel,
            |frame| {
                put_i64(frame, 8, self.control_session_id);
                put_i64(frame, 16, self.correlation_id);
                put_i64(frame, 24, self.recording_id);
                put_i64(frame, 32, self.position);
                put_i64(frame, 40, self.length);
                put_i32(frame, 48, self.replay_stream_id);
                put_i32(frame, 52, self.file_io_max_length);
                put_i64(frame, 56, self.replay_token);
            },
        )
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let header = MessageHeader::decode_from(frame)?;
        let minimum_block_length = if header.version >= 10 {
            Self::BLOCK_LENGTH
        } else if header.version >= 7 {
            48
        } else {
            Self::BLOCK_LENGTH_V6
        };
        let (header, variable_offset) =
            decode_message(frame, Self::TEMPLATE_ID, minimum_block_length)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            recording_id: get_i64(frame, 24),
            position: get_i64(frame, 32),
            length: get_i64(frame, 40),
            replay_stream_id: get_i32(frame, 48),
            file_io_max_length: if header.version >= 7 {
                get_i32(frame, 52)
            } else {
                -1
            },
            replay_token: if header.version >= 10 {
                get_i64(frame, 56)
            } else {
                -1
            },
            replay_channel: decode_ascii(frame, variable_offset)?.0,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateRequest2 {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub src_recording_id: i64,
    pub dst_recording_id: i64,
    pub stop_position: i64,
    pub channel_tag_id: i64,
    pub subscription_tag_id: i64,
    pub src_control_stream_id: i32,
    pub file_io_max_length: i32,
    pub replication_session_id: i32,
    pub src_control_channel: String,
    pub live_destination: String,
    pub replication_channel: String,
    pub encoded_credentials: Vec<u8>,
    pub src_response_channel: String,
}

impl ReplicateRequest2 {
    pub const TEMPLATE_ID: u16 = ControlRequestType::ReplicateRequest2 as u16;
    pub const BLOCK_LENGTH_V6: u16 = 60;
    pub const BLOCK_LENGTH: u16 = 68;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        validate_ascii(&self.src_control_channel)?;
        validate_ascii(&self.live_destination)?;
        validate_ascii(&self.replication_channel)?;
        validate_ascii(&self.src_response_channel)?;
        validate_var_data(&self.encoded_credentials)?;

        let variable_length = [
            self.src_control_channel.len(),
            self.live_destination.len(),
            self.replication_channel.len(),
            self.encoded_credentials.len(),
            self.src_response_channel.len(),
        ]
        .into_iter()
        .map(|length| 4 + length)
        .sum::<usize>();
        let variable_offset = MessageHeader::SIZE + usize::from(Self::BLOCK_LENGTH);
        let mut frame = vec![0; variable_offset + variable_length];
        MessageHeader {
            block_length: Self::BLOCK_LENGTH,
            template_id: Self::TEMPLATE_ID,
            schema_id: SBE_SCHEMA_ID,
            version: SBE_SCHEMA_VERSION,
        }
        .encode_into(&mut frame)?;
        put_i64(&mut frame, 8, self.control_session_id);
        put_i64(&mut frame, 16, self.correlation_id);
        put_i64(&mut frame, 24, self.src_recording_id);
        put_i64(&mut frame, 32, self.dst_recording_id);
        put_i64(&mut frame, 40, self.stop_position);
        put_i64(&mut frame, 48, self.channel_tag_id);
        put_i64(&mut frame, 56, self.subscription_tag_id);
        put_i32(&mut frame, 64, self.src_control_stream_id);
        put_i32(&mut frame, 68, self.file_io_max_length);
        put_i32(&mut frame, 72, self.replication_session_id);

        let mut offset = variable_offset;
        offset = put_ascii(&mut frame, offset, &self.src_control_channel);
        offset = put_ascii(&mut frame, offset, &self.live_destination);
        offset = put_ascii(&mut frame, offset, &self.replication_channel);
        offset = put_var_data(&mut frame, offset, &self.encoded_credentials);
        put_ascii(&mut frame, offset, &self.src_response_channel);
        Ok(frame)
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let header = MessageHeader::decode_from(frame)?;
        let minimum_block_length = if header.version >= 8 {
            Self::BLOCK_LENGTH
        } else if header.version >= 7 {
            64
        } else {
            Self::BLOCK_LENGTH_V6
        };
        let (header, mut offset) = decode_message(frame, Self::TEMPLATE_ID, minimum_block_length)?;
        let (src_control_channel, next) = decode_ascii(frame, offset)?;
        offset = next;
        let (live_destination, next) = decode_ascii(frame, offset)?;
        offset = next;
        let (replication_channel, next) = decode_ascii(frame, offset)?;
        offset = next;
        let (encoded_credentials, next) = if header.version >= 8 {
            decode_var_data(frame, offset)?
        } else {
            (Vec::new(), offset)
        };
        offset = next;
        let src_response_channel = if header.version >= 10 {
            decode_ascii(frame, offset)?.0
        } else {
            String::new()
        };

        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            src_recording_id: get_i64(frame, 24),
            dst_recording_id: get_i64(frame, 32),
            stop_position: get_i64(frame, 40),
            channel_tag_id: get_i64(frame, 48),
            subscription_tag_id: get_i64(frame, 56),
            src_control_stream_id: get_i32(frame, 64),
            file_io_max_length: if header.version >= 7 {
                get_i32(frame, 68)
            } else {
                -1
            },
            replication_session_id: if header.version >= 8 {
                get_i32(frame, 72)
            } else {
                -1
            },
            src_control_channel,
            live_destination,
            replication_channel,
            encoded_credentials,
            src_response_channel,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayTokenRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub recording_id: i64,
}

impl ReplayTokenRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::ReplayTokenRequest as u16;
    pub const BLOCK_LENGTH: u16 = 24;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_fixed_message(Self::TEMPLATE_ID, Self::BLOCK_LENGTH, |frame| {
            put_i64(frame, 8, self.control_session_id);
            put_i64(frame, 16, self.correlation_id);
            put_i64(frame, 24, self.recording_id);
        })
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        decode_fixed_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            recording_id: get_i64(frame, 24),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedReplayRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub recording_id: i64,
    pub position: i64,
    pub length: i64,
    pub limit_counter_id: i32,
    pub replay_stream_id: i32,
    pub file_io_max_length: i32,
    pub replay_token: i64,
    pub replay_channel: String,
}

impl BoundedReplayRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::BoundedReplay as u16;
    pub const BLOCK_LENGTH_V6: u16 = 48;
    pub const BLOCK_LENGTH: u16 = 60;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_var_message(
            Self::TEMPLATE_ID,
            Self::BLOCK_LENGTH,
            &self.replay_channel,
            |frame| {
                put_i64(frame, 8, self.control_session_id);
                put_i64(frame, 16, self.correlation_id);
                put_i64(frame, 24, self.recording_id);
                put_i64(frame, 32, self.position);
                put_i64(frame, 40, self.length);
                put_i32(frame, 48, self.limit_counter_id);
                put_i32(frame, 52, self.replay_stream_id);
                put_i32(frame, 56, self.file_io_max_length);
                put_i64(frame, 60, self.replay_token);
            },
        )
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let header = MessageHeader::decode_from(frame)?;
        let minimum_block_length = if header.version >= 10 {
            Self::BLOCK_LENGTH
        } else if header.version >= 7 {
            52
        } else {
            Self::BLOCK_LENGTH_V6
        };
        let (header, variable_offset) =
            decode_message(frame, Self::TEMPLATE_ID, minimum_block_length)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            recording_id: get_i64(frame, 24),
            position: get_i64(frame, 32),
            length: get_i64(frame, 40),
            limit_counter_id: get_i32(frame, 48),
            replay_stream_id: get_i32(frame, 52),
            file_io_max_length: if header.version >= 7 {
                get_i32(frame, 56)
            } else {
                -1
            },
            replay_token: if header.version >= 10 {
                get_i64(frame, 60)
            } else {
                -1
            },
            replay_channel: decode_ascii(frame, variable_offset)?.0,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopReplayRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub replay_session_id: i64,
}

impl StopReplayRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::StopReplay as u16;
    pub const BLOCK_LENGTH: u16 = 24;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_fixed_message(Self::TEMPLATE_ID, Self::BLOCK_LENGTH, |frame| {
            put_i64(frame, 8, self.control_session_id);
            put_i64(frame, 16, self.correlation_id);
            put_i64(frame, 24, self.replay_session_id);
        })
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        decode_fixed_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            replay_session_id: get_i64(frame, 24),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRecordingsRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub from_recording_id: i64,
    pub record_count: i32,
}

impl ListRecordingsRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::ListRecordings as u16;
    pub const BLOCK_LENGTH: u16 = 28;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_fixed_message(Self::TEMPLATE_ID, Self::BLOCK_LENGTH, |frame| {
            put_i64(frame, 8, self.control_session_id);
            put_i64(frame, 16, self.correlation_id);
            put_i64(frame, 24, self.from_recording_id);
            put_i32(frame, 32, self.record_count);
        })
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        decode_fixed_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            from_recording_id: get_i64(frame, 24),
            record_count: get_i32(frame, 32),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRecordingsForUriRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub from_recording_id: i64,
    pub record_count: i32,
    pub stream_id: i32,
    pub channel: String,
}

impl ListRecordingsForUriRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::ListRecordingsForUri as u16;
    pub const BLOCK_LENGTH: u16 = 32;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_var_message(
            Self::TEMPLATE_ID,
            Self::BLOCK_LENGTH,
            &self.channel,
            |frame| {
                put_i64(frame, 8, self.control_session_id);
                put_i64(frame, 16, self.correlation_id);
                put_i64(frame, 24, self.from_recording_id);
                put_i32(frame, 32, self.record_count);
                put_i32(frame, 36, self.stream_id);
            },
        )
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let (_, variable_offset) = decode_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            from_recording_id: get_i64(frame, 24),
            record_count: get_i32(frame, 32),
            stream_id: get_i32(frame, 36),
            channel: decode_ascii(frame, variable_offset)?.0,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRecordingRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub recording_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtendRecordingRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub recording_id: i64,
    pub stream_id: i32,
    pub source_location: SourceLocation,
    pub channel: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartRecordingRequestV2 {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub stream_id: i32,
    pub source_location: SourceLocation,
    pub auto_stop: bool,
    pub channel: String,
}

impl StartRecordingRequestV2 {
    pub const TEMPLATE_ID: u16 = ControlRequestType::StartRecordingV2 as u16;
    pub const BLOCK_LENGTH: u16 = 28;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_var_message(
            Self::TEMPLATE_ID,
            Self::BLOCK_LENGTH,
            &self.channel,
            |frame| {
                put_i64(frame, 8, self.control_session_id);
                put_i64(frame, 16, self.correlation_id);
                put_i32(frame, 24, self.stream_id);
                put_i32(frame, 28, self.source_location as i32);
                put_i32(frame, 32, i32::from(self.auto_stop));
            },
        )
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let (_, variable_offset) = decode_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            stream_id: get_i32(frame, 24),
            source_location: SourceLocation::try_from(get_i32(frame, 28))?,
            auto_stop: decode_bool(get_i32(frame, 32))?,
            channel: decode_ascii(frame, variable_offset)?.0,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtendRecordingRequestV2 {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub recording_id: i64,
    pub stream_id: i32,
    pub source_location: SourceLocation,
    pub auto_stop: bool,
    pub channel: String,
}

impl ExtendRecordingRequestV2 {
    pub const TEMPLATE_ID: u16 = ControlRequestType::ExtendRecordingV2 as u16;
    pub const BLOCK_LENGTH: u16 = 36;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_var_message(
            Self::TEMPLATE_ID,
            Self::BLOCK_LENGTH,
            &self.channel,
            |frame| {
                put_i64(frame, 8, self.control_session_id);
                put_i64(frame, 16, self.correlation_id);
                put_i64(frame, 24, self.recording_id);
                put_i32(frame, 32, self.stream_id);
                put_i32(frame, 36, self.source_location as i32);
                put_i32(frame, 40, i32::from(self.auto_stop));
            },
        )
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let (_, variable_offset) = decode_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            recording_id: get_i64(frame, 24),
            stream_id: get_i32(frame, 32),
            source_location: SourceLocation::try_from(get_i32(frame, 36))?,
            auto_stop: decode_bool(get_i32(frame, 40))?,
            channel: decode_ascii(frame, variable_offset)?.0,
        })
    }
}

impl ExtendRecordingRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::ExtendRecording as u16;
    pub const BLOCK_LENGTH: u16 = 32;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_var_message(
            Self::TEMPLATE_ID,
            Self::BLOCK_LENGTH,
            &self.channel,
            |frame| {
                put_i64(frame, 8, self.control_session_id);
                put_i64(frame, 16, self.correlation_id);
                put_i64(frame, 24, self.recording_id);
                put_i32(frame, 32, self.stream_id);
                put_i32(frame, 36, self.source_location as i32);
            },
        )
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let (_, variable_offset) = decode_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            recording_id: get_i64(frame, 24),
            stream_id: get_i32(frame, 32),
            source_location: SourceLocation::try_from(get_i32(frame, 36))?,
            channel: decode_ascii(frame, variable_offset)?.0,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncateRecordingRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub recording_id: i64,
    pub position: i64,
}

impl TruncateRecordingRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::TruncateRecording as u16;
    pub const BLOCK_LENGTH: u16 = 32;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_fixed_message(Self::TEMPLATE_ID, Self::BLOCK_LENGTH, |frame| {
            put_i64(frame, 8, self.control_session_id);
            put_i64(frame, 16, self.correlation_id);
            put_i64(frame, 24, self.recording_id);
            put_i64(frame, 32, self.position);
        })
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        decode_fixed_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            recording_id: get_i64(frame, 24),
            position: get_i64(frame, 32),
        })
    }
}

macro_rules! recording_position_request {
    ($name:ident, $request_type:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name {
            pub control_session_id: i64,
            pub correlation_id: i64,
            pub recording_id: i64,
        }

        impl $name {
            pub const TEMPLATE_ID: u16 = ControlRequestType::$request_type as u16;
            pub const BLOCK_LENGTH: u16 = 24;

            pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
                encode_fixed_message(Self::TEMPLATE_ID, Self::BLOCK_LENGTH, |frame| {
                    put_i64(frame, 8, self.control_session_id);
                    put_i64(frame, 16, self.correlation_id);
                    put_i64(frame, 24, self.recording_id);
                })
            }

            pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
                decode_fixed_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
                Ok(Self {
                    control_session_id: get_i64(frame, 8),
                    correlation_id: get_i64(frame, 16),
                    recording_id: get_i64(frame, 24),
                })
            }
        }
    };
}

recording_position_request!(StartPositionRequest, StartPosition);
recording_position_request!(RecordingPositionRequest, RecordingPosition);
recording_position_request!(StopPositionRequest, StopPosition);
recording_position_request!(MaxRecordedPositionRequest, MaxRecordedPosition);
recording_position_request!(PurgeRecordingRequest, PurgeRecording);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindLastMatchingRecordingRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub min_recording_id: i64,
    pub session_id: i32,
    pub stream_id: i32,
    pub channel: String,
}

impl FindLastMatchingRecordingRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::FindLastMatchingRecording as u16;
    pub const BLOCK_LENGTH: u16 = 32;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_var_message(
            Self::TEMPLATE_ID,
            Self::BLOCK_LENGTH,
            &self.channel,
            |frame| {
                put_i64(frame, 8, self.control_session_id);
                put_i64(frame, 16, self.correlation_id);
                put_i64(frame, 24, self.min_recording_id);
                put_i32(frame, 32, self.session_id);
                put_i32(frame, 36, self.stream_id);
            },
        )
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let (_, variable_offset) = decode_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            min_recording_id: get_i64(frame, 24),
            session_id: get_i32(frame, 32),
            stream_id: get_i32(frame, 36),
            channel: decode_ascii(frame, variable_offset)?.0,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopRecordingSubscriptionRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub subscription_id: i64,
}

impl StopRecordingSubscriptionRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::StopRecordingSubscription as u16;
    pub const BLOCK_LENGTH: u16 = 24;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_fixed_message(Self::TEMPLATE_ID, Self::BLOCK_LENGTH, |frame| {
            put_i64(frame, 8, self.control_session_id);
            put_i64(frame, 16, self.correlation_id);
            put_i64(frame, 24, self.subscription_id);
        })
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        decode_fixed_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            subscription_id: get_i64(frame, 24),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRecordingSubscriptionsRequest {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub pseudo_index: i32,
    pub subscription_count: i32,
    pub apply_stream_id: bool,
    pub stream_id: i32,
    pub channel: String,
}

impl ListRecordingSubscriptionsRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::ListRecordingSubscriptions as u16;
    pub const BLOCK_LENGTH: u16 = 32;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_var_message(
            Self::TEMPLATE_ID,
            Self::BLOCK_LENGTH,
            &self.channel,
            |frame| {
                put_i64(frame, 8, self.control_session_id);
                put_i64(frame, 16, self.correlation_id);
                put_i32(frame, 24, self.pseudo_index);
                put_i32(frame, 28, self.subscription_count);
                put_i32(frame, 32, i32::from(self.apply_stream_id));
                put_i32(frame, 36, self.stream_id);
            },
        )
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let (_, variable_offset) = decode_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            pseudo_index: get_i32(frame, 24),
            subscription_count: get_i32(frame, 28),
            apply_stream_id: match get_i32(frame, 32) {
                0 => false,
                1 => true,
                value => return Err(CodecError::UnknownBoolean(value)),
            },
            stream_id: get_i32(frame, 36),
            channel: decode_ascii(frame, variable_offset)?.0,
        })
    }
}

impl ListRecordingRequest {
    pub const TEMPLATE_ID: u16 = 10;
    pub const BLOCK_LENGTH: u16 = 24;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_fixed_message(Self::TEMPLATE_ID, Self::BLOCK_LENGTH, |frame| {
            put_i64(frame, 8, self.control_session_id);
            put_i64(frame, 16, self.correlation_id);
            put_i64(frame, 24, self.recording_id);
        })
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        decode_fixed_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            recording_id: get_i64(frame, 24),
        })
    }
}

impl StartRecordingRequest {
    pub const TEMPLATE_ID: u16 = ControlRequestType::StartRecording as u16;
    pub const BLOCK_LENGTH: u16 = 24;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_var_message(
            Self::TEMPLATE_ID,
            Self::BLOCK_LENGTH,
            &self.channel,
            |frame| {
                put_i64(frame, 8, self.control_session_id);
                put_i64(frame, 16, self.correlation_id);
                put_i32(frame, 24, self.stream_id);
                put_i32(frame, 28, self.source_location as i32);
            },
        )
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let (_, variable_offset) = decode_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            stream_id: get_i32(frame, 24),
            source_location: SourceLocation::try_from(get_i32(frame, 28))?,
            channel: decode_ascii(frame, variable_offset)?.0,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlResponse {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub relevant_id: i64,
    pub code: ControlResponseCode,
    pub version: i32,
    pub error_message: String,
}

impl ControlResponse {
    pub const TEMPLATE_ID: u16 = 1;
    pub const BLOCK_LENGTH: u16 = 32;

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        encode_var_message(
            Self::TEMPLATE_ID,
            Self::BLOCK_LENGTH,
            &self.error_message,
            |frame| {
                put_i64(frame, 8, self.control_session_id);
                put_i64(frame, 16, self.correlation_id);
                put_i64(frame, 24, self.relevant_id);
                put_i32(frame, 32, self.code as i32);
                put_i32(frame, 36, self.version);
            },
        )
    }

    pub fn decode(frame: &[u8]) -> Result<Self, CodecError> {
        let (header, variable_offset) =
            decode_message(frame, Self::TEMPLATE_ID, Self::BLOCK_LENGTH)?;
        Ok(Self {
            control_session_id: get_i64(frame, 8),
            correlation_id: get_i64(frame, 16),
            relevant_id: get_i64(frame, 24),
            code: ControlResponseCode::try_from(get_i32(frame, 32))?,
            version: if header.version >= 4 {
                get_i32(frame, 36)
            } else {
                0
            },
            error_message: decode_ascii(frame, variable_offset)?.0,
        })
    }
}

fn encode_var_message(
    template_id: u16,
    block_length: u16,
    value: &str,
    encode_fixed: impl FnOnce(&mut [u8]),
) -> Result<Vec<u8>, CodecError> {
    validate_ascii(value)?;
    let variable_offset = MessageHeader::SIZE + usize::from(block_length);
    let mut frame = vec![0; variable_offset + 4 + value.len()];
    MessageHeader {
        block_length,
        template_id,
        schema_id: SBE_SCHEMA_ID,
        version: SBE_SCHEMA_VERSION,
    }
    .encode_into(&mut frame)?;
    encode_fixed(&mut frame);
    put_u32(&mut frame, variable_offset, value.len() as u32);
    frame[variable_offset + 4..].copy_from_slice(value.as_bytes());
    Ok(frame)
}

fn encode_fixed_message(
    template_id: u16,
    block_length: u16,
    encode_fixed: impl FnOnce(&mut [u8]),
) -> Result<Vec<u8>, CodecError> {
    let mut frame = vec![0; MessageHeader::SIZE + usize::from(block_length)];
    MessageHeader {
        block_length,
        template_id,
        schema_id: SBE_SCHEMA_ID,
        version: SBE_SCHEMA_VERSION,
    }
    .encode_into(&mut frame)?;
    encode_fixed(&mut frame);
    Ok(frame)
}

fn decode_fixed_message(
    frame: &[u8],
    template_id: u16,
    minimum_block_length: u16,
) -> Result<MessageHeader, CodecError> {
    let header = MessageHeader::decode_from(frame)?;
    header.validate(template_id, minimum_block_length)?;
    require(
        frame,
        MessageHeader::SIZE + usize::from(header.block_length),
    )?;
    Ok(header)
}

fn decode_message(
    frame: &[u8],
    template_id: u16,
    minimum_block_length: u16,
) -> Result<(MessageHeader, usize), CodecError> {
    let header = decode_fixed_message(frame, template_id, minimum_block_length)?;
    let variable_offset = MessageHeader::SIZE + usize::from(header.block_length);
    require(frame, variable_offset + 4)?;
    Ok((header, variable_offset))
}

fn decode_ascii(frame: &[u8], offset: usize) -> Result<(String, usize), CodecError> {
    require(frame, offset + 4)?;
    let length = get_u32(frame, offset) as usize;
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

fn decode_var_data(frame: &[u8], offset: usize) -> Result<(Vec<u8>, usize), CodecError> {
    require(frame, offset + 4)?;
    let length = get_u32(frame, offset) as usize;
    if length > VAR_ASCII_MAX_LENGTH {
        return Err(CodecError::VariableFieldTooLong { length });
    }
    let end = offset
        .checked_add(4)
        .and_then(|start| start.checked_add(length))
        .ok_or(CodecError::VariableFieldTooLong { length })?;
    require(frame, end)?;
    Ok((frame[offset + 4..end].to_vec(), end))
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

fn validate_var_data(value: &[u8]) -> Result<(), CodecError> {
    if value.len() > VAR_ASCII_MAX_LENGTH || value.len() > u32::MAX as usize {
        return Err(CodecError::VariableFieldTooLong {
            length: value.len(),
        });
    }
    Ok(())
}

fn decode_bool(value: i32) -> Result<bool, CodecError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(CodecError::UnknownBoolean(value)),
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

fn put_u16(frame: &mut [u8], offset: usize, value: u16) {
    frame[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(frame: &mut [u8], offset: usize, value: u32) {
    frame[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_ascii(frame: &mut [u8], offset: usize, value: &str) -> usize {
    put_u32(frame, offset, value.len() as u32);
    frame[offset + 4..offset + 4 + value.len()].copy_from_slice(value.as_bytes());
    offset + 4 + value.len()
}

fn put_var_data(frame: &mut [u8], offset: usize, value: &[u8]) -> usize {
    put_u32(frame, offset, value.len() as u32);
    frame[offset + 4..offset + 4 + value.len()].copy_from_slice(value);
    offset + 4 + value.len()
}

fn put_i32(frame: &mut [u8], offset: usize, value: i32) {
    frame[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_i64(frame: &mut [u8], offset: usize, value: i64) {
    frame[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn get_u16(frame: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([frame[offset], frame[offset + 1]])
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
    fn complete_messages_roundtrip() {
        let connect = ConnectRequest {
            correlation_id: 42,
            response_stream_id: 1001,
            version: PROTOCOL_SEMANTIC_VERSION,
            response_channel: "aeron:ipc".into(),
        };
        assert_eq!(
            ConnectRequest::decode(&connect.encode().unwrap()).unwrap(),
            connect
        );

        let start = StartRecordingRequest {
            control_session_id: 7,
            correlation_id: 43,
            stream_id: 1002,
            source_location: SourceLocation::Local,
            channel: "aeron:udp?endpoint=127.0.0.1:40123".into(),
        };
        assert_eq!(
            StartRecordingRequest::decode(&start.encode().unwrap()).unwrap(),
            start
        );

        let start_v2 = StartRecordingRequestV2 {
            control_session_id: 7,
            correlation_id: 53,
            stream_id: 1002,
            source_location: SourceLocation::Remote,
            auto_stop: true,
            channel: "aeron:udp?endpoint=127.0.0.1:40124".into(),
        };
        assert_eq!(
            StartRecordingRequestV2::decode(&start_v2.encode().unwrap()).unwrap(),
            start_v2
        );

        let extend = ExtendRecordingRequest {
            control_session_id: 7,
            correlation_id: 54,
            recording_id: 3,
            stream_id: 1002,
            source_location: SourceLocation::Local,
            channel: "aeron:ipc".into(),
        };
        assert_eq!(
            ExtendRecordingRequest::decode(&extend.encode().unwrap()).unwrap(),
            extend
        );

        let extend_v2 = ExtendRecordingRequestV2 {
            control_session_id: 7,
            correlation_id: 55,
            recording_id: 3,
            stream_id: 1002,
            source_location: SourceLocation::Local,
            auto_stop: false,
            channel: "aeron:ipc".into(),
        };
        assert_eq!(
            ExtendRecordingRequestV2::decode(&extend_v2.encode().unwrap()).unwrap(),
            extend_v2
        );

        let truncate = TruncateRecordingRequest {
            control_session_id: 7,
            correlation_id: 55,
            recording_id: 3,
            position: 64,
        };
        assert_eq!(
            TruncateRecordingRequest::decode(&truncate.encode().unwrap()).unwrap(),
            truncate
        );

        let purge = PurgeRecordingRequest {
            control_session_id: 7,
            correlation_id: 56,
            recording_id: 3,
        };
        assert_eq!(
            PurgeRecordingRequest::decode(&purge.encode().unwrap()).unwrap(),
            purge
        );

        let close = CloseSessionRequest {
            control_session_id: 7,
        };
        assert_eq!(
            CloseSessionRequest::decode(&close.encode().unwrap()).unwrap(),
            close
        );

        let stop = StopRecordingRequest {
            control_session_id: 7,
            correlation_id: 44,
            stream_id: 1002,
            channel: "aeron:ipc".into(),
        };
        assert_eq!(
            StopRecordingRequest::decode(&stop.encode().unwrap()).unwrap(),
            stop
        );

        let replay = ReplayRequest {
            control_session_id: 7,
            correlation_id: 45,
            recording_id: 3,
            position: 64,
            length: 1024,
            replay_stream_id: 1003,
            file_io_max_length: 4096,
            replay_token: 9,
            replay_channel: "aeron:ipc".into(),
        };
        assert_eq!(
            ReplayRequest::decode(&replay.encode().unwrap()).unwrap(),
            replay
        );

        let stop_replay = StopReplayRequest {
            control_session_id: 7,
            correlation_id: 46,
            replay_session_id: 11,
        };
        assert_eq!(
            StopReplayRequest::decode(&stop_replay.encode().unwrap()).unwrap(),
            stop_replay
        );

        let list = ListRecordingsRequest {
            control_session_id: 7,
            correlation_id: 47,
            from_recording_id: 2,
            record_count: 10,
        };
        assert_eq!(
            ListRecordingsRequest::decode(&list.encode().unwrap()).unwrap(),
            list
        );

        let list_uri = ListRecordingsForUriRequest {
            control_session_id: 7,
            correlation_id: 48,
            from_recording_id: 2,
            record_count: 10,
            stream_id: 1002,
            channel: "aeron:udp?endpoint=127.0.0.1:40123".into(),
        };
        assert_eq!(
            ListRecordingsForUriRequest::decode(&list_uri.encode().unwrap()).unwrap(),
            list_uri
        );

        let start_position = StartPositionRequest {
            control_session_id: 7,
            correlation_id: 49,
            recording_id: 3,
        };
        assert_eq!(
            StartPositionRequest::decode(&start_position.encode().unwrap()).unwrap(),
            start_position
        );

        let recording_position = RecordingPositionRequest {
            control_session_id: 7,
            correlation_id: 50,
            recording_id: 3,
        };
        assert_eq!(
            RecordingPositionRequest::decode(&recording_position.encode().unwrap()).unwrap(),
            recording_position
        );

        let stop_position = StopPositionRequest {
            control_session_id: 7,
            correlation_id: 51,
            recording_id: 3,
        };
        assert_eq!(
            StopPositionRequest::decode(&stop_position.encode().unwrap()).unwrap(),
            stop_position
        );

        let max_position = MaxRecordedPositionRequest {
            control_session_id: 7,
            correlation_id: 52,
            recording_id: 3,
        };
        assert_eq!(
            MaxRecordedPositionRequest::decode(&max_position.encode().unwrap()).unwrap(),
            max_position
        );

        let find_last = FindLastMatchingRecordingRequest {
            control_session_id: 7,
            correlation_id: 53,
            min_recording_id: 1,
            session_id: 22,
            stream_id: 1002,
            channel: "endpoint=127.0.0.1".into(),
        };
        assert_eq!(
            FindLastMatchingRecordingRequest::decode(&find_last.encode().unwrap()).unwrap(),
            find_last
        );

        let list_one = ListRecordingRequest {
            control_session_id: 7,
            correlation_id: 49,
            recording_id: 3,
        };
        assert_eq!(
            ListRecordingRequest::decode(&list_one.encode().unwrap()).unwrap(),
            list_one
        );

        let stop_subscription = StopRecordingSubscriptionRequest {
            control_session_id: 7,
            correlation_id: 50,
            subscription_id: 99,
        };
        assert_eq!(
            StopRecordingSubscriptionRequest::decode(&stop_subscription.encode().unwrap()).unwrap(),
            stop_subscription
        );

        let list_subscriptions = ListRecordingSubscriptionsRequest {
            control_session_id: 7,
            correlation_id: 51,
            pseudo_index: 0,
            subscription_count: 10,
            apply_stream_id: true,
            stream_id: 1002,
            channel: "aeron:ipc".into(),
        };
        assert_eq!(
            ListRecordingSubscriptionsRequest::decode(&list_subscriptions.encode().unwrap())
                .unwrap(),
            list_subscriptions
        );

        let response = ControlResponse {
            control_session_id: 7,
            correlation_id: 43,
            relevant_id: -1,
            code: ControlResponseCode::Error,
            version: PROTOCOL_SEMANTIC_VERSION,
            error_message: "recording unknown".into(),
        };
        assert_eq!(
            ControlResponse::decode(&response.encode().unwrap()).unwrap(),
            response
        );

        let bounded_replay = BoundedReplayRequest {
            control_session_id: 7,
            correlation_id: 55,
            recording_id: 3,
            position: 0,
            length: i64::MAX,
            limit_counter_id: 17,
            replay_stream_id: 1003,
            file_io_max_length: 4096,
            replay_token: 99,
            replay_channel: "aeron:ipc".into(),
        };
        assert_eq!(
            BoundedReplayRequest::decode(&bounded_replay.encode().unwrap()).unwrap(),
            bounded_replay
        );
    }

    #[test]
    fn malformed_frames_are_rejected_without_panicking() {
        let connect = ConnectRequest {
            correlation_id: 1,
            response_stream_id: 2,
            version: PROTOCOL_SEMANTIC_VERSION,
            response_channel: "aeron:ipc".into(),
        }
        .encode()
        .unwrap();
        assert_all_truncations(&connect, ConnectRequest::decode);

        let start = StartRecordingRequest {
            control_session_id: 1,
            correlation_id: 2,
            stream_id: 3,
            source_location: SourceLocation::Remote,
            channel: "aeron:ipc".into(),
        }
        .encode()
        .unwrap();
        assert_all_truncations(&start, StartRecordingRequest::decode);

        let mut start_v2 = StartRecordingRequestV2 {
            control_session_id: 1,
            correlation_id: 2,
            stream_id: 3,
            source_location: SourceLocation::Remote,
            auto_stop: true,
            channel: "aeron:ipc".into(),
        }
        .encode()
        .unwrap();
        assert_all_truncations(&start_v2, StartRecordingRequestV2::decode);
        put_i32(&mut start_v2, 32, 99);
        assert_eq!(
            StartRecordingRequestV2::decode(&start_v2),
            Err(CodecError::UnknownBoolean(99))
        );

        let extend_v2 = ExtendRecordingRequestV2 {
            control_session_id: 1,
            correlation_id: 2,
            recording_id: 3,
            stream_id: 4,
            source_location: SourceLocation::Local,
            auto_stop: false,
            channel: "aeron:ipc".into(),
        }
        .encode()
        .unwrap();
        assert_all_truncations(&extend_v2, ExtendRecordingRequestV2::decode);

        let response = ControlResponse {
            control_session_id: 1,
            correlation_id: 2,
            relevant_id: 3,
            code: ControlResponseCode::Ok,
            version: PROTOCOL_SEMANTIC_VERSION,
            error_message: String::new(),
        }
        .encode()
        .unwrap();
        assert_all_truncations(&response, ControlResponse::decode);

        let fixed = ListRecordingsRequest {
            control_session_id: 1,
            correlation_id: 2,
            from_recording_id: 3,
            record_count: 4,
        }
        .encode()
        .unwrap();
        assert_all_truncations(&fixed, ListRecordingsRequest::decode);

        let position = MaxRecordedPositionRequest {
            control_session_id: 1,
            correlation_id: 2,
            recording_id: 3,
        }
        .encode()
        .unwrap();
        assert_all_truncations(&position, MaxRecordedPositionRequest::decode);

        let find_last = FindLastMatchingRecordingRequest {
            control_session_id: 1,
            correlation_id: 2,
            min_recording_id: 0,
            session_id: 3,
            stream_id: 4,
            channel: "aeron:ipc".into(),
        }
        .encode()
        .unwrap();
        assert_all_truncations(&find_last, FindLastMatchingRecordingRequest::decode);

        let extend = ExtendRecordingRequest {
            control_session_id: 1,
            correlation_id: 2,
            recording_id: 3,
            stream_id: 4,
            source_location: SourceLocation::Local,
            channel: "aeron:ipc".into(),
        }
        .encode()
        .unwrap();
        assert_all_truncations(&extend, ExtendRecordingRequest::decode);

        let truncate = TruncateRecordingRequest {
            control_session_id: 1,
            correlation_id: 2,
            recording_id: 3,
            position: 64,
        }
        .encode()
        .unwrap();
        assert_all_truncations(&truncate, TruncateRecordingRequest::decode);

        let mut wrong_schema = connect.clone();
        wrong_schema[4..6].copy_from_slice(&999_u16.to_le_bytes());
        assert!(matches!(
            ConnectRequest::decode(&wrong_schema),
            Err(CodecError::WrongSchema { .. })
        ));

        let mut wrong_template = connect.clone();
        wrong_template[2..4].copy_from_slice(&999_u16.to_le_bytes());
        assert!(matches!(
            ConnectRequest::decode(&wrong_template),
            Err(CodecError::WrongTemplate { .. })
        ));

        let mut short_block = connect.clone();
        short_block[0..2].copy_from_slice(&8_u16.to_le_bytes());
        assert!(matches!(
            ConnectRequest::decode(&short_block),
            Err(CodecError::BlockLength { .. })
        ));

        let mut invalid_length = connect;
        invalid_length[24..28].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            ConnectRequest::decode(&invalid_length),
            Err(CodecError::VariableFieldTooLong { .. })
        ));

        let mut unknown_source = start;
        unknown_source[28..32].copy_from_slice(&99_i32.to_le_bytes());
        assert!(matches!(
            StartRecordingRequest::decode(&unknown_source),
            Err(CodecError::UnknownSourceLocation(99))
        ));

        let mut unknown_code = response;
        unknown_code[32..36].copy_from_slice(&99_i32.to_le_bytes());
        assert!(matches!(
            ControlResponse::decode(&unknown_code),
            Err(CodecError::UnknownResponseCode(99))
        ));

        let stop_subscription = StopRecordingSubscriptionRequest {
            control_session_id: 1,
            correlation_id: 2,
            subscription_id: 3,
        }
        .encode()
        .unwrap();
        assert_all_truncations(&stop_subscription, StopRecordingSubscriptionRequest::decode);

        let mut list_subscriptions = ListRecordingSubscriptionsRequest {
            control_session_id: 1,
            correlation_id: 2,
            pseudo_index: 0,
            subscription_count: 1,
            apply_stream_id: true,
            stream_id: 3,
            channel: "aeron:ipc".into(),
        }
        .encode()
        .unwrap();
        assert_all_truncations(
            &list_subscriptions,
            ListRecordingSubscriptionsRequest::decode,
        );
        list_subscriptions[32..36].copy_from_slice(&99_i32.to_le_bytes());
        assert_eq!(
            ListRecordingSubscriptionsRequest::decode(&list_subscriptions),
            Err(CodecError::UnknownBoolean(99))
        );
    }

    #[test]
    fn matches_java_generated_v13_golden_vectors() {
        let connect = ConnectRequest {
            correlation_id: 42,
            response_stream_id: 1001,
            version: PROTOCOL_SEMANTIC_VERSION,
            response_channel: "aeron:ipc".into(),
        };
        assert_eq!(
            to_hex(&connect.encode().unwrap()),
            "1000020065000d002a00000000000000e9030000000c0100090000006165726f6e3a697063"
        );

        let response = ControlResponse {
            control_session_id: 7,
            correlation_id: 43,
            relevant_id: -1,
            code: ControlResponseCode::Error,
            version: PROTOCOL_SEMANTIC_VERSION,
            error_message: "recording unknown".into(),
        };
        assert_eq!(
            to_hex(&response.encode().unwrap()),
            "2000010065000d0007000000000000002b00000000000000ffffffffffffffff01000000000c0100110000007265636f7264696e6720756e6b6e6f776e"
        );

        let start = StartRecordingRequest {
            control_session_id: 7,
            correlation_id: 43,
            stream_id: 1002,
            source_location: SourceLocation::Local,
            channel: "aeron:udp?endpoint=127.0.0.1:40123".into(),
        };
        assert_eq!(
            to_hex(&start.encode().unwrap()),
            "1800040065000d0007000000000000002b00000000000000ea03000000000000220000006165726f6e3a7564703f656e64706f696e743d3132372e302e302e313a3430313233"
        );
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn assert_all_truncations<T>(frame: &[u8], decode: impl Fn(&[u8]) -> Result<T, CodecError>) {
        for length in 0..frame.len() {
            assert!(decode(&frame[..length]).is_err(), "length {length}");
        }
    }
}
