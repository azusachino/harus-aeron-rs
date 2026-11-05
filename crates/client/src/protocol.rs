//! Aeron wire protocol frame codecs.
//!
//! Mirrors upstream `io.aeron.protocol`. Wire-compatible frame layouts for the Aeron
//! UDP protocol (<https://github.com/aeron-io/aeron>).

use agrona::AtomicBuffer;

/// Header type PAD.
pub const HDR_TYPE_PAD: u16 = 0x00;
/// Header type DATA.
pub const HDR_TYPE_DATA: u16 = 0x01;
/// Header type NAK.
pub const HDR_TYPE_NAK: u16 = 0x02;
/// Header type SM.
pub const HDR_TYPE_SM: u16 = 0x03;
/// Header type ERR.
pub const HDR_TYPE_ERR: u16 = 0x04;
/// Header type SETUP.
pub const HDR_TYPE_SETUP: u16 = 0x05;
/// Header type RTT Measurement.
pub const HDR_TYPE_RTTM: u16 = 0x06;
/// Header type RESOLUTION.
pub const HDR_TYPE_RES: u16 = 0x07;
/// Header type ATS Data.
pub const HDR_TYPE_ATS_DATA: u16 = 0x08;
/// Header type ATS Status Message.
pub const HDR_TYPE_ATS_SM: u16 = 0x09;
/// Header type ATS Setup.
pub const HDR_TYPE_ATS_SETUP: u16 = 0x0A;
/// Header type Response Setup.
pub const HDR_TYPE_RSP_SETUP: u16 = 0x0B;
/// Header type EXT.
pub const HDR_TYPE_EXT: u16 = 0xFFFF;

/// Default version.
pub const CURRENT_VERSION: u8 = 0x0;

/// Offset in the frame at which the frame length field begins.
pub const FRAME_LENGTH_FIELD_OFFSET: usize = 0;
/// Offset in the frame at which the version field begins.
pub const VERSION_FIELD_OFFSET: usize = 4;
/// Offset in the frame at which the flags field begins.
pub const FLAGS_FIELD_OFFSET: usize = 5;
/// Offset in the frame at which the frame type field begins.
pub const TYPE_FIELD_OFFSET: usize = 6;

/// Minimum length of any Aeron frame.
pub const MIN_HEADER_LENGTH: usize = TYPE_FIELD_OFFSET + std::mem::size_of::<i16>();

/// Flyweight for the general Aeron network protocol header of a message frame.
///
/// ```text
///   0                   1                   2                   3
///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///  |                        Frame Length                           |
///  +---------------------------------------------------------------+
///  |  Version    |     Flags     |               Type              |
///  +-------------+---------------+---------------------------------+
///  |                       Depends on Type                        ...
/// ```
#[derive(Clone)]
pub struct HeaderFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
}

impl<'a> HeaderFlyweight<'a> {
    /// Construct a flyweight which wraps an [`AtomicBuffer`] over the frame.
    pub fn new(buffer: AtomicBuffer<'a>) -> Self {
        Self { buffer }
    }

    /// The version field value.
    pub fn version(&self) -> u8 {
        self.buffer.get_u8(VERSION_FIELD_OFFSET)
    }

    /// Set the version field value.
    pub fn set_version(&self, version: u8) -> &Self {
        self.buffer.put_u8(VERSION_FIELD_OFFSET, version);
        self
    }

    /// The flags field value.
    pub fn flags(&self) -> u8 {
        self.buffer.get_u8(FLAGS_FIELD_OFFSET)
    }

    /// Set the flags field value.
    pub fn set_flags(&self, flags: u8) -> &Self {
        self.buffer.put_u8(FLAGS_FIELD_OFFSET, flags);
        self
    }

    /// The type field value.
    pub fn header_type(&self) -> u16 {
        self.buffer.get_i16(TYPE_FIELD_OFFSET) as u16
    }

    /// Set the type field value.
    pub fn set_header_type(&self, header_type: u16) -> &Self {
        self.buffer.put_i16(TYPE_FIELD_OFFSET, header_type as i16);
        self
    }

    /// The length of the frame field value.
    pub fn frame_length(&self) -> i32 {
        self.buffer.get_i32(FRAME_LENGTH_FIELD_OFFSET)
    }

    /// Set the length of the frame field value.
    pub fn set_frame_length(&self, length: i32) -> &Self {
        self.buffer.put_i32(FRAME_LENGTH_FIELD_OFFSET, length);
        self
    }
}

/// Convert header flags to an array of chars to be human-readable.
pub fn flags_to_chars(flags: u8) -> [char; 8] {
    let mut chars = ['0'; 8];
    let mut mask: u8 = 1 << 7;

    for c in chars.iter_mut() {
        if flags & mask == mask {
            *c = '1';
        }
        mask >>= 1;
    }

    chars
}

/// Append header flags to a `String` to be human-readable.
pub fn append_flags_as_chars(flags: u8, out: &mut String) {
    let mut mask: u8 = 1 << 7;

    for _ in 0..8 {
        out.push(if flags & mask == mask { '1' } else { '0' });
        mask >>= 1;
    }
}

/// Offset in the Data frame at which the term-offset field begins.
pub const DATA_TERM_OFFSET_FIELD_OFFSET: usize = 8;
/// Offset in the Data frame at which the session-id field begins.
pub const DATA_SESSION_ID_FIELD_OFFSET: usize = 12;
/// Offset in the Data frame at which the stream-id field begins.
pub const DATA_STREAM_ID_FIELD_OFFSET: usize = 16;
/// Offset in the Data frame at which the term-id field begins.
pub const DATA_TERM_ID_FIELD_OFFSET: usize = 20;
/// Offset in the Data frame at which the reserved value field begins.
pub const DATA_RESERVED_VALUE_OFFSET: usize = 24;

/// Length of the Data Header frame (32 bytes).
pub const DATA_HEADER_LENGTH: usize = 32;

/// Fragment that Begins a message Flag.
pub const BEGIN_FLAG: u8 = 0x80;
/// Fragment that Ends a message Flag.
pub const END_FLAG: u8 = 0x40;
/// Begin and End Flags.
pub const BEGIN_AND_END_FLAGS: u8 = BEGIN_FLAG | END_FLAG;
/// End of Stream (EOS) Flag.
pub const EOS_FLAG: u8 = 0x20;

pub const BEGIN_FRAG_FLAG: u8 = BEGIN_FLAG;
pub const END_FRAG_FLAG: u8 = END_FLAG;
pub const UNFRAGMENTED: u8 = BEGIN_AND_END_FLAGS;

/// Length in bytes of the string length header used by [`agrona::AtomicBuffer`]'s
/// string encoding (a 4-byte length prefix).
const STR_HEADER_LEN: usize = 4;

/// Offset in the frame at which the session-id field begins.
pub const SESSION_ID_FIELD_OFFSET: usize = MIN_HEADER_LENGTH;
/// Offset in the frame at which the stream-id field begins.
pub const STREAM_ID_FIELD_OFFSET: usize = SESSION_ID_FIELD_OFFSET + std::mem::size_of::<i32>();
/// Offset in the frame at which the receiver-id field begins.
pub const RECEIVER_ID_FIELD_OFFSET: usize = STREAM_ID_FIELD_OFFSET + std::mem::size_of::<i32>();
/// Offset in the frame at which the group-tag field begins.
pub const GROUP_TAG_FIELD_OFFSET: usize = RECEIVER_ID_FIELD_OFFSET + std::mem::size_of::<i64>();
/// Offset in the frame at which the error code field begins.
pub const ERROR_CODE_FIELD_OFFSET: usize = GROUP_TAG_FIELD_OFFSET + std::mem::size_of::<i64>();
/// Offset in the frame at which the error string field begins.
pub const ERROR_STRING_FIELD_OFFSET: usize = ERROR_CODE_FIELD_OFFSET + std::mem::size_of::<i32>();

/// Length of the Error Header, excluding the error message contents.
pub const ERROR_HEADER_LENGTH: usize = ERROR_STRING_FIELD_OFFSET + std::mem::size_of::<i32>();

/// Maximum length that an error message can be.
pub const MAX_ERROR_MESSAGE_LENGTH: usize = 1023;

/// Maximum length of an error frame: header length plus the maximum message length.
pub const MAX_ERROR_FRAME_LENGTH: usize = ERROR_HEADER_LENGTH + MAX_ERROR_MESSAGE_LENGTH;

/// Flag to indicate that the group tag field is relevant; if unset the value should be ignored.
pub const HAS_GROUP_ID_FLAG: u8 = 0x08;

/// Flyweight for the general Aeron network protocol error frame.
///
/// ```text
///    0                   1                   2                   3
///    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// 0  |R|                 Frame Length (varies)                       |
///    +---------------+---------------+-------------------------------+
/// 4  |   Version     |     Flags     |         Type (=0x04)          |
///    +---------------+---------------+-------------------------------+
/// 8  |                          Session ID                           |
///    +---------------------------------------------------------------+
/// 12 |                           Stream ID                           |
///    +---------------------------------------------------------------+
/// 16 |                          Receiver ID                          ...
///    +---------------------------------------------------------------+
/// 24 |                           Group Tag                           ...
///    +---------------------------------------------------------------+
/// 32 |                          Error Code                           |
///    +---------------------------------------------------------------+
/// 36 |                     Error String Length                       |
///    +---------------------------------------------------------------+
/// 40 |                         Error String                        ...
///    +---------------------------------------------------------------+
/// ```
#[derive(Clone)]
pub struct ErrorFlyweight<'a> {
    header: HeaderFlyweight<'a>,
}

impl<'a> ErrorFlyweight<'a> {
    /// Construct the `ErrorFlyweight` over a buffer containing the frame.
    pub fn new(buffer: AtomicBuffer<'a>) -> Self {
        Self {
            header: HeaderFlyweight::new(buffer),
        }
    }

    fn buffer(&self) -> &AtomicBuffer<'a> {
        &self.header.buffer
    }

    /// The version field value.
    pub fn version(&self) -> u8 {
        self.header.version()
    }

    /// The flags field value.
    pub fn flags(&self) -> u8 {
        self.header.flags()
    }

    /// Set the flags field value.
    pub fn set_flags(&self, flags: u8) -> &Self {
        self.header.set_flags(flags);
        self
    }

    /// The type field value.
    pub fn header_type(&self) -> u16 {
        self.header.header_type()
    }

    /// The length of the frame field value.
    pub fn frame_length(&self) -> i32 {
        self.header.frame_length()
    }

    /// Set the length of the frame field value.
    pub fn set_frame_length(&self, length: i32) -> &Self {
        self.header.set_frame_length(length);
        self
    }

    /// The session-id for the stream.
    pub fn session_id(&self) -> i32 {
        self.buffer().get_i32(SESSION_ID_FIELD_OFFSET)
    }

    /// Set session-id for the stream.
    pub fn set_session_id(&self, session_id: i32) -> &Self {
        self.buffer().put_i32(SESSION_ID_FIELD_OFFSET, session_id);
        self
    }

    /// The stream-id for the stream.
    pub fn stream_id(&self) -> i32 {
        self.buffer().get_i32(STREAM_ID_FIELD_OFFSET)
    }

    /// Set stream-id for the stream.
    pub fn set_stream_id(&self, stream_id: i32) -> &Self {
        self.buffer().put_i32(STREAM_ID_FIELD_OFFSET, stream_id);
        self
    }

    /// The receiver-id for the stream.
    pub fn receiver_id(&self) -> i64 {
        self.buffer().get_i64(RECEIVER_ID_FIELD_OFFSET)
    }

    /// Set receiver-id for the stream.
    pub fn set_receiver_id(&self, receiver_id: i64) -> &Self {
        self.buffer().put_i64(RECEIVER_ID_FIELD_OFFSET, receiver_id);
        self
    }

    /// Get the group tag for the message.
    pub fn group_tag(&self) -> i64 {
        self.buffer().get_i64(GROUP_TAG_FIELD_OFFSET)
    }

    /// Determines if this message has the group tag flag set.
    pub fn has_group_tag(&self) -> bool {
        self.flags() & HAS_GROUP_ID_FLAG == HAS_GROUP_ID_FLAG
    }

    /// Set an optional group tag; `None` clears the [`HAS_GROUP_ID_FLAG`] and leaves the value
    /// unset. `Some` sets the value and the flag.
    pub fn set_group_tag(&self, group_tag: Option<i64>) -> &Self {
        match group_tag {
            None => {
                self.set_flags(self.flags() & !HAS_GROUP_ID_FLAG);
            }
            Some(tag) => {
                self.buffer().put_i64(GROUP_TAG_FIELD_OFFSET, tag);
                self.set_flags(self.flags() | HAS_GROUP_ID_FLAG);
            }
        }
        self
    }

    /// The error-code for the message.
    pub fn error_code(&self) -> i32 {
        self.buffer().get_i32(ERROR_CODE_FIELD_OFFSET)
    }

    /// Set error-code for the message.
    pub fn set_error_code(&self, error_code: i32) -> &Self {
        self.buffer().put_i32(ERROR_CODE_FIELD_OFFSET, error_code);
        self
    }

    /// Get the error string for the message.
    pub fn error_message(&self) -> String {
        self.buffer().get_string_utf8(ERROR_STRING_FIELD_OFFSET)
    }

    /// Set the error string for the message.
    pub fn set_error_message(&self, error_message: &str) -> &Self {
        let header_and_message_length = self
            .buffer()
            .put_string_utf8(ERROR_STRING_FIELD_OFFSET, error_message);
        self.set_frame_length(
            (ERROR_HEADER_LENGTH + (header_and_message_length - STR_HEADER_LEN)) as i32,
        );
        self
    }
}

/// Flyweight for Data Frame header of a message fragment.
#[derive(Clone)]
pub struct DataHeaderFlyweight<'a> {
    header: HeaderFlyweight<'a>,
}

impl<'a> DataHeaderFlyweight<'a> {
    /// Construct the `DataHeaderFlyweight` over a buffer containing the frame.
    pub fn new(buffer: AtomicBuffer<'a>) -> Self {
        Self {
            header: HeaderFlyweight::new(buffer),
        }
    }

    fn buffer(&self) -> &AtomicBuffer<'a> {
        &self.header.buffer
    }

    /// The version field value.
    pub fn version(&self) -> u8 {
        self.header.version()
    }

    /// Set the version field value.
    pub fn set_version(&self, version: u8) -> &Self {
        self.header.set_version(version);
        self
    }

    /// The flags field value.
    pub fn flags(&self) -> u8 {
        self.header.flags()
    }

    /// Set the flags field value.
    pub fn set_flags(&self, flags: u8) -> &Self {
        self.header.set_flags(flags);
        self
    }

    /// The type field value.
    pub fn header_type(&self) -> u16 {
        self.header.header_type()
    }

    /// Set the type field value.
    pub fn set_header_type(&self, header_type: u16) -> &Self {
        self.header.set_header_type(header_type);
        self
    }

    /// The length of the frame field value.
    pub fn frame_length(&self) -> i32 {
        self.header.frame_length()
    }

    /// Set the length of the frame field value.
    pub fn set_frame_length(&self, length: i32) -> &Self {
        self.header.set_frame_length(length);
        self
    }

    /// The term offset field value.
    pub fn term_offset(&self) -> i32 {
        self.buffer().get_i32(DATA_TERM_OFFSET_FIELD_OFFSET)
    }

    /// Set the term offset field value.
    pub fn set_term_offset(&self, term_offset: i32) -> &Self {
        self.buffer()
            .put_i32(DATA_TERM_OFFSET_FIELD_OFFSET, term_offset);
        self
    }

    /// The session ID field value.
    pub fn session_id(&self) -> i32 {
        self.buffer().get_i32(DATA_SESSION_ID_FIELD_OFFSET)
    }

    /// Set the session ID field value.
    pub fn set_session_id(&self, session_id: i32) -> &Self {
        self.buffer()
            .put_i32(DATA_SESSION_ID_FIELD_OFFSET, session_id);
        self
    }

    /// The stream ID field value.
    pub fn stream_id(&self) -> i32 {
        self.buffer().get_i32(DATA_STREAM_ID_FIELD_OFFSET)
    }

    /// Set the stream ID field value.
    pub fn set_stream_id(&self, stream_id: i32) -> &Self {
        self.buffer()
            .put_i32(DATA_STREAM_ID_FIELD_OFFSET, stream_id);
        self
    }

    /// The term ID field value.
    pub fn term_id(&self) -> i32 {
        self.buffer().get_i32(DATA_TERM_ID_FIELD_OFFSET)
    }

    /// Set the term ID field value.
    pub fn set_term_id(&self, term_id: i32) -> &Self {
        self.buffer().put_i32(DATA_TERM_ID_FIELD_OFFSET, term_id);
        self
    }

    /// The reserved value field value.
    pub fn reserved_value(&self) -> i64 {
        self.buffer().get_i64(DATA_RESERVED_VALUE_OFFSET)
    }

    /// Set the reserved value field value.
    pub fn set_reserved_value(&self, reserved_value: i64) -> &Self {
        self.buffer()
            .put_i64(DATA_RESERVED_VALUE_OFFSET, reserved_value);
        self
    }
}

// ---------------------------------------------------------------------------
// Control-plane frames. Field offsets mirror vendor io.aeron.protocol.* (1.50.x).
// ---------------------------------------------------------------------------

/// Reply-requested flag for RTT measurement frames.
pub const RTTM_REPLY_FLAG: u8 = 0x80;

/// Status Message (SM) frame: session@8, stream@12, consumptionTermId@16,
/// consumptionTermOffset@20, receiverWindow@24, receiverId@28. Header length 36.
pub const SM_SESSION_ID_FIELD_OFFSET: usize = 8;
pub const SM_STREAM_ID_FIELD_OFFSET: usize = 12;
pub const SM_CONSUMPTION_TERM_ID_FIELD_OFFSET: usize = 16;
pub const SM_CONSUMPTION_TERM_OFFSET_FIELD_OFFSET: usize = 20;
pub const SM_RECEIVER_WINDOW_FIELD_OFFSET: usize = 24;
pub const SM_RECEIVER_ID_FIELD_OFFSET: usize = 28;
/// Length of the Status Message header (36 bytes).
pub const STATUS_MESSAGE_HEADER_LENGTH: usize = 36;
/// Status Message flag requesting the sender to emit a SETUP frame.
///
/// Matches `vendor/aeron/aeron-client/src/main/java/io/aeron/protocol/StatusMessageFlyweight.java`.
pub const SEND_SETUP_FLAG: u8 = 0x80;
/// Publisher should remove the receiver from flow control.
pub const END_OF_STREAM_FLAG: u8 = 0x40;
/// Optional group-tag feedback starts immediately after the fixed status header.
pub const SM_GROUP_TAG_FIELD_OFFSET: usize = STATUS_MESSAGE_HEADER_LENGTH;

/// Flyweight over a Status Message frame.
#[derive(Clone)]
pub struct StatusMessageFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
}

impl<'a> StatusMessageFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>) -> Self {
        Self { buffer }
    }

    /// The base frame header (frame length / version / flags / type).
    pub fn header(&self) -> HeaderFlyweight<'a> {
        HeaderFlyweight::new(self.buffer.clone())
    }

    pub fn flags(&self) -> u8 {
        self.header().flags()
    }

    pub fn set_flags(&self, flags: u8) -> &Self {
        self.header().set_flags(flags);
        self
    }

    pub fn session_id(&self) -> i32 {
        self.buffer.get_i32(SM_SESSION_ID_FIELD_OFFSET)
    }
    pub fn set_session_id(&self, v: i32) -> &Self {
        self.buffer.put_i32(SM_SESSION_ID_FIELD_OFFSET, v);
        self
    }
    pub fn stream_id(&self) -> i32 {
        self.buffer.get_i32(SM_STREAM_ID_FIELD_OFFSET)
    }
    pub fn set_stream_id(&self, v: i32) -> &Self {
        self.buffer.put_i32(SM_STREAM_ID_FIELD_OFFSET, v);
        self
    }
    pub fn consumption_term_id(&self) -> i32 {
        self.buffer.get_i32(SM_CONSUMPTION_TERM_ID_FIELD_OFFSET)
    }
    pub fn set_consumption_term_id(&self, v: i32) -> &Self {
        self.buffer.put_i32(SM_CONSUMPTION_TERM_ID_FIELD_OFFSET, v);
        self
    }
    pub fn consumption_term_offset(&self) -> i32 {
        self.buffer.get_i32(SM_CONSUMPTION_TERM_OFFSET_FIELD_OFFSET)
    }
    pub fn set_consumption_term_offset(&self, v: i32) -> &Self {
        self.buffer
            .put_i32(SM_CONSUMPTION_TERM_OFFSET_FIELD_OFFSET, v);
        self
    }
    pub fn receiver_window_length(&self) -> i32 {
        self.buffer.get_i32(SM_RECEIVER_WINDOW_FIELD_OFFSET)
    }
    pub fn set_receiver_window_length(&self, v: i32) -> &Self {
        self.buffer.put_i32(SM_RECEIVER_WINDOW_FIELD_OFFSET, v);
        self
    }
    pub fn receiver_id(&self) -> i64 {
        self.buffer.get_i64(SM_RECEIVER_ID_FIELD_OFFSET)
    }
    pub fn set_receiver_id(&self, v: i64) -> &Self {
        self.buffer.put_i64(SM_RECEIVER_ID_FIELD_OFFSET, v);
        self
    }

    /// Whether this status message carries an 8-byte group tag.
    pub fn has_group_tag(&self) -> bool {
        self.header().frame_length() > STATUS_MESSAGE_HEADER_LENGTH as i32
    }

    pub fn group_tag(&self) -> i64 {
        self.buffer.get_i64(SM_GROUP_TAG_FIELD_OFFSET)
    }

    /// Set or clear the optional 8-byte group tag and update the frame length.
    pub fn set_group_tag(&self, group_tag: Option<i64>) -> &Self {
        match group_tag {
            Some(group_tag) => {
                self.buffer.put_i64(SM_GROUP_TAG_FIELD_OFFSET, group_tag);
                self.header()
                    .set_frame_length((STATUS_MESSAGE_HEADER_LENGTH + 8) as i32);
            }
            None => {
                self.header()
                    .set_frame_length(STATUS_MESSAGE_HEADER_LENGTH as i32);
            }
        }
        self
    }

    /// Check if the SM frame is valid given the `term_length`.
    ///
    /// Mirrors `StatusMessageFlyweight.isValid` in upstream Aeron:
    /// - consumption term offset must be non-negative, strictly less than `term_length`,
    ///   and frame-aligned (multiple of 32).
    /// - receiver window length must be non-negative and not exceed half of `term_length`.
    pub fn is_valid(&self, term_length: i32) -> bool {
        let term_offset = self.consumption_term_offset();
        let receiver_window_length = self.receiver_window_length();
        term_offset >= 0
            && term_offset < term_length
            && term_offset % crate::logbuffer::FRAME_ALIGNMENT == 0
            && receiver_window_length >= 0
            && receiver_window_length <= (term_length >> 1)
    }
}

/// NAK frame: session@8, stream@12, termId@16, termOffset@20, length@24. Header length 28.
pub const NAK_SESSION_ID_FIELD_OFFSET: usize = 8;
pub const NAK_STREAM_ID_FIELD_OFFSET: usize = 12;
pub const NAK_TERM_ID_FIELD_OFFSET: usize = 16;
pub const NAK_TERM_OFFSET_FIELD_OFFSET: usize = 20;
pub const NAK_LENGTH_FIELD_OFFSET: usize = 24;
/// Length of the NAK header (28 bytes).
pub const NAK_HEADER_LENGTH: usize = 28;

/// Flyweight over a NAK (negative acknowledgement) frame.
#[derive(Clone)]
pub struct NakFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
}

impl<'a> NakFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>) -> Self {
        Self { buffer }
    }
    pub fn header(&self) -> HeaderFlyweight<'a> {
        HeaderFlyweight::new(self.buffer.clone())
    }
    pub fn session_id(&self) -> i32 {
        self.buffer.get_i32(NAK_SESSION_ID_FIELD_OFFSET)
    }
    pub fn set_session_id(&self, v: i32) -> &Self {
        self.buffer.put_i32(NAK_SESSION_ID_FIELD_OFFSET, v);
        self
    }
    pub fn stream_id(&self) -> i32 {
        self.buffer.get_i32(NAK_STREAM_ID_FIELD_OFFSET)
    }
    pub fn set_stream_id(&self, v: i32) -> &Self {
        self.buffer.put_i32(NAK_STREAM_ID_FIELD_OFFSET, v);
        self
    }
    pub fn term_id(&self) -> i32 {
        self.buffer.get_i32(NAK_TERM_ID_FIELD_OFFSET)
    }
    pub fn set_term_id(&self, v: i32) -> &Self {
        self.buffer.put_i32(NAK_TERM_ID_FIELD_OFFSET, v);
        self
    }
    pub fn term_offset(&self) -> i32 {
        self.buffer.get_i32(NAK_TERM_OFFSET_FIELD_OFFSET)
    }
    pub fn set_term_offset(&self, v: i32) -> &Self {
        self.buffer.put_i32(NAK_TERM_OFFSET_FIELD_OFFSET, v);
        self
    }
    pub fn length(&self) -> i32 {
        self.buffer.get_i32(NAK_LENGTH_FIELD_OFFSET)
    }
    pub fn set_length(&self, v: i32) -> &Self {
        self.buffer.put_i32(NAK_LENGTH_FIELD_OFFSET, v);
        self
    }
}

/// Setup frame: termOffset@8, session@12, stream@16, initialTermId@20, activeTermId@24,
/// termLength@28, mtu@32, ttl@36. Header length 40.
pub const SETUP_TERM_OFFSET_FIELD_OFFSET: usize = 8;
pub const SETUP_SESSION_ID_FIELD_OFFSET: usize = 12;
pub const SETUP_STREAM_ID_FIELD_OFFSET: usize = 16;
pub const SETUP_INITIAL_TERM_ID_FIELD_OFFSET: usize = 20;
pub const SETUP_ACTIVE_TERM_ID_FIELD_OFFSET: usize = 24;
pub const SETUP_TERM_LENGTH_FIELD_OFFSET: usize = 28;
pub const SETUP_MTU_FIELD_OFFSET: usize = 32;
pub const SETUP_TTL_FIELD_OFFSET: usize = 36;
/// Length of the Setup header (40 bytes).
pub const SETUP_HEADER_LENGTH: usize = 40;

/// Flyweight over a Setup frame.
#[derive(Clone)]
pub struct SetupFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
}

impl<'a> SetupFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>) -> Self {
        Self { buffer }
    }
    pub fn header(&self) -> HeaderFlyweight<'a> {
        HeaderFlyweight::new(self.buffer.clone())
    }
    pub fn term_offset(&self) -> i32 {
        self.buffer.get_i32(SETUP_TERM_OFFSET_FIELD_OFFSET)
    }
    pub fn set_term_offset(&self, v: i32) -> &Self {
        self.buffer.put_i32(SETUP_TERM_OFFSET_FIELD_OFFSET, v);
        self
    }
    pub fn session_id(&self) -> i32 {
        self.buffer.get_i32(SETUP_SESSION_ID_FIELD_OFFSET)
    }
    pub fn set_session_id(&self, v: i32) -> &Self {
        self.buffer.put_i32(SETUP_SESSION_ID_FIELD_OFFSET, v);
        self
    }
    pub fn stream_id(&self) -> i32 {
        self.buffer.get_i32(SETUP_STREAM_ID_FIELD_OFFSET)
    }
    pub fn set_stream_id(&self, v: i32) -> &Self {
        self.buffer.put_i32(SETUP_STREAM_ID_FIELD_OFFSET, v);
        self
    }
    pub fn initial_term_id(&self) -> i32 {
        self.buffer.get_i32(SETUP_INITIAL_TERM_ID_FIELD_OFFSET)
    }
    pub fn set_initial_term_id(&self, v: i32) -> &Self {
        self.buffer.put_i32(SETUP_INITIAL_TERM_ID_FIELD_OFFSET, v);
        self
    }
    pub fn active_term_id(&self) -> i32 {
        self.buffer.get_i32(SETUP_ACTIVE_TERM_ID_FIELD_OFFSET)
    }
    pub fn set_active_term_id(&self, v: i32) -> &Self {
        self.buffer.put_i32(SETUP_ACTIVE_TERM_ID_FIELD_OFFSET, v);
        self
    }
    pub fn term_length(&self) -> i32 {
        self.buffer.get_i32(SETUP_TERM_LENGTH_FIELD_OFFSET)
    }
    pub fn set_term_length(&self, v: i32) -> &Self {
        self.buffer.put_i32(SETUP_TERM_LENGTH_FIELD_OFFSET, v);
        self
    }
    pub fn mtu_length(&self) -> i32 {
        self.buffer.get_i32(SETUP_MTU_FIELD_OFFSET)
    }
    pub fn set_mtu_length(&self, v: i32) -> &Self {
        self.buffer.put_i32(SETUP_MTU_FIELD_OFFSET, v);
        self
    }
    pub fn ttl(&self) -> i32 {
        self.buffer.get_i32(SETUP_TTL_FIELD_OFFSET)
    }
    pub fn set_ttl(&self, v: i32) -> &Self {
        self.buffer.put_i32(SETUP_TTL_FIELD_OFFSET, v);
        self
    }
}

/// RTT measurement frame: session@8, stream@12, echoTimestamp@16, receptionDelta@24,
/// receiverId@32. Header length 40.
pub const RTTM_SESSION_ID_FIELD_OFFSET: usize = 8;
pub const RTTM_STREAM_ID_FIELD_OFFSET: usize = 12;
pub const RTTM_ECHO_TIMESTAMP_FIELD_OFFSET: usize = 16;
pub const RTTM_RECEPTION_DELTA_FIELD_OFFSET: usize = 24;
pub const RTTM_RECEIVER_ID_FIELD_OFFSET: usize = 32;
/// Length of the RTT measurement header (40 bytes).
pub const RTTM_HEADER_LENGTH: usize = 40;

/// Flyweight over an RTT measurement frame.
#[derive(Clone)]
pub struct RttMeasurementFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
}

impl<'a> RttMeasurementFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>) -> Self {
        Self { buffer }
    }
    pub fn header(&self) -> HeaderFlyweight<'a> {
        HeaderFlyweight::new(self.buffer.clone())
    }
    pub fn session_id(&self) -> i32 {
        self.buffer.get_i32(RTTM_SESSION_ID_FIELD_OFFSET)
    }
    pub fn set_session_id(&self, v: i32) -> &Self {
        self.buffer.put_i32(RTTM_SESSION_ID_FIELD_OFFSET, v);
        self
    }
    pub fn stream_id(&self) -> i32 {
        self.buffer.get_i32(RTTM_STREAM_ID_FIELD_OFFSET)
    }
    pub fn set_stream_id(&self, v: i32) -> &Self {
        self.buffer.put_i32(RTTM_STREAM_ID_FIELD_OFFSET, v);
        self
    }
    pub fn echo_timestamp(&self) -> i64 {
        self.buffer.get_i64(RTTM_ECHO_TIMESTAMP_FIELD_OFFSET)
    }
    pub fn set_echo_timestamp(&self, v: i64) -> &Self {
        self.buffer.put_i64(RTTM_ECHO_TIMESTAMP_FIELD_OFFSET, v);
        self
    }
    pub fn reception_delta(&self) -> i64 {
        self.buffer.get_i64(RTTM_RECEPTION_DELTA_FIELD_OFFSET)
    }
    pub fn set_reception_delta(&self, v: i64) -> &Self {
        self.buffer.put_i64(RTTM_RECEPTION_DELTA_FIELD_OFFSET, v);
        self
    }
    pub fn receiver_id(&self) -> i64 {
        self.buffer.get_i64(RTTM_RECEIVER_ID_FIELD_OFFSET)
    }
    pub fn set_receiver_id(&self, v: i64) -> &Self {
        self.buffer.put_i64(RTTM_RECEIVER_ID_FIELD_OFFSET, v);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_convert_flags() {
        let flags: u8 = 0b01101000;
        let chars = flags_to_chars(flags);
        assert_eq!(chars.iter().collect::<String>(), "01101000");
    }

    #[test]
    fn should_append_flags() {
        let flags: u8 = 0b01100000;
        let mut out = String::new();
        append_flags_as_chars(flags, &mut out);
        assert_eq!(out, "01100000");
    }

    #[test]
    fn should_get_and_set_fields() {
        let mut bytes = [0u8; MIN_HEADER_LENGTH];
        let buffer = AtomicBuffer::new(&mut bytes);
        let header = HeaderFlyweight::new(buffer);

        header.set_frame_length(128);
        header.set_version(CURRENT_VERSION);
        header.set_flags(0xF0);
        header.set_header_type(HDR_TYPE_DATA);

        assert_eq!(header.frame_length(), 128);
        assert_eq!(header.version(), CURRENT_VERSION);
        assert_eq!(header.flags(), 0xF0);
        assert_eq!(header.header_type(), HDR_TYPE_DATA);
    }

    #[test]
    fn should_correctly_set_flags_for_group_tag() {
        let mut bytes = [0u8; MAX_ERROR_FRAME_LENGTH];
        let buffer = AtomicBuffer::new(&mut bytes);
        let error = ErrorFlyweight::new(buffer);

        assert_eq!(error.flags(), 0);
        error.set_group_tag(Some(10));
        assert_eq!(error.flags(), HAS_GROUP_ID_FLAG);

        error.set_group_tag(None);
        assert_ne!(error.flags(), HAS_GROUP_ID_FLAG);
    }

    #[test]
    fn should_correctly_update_existing_flags_for_group_tag() {
        let mut bytes = [0u8; MAX_ERROR_FRAME_LENGTH];
        let buffer = AtomicBuffer::new(&mut bytes);
        let error = ErrorFlyweight::new(buffer);

        let initial_flags: u8 = 0x3;

        error.set_flags(initial_flags);
        assert_eq!(error.flags(), initial_flags);
        error.set_group_tag(Some(10));
        assert_eq!(error.flags() & HAS_GROUP_ID_FLAG, HAS_GROUP_ID_FLAG);
        assert_eq!(error.flags() & initial_flags, initial_flags);

        error.set_group_tag(None);
        assert_eq!(error.flags() & HAS_GROUP_ID_FLAG, 0);
        assert_eq!(error.flags(), initial_flags);
    }

    #[test]
    fn should_round_trip_error_message_and_frame_length() {
        let mut bytes = [0u8; MAX_ERROR_FRAME_LENGTH];
        let buffer = AtomicBuffer::new(&mut bytes);
        let error = ErrorFlyweight::new(buffer);

        error.set_session_id(1).set_stream_id(2).set_error_code(3);
        error.set_error_message("boom");

        assert_eq!(error.session_id(), 1);
        assert_eq!(error.stream_id(), 2);
        assert_eq!(error.error_code(), 3);
        assert_eq!(error.error_message(), "boom");
        assert_eq!(error.frame_length(), (ERROR_HEADER_LENGTH + 4) as i32);
    }

    #[test]
    fn should_round_trip_status_message_frame() {
        let mut bytes = [0u8; STATUS_MESSAGE_HEADER_LENGTH];
        let sm = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        sm.header()
            .set_frame_length(STATUS_MESSAGE_HEADER_LENGTH as i32)
            .set_version(CURRENT_VERSION)
            .set_flags(0)
            .set_header_type(HDR_TYPE_SM);
        sm.set_session_id(11)
            .set_stream_id(22)
            .set_consumption_term_id(3)
            .set_consumption_term_offset(64)
            .set_receiver_window_length(65536)
            .set_receiver_id(0xDEAD_BEEF);

        let reader = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        assert_eq!(reader.header().header_type(), HDR_TYPE_SM);
        assert_eq!(reader.header().frame_length(), 36);
        assert_eq!(reader.session_id(), 11);
        assert_eq!(reader.stream_id(), 22);
        assert_eq!(reader.consumption_term_id(), 3);
        assert_eq!(reader.consumption_term_offset(), 64);
        assert_eq!(reader.receiver_window_length(), 65536);
        assert_eq!(reader.receiver_id(), 0xDEAD_BEEF);
    }

    #[test]
    fn should_round_trip_nak_and_setup_frames() {
        let mut nak_bytes = [0u8; NAK_HEADER_LENGTH];
        let nak = NakFlyweight::new(AtomicBuffer::new(&mut nak_bytes));
        nak.header().set_header_type(HDR_TYPE_NAK);
        nak.set_session_id(1)
            .set_stream_id(2)
            .set_term_id(3)
            .set_term_offset(128)
            .set_length(256);
        let nr = NakFlyweight::new(AtomicBuffer::new(&mut nak_bytes));
        assert_eq!(nr.header().header_type(), HDR_TYPE_NAK);
        assert_eq!((nr.term_offset(), nr.length()), (128, 256));

        let mut setup_bytes = [0u8; SETUP_HEADER_LENGTH];
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
        setup.header().set_header_type(HDR_TYPE_SETUP);
        setup
            .set_term_offset(0)
            .set_session_id(7)
            .set_stream_id(8)
            .set_initial_term_id(100)
            .set_active_term_id(100)
            .set_term_length(1 << 16)
            .set_mtu_length(1408)
            .set_ttl(0);
        let sr = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
        assert_eq!(sr.header().header_type(), HDR_TYPE_SETUP);
        assert_eq!((sr.term_length(), sr.mtu_length()), (1 << 16, 1408));
    }

    #[test]
    fn should_reject_status_message_when_offset_is_out_of_bounds() {
        let mut bytes = [0u8; STATUS_MESSAGE_HEADER_LENGTH];
        let sm = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        let term_length = 2 * 1024 * 1024;
        for offset in [-128, -1, 1024 * 1024 * 1024, term_length] {
            sm.set_consumption_term_offset(offset);
            assert!(!sm.is_valid(term_length));
        }
    }

    #[test]
    fn should_reject_status_message_when_offset_is_not_aligned() {
        let mut bytes = [0u8; STATUS_MESSAGE_HEADER_LENGTH];
        let sm = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        let term_length = 2 * 1024 * 1024;
        for offset in [1, 31, 33, 63] {
            sm.set_consumption_term_offset(offset);
            assert!(!sm.is_valid(term_length));
        }
    }

    #[test]
    fn should_reject_status_message_when_receiver_window_is_out_of_bounds() {
        let mut bytes = [0u8; STATUS_MESSAGE_HEADER_LENGTH];
        let sm = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        let term_length = 2 * 1024 * 1024;
        sm.set_consumption_term_offset(0);
        for window in [i32::MIN, -4, -1, (term_length / 2) + 1, term_length] {
            sm.set_receiver_window_length(window);
            assert!(!sm.is_valid(term_length));
        }
    }

    #[test]
    fn should_recognize_valid_status_message_frame() {
        let mut bytes = [0u8; STATUS_MESSAGE_HEADER_LENGTH];
        let sm = StatusMessageFlyweight::new(AtomicBuffer::new(&mut bytes));
        let term_length = 65536;
        for (offset, window) in [
            (0, 0),
            (0, 1024),
            (32768, 32768),
            (65504, 32768),
            (65504, 10),
        ] {
            sm.set_consumption_term_offset(offset);
            sm.set_receiver_window_length(window);
            assert!(sm.is_valid(term_length));
        }
    }
}
