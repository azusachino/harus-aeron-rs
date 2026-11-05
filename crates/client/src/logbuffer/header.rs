use super::descriptor::*;
use super::frame_descriptor::*;
use crate::protocol;
use agrona::AtomicBuffer;

/// Represents the header of a data frame for accessing metadata fields.
#[derive(Clone)]
pub struct Header<'a> {
    initial_term_id: i32,
    position_bits_to_shift: i32,
    offset: usize,
    buffer: Option<AtomicBuffer<'a>>,
    fragmented_frame_length: Option<i32>,
    context: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
}

impl<'a> Header<'a> {
    /// Construct a header that references a buffer for the log.
    pub fn new(initial_term_id: i32, position_bits_to_shift: i32) -> Self {
        Self {
            initial_term_id,
            position_bits_to_shift,
            offset: 0,
            buffer: None,
            fragmented_frame_length: None,
            context: None,
        }
    }

    /// Construct a header with initial term ID, position bits to shift, and context.
    pub fn with_context(
        initial_term_id: i32,
        position_bits_to_shift: i32,
        context: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        Self {
            initial_term_id,
            position_bits_to_shift,
            offset: 0,
            buffer: None,
            fragmented_frame_length: None,
            context: Some(context),
        }
    }

    /// Context stored for callback state.
    pub fn context(&self) -> Option<&(dyn std::any::Any + Send + Sync)> {
        self.context.as_deref()
    }

    /// Set context.
    pub fn set_context(
        &mut self,
        context: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
    ) -> &mut Self {
        self.context = context;
        self
    }

    /// Calculate absolute stream position after reading this frame.
    pub fn position(&self) -> i64 {
        compute_position(
            self.term_id(),
            self.next_term_offset(),
            self.position_bits_to_shift,
            self.initial_term_id,
        )
    }

    pub fn position_bits_to_shift(&self) -> i32 {
        self.position_bits_to_shift
    }

    pub fn set_position_bits_to_shift(&mut self, position_bits_to_shift: i32) -> &mut Self {
        self.position_bits_to_shift = position_bits_to_shift;
        self
    }

    pub fn initial_term_id(&self) -> i32 {
        self.initial_term_id
    }

    pub fn set_initial_term_id(&mut self, initial_term_id: i32) -> &mut Self {
        self.initial_term_id = initial_term_id;
        self
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn set_offset(&mut self, offset: usize) -> &mut Self {
        self.offset = offset;
        self
    }

    pub fn buffer(&self) -> Option<&AtomicBuffer<'a>> {
        self.buffer.as_ref()
    }

    pub fn set_buffer(&mut self, buffer: AtomicBuffer<'a>) -> &mut Self {
        self.buffer = Some(buffer);
        self
    }

    fn buf(&self) -> &AtomicBuffer<'a> {
        self.buffer.as_ref().expect("buffer not set on Header")
    }

    pub fn frame_length(&self) -> i32 {
        self.buf()
            .get_i32(self.offset + protocol::FRAME_LENGTH_FIELD_OFFSET)
    }

    pub fn version(&self) -> u8 {
        self.buf()
            .get_u8(self.offset + protocol::VERSION_FIELD_OFFSET)
    }

    pub fn flags(&self) -> u8 {
        self.buf()
            .get_u8(self.offset + protocol::FLAGS_FIELD_OFFSET)
    }

    pub fn type_(&self) -> u16 {
        self.buf()
            .get_i16(self.offset + protocol::TYPE_FIELD_OFFSET) as u16
    }

    pub fn term_offset(&self) -> i32 {
        self.buf()
            .get_i32(self.offset + protocol::DATA_TERM_OFFSET_FIELD_OFFSET)
    }

    pub fn session_id(&self) -> i32 {
        self.buf()
            .get_i32(self.offset + protocol::DATA_SESSION_ID_FIELD_OFFSET)
    }

    pub fn stream_id(&self) -> i32 {
        self.buf()
            .get_i32(self.offset + protocol::DATA_STREAM_ID_FIELD_OFFSET)
    }

    pub fn term_id(&self) -> i32 {
        self.buf()
            .get_i32(self.offset + protocol::DATA_TERM_ID_FIELD_OFFSET)
    }

    pub fn reserved_value(&self) -> i64 {
        self.buf()
            .get_i64(self.offset + protocol::DATA_RESERVED_VALUE_OFFSET)
    }

    pub fn fragmented_frame_length(&self) -> Option<i32> {
        self.fragmented_frame_length
    }

    pub fn set_fragmented_frame_length(&mut self, length: Option<i32>) -> &mut Self {
        self.fragmented_frame_length = length;
        self
    }

    fn term_occupancy_length(&self) -> i32 {
        self.fragmented_frame_length
            .unwrap_or_else(|| self.frame_length())
    }

    pub fn next_term_offset(&self) -> i32 {
        align(
            self.term_offset() + self.term_occupancy_length(),
            FRAME_ALIGNMENT,
        )
    }
}

/// Utility for applying a header to a message in a term buffer.
///
/// Precomputes packed words for efficiency when appending frames.
/// Note: This is the Little Endian / LE native implementation. Upstream Aeron
/// also provides `NativeBigEndianHeaderWriter` for BE architectures, which is dropped here.
#[derive(Debug, Copy, Clone)]
pub struct HeaderWriter {
    version_flags_type: i64,
    session_id: i64,
    stream_id: i64,
}

impl HeaderWriter {
    /// Construct a new [`HeaderWriter`] initialized from a default header buffer.
    pub fn new(default_header: &AtomicBuffer) -> Self {
        let version_flags_type =
            (default_header.get_i32(protocol::VERSION_FIELD_OFFSET) as i64) << 32;
        let session_id =
            (default_header.get_i32(protocol::DATA_SESSION_ID_FIELD_OFFSET) as i64) << 32;
        let stream_id =
            (default_header.get_i32(protocol::DATA_STREAM_ID_FIELD_OFFSET) as i64) & 0xFFFF_FFFF;

        Self {
            version_flags_type,
            session_id,
            stream_id,
        }
    }

    /// Write a header to the term buffer in Little Endian format.
    pub fn write(&self, term_buffer: &AtomicBuffer, offset: usize, length: i32, term_id: i32) {
        let frame_len_word = self.version_flags_type | (((-length) as i64) & 0xFFFF_FFFF);
        term_buffer.put_i64_ordered(offset + protocol::FRAME_LENGTH_FIELD_OFFSET, frame_len_word);
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);

        let term_offset_word = self.session_id | (offset as i64 & 0xFFFF_FFFF);
        term_buffer.put_i64(
            offset + protocol::DATA_TERM_OFFSET_FIELD_OFFSET,
            term_offset_word,
        );

        let stream_id_word = ((term_id as i64) << 32) | self.stream_id;
        term_buffer.put_i64(
            offset + protocol::DATA_STREAM_ID_FIELD_OFFSET,
            stream_id_word,
        );
    }
}
