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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{self, DataHeaderFlyweight};
    use agrona::AtomicBuffer;
    use std::sync::Arc;

    #[test]
    fn header_constructor_initialized_data() {
        let initial_term_id = 18;
        let context: Arc<dyn std::any::Any + Send + Sync> = Arc::new(999i64);
        let position_bits_to_shift = 2;
        let header = Header::with_context(initial_term_id, position_bits_to_shift, context.clone());

        assert_eq!(header.initial_term_id(), initial_term_id);
        assert_eq!(header.position_bits_to_shift(), position_bits_to_shift);
        assert!(header.context().is_some());
    }

    #[test]
    fn header_position_calculation_the_end_of_the_message_in_the_log() {
        let cases = [
            (0, 2, 100, 1024, 5, 1172i64),
            (42, 16, 13, 4096, 46, 266272i64),
            (1, 30, 1024, 1073741824, 111, 119185343488i64),
        ];

        for (
            initial_term_id,
            position_bits_to_shift,
            frame_length,
            term_offset,
            term_id,
            expected_position,
        ) in cases
        {
            let mut bytes = vec![0u8; 128];
            let buf = AtomicBuffer::new(&mut bytes);
            let flyweight = DataHeaderFlyweight::new(buf.clone());
            flyweight.set_frame_length(frame_length);
            flyweight.set_term_id(term_id);
            flyweight.set_term_offset(term_offset);

            let mut header = Header::new(initial_term_id, position_bits_to_shift);
            header.set_buffer(buf);
            header.set_offset(0);

            assert_eq!(header.position(), expected_position);
            assert_eq!(header.fragmented_frame_length(), None);
        }
    }

    #[test]
    fn header_offset_is_relative_to_the_buffer_start() {
        let mut header = Header::new(42, 3);
        assert_eq!(header.offset(), 0);

        header.set_offset(142);
        assert_eq!(header.offset(), 142);
    }

    #[test]
    fn header_should_read_data_from_the_buffer() {
        let cases = [
            (
                103,
                0x3u8,
                0x1Au8,
                0x6u16,
                2080,
                -46234,
                333,
                5,
                909090909090909i64,
            ),
            (
                512,
                0x1u8,
                0xCu8,
                0x1u16,
                1073741824,
                42,
                -876,
                1543,
                -4632842384627834687i64,
            ),
        ];

        for (
            frame_length,
            version,
            flags,
            header_type,
            term_offset,
            session_id,
            stream_id,
            term_id,
            reserved_value,
        ) in cases
        {
            let mut bytes = vec![0u8; 100];
            let offset = 16;
            let flyweight_buf = AtomicBuffer::new(&mut bytes[offset..offset + 64]);
            let flyweight = DataHeaderFlyweight::new(flyweight_buf);

            flyweight
                .set_frame_length(frame_length)
                .set_version(version)
                .set_flags(flags)
                .set_header_type(header_type)
                .set_term_offset(term_offset)
                .set_session_id(session_id)
                .set_stream_id(stream_id)
                .set_term_id(term_id)
                .set_reserved_value(reserved_value);

            let mut header = Header::new(5, 22);
            let full_buf = AtomicBuffer::new(&mut bytes);
            header.set_buffer(full_buf);
            header.set_offset(offset);

            assert_eq!(header.frame_length(), frame_length);
            assert_eq!(header.flags(), flags);
            assert_eq!(header.type_(), header_type);
            assert_eq!(header.term_offset(), term_offset);
            assert_eq!(header.session_id(), session_id);
            assert_eq!(header.stream_id(), stream_id);
            assert_eq!(header.term_id(), term_id);
            assert_eq!(header.reserved_value(), reserved_value);
        }
    }

    #[test]
    fn header_should_override_initial_term_id() {
        let initial_term_id = -178;
        let new_initial_term_id = 871;
        let mut header = Header::new(initial_term_id, 3);
        assert_eq!(header.initial_term_id(), initial_term_id);

        header.set_initial_term_id(new_initial_term_id);
        assert_eq!(header.initial_term_id(), new_initial_term_id);
    }

    #[test]
    fn header_should_override_position_bits_to_shift() {
        let position_bits_to_shift = -6;
        let new_position_bits_to_shift = 20;
        let mut header = Header::new(42, position_bits_to_shift);
        assert_eq!(header.position_bits_to_shift(), position_bits_to_shift);

        header.set_position_bits_to_shift(new_position_bits_to_shift);
        assert_eq!(header.position_bits_to_shift(), new_position_bits_to_shift);
    }

    #[test]
    fn should_encode_header_using_little_endian_byte_order() {
        let cases = [
            (100, 8u8, 5u8, 9i16, 352, -777, -1000, -33),
            (-99, -2i8 as u8, 7u8, 1i16, 8, 42, 3, 89),
            (123, 0u8, 0u8, 0i16, 0, 0, 0, 0),
            (
                32,
                1u8,
                -128i8 as u8,
                4i16,
                96,
                2147483647,
                -2147483648,
                -2147483648,
            ),
        ];

        for (
            frame_length,
            version,
            flags,
            header_type,
            term_offset,
            session_id,
            stream_id,
            term_id,
        ) in cases
        {
            let mut default_bytes = [0xFFu8; 32];
            let default_buffer = AtomicBuffer::new(&mut default_bytes);

            default_buffer.put_u8(protocol::VERSION_FIELD_OFFSET, version);
            default_buffer.put_u8(protocol::FLAGS_FIELD_OFFSET, flags);
            default_buffer.put_i16(protocol::TYPE_FIELD_OFFSET, header_type);
            default_buffer.put_i32(protocol::DATA_SESSION_ID_FIELD_OFFSET, session_id);
            default_buffer.put_i32(protocol::DATA_STREAM_ID_FIELD_OFFSET, stream_id);

            let mut term_bytes = [0xFFu8; 1024];
            let term_buffer = AtomicBuffer::new(&mut term_bytes);

            let header_writer = HeaderWriter::new(&default_buffer);
            header_writer.write(&term_buffer, term_offset as usize, frame_length, term_id);

            let offset = term_offset as usize;
            assert_eq!(
                term_buffer.get_i32(offset + protocol::FRAME_LENGTH_FIELD_OFFSET),
                -frame_length
            );
            assert_eq!(
                term_buffer.get_u8(offset + protocol::VERSION_FIELD_OFFSET),
                version
            );
            assert_eq!(
                term_buffer.get_u8(offset + protocol::FLAGS_FIELD_OFFSET),
                flags
            );
            assert_eq!(
                term_buffer.get_i16(offset + protocol::TYPE_FIELD_OFFSET),
                header_type
            );
            assert_eq!(
                term_buffer.get_i32(offset + protocol::DATA_TERM_OFFSET_FIELD_OFFSET),
                term_offset
            );
            assert_eq!(
                term_buffer.get_i32(offset + protocol::DATA_SESSION_ID_FIELD_OFFSET),
                session_id
            );
            assert_eq!(
                term_buffer.get_i32(offset + protocol::DATA_STREAM_ID_FIELD_OFFSET),
                stream_id
            );
            assert_eq!(
                term_buffer.get_i32(offset + protocol::DATA_TERM_ID_FIELD_OFFSET),
                term_id
            );
        }
    }
}
