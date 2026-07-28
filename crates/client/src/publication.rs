//! Publication: Aeron publisher API for offering messages to a channel/streamId stream.

use crate::error::{AeronError, Result};
use crate::log_buffers::LogBuffers;
use crate::logbuffer::{
    FRAME_ALIGNMENT, HEADER_LENGTH, HeaderWriter, LOG_DEFAULT_FRAME_HEADER_OFFSET,
    TERM_TAIL_COUNTERS_OFFSET, active_term_count, align, compute_fragmented_frame_length,
    compute_max_message_length, compute_position, index_by_term_count, is_connected,
    mtu_length as meta_mtu_length, position_bits_to_shift, raw_tail_volatile_active,
    set_frame_flags, set_frame_length_ordered, set_frame_type, term_id as unpack_term_id,
    term_offset as unpack_term_offset,
};
use crate::protocol::{BEGIN_FLAG, END_FLAG, HDR_TYPE_PAD};
use agrona::AtomicBuffer;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

pub const NOT_CONNECTED: i64 = -1;
pub const BACK_PRESSURED: i64 = -2;
pub const ADMIN_ACTION: i64 = -3;
pub const CLOSED: i64 = -4;
pub const MAX_POSITION_EXCEEDED: i64 = -5;

/// Publisher for sending messages over an Aeron channel and stream.
pub struct ConcurrentPublication {
    channel: String,
    stream_id: i32,
    session_id: i32,
    initial_term_id: i32,
    registration_id: i64,
    original_registration_id: i64,
    max_possible_position: i64,
    max_message_length: usize,
    max_payload_length: usize,
    position_bits_to_shift: usize,
    term_buffer_length: usize,
    is_closed: bool,
    publication_limit: Arc<AtomicI64>,
    header_writer: HeaderWriter,
    // Kept alive so `term_buffers`/`log_metadata_buffer`'s raw-pointer AtomicBuffer views stay
    // valid; any Image/Subscription over the same log clones the same Arc, so writes here are
    // visible there — see LogBuffers' doc comment.
    #[allow(dead_code)]
    log_buffers: LogBuffers,
    term_buffers: Vec<AtomicBuffer<'static>>,
    log_metadata_buffer: AtomicBuffer<'static>,
}

impl ConcurrentPublication {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        channel: String,
        stream_id: i32,
        session_id: i32,
        initial_term_id: i32,
        registration_id: i64,
        original_registration_id: i64,
        publication_limit: Arc<AtomicI64>,
        log_buffers: LogBuffers,
    ) -> Self {
        let term_buffer_length = log_buffers.term_length();
        let position_bits_to_shift = position_bits_to_shift(term_buffer_length);

        let term_buffers = log_buffers.duplicate_term_buffers();
        let log_metadata_buffer = log_buffers.meta_data_buffer();

        let max_possible_position = (term_buffer_length as i64) * (1i64 << 31);
        let max_payload_length =
            (meta_mtu_length(&log_metadata_buffer) - HEADER_LENGTH).max(0) as usize;
        let max_message_length = compute_max_message_length(term_buffer_length as i32) as usize;

        let mut default_header_bytes = [0u8; HEADER_LENGTH as usize];
        log_metadata_buffer.get_bytes(LOG_DEFAULT_FRAME_HEADER_OFFSET, &mut default_header_bytes);
        let default_header_buf = AtomicBuffer::new(&mut default_header_bytes);
        let header_writer = HeaderWriter::new(&default_header_buf);

        Self {
            channel,
            stream_id,
            session_id,
            initial_term_id,
            registration_id,
            original_registration_id,
            max_possible_position,
            max_message_length,
            max_payload_length,
            position_bits_to_shift,
            term_buffer_length,
            is_closed: false,
            publication_limit,
            header_writer,
            log_buffers,
            term_buffers,
            log_metadata_buffer,
        }
    }

    pub fn channel(&self) -> &str {
        &self.channel
    }

    pub fn stream_id(&self) -> i32 {
        self.stream_id
    }

    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    pub fn initial_term_id(&self) -> i32 {
        self.initial_term_id
    }

    pub fn registration_id(&self) -> i64 {
        self.registration_id
    }

    pub fn original_registration_id(&self) -> i64 {
        self.original_registration_id
    }

    pub fn max_payload_length(&self) -> usize {
        self.max_payload_length
    }

    pub fn term_buffer_length(&self) -> usize {
        self.term_buffer_length
    }

    pub fn max_message_length(&self) -> usize {
        self.max_message_length
    }

    pub fn max_possible_position(&self) -> i64 {
        self.max_possible_position
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn close(&mut self) {
        self.is_closed = true;
    }

    pub fn is_connected(&self) -> bool {
        !self.is_closed && is_connected(&self.log_metadata_buffer)
    }

    /// Current stream position: the position of the tail of the active term.
    pub fn position(&self) -> i64 {
        if self.is_closed {
            return CLOSED;
        }

        let raw_tail = raw_tail_volatile_active(&self.log_metadata_buffer);
        let term_offset = unpack_term_offset(raw_tail, self.term_buffer_length) as i32;
        compute_position(
            unpack_term_id(raw_tail),
            term_offset,
            self.position_bits_to_shift as i32,
            self.initial_term_id,
        )
    }

    /// Non-blocking publish of a buffer containing a message.
    ///
    /// Returns the new stream position, otherwise a negative error value of
    /// [`NOT_CONNECTED`], [`BACK_PRESSURED`], [`ADMIN_ACTION`], [`CLOSED`], or
    /// [`MAX_POSITION_EXCEEDED`].
    pub fn offer(
        &self,
        src_buffer: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) -> Result<i64> {
        if self.is_closed {
            return Ok(CLOSED);
        }

        if length > self.max_message_length {
            return Err(AeronError::IllegalArgument(format!(
                "encoded message exceeds max_message_length of {}, length={}",
                self.max_message_length, length
            )));
        }

        let limit = self.publication_limit.load(Ordering::Acquire);
        let term_count = active_term_count(&self.log_metadata_buffer);
        let index = index_by_term_count(term_count as i64) as usize;
        let term_buffer = self.term_buffers[index].clone();
        let tail_counter_offset = TERM_TAIL_COUNTERS_OFFSET + index * std::mem::size_of::<i64>();

        let raw_tail = self
            .log_metadata_buffer
            .get_i64_volatile(tail_counter_offset);
        let term_offset = unpack_term_offset(raw_tail, self.term_buffer_length);
        let term_id = unpack_term_id(raw_tail);

        if term_count != term_id - self.initial_term_id {
            return Ok(ADMIN_ACTION);
        }

        let position = compute_position(
            term_id,
            term_offset as i32,
            self.position_bits_to_shift as i32,
            self.initial_term_id,
        );

        if position >= limit {
            return Ok(self.back_pressure_status(position, length as i32));
        }

        let new_position = if length <= self.max_payload_length {
            self.append_unfragmented_message(
                &term_buffer,
                tail_counter_offset,
                src_buffer,
                src_offset,
                length,
            )
        } else {
            self.append_fragmented_message(
                &term_buffer,
                tail_counter_offset,
                src_buffer,
                src_offset,
                length,
            )
        };

        Ok(new_position)
    }

    fn back_pressure_status(&self, current_position: i64, message_length: i32) -> i64 {
        let framed = align(message_length + HEADER_LENGTH, FRAME_ALIGNMENT) as i64;
        if current_position + framed >= self.max_possible_position {
            return MAX_POSITION_EXCEEDED;
        }

        if is_connected(&self.log_metadata_buffer) {
            BACK_PRESSURED
        } else {
            NOT_CONNECTED
        }
    }

    fn append_unfragmented_message(
        &self,
        term_buffer: &AtomicBuffer,
        tail_counter_offset: usize,
        src_buffer: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) -> i64 {
        let frame_length = length as i32 + HEADER_LENGTH;
        let aligned_length = align(frame_length, FRAME_ALIGNMENT);
        let term_length = term_buffer.capacity();

        let raw_tail = self
            .log_metadata_buffer
            .get_and_add_i64(tail_counter_offset, aligned_length as i64);
        let term_id = unpack_term_id(raw_tail);
        let term_offset = unpack_term_offset(raw_tail, term_length);

        let resulting_offset = term_offset + aligned_length as usize;
        let position = compute_position(
            term_id,
            resulting_offset as i32,
            self.position_bits_to_shift as i32,
            self.initial_term_id,
        );

        if resulting_offset > term_length {
            return self.handle_end_of_log(
                term_buffer,
                term_length,
                term_id,
                term_offset,
                position,
            );
        }

        self.header_writer
            .write(term_buffer, term_offset, frame_length, term_id);
        term_buffer.put_bytes_from(
            term_offset + HEADER_LENGTH as usize,
            src_buffer,
            src_offset,
            length,
        );
        set_frame_length_ordered(term_buffer, term_offset, frame_length);

        position
    }

    #[allow(clippy::too_many_arguments)]
    fn append_fragmented_message(
        &self,
        term_buffer: &AtomicBuffer,
        tail_counter_offset: usize,
        src_buffer: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) -> i64 {
        let framed_length =
            compute_fragmented_frame_length(length as i32, self.max_payload_length as i32);
        let term_length = term_buffer.capacity();

        let raw_tail = self
            .log_metadata_buffer
            .get_and_add_i64(tail_counter_offset, framed_length as i64);
        let term_id = unpack_term_id(raw_tail);
        let term_offset = unpack_term_offset(raw_tail, term_length);

        let resulting_offset = term_offset + framed_length as usize;
        let position = compute_position(
            term_id,
            resulting_offset as i32,
            self.position_bits_to_shift as i32,
            self.initial_term_id,
        );

        if resulting_offset > term_length {
            return self.handle_end_of_log(
                term_buffer,
                term_length,
                term_id,
                term_offset,
                position,
            );
        }

        let mut frame_offset = term_offset;
        let mut flags = BEGIN_FLAG;
        let mut remaining = length;

        loop {
            let bytes_to_write = remaining.min(self.max_payload_length);
            let frame_length = bytes_to_write as i32 + HEADER_LENGTH;
            let aligned_length = align(frame_length, FRAME_ALIGNMENT) as usize;

            self.header_writer
                .write(term_buffer, frame_offset, frame_length, term_id);
            term_buffer.put_bytes_from(
                frame_offset + HEADER_LENGTH as usize,
                src_buffer,
                src_offset + (length - remaining),
                bytes_to_write,
            );

            if remaining <= self.max_payload_length {
                flags |= END_FLAG;
            }
            set_frame_flags(term_buffer, frame_offset, flags);
            set_frame_length_ordered(term_buffer, frame_offset, frame_length);

            flags = 0;
            frame_offset += aligned_length;
            remaining -= bytes_to_write;

            if remaining == 0 {
                break;
            }
        }

        position
    }

    fn handle_end_of_log(
        &self,
        term_buffer: &AtomicBuffer,
        term_length: usize,
        term_id: i32,
        term_offset: usize,
        position: i64,
    ) -> i64 {
        if term_offset < term_length {
            let padding_length = (term_length - term_offset) as i32;
            self.header_writer
                .write(term_buffer, term_offset, padding_length, term_id);
            set_frame_type(term_buffer, term_offset, HDR_TYPE_PAD);
            set_frame_length_ordered(term_buffer, term_offset, padding_length);
        }

        if position >= self.max_possible_position {
            return MAX_POSITION_EXCEEDED;
        }

        crate::logbuffer::rotate_log(
            &self.log_metadata_buffer,
            term_id - self.initial_term_id,
            term_id,
        );

        ADMIN_ACTION
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::image::Image;
    use crate::logbuffer::{LOG_DEFAULT_FRAME_HEADER_OFFSET, PAGE_MIN_SIZE, TERM_MIN_LENGTH};
    use crate::protocol::{DataHeaderFlyweight, HDR_TYPE_DATA};

    #[test]
    fn constants_should_have_correct_values() {
        assert_eq!(NOT_CONNECTED, -1);
        assert_eq!(BACK_PRESSURED, -2);
        assert_eq!(ADMIN_ACTION, -3);
        assert_eq!(CLOSED, -4);
        assert_eq!(MAX_POSITION_EXCEEDED, -5);
    }

    fn create_test_log_buffers(mtu: i32) -> LogBuffers {
        let term_len = TERM_MIN_LENGTH as usize;
        let total_len = term_len * 3 + crate::logbuffer::LOG_META_DATA_LENGTH;
        let mut contents = vec![0u8; total_len];

        let meta_offset = total_len - crate::logbuffer::LOG_META_DATA_LENGTH;
        let mut meta_slice = contents[meta_offset..].to_vec();
        let meta_buf = AtomicBuffer::new(&mut meta_slice);
        crate::logbuffer::set_term_length(&meta_buf, TERM_MIN_LENGTH);
        crate::logbuffer::set_page_size(&meta_buf, PAGE_MIN_SIZE);
        crate::logbuffer::set_mtu_length(&meta_buf, mtu);
        crate::logbuffer::set_initial_term_id(&meta_buf, 1);
        crate::logbuffer::initialise_tail_with_term_id(&meta_buf, 0, 1);
        crate::logbuffer::set_active_term_count(&meta_buf, 0);

        // Default frame header template: without this, HeaderWriter stamps HDR_TYPE_PAD
        // (all-zero) into every frame it writes, and readers correctly skip it as padding.
        let mut default_header_bytes = [0u8; HEADER_LENGTH as usize];
        let default_header_buf = AtomicBuffer::new(&mut default_header_bytes);
        let default_header = DataHeaderFlyweight::new(default_header_buf);
        default_header.set_header_type(HDR_TYPE_DATA);
        default_header.set_session_id(42);
        default_header.set_stream_id(1);
        meta_buf.put_bytes(LOG_DEFAULT_FRAME_HEADER_OFFSET, &default_header_bytes);

        contents[meta_offset..].copy_from_slice(&meta_slice);

        LogBuffers::from_bytes(contents).unwrap()
    }

    #[test]
    fn should_offer_a_message_and_advance_position() {
        let log_buffers = create_test_log_buffers(4096);
        let publication_limit = Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 2) as i64));

        let publication = ConcurrentPublication::new(
            "aeron:ipc".to_string(),
            1,
            42,
            1,
            2000,
            2000,
            publication_limit,
            log_buffers.clone(),
        );

        crate::logbuffer::set_is_connected(&log_buffers.meta_data_buffer(), true);

        assert_eq!(publication.position(), 0);

        let mut msg = [7u8; 64];
        let msg_len = msg.len();
        let msg_buf = AtomicBuffer::new(&mut msg);
        let result = publication.offer(&msg_buf, 0, msg_len).unwrap();

        let expected_aligned = align(HEADER_LENGTH + msg.len() as i32, FRAME_ALIGNMENT) as i64;
        assert_eq!(result, expected_aligned);
        assert_eq!(publication.position(), expected_aligned);

        // The write must be visible through an independently duplicated term-buffer view,
        // i.e. term buffers are genuinely shared rather than private per-owner copies.
        let reader_terms = log_buffers.duplicate_term_buffers();
        assert_eq!(reader_terms[0].get_i32(0), HEADER_LENGTH + msg.len() as i32);
    }

    #[test]
    fn should_deliver_offered_message_to_an_independently_constructed_image() {
        // End-to-end proof that Publication and Image share real memory: a message written
        // through `Publication::offer` must be observable by an `Image` built from a *clone*
        // of the same `LogBuffers`, exactly as a real publisher and subscriber sharing one
        // mapped log file would see each other's writes without any explicit hand-off.
        let log_buffers = create_test_log_buffers(4096);
        crate::logbuffer::set_is_connected(&log_buffers.meta_data_buffer(), true);
        let publication_limit = Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 2) as i64));

        let publication = ConcurrentPublication::new(
            "aeron:ipc".to_string(),
            1,
            42,
            1,
            2000,
            2000,
            publication_limit,
            log_buffers.clone(),
        );

        let mut msg = *b"hello, shared memory!!!!!!!!!!!!";
        let msg_len = msg.len();
        let msg_buf = AtomicBuffer::new(&mut msg);
        let position = publication.offer(&msg_buf, 0, msg_len).unwrap();
        assert!(position > 0);

        let subscriber_position = Arc::new(AtomicI64::new(0));
        let mut image = Image::new(
            1,
            publication.session_id(),
            publication.initial_term_id(),
            0,
            subscriber_position.clone(),
            log_buffers,
            "ipc".to_string(),
            2001,
        );

        let mut received = Vec::new();
        let fragments = image.poll(
            |buf, offset, len, _hdr| {
                let mut bytes = vec![0u8; len];
                buf.get_bytes(offset, &mut bytes);
                received.push(bytes);
            },
            usize::MAX,
        );

        assert_eq!(fragments, 1);
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], msg.to_vec());
        assert_eq!(subscriber_position.load(Ordering::Acquire), position);
    }

    #[test]
    fn should_report_not_connected_when_position_reaches_limit_and_not_connected() {
        let log_buffers = create_test_log_buffers(4096);
        let publication_limit = Arc::new(AtomicI64::new(0));

        let publication = ConcurrentPublication::new(
            "aeron:ipc".to_string(),
            1,
            42,
            1,
            2000,
            2000,
            publication_limit,
            log_buffers,
        );

        let mut msg = [1u8; 16];
        let msg_len = msg.len();
        let msg_buf = AtomicBuffer::new(&mut msg);
        let result = publication.offer(&msg_buf, 0, msg_len).unwrap();
        assert_eq!(result, NOT_CONNECTED);
    }

    #[test]
    fn should_report_back_pressured_when_position_reaches_limit_and_connected() {
        let log_buffers = create_test_log_buffers(4096);
        crate::logbuffer::set_is_connected(&log_buffers.meta_data_buffer(), true);
        let publication_limit = Arc::new(AtomicI64::new(0));

        let publication = ConcurrentPublication::new(
            "aeron:ipc".to_string(),
            1,
            42,
            1,
            2000,
            2000,
            publication_limit,
            log_buffers,
        );

        let mut msg = [1u8; 16];
        let msg_len = msg.len();
        let msg_buf = AtomicBuffer::new(&mut msg);
        let result = publication.offer(&msg_buf, 0, msg_len).unwrap();
        assert_eq!(result, BACK_PRESSURED);
    }

    #[test]
    fn should_reject_message_exceeding_max_message_length() {
        let log_buffers = create_test_log_buffers(4096);
        let publication_limit = Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 4) as i64));

        let publication = ConcurrentPublication::new(
            "aeron:ipc".to_string(),
            1,
            42,
            1,
            2000,
            2000,
            publication_limit,
            log_buffers,
        );

        let too_big = publication.max_message_length() + 1;
        let mut msg = vec![0u8; too_big];
        let msg_len = msg.len();
        let msg_buf = AtomicBuffer::new(&mut msg);
        let err = publication.offer(&msg_buf, 0, msg_len).unwrap_err();
        assert!(matches!(err, AeronError::IllegalArgument(_)));
    }

    #[test]
    fn should_close() {
        let log_buffers = create_test_log_buffers(4096);
        let publication_limit = Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 2) as i64));

        let mut publication = ConcurrentPublication::new(
            "aeron:ipc".to_string(),
            1,
            42,
            1,
            2000,
            2000,
            publication_limit,
            log_buffers,
        );

        publication.close();
        assert!(publication.is_closed());
        assert_eq!(publication.position(), CLOSED);
        let mut msg = [0u8; 4];
        let msg_len = msg.len();
        let msg_buf = AtomicBuffer::new(&mut msg);
        assert_eq!(publication.offer(&msg_buf, 0, msg_len).unwrap(), CLOSED);
    }
}
