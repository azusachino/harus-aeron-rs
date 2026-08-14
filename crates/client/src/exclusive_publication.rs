//! ExclusivePublication: single-writer Aeron publisher API.
//!
//! Mirrors upstream `io.aeron.ExclusivePublication`. Each `ExclusivePublication` gets its own
//! session id, so multiple can be concurrently active on the same driver as independent streams.
//! Unlike [`crate::publication::ConcurrentPublication`], `offer`/`try_claim` here are *not*
//! thread-safe: the local `term_id`/`term_offset`/`active_partition_index`/`term_begin_position`
//! fields replace `ConcurrentPublication`'s CAS-based tail reservation, since a single writer
//! never needs to arbitrate with a concurrent one. Readers still observe progress correctly
//! because every append writes the shared tail counter with a release store before touching the
//! frame — matches upstream's `putLongRelease` ahead of `headerWriter.write`.
//!
//! Bounded relative to upstream: no two-buffer `offer`, `appendPadding`, or revoke support.

use crate::error::{AeronError, Result};
use crate::log_buffers::LogBuffers;
use crate::logbuffer::{
    BufferClaim, FRAME_ALIGNMENT, HEADER_LENGTH, HeaderWriter, LOG_DEFAULT_FRAME_HEADER_OFFSET,
    PARTITION_COUNT, TERM_TAIL_COUNTERS_OFFSET, active_term_count, align,
    compute_fragmented_frame_length, compute_max_message_length, compute_position,
    index_by_term_count, is_connected, mtu_length as meta_mtu_length, pack_tail,
    position_bits_to_shift, raw_tail, rotate_log, set_frame_flags, set_frame_length_ordered,
    set_frame_type, term_id as unpack_term_id, term_offset as unpack_term_offset,
};
use crate::protocol::{BEGIN_FLAG, END_FLAG, HDR_TYPE_PAD};
pub use crate::publication::{
    ADMIN_ACTION, BACK_PRESSURED, CLOSED, MAX_POSITION_EXCEEDED, NOT_CONNECTED,
};
use agrona::AtomicBuffer;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// Single-writer publisher for sending messages over an Aeron channel and stream.
pub struct ExclusivePublication {
    channel: String,
    stream_id: i32,
    session_id: i32,
    initial_term_id: i32,
    registration_id: i64,
    original_registration_id: i64,
    max_possible_position: i64,
    max_message_length: usize,
    max_payload_length: usize,
    term_buffer_length: usize,
    is_closed: bool,
    publication_limit: Arc<AtomicI64>,
    header_writer: HeaderWriter,
    // Kept alive so `term_buffers`/`log_metadata_buffer`'s raw-pointer AtomicBuffer views stay
    // valid — see LogBuffers' doc comment and ConcurrentPublication's identical field.
    #[allow(dead_code)]
    log_buffers: LogBuffers,
    term_buffers: Vec<AtomicBuffer<'static>>,
    log_metadata_buffer: AtomicBuffer<'static>,
    active_partition_index: usize,
    term_id: i32,
    term_offset: i32,
    term_begin_position: i64,
}

impl ExclusivePublication {
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

        // Seed local tail state from the shared metadata once at construction — from here on,
        // this publication is the sole authority for how term_id/term_offset advance.
        let term_count = active_term_count(&log_metadata_buffer);
        let active_partition_index = index_by_term_count(term_count as i64) as usize;
        let raw_tail_value = raw_tail(&log_metadata_buffer, active_partition_index);
        let term_id = unpack_term_id(raw_tail_value);
        let term_offset = unpack_term_offset(raw_tail_value, term_buffer_length) as i32;
        let term_begin_position =
            compute_position(term_id, 0, position_bits_to_shift as i32, initial_term_id);

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
            term_buffer_length,
            is_closed: false,
            publication_limit,
            header_writer,
            log_buffers,
            term_buffers,
            log_metadata_buffer,
            active_partition_index,
            term_id,
            term_offset,
            term_begin_position,
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

    /// The current term-id of the publication.
    pub fn term_id(&self) -> i32 {
        self.term_id
    }

    /// The current term-offset of the publication.
    pub fn term_offset(&self) -> i32 {
        self.term_offset
    }

    /// Current stream position: the position of the tail of the active term.
    pub fn position(&self) -> i64 {
        if self.is_closed {
            CLOSED
        } else {
            self.term_begin_position + self.term_offset as i64
        }
    }

    /// Non-blocking publish of a buffer containing a message.
    ///
    /// Returns the new stream position, otherwise a negative error value of
    /// [`NOT_CONNECTED`], [`BACK_PRESSURED`], [`ADMIN_ACTION`], [`CLOSED`], or
    /// [`MAX_POSITION_EXCEEDED`].
    ///
    /// Not thread-safe — see the module doc.
    pub fn offer(
        &mut self,
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
        let position = self.term_begin_position + self.term_offset as i64;

        if position >= limit {
            return Ok(self.back_pressure_status(position, length as i32));
        }

        let tail_counter_offset =
            TERM_TAIL_COUNTERS_OFFSET + self.active_partition_index * std::mem::size_of::<i64>();
        let term_buffer = self.term_buffers[self.active_partition_index].clone();

        let resulting_offset = if length <= self.max_payload_length {
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

        Ok(self.new_position(resulting_offset))
    }

    /// Non-blocking claim of buffer space for a message without copy semantics.
    ///
    /// See [`crate::publication::ConcurrentPublication::try_claim`] for the commit/abort
    /// contract; the return convention is identical here. Not thread-safe — see the module doc.
    pub fn try_claim(&mut self, length: usize) -> Result<(i64, Option<BufferClaim>)> {
        if self.is_closed {
            return Ok((CLOSED, None));
        }

        if length > self.max_payload_length {
            return Err(AeronError::IllegalArgument(format!(
                "claim exceeds max_payload_length of {}, length={}",
                self.max_payload_length, length
            )));
        }

        let limit = self.publication_limit.load(Ordering::Acquire);
        let position = self.term_begin_position + self.term_offset as i64;

        if position >= limit {
            return Ok((self.back_pressure_status(position, length as i32), None));
        }

        let tail_counter_offset =
            TERM_TAIL_COUNTERS_OFFSET + self.active_partition_index * std::mem::size_of::<i64>();
        let term_buffer = self.term_buffers[self.active_partition_index].clone();

        let (resulting_offset, claim) =
            self.claim_capacity(&term_buffer, tail_counter_offset, length);
        let position = self.new_position(resulting_offset);
        Ok((position, claim))
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

    /// Apply a completed append: advance `term_offset` on success, or roll the term and report
    /// `ADMIN_ACTION` when the frame didn't fit in the remainder of the term. Mirrors upstream
    /// `ExclusivePublication.newPosition`.
    fn new_position(&mut self, resulting_offset: i32) -> i64 {
        if resulting_offset > 0 {
            self.term_offset = resulting_offset;
            return self.term_begin_position + resulting_offset as i64;
        }

        if self.term_begin_position + self.term_buffer_length as i64 >= self.max_possible_position {
            return MAX_POSITION_EXCEEDED;
        }

        self.rotate_term();
        ADMIN_ACTION
    }

    /// Advance to the next partition and term, mirroring upstream `rotateTerm`. Reuses
    /// `rotate_log`'s CAS for the shared metadata write; the CAS is uncontested here since this
    /// publication is the only writer, but reusing it avoids a second near-duplicate primitive.
    fn rotate_term(&mut self) {
        let next_index = (self.active_partition_index + 1) % PARTITION_COUNT as usize;
        let next_term_id = self.term_id + 1;

        rotate_log(
            &self.log_metadata_buffer,
            self.term_id - self.initial_term_id,
            self.term_id,
        );

        self.active_partition_index = next_index;
        self.term_offset = 0;
        self.term_id = next_term_id;
        self.term_begin_position += self.term_buffer_length as i64;
    }

    fn handle_end_of_log(&self, term_buffer: &AtomicBuffer<'static>, term_length: usize) -> i32 {
        let offset = self.term_offset as usize;
        if offset < term_length {
            let padding_length = (term_length - offset) as i32;
            self.header_writer
                .write(term_buffer, offset, padding_length, self.term_id);
            set_frame_type(term_buffer, offset, HDR_TYPE_PAD);
            set_frame_length_ordered(term_buffer, offset, padding_length);
        }

        -1
    }

    fn append_unfragmented_message(
        &self,
        term_buffer: &AtomicBuffer<'static>,
        tail_counter_offset: usize,
        src_buffer: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) -> i32 {
        let frame_length = length as i32 + HEADER_LENGTH;
        let aligned_length = align(frame_length, FRAME_ALIGNMENT);
        let term_length = term_buffer.capacity();

        let resulting_offset = self.term_offset + aligned_length;
        self.log_metadata_buffer.put_i64_ordered(
            tail_counter_offset,
            pack_tail(self.term_id, resulting_offset),
        );

        if resulting_offset as usize > term_length {
            return self.handle_end_of_log(term_buffer, term_length);
        }

        let term_offset = self.term_offset as usize;
        self.header_writer
            .write(term_buffer, term_offset, frame_length, self.term_id);
        term_buffer.put_bytes_from(
            term_offset + HEADER_LENGTH as usize,
            src_buffer,
            src_offset,
            length,
        );
        set_frame_length_ordered(term_buffer, term_offset, frame_length);

        resulting_offset
    }

    fn append_fragmented_message(
        &self,
        term_buffer: &AtomicBuffer<'static>,
        tail_counter_offset: usize,
        src_buffer: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) -> i32 {
        let framed_length =
            compute_fragmented_frame_length(length as i32, self.max_payload_length as i32);
        let term_length = term_buffer.capacity();

        let resulting_offset = self.term_offset + framed_length;
        self.log_metadata_buffer.put_i64_ordered(
            tail_counter_offset,
            pack_tail(self.term_id, resulting_offset),
        );

        if resulting_offset as usize > term_length {
            return self.handle_end_of_log(term_buffer, term_length);
        }

        let mut frame_offset = self.term_offset as usize;
        let mut flags = BEGIN_FLAG;
        let mut remaining = length;

        loop {
            let bytes_to_write = remaining.min(self.max_payload_length);
            let frame_length = bytes_to_write as i32 + HEADER_LENGTH;
            let aligned_length = align(frame_length, FRAME_ALIGNMENT) as usize;

            self.header_writer
                .write(term_buffer, frame_offset, frame_length, self.term_id);
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

        resulting_offset
    }

    fn claim_capacity(
        &self,
        term_buffer: &AtomicBuffer<'static>,
        tail_counter_offset: usize,
        length: usize,
    ) -> (i32, Option<BufferClaim>) {
        let frame_length = length as i32 + HEADER_LENGTH;
        let aligned_length = align(frame_length, FRAME_ALIGNMENT);
        let term_length = term_buffer.capacity();

        let resulting_offset = self.term_offset + aligned_length;
        self.log_metadata_buffer.put_i64_ordered(
            tail_counter_offset,
            pack_tail(self.term_id, resulting_offset),
        );

        if resulting_offset as usize > term_length {
            return (self.handle_end_of_log(term_buffer, term_length), None);
        }

        let term_offset = self.term_offset as usize;
        self.header_writer
            .write(term_buffer, term_offset, frame_length, self.term_id);

        (
            resulting_offset,
            Some(BufferClaim::wrap(
                term_buffer.clone(),
                term_offset,
                frame_length,
            )),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::image::Image;
    use crate::logbuffer::{
        LOG_DEFAULT_FRAME_HEADER_OFFSET, PAGE_MIN_SIZE, TERM_MIN_LENGTH, set_raw_tail,
    };
    use crate::protocol::{DataHeaderFlyweight, HDR_TYPE_DATA};

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
        crate::logbuffer::set_is_connected(&log_buffers.meta_data_buffer(), true);

        let mut publication = ExclusivePublication::new(
            "aeron:ipc".to_string(),
            1,
            42,
            1,
            2000,
            2000,
            publication_limit,
            log_buffers.clone(),
        );

        assert_eq!(publication.position(), 0);
        assert_eq!(publication.term_id(), 1);
        assert_eq!(publication.term_offset(), 0);

        let mut msg = [7u8; 64];
        let msg_len = msg.len();
        let msg_buf = AtomicBuffer::new(&mut msg);
        let result = publication.offer(&msg_buf, 0, msg_len).unwrap();

        let expected_aligned = align(HEADER_LENGTH + msg.len() as i32, FRAME_ALIGNMENT) as i64;
        assert_eq!(result, expected_aligned);
        assert_eq!(publication.position(), expected_aligned);
        assert_eq!(publication.term_offset(), expected_aligned as i32);

        let reader_terms = log_buffers.duplicate_term_buffers();
        assert_eq!(reader_terms[0].get_i32(0), HEADER_LENGTH + msg.len() as i32);
    }

    #[test]
    fn should_write_distinct_sequential_messages() {
        let log_buffers = create_test_log_buffers(4096);
        crate::logbuffer::set_is_connected(&log_buffers.meta_data_buffer(), true);
        let publication_limit = Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 2) as i64));
        let mut publication = ExclusivePublication::new(
            "aeron:ipc".to_string(),
            1,
            42,
            1,
            2000,
            2000,
            publication_limit,
            log_buffers.clone(),
        );

        for sequence in 0..2u8 {
            let mut msg = [0u8; 8];
            msg[0] = sequence;
            let msg_len = msg.len();
            let msg_buf = AtomicBuffer::new(&mut msg);
            assert!(publication.offer(&msg_buf, 0, msg_len).unwrap() > 0);
        }

        let terms = log_buffers.duplicate_term_buffers();
        assert_eq!(terms[0].get_i32(0), HEADER_LENGTH + 8);
        assert_eq!(terms[0].get_i32(64), HEADER_LENGTH + 8);
        assert_eq!(terms[0].get_u8(HEADER_LENGTH as usize), 0);
        assert_eq!(terms[0].get_u8(64 + HEADER_LENGTH as usize), 1);
    }

    #[test]
    fn should_seed_term_state_from_an_existing_non_zero_tail() {
        // Simulates reconnecting to a term already in progress (e.g. after a driver restart
        // handed back an in-use log): the constructor must read the real tail rather than
        // assuming a fresh term_offset of 0, since it never gets a second chance to observe it.
        let log_buffers = create_test_log_buffers(4096);
        set_raw_tail(&log_buffers.meta_data_buffer(), 0, pack_tail(1, 128));
        let publication_limit = Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 2) as i64));

        let publication = ExclusivePublication::new(
            "aeron:ipc".to_string(),
            1,
            42,
            1,
            2000,
            2000,
            publication_limit,
            log_buffers,
        );

        assert_eq!(publication.term_id(), 1);
        assert_eq!(publication.term_offset(), 128);
        assert_eq!(publication.position(), 128);
    }

    #[test]
    fn should_claim_write_and_commit_a_message_visible_to_an_image() {
        let log_buffers = create_test_log_buffers(4096);
        crate::logbuffer::set_is_connected(&log_buffers.meta_data_buffer(), true);
        let publication_limit = Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 2) as i64));
        let mut publication = ExclusivePublication::new(
            "aeron:ipc".to_string(),
            1,
            42,
            1,
            2000,
            2000,
            publication_limit,
            log_buffers.clone(),
        );

        let mut payload = *b"claimed, not copied";
        let payload_len = payload.len();
        let payload_buf = AtomicBuffer::new(&mut payload);
        let (position, claim) = publication.try_claim(payload_len).unwrap();
        let claim = claim.expect("claim should succeed");
        claim.put_bytes(&payload_buf, 0, payload_len);
        claim.commit();

        assert_eq!(publication.position(), position);

        let subscriber_position = Arc::new(AtomicI64::new(0));
        let mut image = Image::new(
            1,
            publication.session_id(),
            publication.initial_term_id(),
            0,
            subscriber_position,
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
        assert_eq!(received[0], payload.to_vec());
    }

    #[test]
    fn should_abort_a_claim_so_it_is_skipped_as_padding() {
        let log_buffers = create_test_log_buffers(4096);
        crate::logbuffer::set_is_connected(&log_buffers.meta_data_buffer(), true);
        let publication_limit = Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 2) as i64));
        let mut publication = ExclusivePublication::new(
            "aeron:ipc".to_string(),
            1,
            42,
            1,
            2000,
            2000,
            publication_limit,
            log_buffers.clone(),
        );

        let (_position, claim) = publication.try_claim(64).unwrap();
        claim.expect("claim should succeed").abort();

        let terms = log_buffers.duplicate_term_buffers();
        assert!(
            crate::logbuffer::is_padding_frame(&terms[0], 0),
            "aborted claim must read back as a padding frame"
        );
        assert_eq!(terms[0].get_i32(0), HEADER_LENGTH + 64);
    }

    #[test]
    fn should_report_back_pressured_when_position_reaches_limit_and_connected() {
        let log_buffers = create_test_log_buffers(4096);
        crate::logbuffer::set_is_connected(&log_buffers.meta_data_buffer(), true);
        let publication_limit = Arc::new(AtomicI64::new(0));

        let mut publication = ExclusivePublication::new(
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
        assert_eq!(
            publication.offer(&msg_buf, 0, msg_len).unwrap(),
            BACK_PRESSURED
        );

        let (position, claim) = publication.try_claim(16).unwrap();
        assert_eq!(position, BACK_PRESSURED);
        assert!(claim.is_none());
    }

    #[test]
    fn should_report_not_connected_when_position_reaches_limit_and_not_connected() {
        let log_buffers = create_test_log_buffers(4096);
        let publication_limit = Arc::new(AtomicI64::new(0));

        let mut publication = ExclusivePublication::new(
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
        assert_eq!(
            publication.offer(&msg_buf, 0, msg_len).unwrap(),
            NOT_CONNECTED
        );
    }

    #[test]
    fn should_reject_message_exceeding_max_message_length() {
        let log_buffers = create_test_log_buffers(4096);
        let publication_limit = Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 4) as i64));

        let mut publication = ExclusivePublication::new(
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
        let err = match publication.offer(&msg_buf, 0, msg_len) {
            Err(AeronError::IllegalArgument(reason)) => reason,
            other => panic!(
                "expected IllegalArgument, got a position result instead: {}",
                other.is_ok()
            ),
        };
        assert!(err.contains("max_message_length"));
    }

    #[test]
    fn should_reject_claim_exceeding_max_payload_length() {
        let log_buffers = create_test_log_buffers(4096);
        let publication_limit = Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 4) as i64));

        let mut publication = ExclusivePublication::new(
            "aeron:ipc".to_string(),
            1,
            42,
            1,
            2000,
            2000,
            publication_limit,
            log_buffers,
        );

        let too_big = publication.max_payload_length() + 1;
        match publication.try_claim(too_big) {
            Err(AeronError::IllegalArgument(_)) => {}
            other => panic!(
                "expected IllegalArgument, got a claim result instead: {}",
                other.is_ok()
            ),
        }
    }

    #[test]
    fn should_close() {
        let log_buffers = create_test_log_buffers(4096);
        let publication_limit = Arc::new(AtomicI64::new((TERM_MIN_LENGTH * 2) as i64));

        let mut publication = ExclusivePublication::new(
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
