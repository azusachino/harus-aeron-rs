//! Image: read-side stream view of an Aeron publication over log buffers.

use crate::error::{AeronError, Result};
use crate::log_buffers::LogBuffers;
use crate::logbuffer::{Header, PARTITION_COUNT, TermReader, index_by_position};
use agrona::AtomicBuffer;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// Represents a replicated `Publication` stream from a subscriber's perspective.
pub struct Image {
    subscription_registration_id: i64,
    session_id: i32,
    initial_term_id: i32,
    term_length: usize,
    term_length_mask: usize,
    position_bits_to_shift: usize,
    correlation_id: i64,
    source_identity: String,
    subscriber_position: Arc<AtomicI64>,
    log_buffers: LogBuffers,
    term_buffers: Vec<Vec<u8>>,
    is_closed: bool,
}

impl Image {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        subscription_registration_id: i64,
        session_id: i32,
        initial_term_id: i32,
        initial_position: i64,
        subscriber_position: Arc<AtomicI64>,
        mut log_buffers: LogBuffers,
        source_identity: String,
        correlation_id: i64,
    ) -> Self {
        let term_length = log_buffers.term_length();
        let term_length_mask = term_length - 1;
        let position_bits_to_shift = term_length.trailing_zeros() as usize;

        subscriber_position.store(initial_position, Ordering::Release);

        let term_atomic_bufs = log_buffers.duplicate_term_buffers();
        let mut term_buffers = Vec::with_capacity(PARTITION_COUNT as usize);
        for buf in term_atomic_bufs {
            let mut bytes = vec![0u8; buf.capacity()];
            buf.get_bytes(0, &mut bytes);
            term_buffers.push(bytes);
        }

        Self {
            subscription_registration_id,
            session_id,
            initial_term_id,
            term_length,
            term_length_mask,
            position_bits_to_shift,
            correlation_id,
            source_identity,
            subscriber_position,
            log_buffers,
            term_buffers,
            is_closed: false,
        }
    }

    pub fn subscription_registration_id(&self) -> i64 {
        self.subscription_registration_id
    }

    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    pub fn initial_term_id(&self) -> i32 {
        self.initial_term_id
    }

    pub fn term_length(&self) -> usize {
        self.term_length
    }

    pub fn correlation_id(&self) -> i64 {
        self.correlation_id
    }

    pub fn source_identity(&self) -> &str {
        &self.source_identity
    }

    pub fn log_buffers(&self) -> &LogBuffers {
        &self.log_buffers
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn close(&mut self) {
        self.is_closed = true;
    }

    pub fn position(&self) -> i64 {
        if self.is_closed {
            0
        } else {
            self.subscriber_position.load(Ordering::Acquire)
        }
    }

    pub fn set_position(&mut self, new_position: i64) -> Result<()> {
        if self.is_closed {
            return Err(AeronError::IllegalState("Image is closed".to_string()));
        }

        let current_position = self.subscriber_position.load(Ordering::Acquire);
        if new_position < current_position {
            return Err(AeronError::IllegalArgument(format!(
                "new position {} < current position {}",
                new_position, current_position
            )));
        }

        let term_offset = (new_position & self.term_length_mask as i64) as usize;
        if term_offset != 0 && (term_offset % 32 != 0) {
            return Err(AeronError::IllegalArgument(format!(
                "new position {} is not frame aligned",
                new_position
            )));
        }

        self.subscriber_position
            .store(new_position, Ordering::Release);
        Ok(())
    }

    pub fn poll<F>(&mut self, mut fragment_handler: F, fragment_limit: usize) -> usize
    where
        F: FnMut(&AtomicBuffer, usize, usize, &Header),
    {
        if self.is_closed {
            return 0;
        }

        let position = self.subscriber_position.load(Ordering::Acquire);
        let index = index_by_position(position, self.position_bits_to_shift);
        let term_offset = (position & self.term_length_mask as i64) as usize;

        let term_buf_bytes = &mut self.term_buffers[index];
        let term_buffer = AtomicBuffer::new(term_buf_bytes);

        let mut handler_adapter = |buf: &AtomicBuffer, offset: usize, length: i32, hdr: &Header| {
            fragment_handler(buf, offset, length as usize, hdr);
        };

        let mut header = Header::new(self.initial_term_id, self.position_bits_to_shift as i32);

        let read_result = TermReader::read(
            &term_buffer,
            term_offset,
            &mut handler_adapter,
            fragment_limit,
            &mut header,
            |_| {},
        );

        let fragments_read = TermReader::fragments_read(read_result) as usize;
        let bytes_read = TermReader::offset(read_result) as usize - term_offset;

        if bytes_read > 0 {
            let new_position = position + bytes_read as i64;
            self.subscriber_position
                .store(new_position, Ordering::Release);
        }

        fragments_read
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logbuffer::{
        FRAME_ALIGNMENT, HEADER_LENGTH, PAGE_MIN_SIZE, TERM_MIN_LENGTH, align, compute_position,
    };
    use crate::protocol::DataHeaderFlyweight;

    const CORRELATION_ID: i64 = 0xC044E1A;
    const SESSION_ID: i32 = 0x5E55101D;
    const INITIAL_TERM_ID: i32 = 0xEE81D;

    fn create_test_log_buffers() -> LogBuffers {
        let term_len = TERM_MIN_LENGTH as usize;
        let total_len = term_len * 3 + 4096;
        let mut contents = vec![0u8; total_len];
        let meta_offset = total_len - 4096;
        let mut meta_slice = contents[meta_offset..].to_vec();
        let meta_buf = AtomicBuffer::new(&mut meta_slice);
        crate::logbuffer::set_term_length(&meta_buf, TERM_MIN_LENGTH);
        crate::logbuffer::set_page_size(&meta_buf, PAGE_MIN_SIZE);
        contents[meta_offset..].copy_from_slice(&meta_slice);
        LogBuffers::from_bytes(contents).unwrap()
    }

    #[test]
    fn should_handle_closed_image() {
        let subscriber_pos = Arc::new(AtomicI64::new(0));
        let log_buffers = create_test_log_buffers();
        let mut image = Image::new(
            1,
            SESSION_ID,
            INITIAL_TERM_ID,
            0,
            subscriber_pos,
            log_buffers,
            "ipc".to_string(),
            CORRELATION_ID,
        );

        image.close();
        assert!(image.is_closed());
        assert_eq!(image.poll(|_, _, _, _| {}, usize::MAX), 0);
        assert_eq!(image.position(), 0);
    }

    #[test]
    fn should_allow_valid_position() {
        let subscriber_pos = Arc::new(AtomicI64::new(0));
        let log_buffers = create_test_log_buffers();
        let mut image = Image::new(
            1,
            SESSION_ID,
            INITIAL_TERM_ID,
            0,
            subscriber_pos.clone(),
            log_buffers,
            "ipc".to_string(),
            CORRELATION_ID,
        );

        let expected_position = (TERM_MIN_LENGTH - 32) as i64;
        subscriber_pos.store(expected_position, Ordering::Release);
        assert_eq!(image.position(), expected_position);

        image.set_position(TERM_MIN_LENGTH as i64).unwrap();
        assert_eq!(image.position(), TERM_MIN_LENGTH as i64);
    }

    #[test]
    fn should_report_correct_position_on_reception() {
        let position_bits_to_shift = (TERM_MIN_LENGTH as usize).trailing_zeros() as i32;
        let initial_position =
            compute_position(INITIAL_TERM_ID, 0, position_bits_to_shift, INITIAL_TERM_ID);
        let subscriber_pos = Arc::new(AtomicI64::new(initial_position));
        let mut log_buffers = create_test_log_buffers();

        let data = [1u8; 36];
        let msg_len = HEADER_LENGTH as usize + data.len();

        let aligned_frame_len = align(msg_len as i32, FRAME_ALIGNMENT) as usize;

        let terms = log_buffers.duplicate_term_buffers();

        let mut frame_bytes = vec![0u8; aligned_frame_len];
        let frame_buf = AtomicBuffer::new(&mut frame_bytes);
        let data_header = DataHeaderFlyweight::new(frame_buf);
        data_header.set_header_type(crate::protocol::HDR_TYPE_DATA);
        data_header.set_frame_length(msg_len as i32);
        data_header.set_session_id(SESSION_ID);
        data_header.set_term_id(INITIAL_TERM_ID);
        data_header.set_term_offset(0);

        terms[0].put_bytes(0, &frame_bytes);

        let mut image = Image::new(
            1,
            SESSION_ID,
            INITIAL_TERM_ID,
            initial_position,
            subscriber_pos.clone(),
            log_buffers,
            "ipc".to_string(),
            CORRELATION_ID,
        );

        image.term_buffers[0][..aligned_frame_len].copy_from_slice(&frame_bytes);

        let mut called = false;
        let read = image.poll(
            |_buf, offset, len, hdr| {
                called = true;
                assert_eq!(offset, HEADER_LENGTH as usize);
                assert_eq!(len, data.len());
                assert_eq!(hdr.session_id(), SESSION_ID);
            },
            usize::MAX,
        );

        assert_eq!(read, 1);
        assert!(called);
        assert_eq!(
            subscriber_pos.load(Ordering::Acquire),
            initial_position + aligned_frame_len as i64
        );
    }
}
