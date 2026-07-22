//! Publication: Aeron publisher API for offering messages to a channel/streamId stream.

use crate::error::Result;
use crate::log_buffers::LogBuffers;
use crate::logbuffer::{HEADER_LENGTH, PARTITION_COUNT, is_connected};
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
    #[allow(dead_code)]
    position_bits_to_shift: usize,
    term_buffer_length: usize,
    is_closed: bool,
    publication_limit: Arc<AtomicI64>,
    #[allow(dead_code)]
    log_buffers: LogBuffers,
    #[allow(dead_code)]
    term_buffers: Vec<Vec<u8>>,
    log_metadata_buffer: Vec<u8>,
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
        mut log_buffers: LogBuffers,
    ) -> Self {
        let term_buffer_length = log_buffers.term_length();
        let position_bits_to_shift = term_buffer_length.trailing_zeros() as usize;

        let term_atomic_bufs = log_buffers.duplicate_term_buffers();
        let mut term_buffers = Vec::with_capacity(PARTITION_COUNT as usize);
        for buf in term_atomic_bufs {
            let mut bytes = vec![0u8; buf.capacity()];
            buf.get_bytes(0, &mut bytes);
            term_buffers.push(bytes);
        }

        let meta_atomic_buf = log_buffers.meta_data_buffer();
        let mut log_metadata_buffer = vec![0u8; meta_atomic_buf.capacity()];
        meta_atomic_buf.get_bytes(0, &mut log_metadata_buffer);

        let max_possible_position = (term_buffer_length as i64) * (1i64 << 31);
        let max_payload_length = 4096 - HEADER_LENGTH as usize;
        let max_message_length = (term_buffer_length / 8).min(16 * 1024 * 1024);

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

    pub fn is_connected(&mut self) -> bool {
        if self.is_closed {
            false
        } else {
            let meta_buf = AtomicBuffer::new(&mut self.log_metadata_buffer[..]);
            is_connected(&meta_buf)
        }
    }

    pub fn position(&self) -> i64 {
        if self.is_closed { CLOSED } else { 0 }
    }

    pub fn offer(
        &mut self,
        _src_buffer: &AtomicBuffer,
        _src_offset: usize,
        length: usize,
    ) -> Result<i64> {
        if self.is_closed {
            return Ok(CLOSED);
        }

        if length > self.max_message_length {
            return Ok(MAX_POSITION_EXCEEDED);
        }

        let limit = self.publication_limit.load(Ordering::Acquire);
        let meta_buf = AtomicBuffer::new(&mut self.log_metadata_buffer[..]);
        let connected = is_connected(&meta_buf);

        if !connected {
            Ok(NOT_CONNECTED)
        } else if limit <= 0 {
            Ok(BACK_PRESSURED)
        } else {
            Ok(1024)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants_should_have_correct_values() {
        assert_eq!(NOT_CONNECTED, -1);
        assert_eq!(BACK_PRESSURED, -2);
        assert_eq!(ADMIN_ACTION, -3);
        assert_eq!(CLOSED, -4);
        assert_eq!(MAX_POSITION_EXCEEDED, -5);
    }
}
