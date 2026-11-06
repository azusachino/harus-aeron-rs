//! Many-Producer Single-Consumer (MPSC) Ring Buffer.
//!
//! Mirrors `org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer` from Java Agrona
//! and `aeron_mpsc_rb.c` from the Aeron C client.

use crate::atomic_buffer::AtomicBuffer;
use crate::error::{AgronaError, Result};
use crate::ring_buffer::{
    ALIGNMENT, HEAD_CACHE_POSITION_OFFSET, HEAD_POSITION_OFFSET, HEADER_LENGTH, LENGTH_OFFSET,
    MIN_CAPACITY, PADDING_MSG_TYPE_ID, TAIL_POSITION_OFFSET, TRAILER_LENGTH, TYPE_OFFSET, align,
};

/// Action taken by a controlled read handler in a ring buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlledReadAction {
    /// Abort reading: stop and do not consume this record.
    Abort,
    /// Break reading: consume this record and stop.
    Break,
    /// Commit reading: commit consumption up to this record immediately, then continue.
    Commit,
    /// Continue reading: consume this record and continue.
    Continue,
}

/// A Many-Producer Single-Consumer (MPSC) ring buffer view over an [`AtomicBuffer`].
///
/// Thread-safe for multiple concurrent writers and a single reader.
#[derive(Clone)]
pub struct ManyToOneRingBuffer<'a> {
    buffer: AtomicBuffer<'a>,
    capacity: usize,
    max_message_length: usize,
}

impl<'a> ManyToOneRingBuffer<'a> {
    /// Create a new `ManyToOneRingBuffer` wrapping the given [`AtomicBuffer`].
    ///
    /// The buffer capacity must be large enough to contain the trailer (`TRAILER_LENGTH`)
    /// plus at least `MIN_CAPACITY`, and the remaining data capacity must be a power of two.
    pub fn new(buffer: AtomicBuffer<'a>) -> Result<Self> {
        let capacity = buffer.capacity();
        if capacity < TRAILER_LENGTH + MIN_CAPACITY {
            return Err(AgronaError::InvalidCapacity(capacity));
        }
        let data_capacity = capacity - TRAILER_LENGTH;
        if !data_capacity.is_power_of_two() {
            return Err(AgronaError::InvalidCapacity(data_capacity));
        }
        let max_message_length = if data_capacity == MIN_CAPACITY {
            0
        } else {
            data_capacity / 8
        };
        Ok(Self {
            buffer,
            capacity: data_capacity,
            max_message_length,
        })
    }

    /// Returns the data capacity of the ring buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Maximum supported message payload length in bytes.
    pub fn max_message_length(&self) -> usize {
        self.max_message_length
    }

    /// Get the tail position of the buffer.
    pub fn tail(&self) -> i64 {
        let tail_offset = self.capacity + TAIL_POSITION_OFFSET;
        self.buffer.get_i64_volatile(tail_offset)
    }

    /// Get the head position of the buffer.
    pub fn head(&self) -> i64 {
        let head_offset = self.capacity + HEAD_POSITION_OFFSET;
        self.buffer.get_i64_volatile(head_offset)
    }

    /// Returns the number of unconsumed bytes currently held in the ring buffer.
    pub fn size(&self) -> usize {
        let head = self.head();
        let tail = self.tail();
        tail.saturating_sub(head) as usize
    }

    /// Allocate the next correlation ID atomically from the shared trailer counter.
    pub fn next_correlation_id(&self) -> i64 {
        self.buffer.get_and_add_i64(
            self.capacity + crate::ring_buffer::CORRELATION_COUNTER_OFFSET,
            1,
        )
    }

    /// Read the consumer heartbeat timestamp stored in the shared trailer.
    pub fn consumer_heartbeat_time(&self) -> i64 {
        self.buffer
            .get_i64_volatile(self.capacity + crate::ring_buffer::CONSUMER_HEARTBEAT_OFFSET)
    }

    /// Publish the consumer heartbeat timestamp stored in the shared trailer.
    pub fn set_consumer_heartbeat_time(&self, timestamp: i64) {
        self.buffer.put_i64_ordered(
            self.capacity + crate::ring_buffer::CONSUMER_HEARTBEAT_OFFSET,
            timestamp,
        );
    }

    /// Claim capacity in the ring buffer, returning the record index within the data region.
    ///
    /// Atomically reserves space for the record and any necessary padding at the end of the buffer
    /// in a single CAS step.
    fn claim_capacity(&self, record_length: usize) -> Result<Option<usize>> {
        let required_capacity = align(record_length, ALIGNMENT);
        let mask = self.capacity - 1;
        let head_cache_offset = self.capacity + HEAD_CACHE_POSITION_OFFSET;
        let head_offset = self.capacity + HEAD_POSITION_OFFSET;
        let tail_offset = self.capacity + TAIL_POSITION_OFFSET;

        let mut head = self.buffer.get_i64(head_cache_offset);

        loop {
            let tail = self.buffer.get_i64_volatile(tail_offset);
            let available_capacity = (self.capacity as i64) - (tail - head);

            if (required_capacity as i64) > available_capacity {
                head = self.buffer.get_i64_volatile(head_offset);
                if (required_capacity as i64) > ((self.capacity as i64) - (tail - head)) {
                    return Ok(None);
                }
                self.buffer.put_i64(head_cache_offset, head);
            }

            let mut padding = 0;
            let mut tail_index = (tail as usize) & mask;
            let to_buffer_end = self.capacity - tail_index;

            if required_capacity > to_buffer_end {
                let mut head_index = (head as usize) & mask;
                if required_capacity > head_index {
                    head = self.buffer.get_i64_volatile(head_offset);
                    head_index = (head as usize) & mask;
                    if required_capacity > head_index {
                        return Ok(None);
                    }
                    self.buffer.put_i64(head_cache_offset, head);
                }
                padding = to_buffer_end;
            }

            let delta = (required_capacity + padding) as i64;
            if self
                .buffer
                .compare_and_set_i64(tail_offset, tail, tail + delta)
            {
                if padding != 0 {
                    self.buffer
                        .put_i32_ordered(tail_index + LENGTH_OFFSET, -(padding as i32));
                    self.buffer
                        .put_i32(tail_index + TYPE_OFFSET, PADDING_MSG_TYPE_ID);
                    self.buffer
                        .put_i32_ordered(tail_index + LENGTH_OFFSET, padding as i32);
                    tail_index = 0;
                }
                return Ok(Some(tail_index));
            }
        }
    }

    /// Write a message into the ring buffer.
    ///
    /// Thread-safe for multiple concurrent writers. Returns `Ok(true)` if written,
    /// or `Ok(false)` if the buffer is full.
    pub fn write(
        &self,
        msg_type_id: i32,
        src_buffer: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) -> Result<bool> {
        if length > self.max_message_length {
            return Err(AgronaError::MessageTooLarge {
                length,
                max_length: self.max_message_length,
            });
        }
        if msg_type_id < 1 {
            return Err(AgronaError::InvalidMessageType(msg_type_id));
        }

        let record_length = length + HEADER_LENGTH;
        let Some(record_index) = self.claim_capacity(record_length)? else {
            return Ok(false);
        };

        // Write negative length marker before payload (Aeron PR #2156).
        self.buffer
            .put_i32_ordered(record_index + LENGTH_OFFSET, -(record_length as i32));

        self.buffer
            .put_bytes_from(record_index + HEADER_LENGTH, src_buffer, src_offset, length);
        self.buffer.put_i32(record_index + TYPE_OFFSET, msg_type_id);
        self.buffer
            .put_i32_ordered(record_index + LENGTH_OFFSET, record_length as i32);

        Ok(true)
    }

    /// Attempt to claim capacity for a message to write payload directly into the buffer.
    ///
    /// Thread-safe for concurrent writers.
    /// Returns `Ok(Some(offset))` where payload should be written, or `Ok(None)` if buffer is full.
    pub fn try_claim(&self, msg_type_id: i32, length: usize) -> Result<Option<usize>> {
        if length > self.max_message_length {
            return Err(AgronaError::MessageTooLarge {
                length,
                max_length: self.max_message_length,
            });
        }
        if msg_type_id < 1 {
            return Err(AgronaError::InvalidMessageType(msg_type_id));
        }

        let record_length = length + HEADER_LENGTH;
        let Some(record_index) = self.claim_capacity(record_length)? else {
            return Ok(None);
        };

        // Write negative length marker before publication.
        self.buffer
            .put_i32_ordered(record_index + LENGTH_OFFSET, -(record_length as i32));
        self.buffer.put_i32(record_index + TYPE_OFFSET, msg_type_id);

        Ok(Some(record_index + HEADER_LENGTH))
    }

    /// Commit a previously claimed message at the given payload offset.
    pub fn commit(&self, offset: usize) -> Result<()> {
        if offset < HEADER_LENGTH || offset > self.capacity {
            return Err(AgronaError::InvalidClaimOffset(offset));
        }
        let record_index = offset - HEADER_LENGTH;
        let length = self.buffer.get_i32(record_index + LENGTH_OFFSET);
        if length < 0 {
            self.buffer
                .put_i32_ordered(record_index + LENGTH_OFFSET, -length);
            Ok(())
        } else {
            Err(AgronaError::InvalidClaimOffset(offset))
        }
    }

    /// Abort a previously claimed message, converting it to a padding record.
    pub fn abort(&self, offset: usize) -> Result<()> {
        if offset < HEADER_LENGTH || offset > self.capacity {
            return Err(AgronaError::InvalidClaimOffset(offset));
        }
        let record_index = offset - HEADER_LENGTH;
        let length = self.buffer.get_i32(record_index + LENGTH_OFFSET);
        if length < 0 {
            self.buffer
                .put_i32(record_index + TYPE_OFFSET, PADDING_MSG_TYPE_ID);
            self.buffer
                .put_i32_ordered(record_index + LENGTH_OFFSET, -length);
            Ok(())
        } else {
            Err(AgronaError::InvalidClaimOffset(offset))
        }
    }

    /// Attempt to unblock a stalled producer blocking the consumer at head.
    ///
    /// Mirrors `aeron_mpsc_rb_unblock` and Agrona `ManyToOneRingBuffer.unblock`.
    /// Returns `true` if a stalled record or zeroed gap was unblocked by padding it,
    /// or `false` if the buffer is not blocked or empty.
    pub fn unblock(&self) -> bool {
        let head_offset = self.capacity + HEAD_POSITION_OFFSET;
        let tail_offset = self.capacity + TAIL_POSITION_OFFSET;

        let head = self.buffer.get_i64_volatile(head_offset);
        let tail = self.buffer.get_i64_volatile(tail_offset);

        if head == tail {
            return false;
        }

        let mask = self.capacity - 1;
        let consumer_index = (head as usize) & mask;
        let producer_index = (tail as usize) & mask;

        let length = self.buffer.get_i32_volatile(consumer_index + LENGTH_OFFSET);
        if length < 0 {
            self.buffer
                .put_i32(consumer_index + TYPE_OFFSET, PADDING_MSG_TYPE_ID);
            self.buffer
                .put_i32_ordered(consumer_index + LENGTH_OFFSET, -length);
            true
        } else if length == 0 {
            let limit = if producer_index > consumer_index {
                producer_index
            } else {
                self.capacity
            };
            let mut i = consumer_index + ALIGNMENT;
            while i < limit {
                let rec_len = self.buffer.get_i32_volatile(i + LENGTH_OFFSET);
                if rec_len != 0 {
                    // Scan back to confirm still zeroed
                    let mut all_zeroes = true;
                    let mut back = i - ALIGNMENT;
                    loop {
                        if self.buffer.get_i32_volatile(back + LENGTH_OFFSET) != 0 {
                            all_zeroes = false;
                            break;
                        }
                        if back <= consumer_index {
                            break;
                        }
                        back -= ALIGNMENT;
                    }

                    if all_zeroes {
                        self.buffer
                            .put_i32(consumer_index + TYPE_OFFSET, PADDING_MSG_TYPE_ID);
                        self.buffer.put_i32_ordered(
                            consumer_index + LENGTH_OFFSET,
                            (i - consumer_index) as i32,
                        );
                        return true;
                    }
                    break;
                }
                i += ALIGNMENT;
            }
            false
        } else {
            false
        }
    }

    /// Read messages from the ring buffer, invoking the handler for each.
    ///
    /// Returns the number of messages read.
    pub fn read<F>(&self, handler: F) -> usize
    where
        F: FnMut(i32, &AtomicBuffer, usize, usize),
    {
        self.read_with_limit(handler, usize::MAX)
    }

    /// Read messages up to the specified limit, invoking the handler for each.
    ///
    /// Returns the number of messages read.
    pub fn read_with_limit<F>(&self, mut handler: F, limit: usize) -> usize
    where
        F: FnMut(i32, &AtomicBuffer, usize, usize),
    {
        let head_offset = self.capacity + HEAD_POSITION_OFFSET;
        let head = self.buffer.get_i64(head_offset);
        let head_index = (head as usize) & (self.capacity - 1);
        let contiguous_block_length = self.capacity - head_index;

        let mut messages_read = 0;
        let mut bytes_read = 0;

        while bytes_read < contiguous_block_length && messages_read < limit {
            let record_index = head_index + bytes_read;
            let length = self.buffer.get_i32_volatile(record_index + LENGTH_OFFSET);
            if length <= 0 {
                // In MPSC, writer might have reserved tail space but not yet published length.
                break;
            }

            let record_length = length as usize;
            let aligned_record_length = align(record_length, ALIGNMENT);
            if record_length < HEADER_LENGTH
                || record_index.saturating_add(aligned_record_length) > self.capacity
            {
                break;
            }

            bytes_read += aligned_record_length;
            let msg_type_id = self.buffer.get_i32(record_index + TYPE_OFFSET);

            if msg_type_id == PADDING_MSG_TYPE_ID {
                continue;
            }

            handler(
                msg_type_id,
                &self.buffer,
                record_index + HEADER_LENGTH,
                record_length - HEADER_LENGTH,
            );
            messages_read += 1;
        }

        if bytes_read != 0 {
            self.buffer.set_memory(head_index, bytes_read, 0);
            self.buffer
                .put_i64_ordered(head_offset, head + bytes_read as i64);
        }

        messages_read
    }

    /// Read messages using a controlled handler that decides how to consume each message.
    ///
    /// Returns the number of messages processed.
    pub fn controlled_read<F>(&self, handler: F) -> usize
    where
        F: FnMut(i32, &AtomicBuffer, usize, usize) -> ControlledReadAction,
    {
        self.controlled_read_with_limit(handler, usize::MAX)
    }

    /// Read messages up to the specified limit using a controlled handler.
    pub fn controlled_read_with_limit<F>(&self, mut handler: F, limit: usize) -> usize
    where
        F: FnMut(i32, &AtomicBuffer, usize, usize) -> ControlledReadAction,
    {
        let head_offset = self.capacity + HEAD_POSITION_OFFSET;
        let mut head = self.buffer.get_i64(head_offset);
        let mut head_index = (head as usize) & (self.capacity - 1);
        let contiguous_block_length = self.capacity - head_index;

        let mut messages_read = 0;
        let mut bytes_read = 0;

        while bytes_read < contiguous_block_length && messages_read < limit {
            let record_index = head_index + bytes_read;
            let length = self.buffer.get_i32_volatile(record_index + LENGTH_OFFSET);
            if length <= 0 {
                break;
            }

            let record_length = length as usize;
            let aligned_record_length = align(record_length, ALIGNMENT);
            if record_length < HEADER_LENGTH
                || record_index.saturating_add(aligned_record_length) > self.capacity
            {
                break;
            }

            bytes_read += aligned_record_length;
            let msg_type_id = self.buffer.get_i32(record_index + TYPE_OFFSET);

            if msg_type_id == PADDING_MSG_TYPE_ID {
                continue;
            }

            let action = handler(
                msg_type_id,
                &self.buffer,
                record_index + HEADER_LENGTH,
                record_length - HEADER_LENGTH,
            );

            match action {
                ControlledReadAction::Abort => {
                    bytes_read -= aligned_record_length;
                    break;
                }
                ControlledReadAction::Break => {
                    messages_read += 1;
                    break;
                }
                ControlledReadAction::Commit => {
                    messages_read += 1;
                    self.buffer.set_memory(head_index, bytes_read, 0);
                    self.buffer
                        .put_i64_ordered(head_offset, head + bytes_read as i64);
                    head_index += bytes_read;
                    head += bytes_read as i64;
                    bytes_read = 0;
                }
                ControlledReadAction::Continue => {
                    messages_read += 1;
                }
            }
        }

        if bytes_read != 0 {
            self.buffer.set_memory(head_index, bytes_read, 0);
            self.buffer
                .put_i64_ordered(head_offset, head + bytes_read as i64);
        }

        messages_read
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[cfg(not(loom))]
    #[test]
    fn should_calculate_capacity_for_buffer() {
        let mut backing = [0u8; 1024 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();
        assert_eq!(rb.capacity(), 1024);
        assert_eq!(rb.max_message_length(), 1024 / 8);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_error_for_capacity_not_power_of_two() {
        let mut backing = [0u8; 1023 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        assert!(matches!(
            ManyToOneRingBuffer::new(buf),
            Err(AgronaError::InvalidCapacity(1023))
        ));
    }

    #[cfg(not(loom))]
    #[test]
    fn should_error_for_capacity_less_than_trailer() {
        let mut backing = [0u8; TRAILER_LENGTH + 4];
        let buf = AtomicBuffer::new(&mut backing);
        assert!(matches!(
            ManyToOneRingBuffer::new(buf),
            Err(AgronaError::InvalidCapacity(_))
        ));
    }

    #[cfg(not(loom))]
    #[test]
    fn should_error_when_max_message_size_exceeded() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 64];
        let src = AtomicBuffer::new(&mut src_backing);
        let max_len = rb.max_message_length(); // 128 / 8 = 16

        assert!(matches!(
            rb.write(1, &src, 0, max_len + 1),
            Err(AgronaError::MessageTooLarge { .. })
        ));
    }

    #[cfg(not(loom))]
    #[test]
    fn should_error_when_min_capacity_is_used_and_message_size_is_not_zero() {
        let mut backing = [0u8; MIN_CAPACITY + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        assert_eq!(rb.max_message_length(), 0);
        let mut src_backing = [0u8; 16];
        let src = AtomicBuffer::new(&mut src_backing);

        assert!(matches!(
            rb.write(1, &src, 0, 1),
            Err(AgronaError::MessageTooLarge { .. })
        ));
    }

    #[cfg(not(loom))]
    #[test]
    fn should_write_an_empty_message_when_min_capacity_is_used() {
        let mut backing = [0u8; MIN_CAPACITY + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 16];
        let src = AtomicBuffer::new(&mut src_backing);

        assert!(rb.write(1, &src, 0, 0).unwrap());
        assert_eq!(rb.tail(), 8);
        assert_eq!(rb.head(), 0);

        let mut count = 0;
        let read_count = rb.read(|msg_type_id, _, _, length| {
            assert_eq!(msg_type_id, 1);
            assert_eq!(length, 0);
            count += 1;
        });
        assert_eq!(read_count, 1);
        assert_eq!(count, 1);
        assert_eq!(rb.head(), 8);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_error_when_message_type_is_negative_or_zero() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 16];
        let src = AtomicBuffer::new(&mut src_backing);

        assert!(matches!(
            rb.write(-1, &src, 0, 4),
            Err(AgronaError::InvalidMessageType(-1))
        ));
        assert!(matches!(
            rb.write(0, &src, 0, 4),
            Err(AgronaError::InvalidMessageType(0))
        ));
    }

    #[cfg(not(loom))]
    #[test]
    fn should_write_to_empty_buffer() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_string_utf8(0, "MPSC");

        assert!(rb.write(101, &src, 4, 4).unwrap());
        assert_eq!(rb.tail(), 16);
        assert_eq!(rb.head(), 0);
        assert_eq!(rb.size(), 16);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_reject_write_when_buffer_full() {
        let mut backing = [0u8; 64 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);

        // max_message_length for 64 is 8. Record length = 8 + 8 = 16.
        // Buffer capacity = 64 bytes -> 4 records of 16 bytes.
        assert!(rb.write(1, &src, 0, 8).unwrap());
        assert!(rb.write(2, &src, 0, 8).unwrap());
        assert!(rb.write(3, &src, 0, 8).unwrap());
        assert!(rb.write(4, &src, 0, 8).unwrap());
        assert_eq!(rb.size(), 64);

        // Buffer is full:
        assert!(!rb.write(5, &src, 0, 8).unwrap());
    }

    #[cfg(not(loom))]
    #[test]
    fn should_insert_padding_record_plus_message_on_buffer_wrap() {
        let mut backing = [0u8; 64 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_string_utf8(0, "msg1");

        // Write 3 records of 16 bytes (48 bytes total). Tail = 48.
        assert!(rb.write(1, &src, 0, 4).unwrap());
        assert!(rb.write(2, &src, 0, 4).unwrap());
        assert!(rb.write(3, &src, 0, 4).unwrap());
        assert_eq!(rb.tail(), 48);

        // Read all 3 records. Head becomes 48.
        assert_eq!(rb.read(|_, _, _, _| {}), 3);
        assert_eq!(rb.head(), 48);

        // Next message: payload 8 bytes (record 16 bytes).
        // Remaining at end: 64 - 48 = 16 bytes.
        // Fits exactly at end without wrap!
        assert!(rb.write(4, &src, 0, 8).unwrap());
        assert_eq!(rb.tail(), 64);

        // Read message 4. Head becomes 64.
        assert_eq!(rb.read(|_, _, _, _| {}), 1);
        assert_eq!(rb.head(), 64);

        // Now tail = 64, head = 64.
        // Write message 5: payload 4 bytes (record 16 bytes). Fits at index 0. Tail = 80.
        assert!(rb.write(5, &src, 0, 4).unwrap());
        assert_eq!(rb.tail(), 80);

        // Write message 6 & 7: 16 bytes each. Tail becomes 112. (index 48).
        assert!(rb.write(6, &src, 0, 4).unwrap());
        assert!(rb.write(7, &src, 0, 4).unwrap());
        assert_eq!(rb.tail(), 112);

        // Read messages 5, 6, 7. Head becomes 112.
        assert_eq!(rb.read(|_, _, _, _| {}), 3);
        assert_eq!(rb.head(), 112);

        // Now tail = 112 (index 48), remaining at end = 16 bytes.
        // But write a message with payload 8 bytes (record length 16 bytes).
        // Fits at end! Tail becomes 128.
        assert!(rb.write(8, &src, 0, 8).unwrap());
        assert_eq!(rb.tail(), 128);

        // Read message 8. Head becomes 128.
        assert_eq!(rb.read(|_, _, _, _| {}), 1);
        assert_eq!(rb.head(), 128);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_insert_padding_when_message_does_not_fit_at_buffer_end() {
        let mut backing = [0u8; 64 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);

        // Write 3 records of 16 bytes = 48 bytes. Tail = 48.
        assert!(rb.write(1, &src, 0, 0).unwrap()); // record 8 bytes -> aligned to 8
        assert!(rb.write(2, &src, 0, 8).unwrap()); // record 16 bytes
        assert!(rb.write(3, &src, 0, 8).unwrap()); // record 16 bytes
        assert_eq!(rb.tail(), 40);

        // Read all 3 records to make room at the front.
        assert_eq!(rb.read(|_, _, _, _| {}), 3);
        assert_eq!(rb.head(), 40);

        // Current index is 40. Remaining space at end is 64 - 40 = 24 bytes.
        // Write record of 16 bytes (payload 8).
        assert!(rb.write(4, &src, 0, 8).unwrap()); // tail = 56, index = 56.
        assert_eq!(rb.tail(), 56);

        // Now remaining space at end is 64 - 56 = 8 bytes.
        // Try to write message with payload 8 (record length 16 bytes).
        // 16 > 8, so it doesn't fit at the end!
        // It must write 8 bytes padding at index 56, and write 16 bytes at index 0!
        // Total tail delta = 8 (padding) + 16 (record) = 24.
        // Tail becomes 56 + 24 = 80.
        assert!(rb.write(5, &src, 0, 8).unwrap());
        assert_eq!(rb.tail(), 80);

        // Read message 4.
        let mut read_msgs = Vec::new();
        assert_eq!(
            rb.read(|msg_type_id, _, _, len| {
                read_msgs.push((msg_type_id, len));
            }),
            1
        );
        assert_eq!(read_msgs, vec![(4, 8)]);
        assert_eq!(rb.head(), 64);

        // Next read skips padding and reads message 5 at index 0!
        read_msgs.clear();
        assert_eq!(
            rb.read(|msg_type_id, _, _, len| {
                read_msgs.push((msg_type_id, len));
            }),
            1
        );
        assert_eq!(read_msgs, vec![(5, 8)]);
        assert_eq!(rb.head(), 80);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_read_nothing_from_empty_buffer() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        assert_eq!(rb.read(|_, _, _, _| {}), 0);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_read_single_message() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_string_utf8(0, "MPSC");

        assert!(rb.write(2, &src, 4, 4).unwrap());

        let mut called = false;
        let read_count = rb.read(|msg_type_id, buffer, offset, length| {
            assert_eq!(msg_type_id, 2);
            assert_eq!(length, 4);
            let mut read_bytes = vec![0u8; length];
            buffer.get_bytes(offset, &mut read_bytes);
            assert_eq!(read_bytes, b"MPSC");
            called = true;
        });

        assert_eq!(read_count, 1);
        assert!(called);
        assert_eq!(rb.head(), 16);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_not_read_single_message_part_way_through_writing() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf.clone()).unwrap();

        // Advance tail to reserve 16 bytes, but length is 0 (writer hasn't written it yet).
        buf.put_i64(rb.capacity() + TAIL_POSITION_OFFSET, 16);

        assert_eq!(rb.read(|_, _, _, _| {}), 0);
        assert_eq!(rb.head(), 0);

        // Put negative claim marker (writer is writing payload).
        buf.put_i32(0, -16);
        assert_eq!(rb.read(|_, _, _, _| {}), 0);
        assert_eq!(rb.head(), 0);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_read_two_messages() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_i32(0, 100);
        src.put_i32(4, 200);

        assert!(rb.write(1, &src, 0, 4).unwrap());
        assert!(rb.write(2, &src, 4, 4).unwrap());

        let mut received = Vec::new();
        let read_count = rb.read(|msg_type_id, buf, offset, _| {
            received.push((msg_type_id, buf.get_i32(offset)));
        });

        assert_eq!(read_count, 2);
        assert_eq!(received, vec![(1, 100), (2, 200)]);
        assert_eq!(rb.head(), 32);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_limit_read_of_messages() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);

        assert!(rb.write(1, &src, 0, 4).unwrap());
        assert!(rb.write(2, &src, 0, 4).unwrap());
        assert!(rb.write(3, &src, 0, 4).unwrap());

        let mut received = Vec::new();
        let read_count = rb.read_with_limit(
            |msg_type_id, _, _, _| {
                received.push(msg_type_id);
            },
            2,
        );

        assert_eq!(read_count, 2);
        assert_eq!(received, vec![1, 2]);
        assert_eq!(rb.head(), 32);

        let read_count_2 = rb.read(|msg_type_id, _, _, _| {
            received.push(msg_type_id);
        });
        assert_eq!(read_count_2, 1);
        assert_eq!(received, vec![1, 2, 3]);
        assert_eq!(rb.head(), 48);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_not_unblock_when_empty() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        assert!(!rb.unblock());
    }

    #[cfg(not(loom))]
    #[test]
    fn should_unblock_message_with_header() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf.clone()).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);

        // Writer 1 claims slot and gets stuck (negative marker -16)
        buf.put_i32(0, -16);
        buf.put_i32(4, 101);
        buf.put_i64(rb.capacity() + TAIL_POSITION_OFFSET, 16);

        // Consumer sees blocked:
        assert_eq!(rb.read(|_, _, _, _| {}), 0);

        // Unblock turns it into padding:
        assert!(rb.unblock());
        assert_eq!(buf.get_i32(0), 16);
        assert_eq!(buf.get_i32(4), PADDING_MSG_TYPE_ID);

        // Now writer 2 writes a valid message:
        assert!(rb.write(102, &src, 0, 4).unwrap());
        assert_eq!(rb.tail(), 32);

        // Consumer reads message 102, skipping the unblocked padding:
        let mut received = Vec::new();
        assert_eq!(
            rb.read(|msg_type_id, _, _, _| {
                received.push(msg_type_id);
            }),
            1
        );
        assert_eq!(received, vec![102]);
        assert_eq!(rb.head(), 32);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_unblock_gap_with_zeros() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf.clone()).unwrap();

        // Writer 1 reserved [0..16] but hasn't written anything (length = 0).
        // Writer 2 reserved [16..32] and completed write (length = 16, type = 102).
        buf.put_i32(16, 16);
        buf.put_i32(20, 102);
        buf.put_i64(rb.capacity() + TAIL_POSITION_OFFSET, 32);

        // Consumer cannot read:
        assert_eq!(rb.read(|_, _, _, _| {}), 0);

        // Unblock identifies gap at index 0 and turns [0..16] into padding:
        assert!(rb.unblock());
        assert_eq!(buf.get_i32(0), 16);
        assert_eq!(buf.get_i32(4), PADDING_MSG_TYPE_ID);

        // Consumer can now read message 102:
        let mut received = Vec::new();
        assert_eq!(
            rb.read(|msg_type_id, _, _, _| {
                received.push(msg_type_id);
            }),
            1
        );
        assert_eq!(received, vec![102]);
        assert_eq!(rb.head(), 32);
    }

    #[cfg(not(loom))]
    #[test]
    fn try_claim_and_commit_lifecycle() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf.clone()).unwrap();

        // Type checks
        assert!(matches!(
            rb.try_claim(0, 8),
            Err(AgronaError::InvalidMessageType(0))
        ));
        assert!(matches!(
            rb.try_claim(-1, 8),
            Err(AgronaError::InvalidMessageType(-1))
        ));

        // Max length check
        assert!(matches!(
            rb.try_claim(1, rb.max_message_length() + 1),
            Err(AgronaError::MessageTooLarge { .. })
        ));

        // Successful claim
        let offset = rb.try_claim(101, 8).unwrap().expect("should claim");
        assert_eq!(offset, 8); // index 0 + HEADER_LENGTH
        assert_eq!(rb.tail(), 16);

        // Payload write directly by caller:
        buf.put_i64(offset, 123456789);

        // In-flight claim cannot be read before commit:
        assert_eq!(rb.read(|_, _, _, _| {}), 0);

        // Commit:
        rb.commit(offset).expect("commit should succeed");

        // Read back:
        let mut received_val = 0;
        let read_count = rb.read(|msg_type_id, buffer, off, len| {
            assert_eq!(msg_type_id, 101);
            assert_eq!(len, 8);
            received_val = buffer.get_i64(off);
        });
        assert_eq!(read_count, 1);
        assert_eq!(received_val, 123456789);
        assert_eq!(rb.head(), 16);
    }

    #[cfg(not(loom))]
    #[test]
    fn try_claim_and_abort_lifecycle() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf.clone()).unwrap();

        let offset = rb.try_claim(101, 8).unwrap().expect("should claim");
        assert_eq!(offset, 8);

        // Abort:
        rb.abort(offset).expect("abort should succeed");
        assert_eq!(buf.get_i32(0), 16);
        assert_eq!(buf.get_i32(4), PADDING_MSG_TYPE_ID);

        // Write a second message:
        let mut src_backing = [0u8; 16];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_i32(0, 999);
        assert!(rb.write(102, &src, 0, 4).unwrap());

        // Consumer skips aborted padding and reads message 102:
        let mut received = Vec::new();
        assert_eq!(
            rb.read(|msg_type_id, buf, off, _| {
                received.push((msg_type_id, buf.get_i32(off)));
            }),
            1
        );
        assert_eq!(received, vec![(102, 999)]);
        assert_eq!(rb.head(), 32);
    }

    #[cfg(not(loom))]
    #[test]
    fn commit_and_abort_invalid_offsets() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        assert!(matches!(
            rb.commit(0),
            Err(AgronaError::InvalidClaimOffset(0))
        ));
        assert!(matches!(
            rb.commit(1000),
            Err(AgronaError::InvalidClaimOffset(1000))
        ));
        assert!(matches!(
            rb.abort(0),
            Err(AgronaError::InvalidClaimOffset(0))
        ));
        assert!(matches!(
            rb.abort(1000),
            Err(AgronaError::InvalidClaimOffset(1000))
        ));
    }

    #[cfg(not(loom))]
    #[test]
    fn controlled_read_actions() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);

        assert!(rb.write(1, &src, 0, 4).unwrap());
        assert!(rb.write(2, &src, 0, 4).unwrap());
        assert!(rb.write(3, &src, 0, 4).unwrap());

        // 1. Abort on first record: does not consume record 1, head stays 0
        let count = rb.controlled_read(|msg_type_id, _, _, _| {
            if msg_type_id == 1 {
                ControlledReadAction::Abort
            } else {
                ControlledReadAction::Continue
            }
        });
        assert_eq!(count, 0);
        assert_eq!(rb.head(), 0);

        // 2. Commit on first record, then Break on second:
        let mut visited = Vec::new();
        let count = rb.controlled_read(|msg_type_id, _, _, _| {
            visited.push(msg_type_id);
            if msg_type_id == 1 {
                ControlledReadAction::Commit
            } else if msg_type_id == 2 {
                ControlledReadAction::Break
            } else {
                ControlledReadAction::Continue
            }
        });
        assert_eq!(count, 2);
        assert_eq!(visited, vec![1, 2]);
        assert_eq!(rb.head(), 32);

        // 3. Continue on third record:
        visited.clear();
        let count = rb.controlled_read(|msg_type_id, _, _, _| {
            visited.push(msg_type_id);
            ControlledReadAction::Continue
        });
        assert_eq!(count, 1);
        assert_eq!(visited, vec![3]);
        assert_eq!(rb.head(), 48);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_get_size_and_correlation_ids() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        assert_eq!(rb.size(), 0);
        let mut src_backing = [0u8; 16];
        let src = AtomicBuffer::new(&mut src_backing);

        assert!(rb.write(1, &src, 0, 8).unwrap());
        assert_eq!(rb.size(), 16);

        assert_eq!(rb.next_correlation_id(), 0);
        assert_eq!(rb.next_correlation_id(), 1);
        assert_eq!(rb.next_correlation_id(), 2);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_prevent_stale_reads_on_second_lap_after_wrap() {
        let mut backing = [0u8; 64 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf.clone()).unwrap();

        let mut src_backing = [0u8; 16];
        let src = AtomicBuffer::new(&mut src_backing);

        // First lap: write 4 records (64 bytes total).
        assert!(rb.write(1, &src, 0, 8).unwrap());
        assert!(rb.write(2, &src, 0, 8).unwrap());
        assert!(rb.write(3, &src, 0, 8).unwrap());
        assert!(rb.write(4, &src, 0, 8).unwrap());
        assert_eq!(rb.tail(), 64);

        // Read all 4 records. Because of zeroing on read, all 64 bytes are zeroed!
        assert_eq!(rb.read(|_, _, _, _| {}), 4);
        assert_eq!(rb.head(), 64);

        // Second lap: simulate a writer that has claimed space (tail advanced to 80),
        // but has NOT written length yet (index 0).
        buf.put_i64(rb.capacity() + TAIL_POSITION_OFFSET, 80);

        // Reader arrives at head = 64 (index 0).
        // Since memory was zeroed upon read on lap 1, length at index 0 is 0!
        // Reader does NOT read stale record from lap 1:
        assert_eq!(rb.read(|_, _, _, _| {}), 0);
        assert_eq!(rb.head(), 64);
    }

    #[cfg(not(loom))]
    #[test]
    fn should_exchange_messages_across_threads() {
        let mut backing = vec![0u8; 1024 * 64 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let num_messages = 50_000;
        let num_producers = 2;

        std::thread::scope(|s| {
            for p in 0..num_producers {
                let rb_prod = rb.clone();
                s.spawn(move || {
                    let mut src_backing = [0u8; 16];
                    let src = AtomicBuffer::new(&mut src_backing);
                    for m in 0..num_messages {
                        src.put_i32(0, p);
                        src.put_i32(4, m);
                        while !rb_prod.write(100 + p, &src, 0, 8).unwrap() {
                            std::thread::yield_now();
                        }
                    }
                });
            }

            let mut counts = vec![0; num_producers as usize];
            let mut total_read = 0;
            let expected_total = num_messages * num_producers;

            while total_read < expected_total {
                let read = rb.read(|msg_type_id, buf, offset, _| {
                    let p = (msg_type_id - 100) as usize;
                    let val = buf.get_i32(offset + 4);
                    assert_eq!(counts[p], val, "ordering violation on producer {p}");
                    counts[p] += 1;
                });
                if read == 0 {
                    std::thread::yield_now();
                }
                total_read += read as i32;
            }

            assert_eq!(total_read, expected_total);
        });
    }

    #[cfg(not(loom))]
    #[test]
    fn should_exchange_messages_via_try_claim_across_threads() {
        let mut backing = vec![0u8; 1024 * 64 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let num_messages = 50_000;
        let num_producers = 2;

        std::thread::scope(|s| {
            for p in 0..num_producers {
                let rb_prod = rb.clone();
                s.spawn(move || {
                    for m in 0..num_messages {
                        loop {
                            match rb_prod.try_claim(100 + p, 8).unwrap() {
                                Some(offset) => {
                                    rb_prod.buffer.put_i32(offset, p);
                                    rb_prod.buffer.put_i32(offset + 4, m);
                                    rb_prod.commit(offset).unwrap();
                                    break;
                                }
                                None => {
                                    std::thread::yield_now();
                                }
                            }
                        }
                    }
                });
            }

            let mut counts = vec![0; num_producers as usize];
            let mut total_read = 0;
            let expected_total = num_messages * num_producers;

            while total_read < expected_total {
                let read = rb.read(|msg_type_id, buf, offset, _| {
                    let p = (msg_type_id - 100) as usize;
                    let val = buf.get_i32(offset + 4);
                    assert_eq!(counts[p], val);
                    counts[p] += 1;
                });
                if read == 0 {
                    std::thread::yield_now();
                }
                total_read += read as i32;
            }

            assert_eq!(total_read, expected_total);
        });
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    #[test]
    fn test_mpsc_loom() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.max_duration = Some(std::time::Duration::from_secs(60));
        builder.check(|| {
            let buf = AtomicBuffer::new_loom(128 + TRAILER_LENGTH);
            let rb = ManyToOneRingBuffer::new(buf).unwrap();

            let rb_w1 = rb.clone();
            let rb_w2 = rb.clone();
            let rb_reader = rb.clone();

            let t1 = thread::spawn(move || {
                let src = AtomicBuffer::new_loom(16);
                src.put_i32(0, 10);
                while !rb_w1.write(101, &src, 0, 4).unwrap() {
                    thread::yield_now();
                }
            });

            let t2 = thread::spawn(move || {
                let src = AtomicBuffer::new_loom(16);
                src.put_i32(0, 20);
                while !rb_w2.write(102, &src, 0, 4).unwrap() {
                    thread::yield_now();
                }
            });

            let t3 = thread::spawn(move || {
                let mut count = 0;
                let mut sum = 0;
                while count < 2 {
                    rb_reader.read(|msg_type_id, buf, offset, _length| {
                        count += 1;
                        let val = buf.get_i32(offset);
                        sum += val;
                        assert!(msg_type_id == 101 || msg_type_id == 102);
                    });
                    thread::yield_now();
                }
                assert_eq!(sum, 30);
            });

            t1.join().unwrap();
            t2.join().unwrap();
            t3.join().unwrap();
        });
    }

    #[test]
    fn test_mpsc_loom_try_claim() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.max_duration = Some(std::time::Duration::from_secs(60));
        builder.check(|| {
            let buf = AtomicBuffer::new_loom(128 + TRAILER_LENGTH);
            let rb = ManyToOneRingBuffer::new(buf).unwrap();

            let rb_w1 = rb.clone();
            let rb_w2 = rb.clone();
            let rb_reader = rb.clone();

            let t1 = thread::spawn(move || {
                loop {
                    if let Some(offset) = rb_w1.try_claim(101, 4).unwrap() {
                        rb_w1.buffer.put_i32(offset, 10);
                        rb_w1.commit(offset).unwrap();
                        break;
                    }
                    thread::yield_now();
                }
            });

            let t2 = thread::spawn(move || {
                loop {
                    if let Some(offset) = rb_w2.try_claim(102, 4).unwrap() {
                        rb_w2.buffer.put_i32(offset, 20);
                        rb_w2.commit(offset).unwrap();
                        break;
                    }
                    thread::yield_now();
                }
            });

            let t3 = thread::spawn(move || {
                let mut count = 0;
                let mut sum = 0;
                while count < 2 {
                    rb_reader.read(|msg_type_id, buf, offset, _length| {
                        count += 1;
                        let val = buf.get_i32(offset);
                        sum += val;
                        assert!(msg_type_id == 101 || msg_type_id == 102);
                    });
                    thread::yield_now();
                }
                assert_eq!(sum, 30);
            });

            t1.join().unwrap();
            t2.join().unwrap();
            t3.join().unwrap();
        });
    }

    #[test]
    fn test_mpsc_loom_unblock() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.max_duration = Some(std::time::Duration::from_secs(60));
        builder.check(|| {
            let buf = AtomicBuffer::new_loom(128 + TRAILER_LENGTH);
            let rb = ManyToOneRingBuffer::new(buf).unwrap();

            let rb_w = rb.clone();
            let rb_unblocker = rb.clone();
            let rb_reader = rb.clone();

            // Writer 1 claims and stalls (does not commit)
            let t1 = thread::spawn(move || {
                let _ = rb_w.try_claim(101, 4).unwrap();
            });

            // Unblocker thread detects stall and unblocks
            let t2 = thread::spawn(move || {
                while !rb_unblocker.unblock() {
                    thread::yield_now();
                }
            });

            // Reader thread should eventually observe buffer head moving past unblocked record
            let t3 = thread::spawn(move || {
                while rb_reader.head() == 0 {
                    rb_reader.read(|_, _, _, _| {});
                    thread::yield_now();
                }
                assert_eq!(rb_reader.head(), 16);
            });

            t1.join().unwrap();
            t2.join().unwrap();
            t3.join().unwrap();
        });
    }
}
