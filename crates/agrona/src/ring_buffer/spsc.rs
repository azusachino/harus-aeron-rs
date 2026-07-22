//! Single-Producer Single-Consumer (SPSC) Ring Buffer.
//!
//! Mirrors `org.agrona.concurrent.ringbuffer.OneToOneRingBuffer` from Java Agrona.

use crate::atomic_buffer::AtomicBuffer;
use crate::error::{AgronaError, Result};
use crate::ring_buffer::{
    ALIGNMENT, HEAD_CACHE_POSITION_OFFSET, HEAD_POSITION_OFFSET, HEADER_LENGTH, LENGTH_OFFSET,
    PADDING_MSG_TYPE_ID, TAIL_POSITION_OFFSET, TRAILER_LENGTH, TYPE_OFFSET, align,
};

/// A Single-Producer Single-Consumer (SPSC) ring buffer view over an [`AtomicBuffer`].
///
/// Thread-safe only when used by exactly one producer thread and exactly one consumer thread.
#[derive(Clone)]
pub struct OneToOneRingBuffer<'a> {
    buffer: AtomicBuffer<'a>,
    capacity: usize,
}

impl<'a> OneToOneRingBuffer<'a> {
    /// Create a new `OneToOneRingBuffer` wrapping the given [`AtomicBuffer`].
    ///
    /// The buffer capacity must be large enough to contain the trailer (`TRAILER_LENGTH`),
    /// and the remaining data capacity must be a power of two.
    pub fn new(buffer: AtomicBuffer<'a>) -> Result<Self> {
        let capacity = buffer.capacity();
        if capacity < TRAILER_LENGTH {
            return Err(AgronaError::InvalidCapacity(capacity));
        }
        let data_capacity = capacity - TRAILER_LENGTH;
        if !data_capacity.is_power_of_two() {
            return Err(AgronaError::InvalidCapacity(data_capacity));
        }
        Ok(Self {
            buffer,
            capacity: data_capacity,
        })
    }

    /// Returns the data capacity of the ring buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
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

    /// Write a message into the ring buffer.
    ///
    /// Returns `true` if successful, or `false` if the buffer is full.
    pub fn write(
        &self,
        msg_type_id: i32,
        src_buffer: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) -> Result<bool> {
        let record_length = length + HEADER_LENGTH;
        let aligned_record_length = align(record_length, ALIGNMENT);

        let tail_offset = self.capacity + TAIL_POSITION_OFFSET;
        let head_cache_offset = self.capacity + HEAD_CACHE_POSITION_OFFSET;
        let head_offset = self.capacity + HEAD_POSITION_OFFSET;

        let tail = self.buffer.get_i64(tail_offset);
        let head_cache = self.buffer.get_i64(head_cache_offset);

        let mut head = head_cache;
        if tail - head + (aligned_record_length as i64) > self.capacity as i64 {
            head = self.buffer.get_i64_volatile(head_offset);
            if tail - head + (aligned_record_length as i64) > self.capacity as i64 {
                return Ok(false); // buffer full
            }
            self.buffer.put_i64(head_cache_offset, head);
        }

        let index = (tail as usize) & (self.capacity - 1);
        if index + aligned_record_length > self.capacity {
            // Does not fit at the end of the buffer. Check if it fits at the start.
            let padding_length = self.capacity - index;
            if tail + (padding_length as i64) - head + (aligned_record_length as i64)
                > self.capacity as i64
            {
                head = self.buffer.get_i64_volatile(head_offset);
                if tail + (padding_length as i64) - head + (aligned_record_length as i64)
                    > self.capacity as i64
                {
                    return Ok(false); // buffer full
                }
                self.buffer.put_i64(head_cache_offset, head);
            }

            // Write padding record at the end of buffer
            self.buffer
                .put_i32(index + TYPE_OFFSET, PADDING_MSG_TYPE_ID);
            self.buffer
                .put_i32_ordered(index + LENGTH_OFFSET, padding_length as i32);

            // Wrap tail to the beginning
            let next_tail = tail + padding_length as i64;
            self.buffer.put_i64_ordered(tail_offset, next_tail);

            // Write the actual record at the start of buffer (index 0)
            let next_index = 0;
            self.buffer
                .put_bytes_from(next_index + HEADER_LENGTH, src_buffer, src_offset, length);
            self.buffer.put_i32(next_index + TYPE_OFFSET, msg_type_id);
            self.buffer
                .put_i32_ordered(next_index + LENGTH_OFFSET, record_length as i32);

            // Commit the real write tail
            self.buffer
                .put_i64_ordered(tail_offset, next_tail + aligned_record_length as i64);
        } else {
            // Fits at the end of the buffer
            self.buffer
                .put_bytes_from(index + HEADER_LENGTH, src_buffer, src_offset, length);
            self.buffer.put_i32(index + TYPE_OFFSET, msg_type_id);
            self.buffer
                .put_i32_ordered(index + LENGTH_OFFSET, record_length as i32);

            self.buffer
                .put_i64_ordered(tail_offset, tail + aligned_record_length as i64);
        }

        Ok(true)
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
        let tail_offset = self.capacity + TAIL_POSITION_OFFSET;

        let head = self.buffer.get_i64(head_offset);
        let tail = self.buffer.get_i64_volatile(tail_offset);

        let mut work_count = 0;
        let mut current_head = head;

        while current_head < tail && work_count < limit {
            let index = (current_head as usize) & (self.capacity - 1);
            let length = self.buffer.get_i32_volatile(index + LENGTH_OFFSET);
            if length <= 0 {
                break;
            }

            let msg_type_id = self.buffer.get_i32(index + TYPE_OFFSET);
            let aligned_record_length = align(length as usize, ALIGNMENT);

            if msg_type_id == PADDING_MSG_TYPE_ID {
                current_head += aligned_record_length as i64;
                continue;
            }

            handler(
                msg_type_id,
                &self.buffer,
                index + HEADER_LENGTH,
                (length as usize) - HEADER_LENGTH,
            );

            current_head += aligned_record_length as i64;
            work_count += 1;
        }

        if current_head > head {
            self.buffer.put_i64_ordered(head_offset, current_head);
        }

        work_count
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[cfg(not(loom))]
    #[test]
    fn test_spsc_ring_buffer_read_write() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = OneToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_string_utf8(0, "Aeron");

        // Write a message
        assert!(rb.write(1, &src, 4, 5).unwrap());

        // Verify head and tail
        assert_eq!(rb.tail(), 16); // aligned length of (5 + 8) = 13 aligned to 8 is 16
        assert_eq!(rb.head(), 0);

        // Read it back
        let mut called = false;
        let read_count = rb.read(|msg_type_id, buffer, offset, length| {
            assert_eq!(msg_type_id, 1);
            assert_eq!(length, 5);
            let mut read_bytes = vec![0u8; length];
            buffer.get_bytes(offset, &mut read_bytes);
            assert_eq!(read_bytes, b"Aeron");
            called = true;
        });

        assert_eq!(read_count, 1);
        assert!(called);
        assert_eq!(rb.head(), 16);
    }

    #[cfg(not(loom))]
    #[test]
    fn test_spsc_ring_buffer_wrap_and_padding() {
        let mut backing = [0u8; 64 + TRAILER_LENGTH]; // very small data buffer (64 bytes)
        let buf = AtomicBuffer::new(&mut backing);
        let rb = OneToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_string_utf8(0, "1234"); // payload length 4, record length 12, aligned to 16

        // Fill buffer to near the wrap point (write 3 records of 16 bytes = 48 bytes)
        assert!(rb.write(1, &src, 4, 4).unwrap());
        assert!(rb.write(2, &src, 4, 4).unwrap());
        assert!(rb.write(3, &src, 4, 4).unwrap());
        assert_eq!(rb.tail(), 48);

        // Now, next write requires 16 bytes.
        // Remaining space at end is 64 - 48 = 16 bytes.
        // It fits exactly at the end! So write it.
        assert!(rb.write(4, &src, 4, 4).unwrap());
        assert_eq!(rb.tail(), 64);

        // Read all 4 records to empty the buffer.
        let mut read_types = Vec::new();
        rb.read(|msg_type_id, _, _, _| {
            read_types.push(msg_type_id);
        });
        assert_eq!(read_types, vec![1, 2, 3, 4]);
        assert_eq!(rb.head(), 64);

        // Buffer is empty. Tail = 64, Head = 64.
        // Index is 64 & 63 = 0.
        // Write one record (16 bytes). Index becomes 16, Tail = 80.
        assert!(rb.write(5, &src, 4, 4).unwrap());
        assert_eq!(rb.tail(), 80);

        // Current index is 80 & 63 = 16.
        // Remaining space at end is 64 - 16 = 48 bytes.
        // Let's write three records (16, 16, 16).
        assert!(rb.write(6, &src, 4, 4).unwrap()); // index = 32, tail = 96
        assert!(rb.write(7, &src, 4, 4).unwrap()); // index = 48, tail = 112
        // Now index is 48. Tail = 112. Remaining space at end is 64 - 48 = 16 bytes.

        // Read 5, 6, 7 first to free space
        let mut read_types_2 = Vec::new();
        rb.read(|msg_type_id, _, _, _| {
            read_types_2.push(msg_type_id);
        });
        assert_eq!(read_types_2, vec![5, 6, 7]);
        assert_eq!(rb.head(), 112);

        // Let's write a larger message of payload length 10 (record length 18, aligned to 24).
        // 24 bytes does not fit in the remaining 16 bytes.
        // So it must write a padding record of 16 bytes, wrap, and write at index 0.
        let mut large_src_backing = [0u8; 32];
        let large_src = AtomicBuffer::new(&mut large_src_backing);
        large_src.put_string_utf8(0, "1234567890");
        assert!(rb.write(8, &large_src, 4, 10).unwrap());

        // Tail should be 112 + 16 (padding) + 24 (large record) = 152.
        assert_eq!(rb.tail(), 152);

        // Read the remaining message 8.
        let mut read_types_3 = Vec::new();
        rb.read(|msg_type_id, _, _, _| {
            read_types_3.push(msg_type_id);
        });
        assert_eq!(read_types_3, vec![8]);
        assert_eq!(rb.head(), 152);
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    #[test]
    fn test_spsc_loom() {
        // Bound the search so this can't run away like the MPSC variant did — see
        // docs/plan.md v0.0.2 review.
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.max_duration = Some(std::time::Duration::from_secs(60));
        builder.check(|| {
            let buf = AtomicBuffer::new_loom(128 + TRAILER_LENGTH);
            let rb = OneToOneRingBuffer::new(buf).unwrap();

            let rb_writer = rb.clone();
            let rb_reader = rb.clone();

            let handle = thread::spawn(move || {
                let src = AtomicBuffer::new_loom(16);
                src.put_i32(0, 42);
                rb_writer.write(101, &src, 0, 4).unwrap();
            });

            let handle2 = thread::spawn(move || {
                let mut read_val = 0;
                let mut read_type = 0;
                while read_val == 0 {
                    rb_reader.read(|msg_type_id, buf, offset, _length| {
                        read_type = msg_type_id;
                        read_val = buf.get_i32(offset);
                    });
                    thread::yield_now();
                }
                assert_eq!(read_type, 101);
                assert_eq!(read_val, 42);
            });

            handle.join().unwrap();
            handle2.join().unwrap();
        });
    }
}
