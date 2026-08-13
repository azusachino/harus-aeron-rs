//! Many-Producer Single-Consumer (MPSC) Ring Buffer.
//!
//! Mirrors `org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer` from Java Agrona.

use crate::atomic_buffer::AtomicBuffer;
use crate::error::{AgronaError, Result};
use crate::ring_buffer::{
    ALIGNMENT, HEAD_CACHE_POSITION_OFFSET, HEAD_POSITION_OFFSET, HEADER_LENGTH, LENGTH_OFFSET,
    PADDING_MSG_TYPE_ID, TAIL_POSITION_OFFSET, TRAILER_LENGTH, TYPE_OFFSET, align,
};

/// A Many-Producer Single-Consumer (MPSC) ring buffer view over an [`AtomicBuffer`].
///
/// Thread-safe for multiple concurrent writers and a single reader.
#[derive(Clone)]
pub struct ManyToOneRingBuffer<'a> {
    buffer: AtomicBuffer<'a>,
    capacity: usize,
}

impl<'a> ManyToOneRingBuffer<'a> {
    /// Create a new `ManyToOneRingBuffer` wrapping the given [`AtomicBuffer`].
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

    /// Write a message into the ring buffer.
    ///
    /// Thread-safe. Returns `true` if successful, or `false` if the buffer is full.
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

        let mut head = self.buffer.get_i64(head_cache_offset);

        loop {
            let tail = self.buffer.get_i64_volatile(tail_offset);

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

                // CAS tail past padding
                if self
                    .buffer
                    .compare_and_set_i64(tail_offset, tail, tail + padding_length as i64)
                {
                    // Write padding record
                    self.buffer
                        .put_i32(index + TYPE_OFFSET, PADDING_MSG_TYPE_ID);
                    self.buffer
                        .put_i32_ordered(index + LENGTH_OFFSET, padding_length as i32);
                    // Loop again to try writing the actual message
                }
            } else {
                // Try to reserve the tail space
                if self.buffer.compare_and_set_i64(
                    tail_offset,
                    tail,
                    tail + aligned_record_length as i64,
                ) {
                    // Write the actual record
                    self.buffer.put_bytes_from(
                        index + HEADER_LENGTH,
                        src_buffer,
                        src_offset,
                        length,
                    );
                    self.buffer.put_i32(index + TYPE_OFFSET, msg_type_id);
                    self.buffer
                        .put_i32_ordered(index + LENGTH_OFFSET, record_length as i32);
                    return Ok(true);
                }
            }
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
        let tail_offset = self.capacity + TAIL_POSITION_OFFSET;

        let head = self.buffer.get_i64(head_offset);
        let tail = self.buffer.get_i64_volatile(tail_offset);

        let mut work_count = 0;
        let mut current_head = head;

        while current_head < tail && work_count < limit {
            if current_head < 0 {
                break;
            }
            let index = (current_head as usize) & (self.capacity - 1);
            if index + HEADER_LENGTH > self.capacity {
                break;
            }
            let length = self.buffer.get_i32_volatile(index + LENGTH_OFFSET);
            if length <= 0 {
                // In MPSC, writer might have reserved the tail space but not yet wrote the length.
                break;
            }

            let msg_type_id = self.buffer.get_i32(index + TYPE_OFFSET);
            let record_length = length as usize;
            let aligned_record_length = align(record_length, ALIGNMENT);
            if record_length < HEADER_LENGTH
                || aligned_record_length > self.capacity
                || index.saturating_add(aligned_record_length) > self.capacity
                || (aligned_record_length as i64) > tail.saturating_sub(current_head)
            {
                break;
            }
            let Some(next_head) = current_head.checked_add(aligned_record_length as i64) else {
                break;
            };

            if msg_type_id == PADDING_MSG_TYPE_ID {
                current_head = next_head;
                continue;
            }

            handler(
                msg_type_id,
                &self.buffer,
                index + HEADER_LENGTH,
                record_length - HEADER_LENGTH,
            );

            current_head = next_head;
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
    fn test_mpsc_ring_buffer_read_write() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_string_utf8(0, "MPSC");

        assert!(rb.write(2, &src, 4, 4).unwrap());
        assert_eq!(rb.tail(), 16);

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
    fn test_mpsc_ring_buffer_stops_on_malformed_record_length() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buffer = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buffer.clone()).unwrap();

        buffer.put_i32(0, 4);
        buffer.put_i64(rb.capacity() + TAIL_POSITION_OFFSET, 8);

        assert_eq!(rb.read(|_, _, _, _| {}), 0);
        assert_eq!(rb.head(), 0);
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    #[test]
    fn test_mpsc_loom() {
        // Unbounded exhaustive search over 3 threads (2 producers racing on a CAS retry
        // loop + 1 consumer) explodes combinatorially — cap preemptions/duration so this
        // terminates in CI instead of running for hours. See docs/plan.md v0.0.2 review.
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
}
