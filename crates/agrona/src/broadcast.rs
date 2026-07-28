//! Broadcast transmitter and receiver implementations over an [`AtomicBuffer`].
//!
//! Mirrors `org.agrona.concurrent.broadcast.BroadcastTransmitter` and
//! `org.agrona.concurrent.broadcast.BroadcastReceiver` from Java Agrona.

use crate::atomic_buffer::AtomicBuffer;
use crate::error::{AgronaError, Result};
use crate::ring_buffer::{
    ALIGNMENT, HEADER_LENGTH, LENGTH_OFFSET, PADDING_MSG_TYPE_ID, TYPE_OFFSET, align,
};

/// Offset of the tail intent position counter in the trailer.
pub const TAIL_INTENT_COUNTER_OFFSET: usize = 0;
/// Offset of the published tail position counter in the trailer.
pub const TAIL_COUNTER_OFFSET: usize = 8;
/// Offset of the latest message sequence counter in the trailer.
pub const LATEST_COUNTER_OFFSET: usize = 16;
/// Total length of the trailer at the end of the broadcast buffer. The three counters are
/// packed (no inter-field padding, per vendor `aeron_broadcast_descriptor`); the trailer as a
/// whole is one double cache line.
pub const TRAILER_LENGTH: usize = crate::ring_buffer::CACHE_LINE_LENGTH * 2;

/// A single-writer broadcast transmitter over an [`AtomicBuffer`].
#[derive(Clone)]
pub struct BroadcastTransmitter<'a> {
    buffer: AtomicBuffer<'a>,
    capacity: usize,
}

impl<'a> BroadcastTransmitter<'a> {
    /// Create a new `BroadcastTransmitter` wrapping the given [`AtomicBuffer`].
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

    /// Returns the data capacity of the broadcast buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Transmit a message to all broadcast receivers.
    pub fn transmit(
        &self,
        msg_type_id: i32,
        src_buffer: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) -> Result<()> {
        let record_length = length + HEADER_LENGTH;
        let aligned_record_length = align(record_length, ALIGNMENT);

        let tail_intent_offset = self.capacity + TAIL_INTENT_COUNTER_OFFSET;
        let tail_offset = self.capacity + TAIL_COUNTER_OFFSET;

        let tail = self.buffer.get_i64(tail_intent_offset);
        let index = (tail as usize) & (self.capacity - 1);

        if index + aligned_record_length > self.capacity {
            let padding_length = self.capacity - index;

            // Write padding record
            self.buffer
                .put_i32(index + TYPE_OFFSET, PADDING_MSG_TYPE_ID);
            self.buffer
                .put_i32_ordered(index + LENGTH_OFFSET, padding_length as i32);

            let next_tail = tail + padding_length as i64;
            self.buffer.put_i64_ordered(tail_intent_offset, next_tail);
            self.buffer.put_i64_ordered(tail_offset, next_tail);

            // Write real message at index 0
            let next_index = 0;
            self.buffer
                .put_bytes_from(next_index + HEADER_LENGTH, src_buffer, src_offset, length);
            self.buffer.put_i32(next_index + TYPE_OFFSET, msg_type_id);
            self.buffer
                .put_i32_ordered(next_index + LENGTH_OFFSET, record_length as i32);

            let final_tail = next_tail + aligned_record_length as i64;
            self.buffer.put_i64_ordered(tail_intent_offset, final_tail);
            self.buffer.put_i64_ordered(tail_offset, final_tail);
        } else {
            // Fits at current index
            self.buffer
                .put_bytes_from(index + HEADER_LENGTH, src_buffer, src_offset, length);
            self.buffer.put_i32(index + TYPE_OFFSET, msg_type_id);
            self.buffer
                .put_i32_ordered(index + LENGTH_OFFSET, record_length as i32);

            let next_tail = tail + aligned_record_length as i64;
            self.buffer.put_i64_ordered(tail_intent_offset, next_tail);
            self.buffer.put_i64_ordered(tail_offset, next_tail);
        }

        Ok(())
    }
}

/// A broadcast receiver that reads messages from a [`BroadcastTransmitter`].
#[derive(Clone)]
pub struct BroadcastReceiver<'a> {
    buffer: AtomicBuffer<'a>,
    capacity: usize,
    cursor: i64,
    next_record_offset: usize,
}

impl<'a> BroadcastReceiver<'a> {
    /// Create a new `BroadcastReceiver` wrapping the given [`AtomicBuffer`].
    pub fn new(buffer: AtomicBuffer<'a>) -> Result<Self> {
        let capacity = buffer.capacity();
        if capacity < TRAILER_LENGTH {
            return Err(AgronaError::InvalidCapacity(capacity));
        }
        let data_capacity = capacity - TRAILER_LENGTH;
        if !data_capacity.is_power_of_two() {
            return Err(AgronaError::InvalidCapacity(data_capacity));
        }
        let tail = buffer.get_i64_volatile(data_capacity + TAIL_COUNTER_OFFSET);
        Ok(Self {
            buffer,
            capacity: data_capacity,
            cursor: tail,
            next_record_offset: (tail as usize) & (data_capacity - 1),
        })
    }

    /// Returns the current cursor position of the receiver.
    pub fn cursor(&self) -> i64 {
        self.cursor
    }

    /// Receive the next message, invoking the handler if present.
    ///
    /// Returns `Ok(true)` if a message was received, `Ok(false)` if no message is available,
    /// or `Err(AgronaError::Lapped)` if the receiver was lapped.
    pub fn receive_next<F>(&mut self, mut handler: F) -> Result<bool>
    where
        F: FnMut(i32, &AtomicBuffer, usize, usize),
    {
        let tail_intent_offset = self.capacity + TAIL_INTENT_COUNTER_OFFSET;
        let tail_offset = self.capacity + TAIL_COUNTER_OFFSET;

        let tail = self.buffer.get_i64_volatile(tail_offset);
        if self.cursor >= tail {
            return Ok(false);
        }

        if tail - self.cursor > self.capacity as i64 {
            let old_cursor = self.cursor;
            self.cursor = tail;
            self.next_record_offset = (tail as usize) & (self.capacity - 1);
            return Err(AgronaError::Lapped {
                cursor: old_cursor,
                tail,
            });
        }

        let mut index = (self.cursor as usize) & (self.capacity - 1);

        loop {
            let length = self.buffer.get_i32_volatile(index + LENGTH_OFFSET);
            if length <= 0 {
                return Ok(false);
            }

            let msg_type_id = self.buffer.get_i32(index + TYPE_OFFSET);
            let aligned_record_length = align(length as usize, ALIGNMENT);

            if msg_type_id == PADDING_MSG_TYPE_ID {
                self.cursor += aligned_record_length as i64;
                index = (self.cursor as usize) & (self.capacity - 1);
                if self.cursor >= tail {
                    return Ok(false);
                }
                continue;
            }

            let tail_intent = self.buffer.get_i64_volatile(tail_intent_offset);
            if tail_intent - self.cursor > self.capacity as i64 {
                let old_cursor = self.cursor;
                self.cursor = tail;
                self.next_record_offset = (tail as usize) & (self.capacity - 1);
                return Err(AgronaError::Lapped {
                    cursor: old_cursor,
                    tail,
                });
            }

            handler(
                msg_type_id,
                &self.buffer,
                index + HEADER_LENGTH,
                (length as usize) - HEADER_LENGTH,
            );

            let post_tail_intent = self.buffer.get_i64_volatile(tail_intent_offset);
            if post_tail_intent - self.cursor > self.capacity as i64 {
                let old_cursor = self.cursor;
                self.cursor = tail;
                self.next_record_offset = (tail as usize) & (self.capacity - 1);
                return Err(AgronaError::Lapped {
                    cursor: old_cursor,
                    tail,
                });
            }

            self.cursor += aligned_record_length as i64;
            self.next_record_offset = (self.cursor as usize) & (self.capacity - 1);
            return Ok(true);
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn test_broadcast_transmit_and_receive() {
        let mut backing = [0u8; 128 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);

        let transmitter = BroadcastTransmitter::new(buf.clone()).unwrap();
        let mut receiver1 = BroadcastReceiver::new(buf.clone()).unwrap();
        let mut receiver2 = BroadcastReceiver::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_string_utf8(0, "BroadcastMsg");

        transmitter.transmit(77, &src, 4, 12).unwrap();

        // Both receivers receive the message
        let mut rx1_called = false;
        let res1 = receiver1.receive_next(|msg_type_id, buffer, offset, length| {
            assert_eq!(msg_type_id, 77);
            assert_eq!(length, 12);
            let mut read_bytes = vec![0u8; length];
            buffer.get_bytes(offset, &mut read_bytes);
            assert_eq!(read_bytes, b"BroadcastMsg");
            rx1_called = true;
        });
        assert!(res1.unwrap());
        assert!(rx1_called);

        let mut rx2_called = false;
        let res2 = receiver2.receive_next(|msg_type_id, buffer, offset, length| {
            assert_eq!(msg_type_id, 77);
            assert_eq!(length, 12);
            let mut read_bytes = vec![0u8; length];
            buffer.get_bytes(offset, &mut read_bytes);
            assert_eq!(read_bytes, b"BroadcastMsg");
            rx2_called = true;
        });
        assert!(res2.unwrap());
        assert!(rx2_called);
    }

    #[test]
    fn test_broadcast_lapped_detection() {
        let mut backing = [0u8; 64 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);

        let transmitter = BroadcastTransmitter::new(buf.clone()).unwrap();
        let mut receiver = BroadcastReceiver::new(buf).unwrap();

        let mut src_backing = [0u8; 32];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_string_utf8(0, "1234");

        // Transmit 5 messages of 16 bytes = 80 bytes (capacity is 64)
        for i in 1..=5 {
            transmitter.transmit(i, &src, 4, 4).unwrap();
        }

        // Receiver was at cursor 0, but transmitter is at 80 (> 64 capacity)
        // receiver should report Lapped error
        let res = receiver.receive_next(|_, _, _, _| {});
        assert!(matches!(res, Err(AgronaError::Lapped { .. })));

        // Receiver cursor should be updated to latest tail (80)
        assert_eq!(receiver.cursor(), 80);
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    #[test]
    fn test_broadcast_loom() {
        // Bound the search — see docs/plan.md v0.0.2 review (mpsc's unbounded 3-thread
        // variant took 20+ minutes with no sign of terminating).
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.max_duration = Some(std::time::Duration::from_secs(60));
        builder.check(|| {
            let buf = AtomicBuffer::new_loom(128 + TRAILER_LENGTH);
            let transmitter = BroadcastTransmitter::new(buf.clone()).unwrap();
            let mut receiver1 = BroadcastReceiver::new(buf).unwrap();

            let t_tx = thread::spawn(move || {
                let src = AtomicBuffer::new_loom(16);
                src.put_i32(0, 100);
                transmitter.transmit(10, &src, 0, 4).unwrap();
            });

            let t_rx1 = thread::spawn(move || {
                let mut received = false;
                while !received {
                    if let Ok(true) =
                        receiver1.receive_next(|msg_type_id, buffer, offset, _length| {
                            assert_eq!(msg_type_id, 10);
                            assert_eq!(buffer.get_i32(offset), 100);
                            received = true;
                        })
                    {}
                    thread::yield_now();
                }
            });

            t_tx.join().unwrap();
            t_rx1.join().unwrap();
        });
    }
}
