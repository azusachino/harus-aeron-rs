//! Reusable builder for assembling fragmented message fragments into a single buffer.

use crate::logbuffer::{HEADER_LENGTH, Header, compute_fragmented_frame_length};
use agrona::AtomicBuffer;

pub const MAX_CAPACITY: usize = i32::MAX as usize - 8;
pub const INIT_MIN_CAPACITY: usize = 4096;
pub const NULL_VALUE: i32 = -1;

pub fn find_suitable_capacity(capacity: usize, required_capacity: u64) -> usize {
    let mut new_capacity = capacity.max(INIT_MIN_CAPACITY) as u64;
    while new_capacity < required_capacity {
        new_capacity = new_capacity + (new_capacity >> 1);
        if new_capacity > MAX_CAPACITY as u64 {
            new_capacity = MAX_CAPACITY as u64;
            break;
        }
    }
    new_capacity as usize
}

/// Reusable buffer builder for assembling message fragments.
#[derive(Debug, Clone)]
pub struct BufferBuilder {
    buffer: Vec<u8>,
    limit: usize,
    next_term_offset: i32,
    first_frame_length: i32,
    header_buffer: Vec<u8>,
}

impl Default for BufferBuilder {
    fn default() -> Self {
        Self::new(0)
    }
}

impl BufferBuilder {
    pub fn new(initial_capacity: usize) -> Self {
        if initial_capacity > MAX_CAPACITY {
            panic!(
                "initialCapacity outside range 0 - {}: initialCapacity={}",
                MAX_CAPACITY, initial_capacity
            );
        }
        Self {
            buffer: vec![0u8; initial_capacity],
            limit: 0,
            next_term_offset: NULL_VALUE,
            first_frame_length: 0,
            header_buffer: vec![0u8; HEADER_LENGTH as usize],
        }
    }

    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn set_limit(&mut self, limit: usize) {
        if limit > self.buffer.len() {
            panic!(
                "limit outside range: capacity={} limit={}",
                self.buffer.len(),
                limit
            );
        }
        self.limit = limit;
    }

    pub fn next_term_offset(&self) -> i32 {
        self.next_term_offset
    }

    pub fn set_next_term_offset(&mut self, offset: i32) {
        self.next_term_offset = offset;
    }

    pub fn buffer_mut(&mut self) -> AtomicBuffer<'_> {
        AtomicBuffer::new(&mut self.buffer[..])
    }

    pub fn buffer_bytes(&self) -> &[u8] {
        &self.buffer[..self.limit]
    }

    pub fn reset(&mut self) -> &mut Self {
        self.limit = 0;
        self.next_term_offset = NULL_VALUE;
        self.first_frame_length = 0;
        self
    }

    pub fn compact(&mut self) -> &mut Self {
        let new_capacity = INIT_MIN_CAPACITY.max(self.limit);
        if new_capacity < self.buffer.len() {
            self.buffer.truncate(new_capacity);
            self.buffer.shrink_to_fit();
        }
        self
    }

    pub fn append(
        &mut self,
        src_buffer: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) -> &mut Self {
        self.ensure_capacity(length);
        let dst_offset = self.limit;
        src_buffer.get_bytes(
            src_offset,
            &mut self.buffer[dst_offset..dst_offset + length],
        );
        self.limit += length;
        self
    }

    pub fn capture_header(&mut self, header: &Header) -> &mut Self {
        self.first_frame_length = header.frame_length();
        if let Some(buf) = header.buffer() {
            buf.get_bytes(
                header.offset(),
                &mut self.header_buffer[..HEADER_LENGTH as usize],
            );
        }
        self
    }

    pub fn complete_header(&mut self, header: &Header) -> Header<'_> {
        let _fragmented_frame_length = compute_fragmented_frame_length(
            self.limit as i32,
            self.first_frame_length - HEADER_LENGTH,
        );

        let total_frame_length = (HEADER_LENGTH as usize + self.limit) as i32;
        let header_buf = AtomicBuffer::new(&mut self.header_buffer[..]);
        header_buf.put_i32(0, total_frame_length);

        let current_flags = header_buf.get_u8(5);
        header_buf.put_u8(5, current_flags | header.flags());

        let mut res = Header::new(header.initial_term_id(), header.position_bits_to_shift());
        res.set_buffer(header_buf);
        res.set_offset(0);
        res
    }

    fn ensure_capacity(&mut self, additional_length: usize) {
        let required_capacity = (self.limit + additional_length) as u64;
        let capacity = self.buffer.len();

        if required_capacity > capacity as u64 {
            if required_capacity > MAX_CAPACITY as u64 {
                panic!(
                    "insufficient capacity: maxCapacity={} limit={} additionalLength={}",
                    MAX_CAPACITY, self.limit, additional_length
                );
            }
            let new_capacity = find_suitable_capacity(capacity, required_capacity);
            self.buffer.resize(new_capacity, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_find_suitable_capacity() {
        assert_eq!(find_suitable_capacity(0, 0), 4096);
        assert_eq!(find_suitable_capacity(4095, 1), 4096);
        assert_eq!(find_suitable_capacity(4096, 7), 4096);
        assert_eq!(find_suitable_capacity(100, 200), 4096);
        assert_eq!(find_suitable_capacity(101, 5000), 6144);
        assert_eq!(find_suitable_capacity(5000, 8080), 11250);
        assert_eq!(find_suitable_capacity(6666, 11111), 14998);
    }

    #[test]
    fn should_initialise_to_default_values() {
        let builder = BufferBuilder::default();
        assert_eq!(builder.capacity(), 0);
        assert_eq!(builder.limit(), 0);
        assert_eq!(builder.next_term_offset(), NULL_VALUE);
    }

    #[test]
    fn should_append_and_grow() {
        let mut builder = BufferBuilder::new(0);
        let mut src_bytes = vec![1u8; 10];
        let src_buf = AtomicBuffer::new(&mut src_bytes);

        builder.append(&src_buf, 0, 10);
        assert_eq!(builder.limit(), 10);
        assert_eq!(builder.capacity(), INIT_MIN_CAPACITY);
    }

    #[test]
    fn should_reset() {
        let mut builder = BufferBuilder::new(0);
        let mut src_bytes = vec![1u8; 10];
        let src_buf = AtomicBuffer::new(&mut src_bytes);

        builder.append(&src_buf, 0, 10);
        builder.set_next_term_offset(100);

        builder.reset();
        assert_eq!(builder.limit(), 0);
        assert_eq!(builder.next_term_offset(), NULL_VALUE);
    }
}
