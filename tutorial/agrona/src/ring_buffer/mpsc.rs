//! Learner stub: MPSC ring buffer implementation.

use crate::atomic_buffer::AtomicBuffer;
use crate::error::Result;

#[derive(Clone)]
pub struct ManyToOneRingBuffer<'a> {
    buffer: AtomicBuffer<'a>,
    capacity: usize,
}

impl<'a> ManyToOneRingBuffer<'a> {
    pub fn new(_buffer: AtomicBuffer<'a>) -> Result<Self> {
        todo!("implement ManyToOneRingBuffer::new")
    }

    pub fn capacity(&self) -> usize {
        todo!("implement ManyToOneRingBuffer::capacity")
    }

    pub fn write(
        &self,
        _msg_type_id: i32,
        _src_buffer: &AtomicBuffer,
        _src_offset: usize,
        _length: usize,
    ) -> Result<bool> {
        todo!("implement ManyToOneRingBuffer::write")
    }

    pub fn read<F>(&self, _handler: F) -> usize
    where
        F: FnMut(i32, &AtomicBuffer, usize, usize),
    {
        todo!("implement ManyToOneRingBuffer::read")
    }

    pub fn head(&self) -> i64 {
        todo!("implement ManyToOneRingBuffer::head")
    }

    pub fn tail(&self) -> i64 {
        todo!("implement ManyToOneRingBuffer::tail")
    }
}
