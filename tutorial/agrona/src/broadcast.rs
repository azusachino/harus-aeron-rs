//! Learner stub: Broadcast transmitter and receiver implementations.

use crate::atomic_buffer::AtomicBuffer;
use crate::error::Result;

pub const TAIL_INTENT_COUNTER_OFFSET: usize = 0;
pub const TAIL_COUNTER_OFFSET: usize = 8;
pub const LATEST_COUNTER_OFFSET: usize = 16;
pub const TRAILER_LENGTH: usize = 1024;

#[derive(Clone)]
pub struct BroadcastTransmitter<'a> {
    buffer: AtomicBuffer<'a>,
    capacity: usize,
}

impl<'a> BroadcastTransmitter<'a> {
    pub fn new(_buffer: AtomicBuffer<'a>) -> Result<Self> {
        todo!("implement BroadcastTransmitter::new")
    }

    pub fn capacity(&self) -> usize {
        todo!("implement BroadcastTransmitter::capacity")
    }

    pub fn transmit(
        &self,
        _msg_type_id: i32,
        _src_buffer: &AtomicBuffer,
        _src_offset: usize,
        _length: usize,
    ) -> Result<()> {
        todo!("implement BroadcastTransmitter::transmit")
    }
}

#[derive(Clone)]
pub struct BroadcastReceiver<'a> {
    buffer: AtomicBuffer<'a>,
    capacity: usize,
    cursor: i64,
    next_record_offset: usize,
}

impl<'a> BroadcastReceiver<'a> {
    pub fn new(_buffer: AtomicBuffer<'a>) -> Result<Self> {
        todo!("implement BroadcastReceiver::new")
    }

    pub fn cursor(&self) -> i64 {
        todo!("implement BroadcastReceiver::cursor")
    }

    pub fn receive_next<F>(&mut self, _handler: F) -> Result<bool>
    where
        F: FnMut(i32, &AtomicBuffer, usize, usize),
    {
        todo!("implement BroadcastReceiver::receive_next")
    }
}
