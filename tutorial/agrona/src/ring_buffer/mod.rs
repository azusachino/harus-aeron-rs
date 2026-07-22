//! Learner stub: Ring buffer modules.

pub mod mpsc;
pub mod spsc;

pub const TAIL_POSITION_OFFSET: usize = 0;
pub const HEAD_CACHE_POSITION_OFFSET: usize = 8;
pub const HEAD_POSITION_OFFSET: usize = 16;
pub const CORRELATION_COUNTER_OFFSET: usize = 24;
pub const CONSUMER_HEARTBEAT_OFFSET: usize = 32;
pub const TRAILER_LENGTH: usize = 1024;

pub const ALIGNMENT: usize = 8;
pub const HEADER_LENGTH: usize = 8;
pub const LENGTH_OFFSET: usize = 0;
pub const TYPE_OFFSET: usize = 4;
pub const PADDING_MSG_TYPE_ID: i32 = -1;

pub fn align(value: usize, alignment: usize) -> usize {
    (value + (alignment - 1)) & !(alignment - 1)
}
