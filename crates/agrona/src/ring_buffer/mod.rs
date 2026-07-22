//! Ring buffer implementations for single-producer single-consumer (SPSC)
//! and many-producer single-consumer (MPSC) communication.

pub mod mpsc;
pub mod spsc;

pub use mpsc::ManyToOneRingBuffer;
pub use spsc::OneToOneRingBuffer;

/// Offset of the tail position counter in the trailer.
pub const TAIL_POSITION_OFFSET: usize = 0;
/// Offset of the cached head position counter in the trailer.
pub const HEAD_CACHE_POSITION_OFFSET: usize = 8;
/// Offset of the head position counter in the trailer.
pub const HEAD_POSITION_OFFSET: usize = 16;
/// Offset of the correlation counter in the trailer.
pub const CORRELATION_COUNTER_OFFSET: usize = 24;
/// Offset of the consumer heartbeat timestamp counter in the trailer.
pub const CONSUMER_HEARTBEAT_OFFSET: usize = 32;
/// Total length of the trailer at the end of the ring buffer.
pub const TRAILER_LENGTH: usize = 1024;

/// Length of the record header (4 bytes length + 4 bytes type ID).
pub const HEADER_LENGTH: usize = 8;
/// Offset of the record length in the record header.
pub const LENGTH_OFFSET: usize = 0;
/// Offset of the record type ID in the record header.
pub const TYPE_OFFSET: usize = 4;
/// Alignment requirement for records in bytes.
pub const ALIGNMENT: usize = 8;
/// Message type ID for padding records.
pub const PADDING_MSG_TYPE_ID: i32 = -1;

/// Align a value to the specified alignment (must be a power of two).
#[inline]
pub fn align(value: usize, alignment: usize) -> usize {
    (value + (alignment - 1)) & !(alignment - 1)
}
