//! Ring buffer implementations for single-producer single-consumer (SPSC)
//! and many-producer single-consumer (MPSC) communication.

pub mod mpsc;
pub mod spsc;

pub use mpsc::{ControlledReadAction, ManyToOneRingBuffer};
pub use spsc::OneToOneRingBuffer;

/// Cache line length assumed for false-sharing padding, matching upstream agrona.
pub const CACHE_LINE_LENGTH: usize = 64;
/// Each trailer counter occupies two cache lines so producer- and consumer-written
/// counters never share a cache line (mirrors vendor `RingBufferDescriptor` / C `aeron_rb.h`).
const COUNTER_PADDING: usize = CACHE_LINE_LENGTH * 2;

/// Offset of the tail position counter in the trailer.
///
/// The first cache-line pair is reserved before the counters by Agrona.
pub const TAIL_POSITION_OFFSET: usize = COUNTER_PADDING;
/// Offset of the cached head position counter in the trailer.
pub const HEAD_CACHE_POSITION_OFFSET: usize = COUNTER_PADDING * 2;
/// Offset of the head position counter in the trailer.
pub const HEAD_POSITION_OFFSET: usize = COUNTER_PADDING * 3;
/// Offset of the correlation counter in the trailer.
pub const CORRELATION_COUNTER_OFFSET: usize = COUNTER_PADDING * 4;
/// Offset of the consumer heartbeat timestamp counter in the trailer.
pub const CONSUMER_HEARTBEAT_OFFSET: usize = COUNTER_PADDING * 5;
/// Total length of the trailer at the end of the ring buffer.
///
/// Agrona reserves one additional cache-line pair before and after the five counters. The extra
/// padding is part of the shared-buffer geometry, so omitting it makes a CnC ring buffer
/// unreadable by the Java client even though the Rust producer/consumer can still wrap it.
///
/// Source of truth: Agrona 2.4.0, selected by `vendor/aeron/gradle/libs.versions.toml`,
/// `org/agrona/concurrent/ringbuffer/RingBufferDescriptor.java`:
/// <https://raw.githubusercontent.com/aeron-io/agrona/2.4.0/agrona/src/main/java/org/agrona/concurrent/ringbuffer/RingBufferDescriptor.java>.
/// The vendored Aeron tree does not include Agrona's Java source; the JAR is runtime-only and
/// is not used to derive these offsets. The source defines tail=128, head-cache=256, head=384,
/// correlation=512, heartbeat=640, trailer=768.
pub const TRAILER_LENGTH: usize = COUNTER_PADDING * 6;

/// Length of the record header (4 bytes length + 4 bytes type ID).
pub const HEADER_LENGTH: usize = 8;
/// Minimum data capacity for a ring buffer, equal to record header length.
pub const MIN_CAPACITY: usize = HEADER_LENGTH;
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
