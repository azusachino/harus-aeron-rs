use crate::protocol;
use agrona::AtomicBuffer;

/// Maximum length allowed for a message, mirroring `FrameDescriptor.MAX_MESSAGE_LENGTH`.
pub const MAX_MESSAGE_LENGTH: i32 = 16 * 1024 * 1024;

/// Compute the maximum supported message length for a given term length, mirroring
/// `FrameDescriptor.computeMaxMessageLength`.
pub fn compute_max_message_length(term_length: i32) -> i32 {
    (term_length >> 3).min(MAX_MESSAGE_LENGTH)
}
/// Frame header length (32 bytes).
///
/// Mirrors `DataHeaderFlyweight::HEADER_LENGTH`.
pub const HEADER_LENGTH: i32 = 32;

/// Alignment of frames in the log buffer (32 bytes).
///
/// Mirrors `FrameDescriptor::FRAME_ALIGNMENT`.
pub const FRAME_ALIGNMENT: i32 = 32;

/// Align a value to the specified alignment (must be a power of two).
#[inline]
pub fn align(value: i32, alignment: i32) -> i32 {
    (value + (alignment - 1)) & !(alignment - 1)
}

/// Compute frame length for a message fragmented into chunks of `max_payload_size`.
pub fn compute_fragmented_frame_length(length: i32, max_payload_size: i32) -> i32 {
    let num_max_payloads = length / max_payload_size;
    let remaining_payload = length % max_payload_size;
    let last_frame_length = if remaining_payload > 0 {
        align(remaining_payload + HEADER_LENGTH, FRAME_ALIGNMENT)
    } else {
        0
    };

    (num_max_payloads * (max_payload_size + HEADER_LENGTH)) + last_frame_length
}

/// Read frame length at `term_offset` using volatile load.
pub fn frame_length_volatile(term_buffer: &AtomicBuffer, term_offset: usize) -> i32 {
    term_buffer.get_i32_volatile(term_offset)
}

/// Check if frame at `term_offset` is a padding frame.
pub fn is_padding_frame(term_buffer: &AtomicBuffer, term_offset: usize) -> bool {
    let frame_type = term_buffer.get_i16(term_offset + protocol::TYPE_FIELD_OFFSET) as u16;
    frame_type == protocol::HDR_TYPE_PAD
}

/// Set frame type at `term_offset`.
pub fn set_frame_type(term_buffer: &AtomicBuffer, term_offset: usize, frame_type: u16) {
    term_buffer.put_i16(term_offset + protocol::TYPE_FIELD_OFFSET, frame_type as i16);
}

/// Set term offset field at `term_offset`.
pub fn set_frame_term_offset(term_buffer: &AtomicBuffer, term_offset: usize) {
    term_buffer.put_i32(
        term_offset + protocol::DATA_TERM_OFFSET_FIELD_OFFSET,
        term_offset as i32,
    );
}

/// Set term ID field at `term_offset`.
pub fn set_frame_term_id(term_buffer: &AtomicBuffer, term_offset: usize, term_id: i32) {
    term_buffer.put_i32(term_offset + protocol::DATA_TERM_ID_FIELD_OFFSET, term_id);
}

/// Set the flags byte of the frame at `term_offset`.
pub fn set_frame_flags(term_buffer: &AtomicBuffer, term_offset: usize, flags: u8) {
    term_buffer.put_u8(term_offset + protocol::FLAGS_FIELD_OFFSET, flags);
}

/// Set the reserved-value field of the frame at `term_offset`.
pub fn set_frame_reserved_value(term_buffer: &AtomicBuffer, term_offset: usize, value: i64) {
    term_buffer.put_i64(term_offset + protocol::DATA_RESERVED_VALUE_OFFSET, value);
}

/// Set frame length at `term_offset` using ordered/release store.
pub fn set_frame_length_ordered(term_buffer: &AtomicBuffer, term_offset: usize, frame_length: i32) {
    term_buffer.put_i32_ordered(
        term_offset + protocol::FRAME_LENGTH_FIELD_OFFSET,
        frame_length,
    );
}
