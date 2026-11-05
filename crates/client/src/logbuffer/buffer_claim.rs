use super::frame_descriptor::*;
use crate::protocol;
use agrona::AtomicBuffer;

/// A claimed frame-sized region of a term buffer, filled and committed without copy semantics.
///
/// Mirrors upstream `io.aeron.logbuffer.BufferClaim`, returned by
/// `ConcurrentPublication::try_claim`. Deviates from upstream's zero-based sub-buffer wrap:
/// this codebase addresses term buffers by absolute offset everywhere (`Header`, `TermReader`,
/// `HeaderWriter` above), so [`BufferClaim::buffer`] returns the underlying term buffer itself
/// and [`BufferClaim::offset`]/[`BufferClaim::length`] are the claim's absolute payload bounds
/// within it, rather than a fresh zero-offset view.
pub struct BufferClaim {
    buffer: AtomicBuffer<'static>,
    frame_offset: usize,
    frame_length: i32,
}

impl BufferClaim {
    pub(crate) fn wrap(
        buffer: AtomicBuffer<'static>,
        frame_offset: usize,
        frame_length: i32,
    ) -> Self {
        Self {
            buffer,
            frame_offset,
            frame_length,
        }
    }

    /// The underlying term buffer; write payload bytes at `offset()..offset() + length()`.
    pub fn buffer(&self) -> &AtomicBuffer<'static> {
        &self.buffer
    }

    /// Absolute offset in [`BufferClaim::buffer`] at which the claimed payload region begins.
    pub fn offset(&self) -> usize {
        self.frame_offset + HEADER_LENGTH as usize
    }

    /// Length of the claimed payload region in bytes (excludes the frame header).
    pub fn length(&self) -> usize {
        (self.frame_length - HEADER_LENGTH) as usize
    }

    /// The value of the header type field (lower 16 bits valid). See [`protocol::HDR_TYPE_PAD`].
    pub fn header_type(&self) -> u16 {
        self.buffer
            .get_i16(self.frame_offset + protocol::TYPE_FIELD_OFFSET) as u16
    }

    /// Set the value of the header type field.
    pub fn set_header_type(&self, header_type: u16) -> &Self {
        set_frame_type(&self.buffer, self.frame_offset, header_type);
        self
    }

    /// The value of the header flags field.
    pub fn flags(&self) -> u8 {
        self.buffer
            .get_u8(self.frame_offset + protocol::FLAGS_FIELD_OFFSET)
    }

    /// Set the value of the header flags field.
    pub fn set_flags(&self, flags: u8) -> &Self {
        set_frame_flags(&self.buffer, self.frame_offset, flags);
        self
    }

    /// The value stored in the reserved space at the end of the data frame header.
    pub fn reserved_value(&self) -> i64 {
        self.buffer
            .get_i64(self.frame_offset + protocol::DATA_RESERVED_VALUE_OFFSET)
    }

    /// Write a value into the reserved space at the end of the data frame header.
    pub fn set_reserved_value(&self, value: i64) -> &Self {
        set_frame_reserved_value(&self.buffer, self.frame_offset, value);
        self
    }

    /// Copy bytes into the claimed buffer space for a message.
    ///
    /// To write multiple parts, use [`BufferClaim::buffer`] and [`BufferClaim::offset`] directly.
    pub fn put_bytes(&self, src_buffer: &AtomicBuffer, src_offset: usize, length: usize) -> &Self {
        self.buffer
            .put_bytes_from(self.offset(), src_buffer, src_offset, length);
        self
    }

    /// Commit the message to the log buffer so it is available to subscribers.
    pub fn commit(&self) {
        set_frame_length_ordered(&self.buffer, self.frame_offset, self.frame_length);
    }

    /// Abort the claim so the log can progress by ignoring this claim (marks it as padding).
    pub fn abort(&self) {
        set_frame_type(&self.buffer, self.frame_offset, protocol::HDR_TYPE_PAD);
        set_frame_length_ordered(&self.buffer, self.frame_offset, self.frame_length);
    }
}
