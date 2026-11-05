use super::descriptor::*;
use super::frame_descriptor::*;
use crate::protocol;
use agrona::AtomicBuffer;

/// Rebuild a term buffer from received frames which can be out-of-order.
#[derive(Debug, Copy, Clone)]
pub struct TermRebuilder;

impl TermRebuilder {
    /// Insert a packet of frames into the log at `term_offset`.
    ///
    /// If the packet has already been inserted, this is a no-op.
    pub fn insert(
        term_buffer: &AtomicBuffer,
        term_offset: usize,
        packet: &AtomicBuffer,
        length: usize,
    ) {
        if term_buffer.get_i32(term_offset) == 0 {
            if length > HEADER_LENGTH as usize {
                term_buffer.put_bytes_from(
                    term_offset + HEADER_LENGTH as usize,
                    packet,
                    HEADER_LENGTH as usize,
                    length - HEADER_LENGTH as usize,
                );
            }

            term_buffer.put_i64(term_offset + 24, packet.get_i64(24));
            term_buffer.put_i64(term_offset + 16, packet.get_i64(16));
            term_buffer.put_i64(term_offset + 8, packet.get_i64(8));

            term_buffer.put_i64_ordered(term_offset, packet.get_i64(0));
        }
    }
}

/// Fills a gap in a term with a padding record.
#[derive(Debug, Copy, Clone)]
pub struct TermGapFiller;

impl TermGapFiller {
    /// Try to gap fill the current term at `gap_offset` if the gap contains no data.
    pub fn try_fill_gap(
        log_metadata_buffer: &AtomicBuffer,
        term_buffer: &AtomicBuffer,
        term_id: i32,
        gap_offset: usize,
        gap_length: usize,
    ) -> bool {
        let mut offset = (gap_offset + gap_length) as isize - FRAME_ALIGNMENT as isize;
        let target = gap_offset as isize;

        while offset >= target {
            if term_buffer.get_i32(offset as usize) != 0 {
                return false;
            }
            offset -= FRAME_ALIGNMENT as isize;
        }

        apply_default_header(log_metadata_buffer, term_buffer, gap_offset);
        set_frame_type(term_buffer, gap_offset, protocol::HDR_TYPE_PAD);
        set_frame_term_offset(term_buffer, gap_offset);
        set_frame_term_id(term_buffer, gap_offset, term_id);
        set_frame_length_ordered(term_buffer, gap_offset, gap_length as i32);

        true
    }
}

/// Scan a term buffer for a block of message fragments including padding.
#[derive(Debug, Copy, Clone)]
pub struct TermBlockScanner;

impl TermBlockScanner {
    /// Scan a term buffer for a block of message fragments from `term_offset` up to `limit_offset`.
    pub fn scan(term_buffer: &AtomicBuffer, term_offset: usize, limit_offset: usize) -> usize {
        let mut offset = term_offset;

        while offset < limit_offset {
            let frame_length = frame_length_volatile(term_buffer, offset);
            if frame_length <= 0 {
                break;
            }

            let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT) as usize;

            if is_padding_frame(term_buffer, offset) {
                if term_offset == offset {
                    offset += aligned_frame_length;
                }
                break;
            }

            if offset + aligned_frame_length > limit_offset {
                break;
            }

            offset += aligned_frame_length;
        }

        offset
    }
}
