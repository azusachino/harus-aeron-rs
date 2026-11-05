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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logbuffer::descriptor::{LOG_DEFAULT_FRAME_HEADER_OFFSET, LOG_META_DATA_LENGTH};
    use crate::logbuffer::frame_descriptor::{FRAME_ALIGNMENT, align};
    use crate::protocol;
    use agrona::AtomicBuffer;

    #[test]
    fn should_insert_into_empty_buffer() {
        let term_offset = 0;
        let packet_length = 96;
        let mut packet_bytes = vec![0u8; packet_length];
        let packet = AtomicBuffer::new(&mut packet_bytes);
        packet.put_i32(0, packet_length as i32);
        packet.put_i16(protocol::TYPE_FIELD_OFFSET, protocol::HDR_TYPE_DATA as i16);
        packet.put_i32(protocol::DATA_SESSION_ID_FIELD_OFFSET, 100);
        packet.put_i32(protocol::DATA_STREAM_ID_FIELD_OFFSET, 200);

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);

        TermRebuilder::insert(&term_buffer, term_offset, &packet, packet_length);

        assert_eq!(term_buffer.get_i32(term_offset), packet_length as i32);
        assert_eq!(
            term_buffer.get_i16(term_offset + protocol::TYPE_FIELD_OFFSET),
            protocol::HDR_TYPE_DATA as i16
        );
        assert_eq!(
            term_buffer.get_i32(term_offset + protocol::DATA_SESSION_ID_FIELD_OFFSET),
            100
        );
        assert_eq!(
            term_buffer.get_i32(term_offset + protocol::DATA_STREAM_ID_FIELD_OFFSET),
            200
        );
    }

    #[test]
    fn should_not_insert_if_already_inserted() {
        let term_offset = 0;
        let packet_length = 96;
        let mut packet_bytes = vec![0u8; packet_length];
        let packet = AtomicBuffer::new(&mut packet_bytes);
        packet.put_i32(0, packet_length as i32);
        packet.put_i32(protocol::DATA_SESSION_ID_FIELD_OFFSET, 100);

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);
        term_buffer.put_i32(term_offset, 64);
        term_buffer.put_i32(term_offset + protocol::DATA_SESSION_ID_FIELD_OFFSET, 42);

        TermRebuilder::insert(&term_buffer, term_offset, &packet, packet_length);

        assert_eq!(term_buffer.get_i32(term_offset), 64);
        assert_eq!(
            term_buffer.get_i32(term_offset + protocol::DATA_SESSION_ID_FIELD_OFFSET),
            42
        );
    }

    #[test]
    fn should_fill_gap_at_beginning_of_buffer() {
        let mut meta_bytes = vec![0u8; LOG_META_DATA_LENGTH];
        let meta_buffer = AtomicBuffer::new(&mut meta_bytes);
        meta_buffer.put_i32(
            LOG_DEFAULT_FRAME_HEADER_OFFSET + protocol::DATA_SESSION_ID_FIELD_OFFSET,
            1234,
        );

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);

        let term_id = 7;
        let gap_offset = 0;
        let gap_length = 64;

        let filled = TermGapFiller::try_fill_gap(
            &meta_buffer,
            &term_buffer,
            term_id,
            gap_offset,
            gap_length,
        );

        assert!(filled);
        assert_eq!(term_buffer.get_i32(gap_offset), gap_length as i32);
        assert_eq!(
            term_buffer.get_i16(gap_offset + protocol::TYPE_FIELD_OFFSET),
            protocol::HDR_TYPE_PAD as i16
        );
        assert_eq!(
            term_buffer.get_i32(gap_offset + protocol::DATA_TERM_OFFSET_FIELD_OFFSET),
            gap_offset as i32
        );
        assert_eq!(
            term_buffer.get_i32(gap_offset + protocol::DATA_TERM_ID_FIELD_OFFSET),
            term_id
        );
        assert_eq!(
            term_buffer.get_i32(gap_offset + protocol::DATA_SESSION_ID_FIELD_OFFSET),
            1234
        );
    }

    #[test]
    fn should_not_fill_gap_when_data_present() {
        let mut meta_bytes = vec![0u8; LOG_META_DATA_LENGTH];
        let meta_buffer = AtomicBuffer::new(&mut meta_bytes);

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);
        term_buffer.put_i32(32, 64);

        let term_id = 7;
        let gap_offset = 0;
        let gap_length = 64;

        let filled = TermGapFiller::try_fill_gap(
            &meta_buffer,
            &term_buffer,
            term_id,
            gap_offset,
            gap_length,
        );

        assert!(!filled);
    }

    #[test]
    fn should_scan_empty_buffer() {
        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);
        let new_offset = TermBlockScanner::scan(&term_buffer, 0, 4096);
        assert_eq!(new_offset, 0);
    }

    #[test]
    fn should_read_first_message_in_block_scan() {
        let msg_length = 50;
        let aligned_length = align(msg_length, FRAME_ALIGNMENT) as usize;

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);
        term_buffer.put_i32_volatile(0, msg_length);
        term_buffer.put_i16(protocol::TYPE_FIELD_OFFSET, protocol::HDR_TYPE_DATA as i16);

        let new_offset = TermBlockScanner::scan(&term_buffer, 0, 4096);
        assert_eq!(new_offset, aligned_length);
    }
}
