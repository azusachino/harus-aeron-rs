//! Property-based tests for Aeron client logbuffer and protocol primitives.

use aeron_client::logbuffer::{
    FRAME_ALIGNMENT, HEADER_LENGTH, TermGapScanner, TermReader, TermScanner, align,
};
use aeron_client::protocol::ErrorFlyweight;
use agrona::AtomicBuffer;
use proptest::prelude::*;

proptest! {
    // 1. TermScanner pack/available/padding roundtrip for positive available bytes
    #[test]
    fn prop_term_scanner_pack_roundtrip(
        padding in 0i32..100000i32,
        available in 0i32..100000i32,
    ) {
        let packed = TermScanner::pack(padding, available);
        prop_assert_eq!(TermScanner::padding(packed), padding);
        prop_assert_eq!(TermScanner::available(packed), available);
    }

    // 2. TermReader pack/offset/fragments_read roundtrip
    #[test]
    fn prop_term_reader_pack_roundtrip(
        offset in any::<i32>(),
        fragments_read in any::<i32>(),
    ) {
        let packed = TermReader::pack(offset, fragments_read);
        prop_assert_eq!(TermReader::offset(packed), offset);
        prop_assert_eq!(TermReader::fragments_read(packed), fragments_read);
    }

    // 3. TermGapScanner scan_for_gap property
    #[test]
    fn prop_term_gap_scanner(
        num_frames in 1usize..10usize,
        frame_payload_len in 1i32..100i32,
        gap_frames in 1usize..5usize,
    ) {
        let frame_len = HEADER_LENGTH + frame_payload_len;
        let aligned_frame_len = align(frame_len, FRAME_ALIGNMENT) as usize;

        let written_bytes = num_frames * aligned_frame_len;
        let gap_bytes = gap_frames * (HEADER_LENGTH as usize);
        let total_cap = written_bytes + gap_bytes + (HEADER_LENGTH as usize) * 2;

        let mut bytes = vec![0u8; total_cap];
        let term_buffer = AtomicBuffer::new(&mut bytes);

        let mut offset = 0;
        for _ in 0..num_frames {
            term_buffer.put_i32_volatile(offset, frame_len);
            offset += aligned_frame_len;
        }

        let gap_start = offset;
        let limit_offset = gap_start + gap_bytes;

        let mut gaps = Vec::new();
        let result = TermGapScanner::scan_for_gap(
            &term_buffer,
            42,
            0,
            limit_offset,
            |term_id, gap_begin, length| {
                gaps.push((term_id, gap_begin, length));
            },
        );

        prop_assert_eq!(result, gap_start);
        prop_assert_eq!(gaps, vec![(42, gap_start, gap_bytes)]);
    }

    // 4. ErrorFlyweight error_message roundtrip
    #[test]
    fn prop_error_flyweight_message_roundtrip(
        msg in "[a-zA-Z0-9 _-]{1,200}",
    ) {
        let mut bytes = vec![0u8; 1024];
        let buf = AtomicBuffer::new(&mut bytes);
        let flyweight = ErrorFlyweight::new(buf);

        flyweight.set_error_message(&msg);
        prop_assert_eq!(flyweight.error_message(), msg);
    }
}
