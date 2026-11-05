use super::*;
use crate::protocol::{self, DataHeaderFlyweight};
use agrona::AtomicBuffer;
use std::sync::Arc;

#[test]
fn rotate_log_should_cas_active_term_count_even_when_term_id_does_not_match() {
    let mut bytes = vec![0u8; LOG_META_DATA_LENGTH];
    let metadata_buffer = AtomicBuffer::new(&mut bytes);

    let term_id = 5;
    let term_count = 1;
    set_raw_tail(&metadata_buffer, 2, pack_tail(term_id, 1024));
    set_raw_tail(&metadata_buffer, 0, pack_tail(term_id + 1, 2048));
    set_raw_tail(&metadata_buffer, 1, pack_tail(term_id + 2, 4096));
    set_active_term_count(&metadata_buffer, term_count);

    assert!(rotate_log(&metadata_buffer, term_count, term_id));

    assert_eq!(raw_tail(&metadata_buffer, 2), pack_tail(term_id, 1024));
    assert_eq!(raw_tail(&metadata_buffer, 0), pack_tail(term_id + 1, 2048));
    assert_eq!(raw_tail(&metadata_buffer, 1), pack_tail(term_id + 2, 4096));
    assert_eq!(active_term_count(&metadata_buffer), term_count + 1);
}

#[test]
fn rotate_log_should_cas_active_term_count_after_setting_tail_for_the_next_term() {
    let mut bytes = vec![0u8; LOG_META_DATA_LENGTH];
    let metadata_buffer = AtomicBuffer::new(&mut bytes);

    let term_id = 51;
    let term_count = 19;
    set_raw_tail(&metadata_buffer, 1, pack_tail(term_id, 1024));
    set_raw_tail(
        &metadata_buffer,
        2,
        pack_tail(term_id + 1 - PARTITION_COUNT, 2048),
    );
    set_raw_tail(
        &metadata_buffer,
        0,
        pack_tail(term_id + 2 - PARTITION_COUNT, 4096),
    );
    set_active_term_count(&metadata_buffer, term_count);

    assert!(rotate_log(&metadata_buffer, term_count, term_id));

    assert_eq!(raw_tail(&metadata_buffer, 1), pack_tail(term_id, 1024));
    assert_eq!(raw_tail(&metadata_buffer, 2), pack_tail(term_id + 1, 0));
    assert_eq!(
        raw_tail(&metadata_buffer, 0),
        pack_tail(term_id + 2 - PARTITION_COUNT, 4096)
    );
    assert_eq!(active_term_count(&metadata_buffer), term_count + 1);
}

#[test]
fn rotate_log_is_a_no_op_if_neither_tail_nor_active_term_count_can_be_changed() {
    let mut bytes = vec![0u8; LOG_META_DATA_LENGTH];
    let metadata_buffer = AtomicBuffer::new(&mut bytes);

    let term_id = 23;
    let term_count = 42;
    set_raw_tail(&metadata_buffer, 0, pack_tail(term_id, 1024));
    set_raw_tail(&metadata_buffer, 1, pack_tail(term_id + 18, 2048));
    set_raw_tail(&metadata_buffer, 2, pack_tail(term_id - 19, 4096));
    set_active_term_count(&metadata_buffer, term_count);

    assert!(!rotate_log(&metadata_buffer, 3, term_id));

    assert_eq!(raw_tail(&metadata_buffer, 0), pack_tail(term_id, 1024));
    assert_eq!(raw_tail(&metadata_buffer, 1), pack_tail(term_id + 18, 2048));
    assert_eq!(raw_tail(&metadata_buffer, 2), pack_tail(term_id - 19, 4096));
    assert_eq!(active_term_count(&metadata_buffer), term_count);
}

#[test]
fn should_compute_fragmented_frame_length() {
    let test_cases = [
        (0, 1376, 0),
        (10, 1024, 64),
        (2048, 2048, 2080),
        (4096, 1024, 4224),
        (7997, 992, 8288),
    ];

    for (length, max_payload_size, expected_frame_length) in test_cases {
        assert_eq!(
            compute_fragmented_frame_length(length, max_payload_size),
            expected_frame_length,
            "failed for length={}, max_payload_size={}",
            length,
            max_payload_size
        );
    }
}

#[test]
fn header_constructor_initialized_data() {
    let initial_term_id = 18;
    let context: Arc<dyn std::any::Any + Send + Sync> = Arc::new(999i64);
    let position_bits_to_shift = 2;
    let header = Header::with_context(initial_term_id, position_bits_to_shift, context.clone());

    assert_eq!(header.initial_term_id(), initial_term_id);
    assert_eq!(header.position_bits_to_shift(), position_bits_to_shift);
    assert!(header.context().is_some());
}

#[test]
fn header_position_calculation_the_end_of_the_message_in_the_log() {
    let cases = [
        (0, 2, 100, 1024, 5, 1172i64),
        (42, 16, 13, 4096, 46, 266272i64),
        (1, 30, 1024, 1073741824, 111, 119185343488i64),
    ];

    for (
        initial_term_id,
        position_bits_to_shift,
        frame_length,
        term_offset,
        term_id,
        expected_position,
    ) in cases
    {
        let mut bytes = vec![0u8; 128];
        let buf = AtomicBuffer::new(&mut bytes);
        let flyweight = DataHeaderFlyweight::new(buf.clone());
        flyweight.set_frame_length(frame_length);
        flyweight.set_term_id(term_id);
        flyweight.set_term_offset(term_offset);

        let mut header = Header::new(initial_term_id, position_bits_to_shift);
        header.set_buffer(buf);
        header.set_offset(0);

        assert_eq!(header.position(), expected_position);
        assert_eq!(header.fragmented_frame_length(), None);
    }
}

#[test]
fn header_offset_is_relative_to_the_buffer_start() {
    let mut header = Header::new(42, 3);
    assert_eq!(header.offset(), 0);

    header.set_offset(142);
    assert_eq!(header.offset(), 142);
}

#[test]
fn header_should_read_data_from_the_buffer() {
    let cases = [
        (
            103,
            0x3u8,
            0x1Au8,
            0x6u16,
            2080,
            -46234,
            333,
            5,
            909090909090909i64,
        ),
        (
            512,
            0x1u8,
            0xCu8,
            0x1u16,
            1073741824,
            42,
            -876,
            1543,
            -4632842384627834687i64,
        ),
    ];

    for (
        frame_length,
        version,
        flags,
        header_type,
        term_offset,
        session_id,
        stream_id,
        term_id,
        reserved_value,
    ) in cases
    {
        let mut bytes = vec![0u8; 100];
        let offset = 16;
        let flyweight_buf = AtomicBuffer::new(&mut bytes[offset..offset + 64]);
        let flyweight = DataHeaderFlyweight::new(flyweight_buf);

        flyweight
            .set_frame_length(frame_length)
            .set_version(version)
            .set_flags(flags)
            .set_header_type(header_type)
            .set_term_offset(term_offset)
            .set_session_id(session_id)
            .set_stream_id(stream_id)
            .set_term_id(term_id)
            .set_reserved_value(reserved_value);

        let mut header = Header::new(5, 22);
        let full_buf = AtomicBuffer::new(&mut bytes);
        header.set_buffer(full_buf);
        header.set_offset(offset);

        assert_eq!(header.frame_length(), frame_length);
        assert_eq!(header.flags(), flags);
        assert_eq!(header.type_(), header_type);
        assert_eq!(header.term_offset(), term_offset);
        assert_eq!(header.session_id(), session_id);
        assert_eq!(header.stream_id(), stream_id);
        assert_eq!(header.term_id(), term_id);
        assert_eq!(header.reserved_value(), reserved_value);
    }
}

#[test]
fn header_should_override_initial_term_id() {
    let initial_term_id = -178;
    let new_initial_term_id = 871;
    let mut header = Header::new(initial_term_id, 3);
    assert_eq!(header.initial_term_id(), initial_term_id);

    header.set_initial_term_id(new_initial_term_id);
    assert_eq!(header.initial_term_id(), new_initial_term_id);
}

#[test]
fn header_should_override_position_bits_to_shift() {
    let position_bits_to_shift = -6;
    let new_position_bits_to_shift = 20;
    let mut header = Header::new(42, position_bits_to_shift);
    assert_eq!(header.position_bits_to_shift(), position_bits_to_shift);

    header.set_position_bits_to_shift(new_position_bits_to_shift);
    assert_eq!(header.position_bits_to_shift(), new_position_bits_to_shift);
}

#[test]
fn should_encode_header_using_little_endian_byte_order() {
    let cases = [
        (100, 8u8, 5u8, 9i16, 352, -777, -1000, -33),
        (-99, -2i8 as u8, 7u8, 1i16, 8, 42, 3, 89),
        (123, 0u8, 0u8, 0i16, 0, 0, 0, 0),
        (
            32,
            1u8,
            -128i8 as u8,
            4i16,
            96,
            2147483647,
            -2147483648,
            -2147483648,
        ),
    ];

    for (frame_length, version, flags, header_type, term_offset, session_id, stream_id, term_id) in
        cases
    {
        let mut default_bytes = [0xFFu8; 32];
        let default_buffer = AtomicBuffer::new(&mut default_bytes);

        default_buffer.put_u8(protocol::VERSION_FIELD_OFFSET, version);
        default_buffer.put_u8(protocol::FLAGS_FIELD_OFFSET, flags);
        default_buffer.put_i16(protocol::TYPE_FIELD_OFFSET, header_type);
        default_buffer.put_i32(protocol::DATA_SESSION_ID_FIELD_OFFSET, session_id);
        default_buffer.put_i32(protocol::DATA_STREAM_ID_FIELD_OFFSET, stream_id);

        let mut term_bytes = [0xFFu8; 1024];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);

        let header_writer = HeaderWriter::new(&default_buffer);
        header_writer.write(&term_buffer, term_offset as usize, frame_length, term_id);

        let offset = term_offset as usize;
        assert_eq!(
            term_buffer.get_i32(offset + protocol::FRAME_LENGTH_FIELD_OFFSET),
            -frame_length
        );
        assert_eq!(
            term_buffer.get_u8(offset + protocol::VERSION_FIELD_OFFSET),
            version
        );
        assert_eq!(
            term_buffer.get_u8(offset + protocol::FLAGS_FIELD_OFFSET),
            flags
        );
        assert_eq!(
            term_buffer.get_i16(offset + protocol::TYPE_FIELD_OFFSET),
            header_type
        );
        assert_eq!(
            term_buffer.get_i32(offset + protocol::DATA_TERM_OFFSET_FIELD_OFFSET),
            term_offset
        );
        assert_eq!(
            term_buffer.get_i32(offset + protocol::DATA_SESSION_ID_FIELD_OFFSET),
            session_id
        );
        assert_eq!(
            term_buffer.get_i32(offset + protocol::DATA_STREAM_ID_FIELD_OFFSET),
            stream_id
        );
        assert_eq!(
            term_buffer.get_i32(offset + protocol::DATA_TERM_ID_FIELD_OFFSET),
            term_id
        );
    }
}

#[test]
fn should_pack_padding_and_offset_into_resulting_status() {
    let padding = 77;
    let available = 65000;

    let scan_outcome = TermScanner::pack(padding, available);

    assert_eq!(TermScanner::padding(scan_outcome), padding);
    assert_eq!(TermScanner::available(scan_outcome), available);
}

#[test]
fn should_return_zero_on_empty_log() {
    let mut bytes = vec![0u8; LOG_META_DATA_LENGTH];
    let term_buffer = AtomicBuffer::new(&mut bytes);

    let scan_outcome = TermScanner::scan_for_availability(&term_buffer, 0, 1024);
    assert_eq!(TermScanner::available(scan_outcome), 0);
    assert_eq!(TermScanner::padding(scan_outcome), 0);
}

#[test]
fn should_scan_single_message() {
    let msg_length = 1;
    let frame_length = HEADER_LENGTH + msg_length;
    let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);
    let frame_offset = 0;

    let mut bytes = vec![0u8; 1024];
    let term_buffer = AtomicBuffer::new(&mut bytes);
    term_buffer.put_i32_volatile(frame_offset, frame_length);
    term_buffer.put_i16(
        frame_offset + protocol::TYPE_FIELD_OFFSET,
        protocol::HDR_TYPE_DATA as i16,
    );

    let scan_outcome = TermScanner::scan_for_availability(&term_buffer, frame_offset, 1024);
    assert_eq!(TermScanner::available(scan_outcome), aligned_frame_length);
    assert_eq!(TermScanner::padding(scan_outcome), 0);
}

#[test]
fn should_fail_to_scan_message_larger_than_max_length() {
    let msg_length = 1;
    let frame_length = HEADER_LENGTH + msg_length;
    let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);
    let max_length = (aligned_frame_length - 1) as usize;
    let frame_offset = 0;

    let mut bytes = vec![0u8; 1024];
    let term_buffer = AtomicBuffer::new(&mut bytes);
    term_buffer.put_i32_volatile(frame_offset, frame_length);
    term_buffer.put_i16(
        frame_offset + protocol::TYPE_FIELD_OFFSET,
        protocol::HDR_TYPE_DATA as i16,
    );

    let scan_outcome = TermScanner::scan_for_availability(&term_buffer, frame_offset, max_length);
    assert_eq!(TermScanner::available(scan_outcome), -aligned_frame_length);
    assert_eq!(TermScanner::padding(scan_outcome), -1);
}

#[test]
fn should_scan_two_messages_that_fit_in_single_mtu() {
    let msg_length = 100;
    let frame_length = HEADER_LENGTH + msg_length;
    let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);
    let frame_offset = 0;

    let mut bytes = vec![0u8; 1024];
    let term_buffer = AtomicBuffer::new(&mut bytes);
    term_buffer.put_i32_volatile(frame_offset, frame_length);
    term_buffer.put_i16(
        frame_offset + protocol::TYPE_FIELD_OFFSET,
        protocol::HDR_TYPE_DATA as i16,
    );

    let second_offset = frame_offset + aligned_frame_length as usize;
    term_buffer.put_i32_volatile(second_offset, aligned_frame_length);
    term_buffer.put_i16(
        second_offset + protocol::TYPE_FIELD_OFFSET,
        protocol::HDR_TYPE_DATA as i16,
    );

    let scan_outcome = TermScanner::scan_for_availability(&term_buffer, frame_offset, 1024);
    assert_eq!(
        TermScanner::available(scan_outcome),
        aligned_frame_length * 2
    );
    assert_eq!(TermScanner::padding(scan_outcome), 0);
}

#[test]
fn should_scan_last_message_in_buffer_plus_padding() {
    let aligned_frame_length = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
    let padding_frame_length = align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);
    let term_capacity = 4096;
    let frame_offset = term_capacity - (aligned_frame_length + padding_frame_length) as usize;

    let mut bytes = vec![0u8; term_capacity];
    let term_buffer = AtomicBuffer::new(&mut bytes);
    term_buffer.put_i32_volatile(frame_offset, aligned_frame_length);
    term_buffer.put_i16(
        frame_offset + protocol::TYPE_FIELD_OFFSET,
        protocol::HDR_TYPE_DATA as i16,
    );

    let padding_offset = frame_offset + aligned_frame_length as usize;
    term_buffer.put_i32_volatile(padding_offset, padding_frame_length);
    term_buffer.put_i16(
        padding_offset + protocol::TYPE_FIELD_OFFSET,
        protocol::HDR_TYPE_PAD as i16,
    );

    let scan_outcome = TermScanner::scan_for_availability(&term_buffer, frame_offset, 1024);
    assert_eq!(
        TermScanner::available(scan_outcome),
        aligned_frame_length + HEADER_LENGTH
    );
    assert_eq!(
        TermScanner::padding(scan_outcome),
        padding_frame_length - HEADER_LENGTH
    );
}

#[test]
fn should_report_gap_at_beginning_of_buffer() {
    let frame_offset = align(HEADER_LENGTH * 3, FRAME_ALIGNMENT) as usize;
    let high_water_mark = frame_offset + align(HEADER_LENGTH, FRAME_ALIGNMENT) as usize;

    let mut bytes = vec![0u8; 4096];
    let term_buffer = AtomicBuffer::new(&mut bytes);
    term_buffer.put_i32_volatile(frame_offset, HEADER_LENGTH);

    let mut gaps = Vec::new();
    let result = TermGapScanner::scan_for_gap(
        &term_buffer,
        1,
        0,
        high_water_mark,
        |term_id, offset, length| {
            gaps.push((term_id, offset, length));
        },
    );

    assert_eq!(result, 0);
    assert_eq!(gaps, vec![(1, 0, frame_offset)]);
}

#[test]
fn should_report_single_gap_when_buffer_not_full() {
    let tail = align(HEADER_LENGTH, FRAME_ALIGNMENT) as usize;
    let high_water_mark = (FRAME_ALIGNMENT * 3) as usize;

    let mut bytes = vec![0u8; 4096];
    let term_buffer = AtomicBuffer::new(&mut bytes);
    term_buffer.put_i32_volatile(
        tail - align(HEADER_LENGTH, FRAME_ALIGNMENT) as usize,
        HEADER_LENGTH,
    );
    term_buffer.put_i32_volatile(tail, 0);
    term_buffer.put_i32_volatile(
        high_water_mark - align(HEADER_LENGTH, FRAME_ALIGNMENT) as usize,
        HEADER_LENGTH,
    );

    let mut gaps = Vec::new();
    let result = TermGapScanner::scan_for_gap(
        &term_buffer,
        1,
        tail,
        high_water_mark,
        |term_id, offset, length| {
            gaps.push((term_id, offset, length));
        },
    );

    assert_eq!(result, tail);
    assert_eq!(
        gaps,
        vec![(1, tail, align(HEADER_LENGTH, FRAME_ALIGNMENT) as usize)]
    );
}

#[test]
fn should_report_no_gap_when_hwm_is_in_padding() {
    let padding_length = (align(HEADER_LENGTH, FRAME_ALIGNMENT) * 2) as usize;
    let capacity = 4096;
    let tail = capacity - padding_length;
    let high_water_mark = capacity - padding_length + HEADER_LENGTH as usize;

    let mut bytes = vec![0u8; capacity];
    let term_buffer = AtomicBuffer::new(&mut bytes);
    term_buffer.put_i32_volatile(tail, padding_length as i32);
    term_buffer.put_i32_volatile(tail + HEADER_LENGTH as usize, 0);

    let mut gaps = Vec::new();
    let result = TermGapScanner::scan_for_gap(
        &term_buffer,
        1,
        tail,
        high_water_mark,
        |term_id, offset, length| {
            gaps.push((term_id, offset, length));
        },
    );

    assert_eq!(result, capacity);
    assert!(gaps.is_empty());
}

#[test]
fn should_report_single_header_gap() {
    let offset = 8192 + 384;
    let mut bytes = vec![0u8; 16384];
    let term_buffer = AtomicBuffer::new(&mut bytes);
    term_buffer.put_i32_volatile(offset, 0);
    term_buffer.put_i32_volatile(offset + HEADER_LENGTH as usize, 128);

    let capacity = term_buffer.capacity();
    let mut gaps = Vec::new();
    let result = TermGapScanner::scan_for_gap(
        &term_buffer,
        1,
        offset,
        capacity,
        |term_id, offset, length| {
            gaps.push((term_id, offset, length));
        },
    );

    assert_eq!(result, offset);
    assert_eq!(gaps, vec![(1, offset, HEADER_LENGTH as usize)]);
}

#[test]
fn should_report_gap_at_the_end_of_the_buffer() {
    let capacity = 4096;
    let offset = capacity - 128;
    let mut bytes = vec![0u8; capacity];
    let term_buffer = AtomicBuffer::new(&mut bytes);
    term_buffer.put_i32_volatile(offset, 0);

    let mut gaps = Vec::new();
    let result = TermGapScanner::scan_for_gap(
        &term_buffer,
        1,
        offset,
        capacity,
        |term_id, offset, length| {
            gaps.push((term_id, offset, length));
        },
    );

    assert_eq!(result, offset);
    assert_eq!(gaps, vec![(1, offset, 128)]);
}

#[test]
fn term_reader_should_pack_and_unpack() {
    let offset = 4096;
    let fragments_read = 5;
    let packed = TermReader::pack(offset, fragments_read);

    assert_eq!(TermReader::offset(packed), offset);
    assert_eq!(TermReader::fragments_read(packed), fragments_read);
}

#[test]
fn should_read_first_message() {
    let msg_length = 1;
    let frame_length = HEADER_LENGTH + msg_length;
    let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);
    let term_offset = 0;

    let mut term_bytes = vec![0u8; 4096];
    let term_buffer = AtomicBuffer::new(&mut term_bytes);
    term_buffer.put_i32_volatile(0, frame_length);
    term_buffer.put_i16(protocol::TYPE_FIELD_OFFSET, protocol::HDR_TYPE_DATA as i16);

    let mut counter_values = vec![0u8; 1024];
    let values_buffer = AtomicBuffer::new(&mut counter_values);
    let subscriber_position = agrona::counters::AtomicCounter::new(values_buffer, 0);

    let mut header = Header::new(7, 16);
    let mut fragment_calls = Vec::new();

    let read_outcome = TermReader::read_with_position(
        &term_buffer,
        term_offset,
        |_buf, offset, length, _hdr| {
            fragment_calls.push((offset, length));
        },
        usize::MAX,
        &mut header,
        |_| (),
        0,
        &subscriber_position,
    );

    assert_eq!(read_outcome, 1);
    assert_eq!(fragment_calls, vec![(HEADER_LENGTH as usize, msg_length)]);
    assert_eq!(subscriber_position.get(), aligned_frame_length as i64);
}

#[test]
fn should_not_read_past_tail() {
    let mut term_bytes = vec![0u8; 4096];
    let term_buffer = AtomicBuffer::new(&mut term_bytes);

    let mut counter_values = vec![0u8; 1024];
    let values_buffer = AtomicBuffer::new(&mut counter_values);
    let subscriber_position = agrona::counters::AtomicCounter::new(values_buffer, 0);

    let mut header = Header::new(7, 16);
    let mut fragment_calls = Vec::new();

    let read_outcome = TermReader::read_with_position(
        &term_buffer,
        0,
        |_buf, offset, length, _hdr| {
            fragment_calls.push((offset, length));
        },
        usize::MAX,
        &mut header,
        |_| (),
        0,
        &subscriber_position,
    );

    assert_eq!(read_outcome, 0);
    assert!(fragment_calls.is_empty());
    assert_eq!(subscriber_position.get(), 0);
}

#[test]
fn should_read_one_limited_message() {
    let msg_length = 1;
    let frame_length = HEADER_LENGTH + msg_length;
    let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);

    let mut term_bytes = vec![0u8; 4096];
    let term_buffer = AtomicBuffer::new(&mut term_bytes);
    term_buffer.put_i32_volatile(0, frame_length);
    term_buffer.put_i16(protocol::TYPE_FIELD_OFFSET, protocol::HDR_TYPE_DATA as i16);

    let second_offset = aligned_frame_length as usize;
    term_buffer.put_i32_volatile(second_offset, frame_length);
    term_buffer.put_i16(
        second_offset + protocol::TYPE_FIELD_OFFSET,
        protocol::HDR_TYPE_DATA as i16,
    );

    let mut counter_values = vec![0u8; 1024];
    let values_buffer = AtomicBuffer::new(&mut counter_values);
    let subscriber_position = agrona::counters::AtomicCounter::new(values_buffer, 0);

    let mut header = Header::new(7, 16);
    let mut fragment_calls = Vec::new();

    let read_outcome = TermReader::read_with_position(
        &term_buffer,
        0,
        |_buf, offset, length, _hdr| {
            fragment_calls.push((offset, length));
        },
        1,
        &mut header,
        |_| (),
        0,
        &subscriber_position,
    );

    assert_eq!(read_outcome, 1);
    assert_eq!(fragment_calls.len(), 1);
    assert_eq!(subscriber_position.get(), aligned_frame_length as i64);
}

#[test]
fn should_read_multiple_messages() {
    let msg_length = 1;
    let frame_length = HEADER_LENGTH + msg_length;
    let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);

    let mut term_bytes = vec![0u8; 4096];
    let term_buffer = AtomicBuffer::new(&mut term_bytes);
    term_buffer.put_i32_volatile(0, frame_length);
    term_buffer.put_i16(protocol::TYPE_FIELD_OFFSET, protocol::HDR_TYPE_DATA as i16);

    let second_offset = aligned_frame_length as usize;
    term_buffer.put_i32_volatile(second_offset, frame_length);
    term_buffer.put_i16(
        second_offset + protocol::TYPE_FIELD_OFFSET,
        protocol::HDR_TYPE_DATA as i16,
    );

    let mut counter_values = vec![0u8; 1024];
    let values_buffer = AtomicBuffer::new(&mut counter_values);
    let subscriber_position = agrona::counters::AtomicCounter::new(values_buffer, 0);

    let mut header = Header::new(7, 16);
    let mut fragment_calls = Vec::new();

    let read_outcome = TermReader::read_with_position(
        &term_buffer,
        0,
        |_buf, offset, length, _hdr| {
            fragment_calls.push((offset, length));
        },
        usize::MAX,
        &mut header,
        |_| (),
        0,
        &subscriber_position,
    );

    assert_eq!(read_outcome, 2);
    assert_eq!(fragment_calls.len(), 2);
    assert_eq!(subscriber_position.get(), (aligned_frame_length * 2) as i64);
}

#[test]
fn should_not_read_last_message_when_padding() {
    let msg_length = 1;
    let frame_length = HEADER_LENGTH + msg_length;
    let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);
    let capacity = 4096;
    let frame_offset = capacity - aligned_frame_length as usize;
    let current_position = 1000i64;

    let mut term_bytes = vec![0u8; capacity];
    let term_buffer = AtomicBuffer::new(&mut term_bytes);
    term_buffer.put_i32_volatile(frame_offset, frame_length);
    term_buffer.put_i16(
        frame_offset + protocol::TYPE_FIELD_OFFSET,
        protocol::HDR_TYPE_PAD as i16,
    );

    let mut counter_values = vec![0u8; 1024];
    let values_buffer = AtomicBuffer::new(&mut counter_values);
    let subscriber_position = agrona::counters::AtomicCounter::new(values_buffer, 0);

    let mut header = Header::new(7, 16);
    let mut fragment_calls = Vec::new();

    let read_outcome = TermReader::read_with_position(
        &term_buffer,
        frame_offset,
        |_buf, offset, length, _hdr| {
            fragment_calls.push((offset, length));
        },
        usize::MAX,
        &mut header,
        |_| (),
        current_position,
        &subscriber_position,
    );

    assert_eq!(read_outcome, 0);
    assert!(fragment_calls.is_empty());
    assert_eq!(
        subscriber_position.get(),
        current_position + aligned_frame_length as i64
    );
}

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

    let filled =
        TermGapFiller::try_fill_gap(&meta_buffer, &term_buffer, term_id, gap_offset, gap_length);

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

    let filled =
        TermGapFiller::try_fill_gap(&meta_buffer, &term_buffer, term_id, gap_offset, gap_length);

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

#[test]
fn should_take_no_action_when_message_is_complete() {
    let mut meta_bytes = vec![0u8; LOG_META_DATA_LENGTH];
    let meta_buffer = AtomicBuffer::new(&mut meta_bytes);
    let mut term_bytes = vec![0u8; 4096];
    let term_buffer = AtomicBuffer::new(&mut term_bytes);

    term_buffer.put_i32_volatile(0, HEADER_LENGTH);

    let status = TermUnblocker::unblock(&meta_buffer, &term_buffer, 0, 4096, 7);
    assert_eq!(status, UnblockStatus::NoAction);
}

#[test]
fn should_patch_non_committed_message() {
    let mut meta_bytes = vec![0u8; LOG_META_DATA_LENGTH];
    let meta_buffer = AtomicBuffer::new(&mut meta_bytes);
    let mut term_bytes = vec![0u8; 4096];
    let term_buffer = AtomicBuffer::new(&mut term_bytes);

    let msg_length = HEADER_LENGTH * 4;
    term_buffer.put_i32_volatile(0, -msg_length);

    let status = TermUnblocker::unblock(&meta_buffer, &term_buffer, 0, msg_length as usize, 7);
    assert_eq!(status, UnblockStatus::Unblocked);
    assert_eq!(term_buffer.get_i32(0), msg_length);
    assert_eq!(
        term_buffer.get_i16(protocol::TYPE_FIELD_OFFSET),
        protocol::HDR_TYPE_PAD as i16
    );
}

#[test]
fn should_unblock_log_buffer_when_position_has_non_committed_message() {
    let term_length = 65536;
    let mut meta_bytes = vec![0u8; LOG_META_DATA_LENGTH];
    let meta_buffer = AtomicBuffer::new(&mut meta_bytes);

    let initial_term_id_val = 1;
    set_initial_term_id(&meta_buffer, initial_term_id_val);
    initialise_tail_with_term_id(&meta_buffer, 0, initial_term_id_val);

    let shift = position_bits_to_shift(term_length);
    let blocked_offset = (HEADER_LENGTH * 4) as usize;
    let msg_length = HEADER_LENGTH * 4;
    let blocked_position = compute_position(
        initial_term_id_val,
        blocked_offset as i32,
        shift as i32,
        initial_term_id_val,
    );

    let mut term_0_bytes = vec![0u8; term_length];
    let mut term_1_bytes = vec![0u8; term_length];
    let mut term_2_bytes = vec![0u8; term_length];

    let term_buffer_0 = AtomicBuffer::new(&mut term_0_bytes);
    let term_buffer_1 = AtomicBuffer::new(&mut term_1_bytes);
    let term_buffer_2 = AtomicBuffer::new(&mut term_2_bytes);

    term_buffer_0.put_i32_volatile(blocked_offset, -msg_length);

    let term_buffers = [term_buffer_0, term_buffer_1, term_buffer_2];

    let unblocked =
        LogBufferUnblocker::unblock(&term_buffers, &meta_buffer, blocked_position, term_length);
    assert!(unblocked);

    let raw_tail_val = raw_tail_volatile_active(&meta_buffer);
    assert_eq!(
        blocked_position + msg_length as i64,
        compute_position(
            term_id(raw_tail_val),
            (blocked_offset + msg_length as usize) as i32,
            shift as i32,
            initial_term_id_val,
        )
    );
}

#[test]
fn position_bits_to_shift_accepts_valid_power_of_two_term_lengths() {
    assert_eq!(position_bits_to_shift(64 * 1024), 16);
    assert_eq!(position_bits_to_shift(1024 * 1024), 20);
    assert_eq!(position_bits_to_shift(1024 * 1024 * 1024), 30);
}

#[test]
#[should_panic(expected = "invalid term buffer length")]
fn position_bits_to_shift_rejects_non_power_of_two() {
    position_bits_to_shift(100);
}

#[test]
#[should_panic(expected = "invalid term buffer length")]
fn position_bits_to_shift_rejects_out_of_range_power_of_two() {
    position_bits_to_shift(32 * 1024);
}
