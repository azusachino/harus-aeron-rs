//! Property-based tests for Aeron client logbuffer and protocol primitives.

use aeron_client::command::{PublicationMessageFlyweight, SubscriptionMessageFlyweight};
use aeron_client::driver_events::PublicationBuffersReadyFlyweight;
use aeron_client::logbuffer::{
    FRAME_ALIGNMENT, HEADER_LENGTH, TermGapScanner, TermReader, TermScanner, align,
};
use aeron_client::protocol::ErrorFlyweight;
use aeron_client::uri::{ChannelUri, ChannelUriStringBuilder};
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

    #[test]
    fn prop_channel_uri_builder_roundtrip(
        media in prop::sample::select(vec!["ipc", "udp"]),
        endpoint in "[a-z]{1,12}:[0-9]{1,5}",
        alias in "[a-zA-Z0-9_-]{1,16}",
        stream_id in any::<i32>(),
    ) {
        let mut builder = ChannelUriStringBuilder::new();
        builder.media(media).endpoint(endpoint).alias(alias).session_id(stream_id as i64);
        let uri = builder.build();
        let parsed = ChannelUri::parse(&uri).unwrap();
        let rebuilt = parsed.to_string();

        prop_assert_eq!(ChannelUri::parse(&rebuilt).unwrap(), parsed);
    }

    #[test]
    fn prop_publication_command_roundtrip(
        client_id in any::<i64>(),
        correlation_id in any::<i64>(),
        stream_id in any::<i32>(),
        channel in "[a-zA-Z0-9:/?=_-]{0,128}",
    ) {
        let mut bytes = vec![0u8; PublicationMessageFlyweight::compute_length(channel.len())];
        let buffer = AtomicBuffer::new(&mut bytes);
        let command = PublicationMessageFlyweight::new(buffer, 0);
        command.set_client_id(client_id);
        command.set_correlation_id(correlation_id);
        command.set_stream_id(stream_id);
        command.set_channel(&channel);

        prop_assert_eq!(command.client_id(), client_id);
        prop_assert_eq!(command.correlation_id(), correlation_id);
        prop_assert_eq!(command.stream_id(), stream_id);
        prop_assert_eq!(command.channel(), channel);
        prop_assert_eq!(command.length(), bytes.len());
    }

    #[test]
    fn prop_subscription_command_roundtrip(
        client_id in any::<i64>(),
        correlation_id in any::<i64>(),
        registration_correlation_id in any::<i64>(),
        stream_id in any::<i32>(),
        channel in "[a-zA-Z0-9:/?=_-]{0,128}",
    ) {
        let mut bytes = vec![0u8; SubscriptionMessageFlyweight::compute_length(channel.len())];
        let buffer = AtomicBuffer::new(&mut bytes);
        let command = SubscriptionMessageFlyweight::new(buffer, 0);
        command.set_client_id(client_id);
        command.set_correlation_id(correlation_id);
        command.set_registration_correlation_id(registration_correlation_id);
        command.set_stream_id(stream_id);
        command.set_channel(&channel);

        prop_assert_eq!(command.client_id(), client_id);
        prop_assert_eq!(command.correlation_id(), correlation_id);
        prop_assert_eq!(command.registration_correlation_id(), registration_correlation_id);
        prop_assert_eq!(command.stream_id(), stream_id);
        prop_assert_eq!(command.channel(), channel);
        prop_assert_eq!(command.length(), bytes.len());
    }

    #[test]
    fn prop_publication_ready_response_roundtrip(
        correlation_id in any::<i64>(),
        registration_id in any::<i64>(),
        session_id in any::<i32>(),
        stream_id in any::<i32>(),
        log_file_name in "[a-zA-Z0-9_./-]{1,64}",
    ) {
        let mut bytes = vec![0u8; 128];
        let buffer = AtomicBuffer::new(&mut bytes);
        let response = PublicationBuffersReadyFlyweight::wrap(buffer, 0);
        response.set_correlation_id(correlation_id);
        response.set_registration_id(registration_id);
        response.set_session_id(session_id);
        response.set_stream_id(stream_id);
        response.set_publication_limit_counter_id(17);
        response.set_channel_status_counter_id(18);
        response.set_log_file_name(&log_file_name);

        prop_assert_eq!(response.correlation_id(), correlation_id);
        prop_assert_eq!(response.registration_id(), registration_id);
        prop_assert_eq!(response.session_id(), session_id);
        prop_assert_eq!(response.stream_id(), stream_id);
        prop_assert_eq!(response.log_file_name().unwrap(), log_file_name);
    }
}
