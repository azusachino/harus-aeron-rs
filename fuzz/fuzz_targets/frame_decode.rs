#![no_main]

use aeron_client::protocol::{
    DATA_HEADER_LENGTH, DataHeaderFlyweight, ERROR_HEADER_LENGTH, ErrorFlyweight, HeaderFlyweight,
    MIN_HEADER_LENGTH, NAK_HEADER_LENGTH, NakFlyweight, RTTM_HEADER_LENGTH,
    RttMeasurementFlyweight, SETUP_HEADER_LENGTH, STATUS_MESSAGE_HEADER_LENGTH, SetupFlyweight,
    StatusMessageFlyweight,
};
use agrona::AtomicBuffer;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < MIN_HEADER_LENGTH {
        return;
    }

    let mut bytes = data.to_vec();
    let buffer = AtomicBuffer::new(&mut bytes);
    let header = HeaderFlyweight::new(buffer.clone());
    let _ = (
        header.frame_length(),
        header.version(),
        header.flags(),
        header.header_type(),
    );

    if data.len() >= DATA_HEADER_LENGTH {
        let frame = DataHeaderFlyweight::new(buffer.clone());
        let _ = (
            frame.frame_length(),
            frame.term_offset(),
            frame.session_id(),
            frame.stream_id(),
            frame.term_id(),
            frame.reserved_value(),
        );
    }

    if data.len() >= ERROR_HEADER_LENGTH {
        let frame = ErrorFlyweight::new(buffer.clone());
        let _ = (
            frame.frame_length(),
            frame.session_id(),
            frame.stream_id(),
            frame.receiver_id(),
            frame.group_tag(),
            frame.error_code(),
        );
    }

    if data.len() >= STATUS_MESSAGE_HEADER_LENGTH {
        let frame = StatusMessageFlyweight::new(buffer.clone());
        let _ = (
            frame.session_id(),
            frame.stream_id(),
            frame.consumption_term_id(),
            frame.consumption_term_offset(),
            frame.receiver_window_length(),
            frame.receiver_id(),
        );
    }

    if data.len() >= NAK_HEADER_LENGTH {
        let frame = NakFlyweight::new(buffer.clone());
        let _ = (
            frame.session_id(),
            frame.stream_id(),
            frame.term_id(),
            frame.term_offset(),
            frame.length(),
        );
    }

    if data.len() >= SETUP_HEADER_LENGTH {
        let frame = SetupFlyweight::new(buffer.clone());
        let _ = (
            frame.term_offset(),
            frame.session_id(),
            frame.stream_id(),
            frame.initial_term_id(),
            frame.active_term_id(),
            frame.term_length(),
            frame.mtu_length(),
            frame.ttl(),
        );
    }

    if data.len() >= RTTM_HEADER_LENGTH {
        let frame = RttMeasurementFlyweight::new(buffer);
        let _ = (
            frame.session_id(),
            frame.stream_id(),
            frame.echo_timestamp(),
            frame.reception_delta(),
            frame.receiver_id(),
        );
    }
});
