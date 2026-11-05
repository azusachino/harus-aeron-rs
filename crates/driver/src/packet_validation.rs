//! Preflight untrusted UDP datagrams before receiver state or image logs change.

use aeron_client::logbuffer::{FRAME_ALIGNMENT, TERM_MAX_LENGTH, TERM_MIN_LENGTH};
use aeron_client::protocol::{
    CURRENT_VERSION, DATA_HEADER_LENGTH, HDR_TYPE_DATA, HDR_TYPE_PAD, HDR_TYPE_RTTM,
    HDR_TYPE_SETUP, RTTM_HEADER_LENGTH, SETUP_HEADER_LENGTH,
};

fn i32_at(bytes: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn kind(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset + 6], bytes[offset + 7]])
}

/// Validate supported receiver datagrams. `term_length` is the image geometry when known.
pub(crate) fn valid_receive_packet(bytes: &[u8], term_length: i32) -> bool {
    if bytes.len() < 8 || bytes[4] != CURRENT_VERSION {
        return false;
    }
    let length = i32_at(bytes, 0);
    match kind(bytes, 0) {
        HDR_TYPE_DATA | HDR_TYPE_PAD => valid_data_packet(bytes, term_length),
        HDR_TYPE_SETUP => {
            if bytes.len() < SETUP_HEADER_LENGTH
                || length < SETUP_HEADER_LENGTH as i32
                || length as usize > bytes.len()
            {
                return false;
            }
            let offset = i32_at(bytes, 8);
            let term = i32_at(bytes, 28);
            let mtu = i32_at(bytes, 32);
            (TERM_MIN_LENGTH..=TERM_MAX_LENGTH).contains(&term)
                && (term as u32).is_power_of_two()
                && offset >= 0
                && offset < term
                && offset % FRAME_ALIGNMENT == 0
                && (DATA_HEADER_LENGTH as i32..=65504).contains(&mtu)
                && mtu % FRAME_ALIGNMENT == 0
        }
        HDR_TYPE_RTTM => {
            bytes.len() >= RTTM_HEADER_LENGTH
                && length >= RTTM_HEADER_LENGTH as i32
                && length as usize <= bytes.len()
        }
        _ => false,
    }
}

fn valid_data_packet(bytes: &[u8], term_length: i32) -> bool {
    if bytes.len() < DATA_HEADER_LENGTH || bytes.len() % FRAME_ALIGNMENT as usize != 0 {
        return false;
    }
    let initial_offset = i32_at(bytes, 8);
    if initial_offset < 0 || initial_offset >= term_length || initial_offset % FRAME_ALIGNMENT != 0
    {
        return false;
    }
    let session = i32_at(bytes, 12);
    let stream = i32_at(bytes, 16);
    let term = i32_at(bytes, 20);
    if i32_at(bytes, 0) == 0 {
        return bytes.len() == DATA_HEADER_LENGTH && kind(bytes, 0) == HDR_TYPE_DATA;
    }
    let mut offset = 0usize;
    let mut term_offset = initial_offset as i64;
    loop {
        if bytes.len().saturating_sub(offset) < DATA_HEADER_LENGTH {
            return false;
        }
        let frame_len = i32_at(bytes, offset);
        let frame_type = kind(bytes, offset);
        if frame_len < DATA_HEADER_LENGTH as i32
            || !matches!(frame_type, HDR_TYPE_DATA | HDR_TYPE_PAD)
            || bytes[offset + 4] != CURRENT_VERSION
            || i32_at(bytes, offset + 8) as i64 != term_offset
            || i32_at(bytes, offset + 12) != session
            || i32_at(bytes, offset + 16) != stream
            || i32_at(bytes, offset + 20) != term
        {
            return false;
        }
        let aligned = ((frame_len as i64 + 31) & !31) as usize;
        term_offset += aligned as i64;
        if term_offset > term_length as i64 {
            return false;
        }
        let Some(next) = offset.checked_add(aligned) else {
            return false;
        };
        if next >= bytes.len() {
            return next == bytes.len() || frame_type == HDR_TYPE_PAD;
        }
        offset = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn data(length: i32, frame_type: u16, offset: i32) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&length.to_le_bytes());
        bytes[6..8].copy_from_slice(&frame_type.to_le_bytes());
        bytes[8..12].copy_from_slice(&offset.to_le_bytes());
        bytes
    }

    #[test]
    fn heartbeat_and_truncated_padding_have_distinct_rules() {
        assert!(valid_receive_packet(&data(0, HDR_TYPE_DATA, 0), 65536));
        assert!(!valid_receive_packet(&data(0, HDR_TYPE_PAD, 0), 65536));
        assert!(valid_receive_packet(&data(128, HDR_TYPE_PAD, 65408), 65536));
        assert!(!valid_receive_packet(
            &data(128, HDR_TYPE_DATA, 65408),
            65536
        ));
        assert!(!valid_receive_packet(
            &data(160, HDR_TYPE_PAD, 65408),
            65536
        ));
        assert!(!valid_receive_packet(&data(0, HDR_TYPE_DATA, 65536), 65536));
    }

    #[test]
    fn rejects_later_header_mismatches_and_partial_frames() {
        let mut packet = [0u8; 64];
        packet[..32].copy_from_slice(&data(32, HDR_TYPE_DATA, 0));
        packet[32..].copy_from_slice(&data(32, HDR_TYPE_DATA, 32));
        assert!(valid_receive_packet(&packet, 65536));
        for offset in [36, 38, 40, 44, 48, 52] {
            let mut changed = packet;
            changed[offset] ^= if offset == 38 { 8 } else { 1 };
            assert!(!valid_receive_packet(&changed, 65536), "offset={offset}");
        }
        assert!(!valid_receive_packet(&packet[..63], 65536));
    }

    proptest! {
        #[test]
        fn arbitrary_datagrams_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..2048)) {
            let _ = valid_receive_packet(&bytes, 65536);
        }
    }
}
