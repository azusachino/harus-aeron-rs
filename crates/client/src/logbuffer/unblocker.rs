use super::descriptor::*;
use super::frame_descriptor::*;
use crate::protocol;
use agrona::AtomicBuffer;

/// Status result of an `unblock` operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UnblockStatus {
    NoAction,
    Unblocked,
    UnblockedToEnd,
}

/// Unblocks a term buffer if a publisher has died leaving the log with a partial log entry.
#[derive(Debug, Copy, Clone)]
pub struct TermUnblocker;

impl TermUnblocker {
    /// Attempt to unblock the current term at `blocked_offset`.
    pub fn unblock(
        log_metadata_buffer: &AtomicBuffer,
        term_buffer: &AtomicBuffer,
        blocked_offset: usize,
        tail_offset: usize,
        term_id: i32,
    ) -> UnblockStatus {
        let mut status = UnblockStatus::NoAction;
        let mut frame_length = frame_length_volatile(term_buffer, blocked_offset);

        if frame_length < 0 {
            Self::reset_header(
                log_metadata_buffer,
                term_buffer,
                blocked_offset,
                term_id,
                -frame_length,
            );
            status = UnblockStatus::Unblocked;
        } else if frame_length == 0 {
            let mut current_offset = blocked_offset + FRAME_ALIGNMENT as usize;

            while current_offset < tail_offset {
                frame_length = frame_length_volatile(term_buffer, current_offset);

                if frame_length != 0 {
                    if Self::scan_back_to_confirm_zeroed(
                        term_buffer,
                        current_offset,
                        blocked_offset,
                    ) {
                        let length = (current_offset - blocked_offset) as i32;
                        Self::reset_header(
                            log_metadata_buffer,
                            term_buffer,
                            blocked_offset,
                            term_id,
                            length,
                        );
                        status = UnblockStatus::Unblocked;
                    }
                    break;
                }

                current_offset += FRAME_ALIGNMENT as usize;
            }

            if current_offset == term_buffer.capacity()
                && frame_length_volatile(term_buffer, blocked_offset) == 0
            {
                let length = (current_offset - blocked_offset) as i32;
                Self::reset_header(
                    log_metadata_buffer,
                    term_buffer,
                    blocked_offset,
                    term_id,
                    length,
                );
                status = UnblockStatus::UnblockedToEnd;
            }
        }

        status
    }

    fn reset_header(
        log_metadata_buffer: &AtomicBuffer,
        term_buffer: &AtomicBuffer,
        term_offset: usize,
        term_id: i32,
        frame_length: i32,
    ) {
        apply_default_header(log_metadata_buffer, term_buffer, term_offset);
        set_frame_type(term_buffer, term_offset, protocol::HDR_TYPE_PAD);
        set_frame_term_offset(term_buffer, term_offset);
        set_frame_term_id(term_buffer, term_offset, term_id);
        set_frame_length_ordered(term_buffer, term_offset, frame_length);
    }

    fn scan_back_to_confirm_zeroed(buffer: &AtomicBuffer, from: usize, limit: usize) -> bool {
        let mut i = from as isize - FRAME_ALIGNMENT as isize;
        let target = limit as isize;
        while i >= target {
            if frame_length_volatile(buffer, i as usize) != 0 {
                return false;
            }
            i -= FRAME_ALIGNMENT as isize;
        }
        true
    }
}

/// Provides the functionality to unblock a log at a given position.
#[derive(Debug, Copy, Clone)]
pub struct LogBufferUnblocker;

impl LogBufferUnblocker {
    /// Attempt to unblock a log buffer at `blocked_position`.
    pub fn unblock(
        term_buffers: &[AtomicBuffer],
        log_metadata_buffer: &AtomicBuffer,
        blocked_position: i64,
        term_length: usize,
    ) -> bool {
        let shift = position_bits_to_shift(term_length);
        let blocked_term_count = (blocked_position >> shift) as i32;
        let blocked_offset = (blocked_position as usize) & (term_length - 1);
        let active_term_count_val = active_term_count(log_metadata_buffer);

        if active_term_count_val == (blocked_term_count - 1) && blocked_offset == 0 {
            let current_term_id = term_id(raw_tail_volatile(
                log_metadata_buffer,
                index_by_term_count(active_term_count_val as i64) as usize,
            ));
            rotate_log(log_metadata_buffer, active_term_count_val, current_term_id);
            return true;
        }

        let blocked_index = index_by_term_count(blocked_term_count as i64) as usize;
        let raw_tail_val = raw_tail_volatile(log_metadata_buffer, blocked_index);
        let term_id_val = term_id(raw_tail_val);
        let tail_offset_val = term_offset(raw_tail_val, term_length);
        let term_buffer = &term_buffers[blocked_index];

        match TermUnblocker::unblock(
            log_metadata_buffer,
            term_buffer,
            blocked_offset,
            tail_offset_val,
            term_id_val,
        ) {
            UnblockStatus::NoAction => false,
            UnblockStatus::UnblockedToEnd => {
                rotate_log(log_metadata_buffer, blocked_term_count, term_id_val);
                true
            }
            UnblockStatus::Unblocked => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logbuffer::descriptor::{
        LOG_META_DATA_LENGTH, compute_position, initialise_tail_with_term_id,
        position_bits_to_shift, raw_tail_volatile_active, set_initial_term_id, term_id,
    };
    use crate::logbuffer::frame_descriptor::HEADER_LENGTH;
    use crate::protocol;
    use agrona::AtomicBuffer;

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
}
