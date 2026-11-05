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
