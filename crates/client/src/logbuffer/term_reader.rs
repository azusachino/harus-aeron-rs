use super::frame_descriptor::*;
use super::header::Header;
use agrona::AtomicBuffer;

/// Scans a term buffer for availability of message fragments.
#[derive(Debug, Copy, Clone)]
pub struct TermScanner;

impl TermScanner {
    /// Pack padding and available bytes into an i64.
    #[inline]
    pub fn pack(padding: i32, available: i32) -> i64 {
        ((padding as i64) << 32) | (available as i64)
    }

    /// Extract available bytes count from packed status.
    #[inline]
    pub fn available(result: i64) -> i32 {
        result as i32
    }

    /// Extract padding bytes count from packed status.
    #[inline]
    pub fn padding(result: i64) -> i32 {
        ((result as u64) >> 32) as i32
    }

    /// Scan the term buffer for availability of new message fragments from `offset` up to `max_length` bytes.
    pub fn scan_for_availability(
        term_buffer: &AtomicBuffer,
        offset: usize,
        max_length: usize,
    ) -> i64 {
        let capacity = term_buffer.capacity();
        let limit = max_length.min(capacity.saturating_sub(offset));
        let mut available: i32 = 0;
        let mut padding: i32 = 0;

        loop {
            let term_offset = offset + available as usize;
            let frame_length = frame_length_volatile(term_buffer, term_offset);
            if frame_length <= 0 {
                break;
            }

            let mut aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);
            if is_padding_frame(term_buffer, term_offset) {
                padding = aligned_frame_length - HEADER_LENGTH;
                aligned_frame_length = HEADER_LENGTH;
            }

            available += aligned_frame_length;

            if available as usize > limit {
                available = if aligned_frame_length == available {
                    -available
                } else {
                    available - aligned_frame_length
                };
                padding = 0;
                break;
            }

            if padding != 0 || (available as usize) >= limit {
                break;
            }
        }

        Self::pack(padding, available)
    }
}

/// Scans for gaps in the sequence of bytes in a term buffer.
#[derive(Debug, Copy, Clone)]
pub struct TermGapScanner;

impl TermGapScanner {
    /// Scan for gaps from `term_offset` up to `limit_offset`.
    /// Calls `handler(term_id, gap_begin_offset, gap_length)` for any gap found.
    /// Returns the offset of the last contiguous frame.
    pub fn scan_for_gap<F>(
        term_buffer: &AtomicBuffer,
        term_id: i32,
        term_offset: usize,
        limit_offset: usize,
        mut handler: F,
    ) -> usize
    where
        F: FnMut(i32, usize, usize),
    {
        let mut offset = term_offset;
        while offset < limit_offset {
            let frame_length = frame_length_volatile(term_buffer, offset);
            if frame_length <= 0 {
                break;
            }

            offset += align(frame_length, FRAME_ALIGNMENT) as usize;
        }

        let gap_begin_offset = offset;
        if offset < limit_offset {
            offset += HEADER_LENGTH as usize;
            while offset < limit_offset {
                if frame_length_volatile(term_buffer, offset) != 0 {
                    break;
                }
                offset += HEADER_LENGTH as usize;
            }

            let gap_length = offset - gap_begin_offset;
            handler(term_id, gap_begin_offset, gap_length);
        }

        gap_begin_offset
    }
}

/// Utility functions for reading a term within a log buffer.
#[derive(Debug, Copy, Clone)]
pub struct TermReader;

impl TermReader {
    /// Pack offset and fragments_read into an i64.
    #[inline]
    pub fn pack(offset: i32, fragments_read: i32) -> i64 {
        ((offset as i64) << 32) | (fragments_read as i64 & 0xFFFF_FFFF)
    }

    /// The number of fragments read from a packed outcome.
    #[inline]
    pub fn fragments_read(read_outcome: i64) -> i32 {
        read_outcome as i32
    }

    /// The offset up to which the term progressed from a packed outcome.
    #[inline]
    pub fn offset(read_outcome: i64) -> i32 {
        ((read_outcome as u64) >> 32) as i32
    }

    /// Reads data from a term in a log buffer and updates `subscriber_position`.
    #[allow(clippy::too_many_arguments)]
    pub fn read_with_position<'a, F, E>(
        term_buffer: &AtomicBuffer<'a>,
        term_offset: usize,
        mut handler: F,
        fragments_limit: usize,
        header: &mut Header<'a>,
        mut _error_handler: E,
        current_position: i64,
        subscriber_position: &agrona::counters::AtomicCounter,
    ) -> usize
    where
        F: FnMut(&AtomicBuffer<'a>, usize, i32, &Header<'a>),
        E: FnMut(Box<dyn std::error::Error>),
    {
        let mut fragments_read = 0;
        let mut offset = term_offset;
        let capacity = term_buffer.capacity();
        header.set_buffer(term_buffer.clone());

        while fragments_read < fragments_limit && offset < capacity {
            let frame_length = frame_length_volatile(term_buffer, offset);
            if frame_length <= 0 {
                break;
            }

            let frame_offset = offset;
            offset += align(frame_length, FRAME_ALIGNMENT) as usize;

            if !is_padding_frame(term_buffer, frame_offset) {
                fragments_read += 1;
                header.set_offset(frame_offset);
                handler(
                    term_buffer,
                    frame_offset + HEADER_LENGTH as usize,
                    frame_length - HEADER_LENGTH,
                    header,
                );
            }
        }

        let new_position = current_position + (offset - term_offset) as i64;
        if new_position > current_position {
            subscriber_position.set_ordered(new_position);
        }

        fragments_read
    }

    /// Reads data from a term in a log buffer and returns packed `(offset, fragments_read)`.
    pub fn read<'a, F, E>(
        term_buffer: &AtomicBuffer<'a>,
        term_offset: usize,
        mut handler: F,
        fragments_limit: usize,
        header: &mut Header<'a>,
        mut _error_handler: E,
    ) -> i64
    where
        F: FnMut(&AtomicBuffer<'a>, usize, i32, &Header<'a>),
        E: FnMut(Box<dyn std::error::Error>),
    {
        let mut fragments_read = 0;
        let mut offset = term_offset;
        let capacity = term_buffer.capacity();
        header.set_buffer(term_buffer.clone());

        while fragments_read < fragments_limit && offset < capacity {
            let frame_length = frame_length_volatile(term_buffer, offset);
            if frame_length <= 0 {
                break;
            }

            let frame_offset = offset;
            offset += align(frame_length, FRAME_ALIGNMENT) as usize;

            if !is_padding_frame(term_buffer, frame_offset) {
                fragments_read += 1;
                header.set_offset(frame_offset);
                handler(
                    term_buffer,
                    frame_offset + HEADER_LENGTH as usize,
                    frame_length - HEADER_LENGTH,
                    header,
                );
            }
        }

        Self::pack(offset as i32, fragments_read as i32)
    }

    /// Reads data from a term, giving the handler control over commit/abort/break of the scan.
    ///
    /// Returns packed `(offset, fragments_read)`, same convention as [`TermReader::read`]. Kept
    /// as a separate loop rather than sharing one with `read`, mirroring how upstream keeps
    /// `Image.poll`/`Image.controlledPoll` as two distinct methods rather than one shared loop —
    /// the two have materially different position-commit semantics. `on_commit(offset)` is
    /// invoked with the absolute term offset whenever the handler returns
    /// [`ControlledPollAction::Commit`], so the caller (which owns the subscriber-position
    /// handle) can apply an intermediate flow-control commit.
    pub fn controlled_read<'a, F, C>(
        term_buffer: &AtomicBuffer<'a>,
        term_offset: usize,
        mut handler: F,
        fragments_limit: usize,
        header: &mut Header<'a>,
        mut on_commit: C,
    ) -> i64
    where
        F: FnMut(&AtomicBuffer<'a>, usize, i32, &Header<'a>) -> ControlledPollAction,
        C: FnMut(usize),
    {
        let mut fragments_read = 0;
        let mut offset = term_offset;
        let capacity = term_buffer.capacity();
        header.set_buffer(term_buffer.clone());

        while fragments_read < fragments_limit && offset < capacity {
            let frame_length = frame_length_volatile(term_buffer, offset);
            if frame_length <= 0 {
                break;
            }

            let frame_offset = offset;
            let aligned_length = align(frame_length, FRAME_ALIGNMENT) as usize;
            offset += aligned_length;

            if is_padding_frame(term_buffer, frame_offset) {
                continue;
            }

            fragments_read += 1;
            header.set_offset(frame_offset);

            let action = handler(
                term_buffer,
                frame_offset + HEADER_LENGTH as usize,
                frame_length - HEADER_LENGTH,
                header,
            );

            match action {
                ControlledPollAction::Abort => {
                    fragments_read -= 1;
                    offset -= aligned_length;
                    break;
                }
                ControlledPollAction::Break => break,
                ControlledPollAction::Commit => on_commit(offset),
                ControlledPollAction::Continue => {}
            }
        }

        Self::pack(offset as i32, fragments_read as i32)
    }
}

/// Action returned from a controlled-fragment handler to direct [`TermReader::controlled_read`].
///
/// Mirrors upstream `io.aeron.logbuffer.ControlledFragmentHandler.Action`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlledPollAction {
    /// Abort the current polling operation and do not advance the position for this fragment.
    Abort,
    /// Break from the current polling operation and commit the position as of the end of the
    /// current fragment being handled.
    Break,
    /// Continue processing but commit the position as of the end of the current fragment, so
    /// that flow control is applied to this point.
    Commit,
    /// Continue processing until the fragment limit or no fragments remain, committing position
    /// at the end of the poll as [`TermReader::read`] does.
    Continue,
}
