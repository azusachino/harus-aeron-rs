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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logbuffer::descriptor::*;
    use crate::protocol;
    use agrona::AtomicBuffer;

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

        let scan_outcome =
            TermScanner::scan_for_availability(&term_buffer, frame_offset, max_length);
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
}
