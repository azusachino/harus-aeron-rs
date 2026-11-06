//! Image: read-side stream view of an Aeron publication over log buffers.

use crate::error::{AeronError, Result};
use crate::log_buffers::LogBuffers;
use crate::logbuffer::{
    ControlledPollAction, Header, NO_END_OF_STREAM_POSITION, TermReader, end_of_stream_position,
    index_by_position,
};
use agrona::AtomicBuffer;
use agrona::counters::AtomicCounter;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// Represents a replicated `Publication` stream from a subscriber's perspective.
pub struct Image {
    subscription_registration_id: i64,
    session_id: i32,
    initial_term_id: i32,
    term_length: usize,
    term_length_mask: usize,
    position_bits_to_shift: usize,
    correlation_id: i64,
    source_identity: String,
    subscriber_position: Arc<AtomicI64>,
    subscriber_position_id: i32,
    cnc_subscriber_position: Option<AtomicCounter<'static>>,
    // Kept alive so `term_buffers`' raw-pointer AtomicBuffer views stay valid; both this
    // Image and any Publication/Subscription over the same log clone the same Arc, so writes
    // through one are visible through all — see LogBuffers' doc comment.
    #[allow(dead_code)]
    log_buffers: LogBuffers,
    term_buffers: Vec<AtomicBuffer<'static>>,
    is_closed: bool,
}

impl Image {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        subscription_registration_id: i64,
        session_id: i32,
        initial_term_id: i32,
        initial_position: i64,
        subscriber_position: Arc<AtomicI64>,
        log_buffers: LogBuffers,
        source_identity: String,
        correlation_id: i64,
    ) -> Self {
        let term_length = log_buffers.term_length();
        let term_length_mask = term_length - 1;
        let position_bits_to_shift = term_length.trailing_zeros() as usize;

        subscriber_position.store(initial_position, Ordering::Release);

        let term_buffers = log_buffers.duplicate_term_buffers();

        Self {
            subscription_registration_id,
            session_id,
            initial_term_id,
            term_length,
            term_length_mask,
            position_bits_to_shift,
            correlation_id,
            source_identity,
            subscriber_position,
            subscriber_position_id: 0,
            cnc_subscriber_position: None,
            log_buffers,
            term_buffers,
            is_closed: false,
        }
    }

    pub fn subscription_registration_id(&self) -> i64 {
        self.subscription_registration_id
    }

    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    pub fn initial_term_id(&self) -> i32 {
        self.initial_term_id
    }

    pub fn term_length(&self) -> usize {
        self.term_length
    }

    pub fn correlation_id(&self) -> i64 {
        self.correlation_id
    }

    pub fn source_identity(&self) -> &str {
        &self.source_identity
    }

    pub fn log_buffers(&self) -> &LogBuffers {
        &self.log_buffers
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /// Whether the subscriber has reached the publication's shared EOS position.
    ///
    /// Matches vendored `io.aeron.Image.isEndOfStream()` and reads the marker written by the
    /// driver-side `PublicationImage` through the shared log metadata.
    pub fn is_end_of_stream(&self) -> bool {
        if self.is_closed {
            return false;
        }
        let eos_position = end_of_stream_position(&self.log_buffers.meta_data_buffer());
        eos_position != NO_END_OF_STREAM_POSITION && self.position() >= eos_position
    }

    /// Return the shared EOS position, or `i64::MAX` while the stream is still open.
    pub fn end_of_stream_position(&self) -> i64 {
        end_of_stream_position(&self.log_buffers.meta_data_buffer())
    }

    pub fn close(&mut self) {
        self.is_closed = true;
    }

    pub fn subscriber_position_id(&self) -> i32 {
        self.subscriber_position_id
    }

    pub fn set_subscriber_position_id(&mut self, id: i32) {
        self.subscriber_position_id = id;
    }

    pub(crate) fn set_cnc_subscriber_position(&mut self, counter: AtomicCounter<'static>) {
        self.cnc_subscriber_position = Some(counter);
    }

    pub fn position(&self) -> i64 {
        if self.is_closed {
            0
        } else {
            self.subscriber_position.load(Ordering::Acquire)
        }
    }

    pub fn set_position(&mut self, new_position: i64) -> Result<()> {
        if self.is_closed {
            return Err(AeronError::IllegalState("Image is closed".to_string()));
        }

        let current_position = self.subscriber_position.load(Ordering::Acquire);
        if new_position < current_position {
            return Err(AeronError::IllegalArgument(format!(
                "new position {} < current position {}",
                new_position, current_position
            )));
        }

        let term_offset = (new_position & self.term_length_mask as i64) as usize;
        if term_offset != 0 && (term_offset % 32 != 0) {
            return Err(AeronError::IllegalArgument(format!(
                "new position {} is not frame aligned",
                new_position
            )));
        }

        self.subscriber_position
            .store(new_position, Ordering::Release);
        if let Some(counter) = &self.cnc_subscriber_position {
            counter.set_ordered(new_position);
        }
        Ok(())
    }

    pub fn poll<F>(&mut self, mut fragment_handler: F, fragment_limit: usize) -> usize
    where
        F: FnMut(&AtomicBuffer, usize, usize, &Header),
    {
        if self.is_closed {
            return 0;
        }

        let position = self.subscriber_position.load(Ordering::Acquire);
        let index = index_by_position(position, self.position_bits_to_shift);
        let term_offset = (position & self.term_length_mask as i64) as usize;

        let term_buffer = self.term_buffers[index].clone();

        let mut handler_adapter = |buf: &AtomicBuffer, offset: usize, length: i32, hdr: &Header| {
            fragment_handler(buf, offset, length as usize, hdr);
        };

        let mut header = Header::new(self.initial_term_id, self.position_bits_to_shift as i32);

        let read_result = TermReader::read(
            &term_buffer,
            term_offset,
            &mut handler_adapter,
            fragment_limit,
            &mut header,
            |_| {},
        );

        let fragments_read = TermReader::fragments_read(read_result) as usize;
        let bytes_read = TermReader::offset(read_result) as usize - term_offset;

        if bytes_read > 0 {
            let new_position = position + bytes_read as i64;
            self.subscriber_position
                .store(new_position, Ordering::Release);
            if let Some(counter) = &self.cnc_subscriber_position {
                counter.set_ordered(new_position);
            }
        }

        fragments_read
    }

    /// Poll for new messages, giving `fragment_handler` control over commit/abort/break of the
    /// scan via [`ControlledPollAction`], instead of always committing the whole batch at once.
    ///
    /// Mirrors upstream `Image.controlledPoll`. Use [`ControlledPollAction::Commit`] to make
    /// flow control apply as of a fragment before the batch completes, or
    /// [`ControlledPollAction::Abort`] to leave a fragment for the next poll call entirely.
    pub fn controlled_poll<F>(&mut self, mut fragment_handler: F, fragment_limit: usize) -> usize
    where
        F: FnMut(&AtomicBuffer, usize, usize, &Header) -> ControlledPollAction,
    {
        if self.is_closed {
            return 0;
        }

        let position = self.subscriber_position.load(Ordering::Acquire);
        let index = index_by_position(position, self.position_bits_to_shift);
        let term_offset = (position & self.term_length_mask as i64) as usize;

        let term_buffer = self.term_buffers[index].clone();
        let is_closed = self.is_closed;
        let subscriber_position = &self.subscriber_position;
        let cnc_subscriber_position = &self.cnc_subscriber_position;

        let mut handler_adapter = |buf: &AtomicBuffer, offset: usize, length: i32, hdr: &Header| {
            fragment_handler(buf, offset, length as usize, hdr)
        };

        let mut header = Header::new(self.initial_term_id, self.position_bits_to_shift as i32);

        let read_result = TermReader::controlled_read(
            &term_buffer,
            term_offset,
            &mut handler_adapter,
            fragment_limit,
            &mut header,
            |commit_offset| {
                let new_position = position + (commit_offset - term_offset) as i64;
                if !is_closed {
                    subscriber_position.store(new_position, Ordering::Release);
                    if let Some(counter) = cnc_subscriber_position {
                        counter.set_ordered(new_position);
                    }
                }
            },
        );

        let fragments_read = TermReader::fragments_read(read_result) as usize;
        let final_offset = TermReader::offset(read_result) as usize;
        let bytes_read = final_offset - term_offset;

        if bytes_read > 0 {
            let new_position = position + bytes_read as i64;
            self.subscriber_position
                .store(new_position, Ordering::Release);
            if let Some(counter) = &self.cnc_subscriber_position {
                counter.set_ordered(new_position);
            }
        }

        fragments_read
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cnc_file::{
        COUNTER_METADATA_LENGTH, CncFile, DEFAULT_COUNTER_VALUES_LENGTH, DEFAULT_ERROR_LOG_LENGTH,
        DEFAULT_TO_CLIENTS_LENGTH, DEFAULT_TO_DRIVER_LENGTH,
    };
    use crate::logbuffer::{
        FRAME_ALIGNMENT, HEADER_LENGTH, PAGE_MIN_SIZE, TERM_MIN_LENGTH, align, compute_position,
    };
    use crate::protocol::DataHeaderFlyweight;

    const CORRELATION_ID: i64 = 0xC044E1A;
    const SESSION_ID: i32 = 0x5E55101D;
    const INITIAL_TERM_ID: i32 = 0xEE81D;

    fn create_test_log_buffers() -> LogBuffers {
        let term_len = TERM_MIN_LENGTH as usize;
        let total_len = term_len * 3 + 4096;
        let mut contents = vec![0u8; total_len];
        let meta_offset = total_len - 4096;
        let mut meta_slice = contents[meta_offset..].to_vec();
        let meta_buf = AtomicBuffer::new(&mut meta_slice);
        crate::logbuffer::set_term_length(&meta_buf, TERM_MIN_LENGTH);
        crate::logbuffer::set_page_size(&meta_buf, PAGE_MIN_SIZE);
        crate::logbuffer::set_end_of_stream_position(
            &meta_buf,
            crate::logbuffer::NO_END_OF_STREAM_POSITION,
        );
        contents[meta_offset..].copy_from_slice(&meta_slice);
        LogBuffers::from_bytes(contents).unwrap()
    }

    #[test]
    fn should_handle_closed_image() {
        let subscriber_pos = Arc::new(AtomicI64::new(0));
        let log_buffers = create_test_log_buffers();
        let mut image = Image::new(
            1,
            SESSION_ID,
            INITIAL_TERM_ID,
            0,
            subscriber_pos,
            log_buffers,
            "ipc".to_string(),
            CORRELATION_ID,
        );

        image.close();
        assert!(image.is_closed());
        assert_eq!(image.poll(|_, _, _, _| {}, usize::MAX), 0);
        assert_eq!(image.position(), 0);
    }

    #[test]
    fn should_allow_valid_position() {
        let subscriber_pos = Arc::new(AtomicI64::new(0));
        let log_buffers = create_test_log_buffers();
        let mut image = Image::new(
            1,
            SESSION_ID,
            INITIAL_TERM_ID,
            0,
            subscriber_pos.clone(),
            log_buffers,
            "ipc".to_string(),
            CORRELATION_ID,
        );

        let expected_position = (TERM_MIN_LENGTH - 32) as i64;
        subscriber_pos.store(expected_position, Ordering::Release);
        assert_eq!(image.position(), expected_position);

        image.set_position(TERM_MIN_LENGTH as i64).unwrap();
        assert_eq!(image.position(), TERM_MIN_LENGTH as i64);
        assert!(!image.is_end_of_stream());
        crate::logbuffer::set_end_of_stream_position(
            &image.log_buffers.meta_data_buffer(),
            TERM_MIN_LENGTH as i64,
        );
        assert!(image.is_end_of_stream());
        assert_eq!(image.end_of_stream_position(), TERM_MIN_LENGTH as i64);
    }

    #[test]
    fn should_reject_backward_and_unaligned_positions() {
        let subscriber_pos = Arc::new(AtomicI64::new(0));
        let log_buffers = create_test_log_buffers();
        let mut image = Image::new(
            1,
            SESSION_ID,
            INITIAL_TERM_ID,
            0,
            subscriber_pos,
            log_buffers,
            "ipc".to_string(),
            CORRELATION_ID,
        );

        image.set_position(64).unwrap();
        assert!(image.set_position(32).is_err());
        assert!(image.set_position(65).is_err());
    }

    #[test]
    fn should_report_correct_position_on_reception() {
        let position_bits_to_shift = (TERM_MIN_LENGTH as usize).trailing_zeros() as i32;
        let initial_position =
            compute_position(INITIAL_TERM_ID, 0, position_bits_to_shift, INITIAL_TERM_ID);
        let subscriber_pos = Arc::new(AtomicI64::new(initial_position));
        let log_buffers = create_test_log_buffers();

        let data = [1u8; 36];
        let msg_len = HEADER_LENGTH as usize + data.len();

        let aligned_frame_len = align(msg_len as i32, FRAME_ALIGNMENT) as usize;

        let terms = log_buffers.duplicate_term_buffers();

        let mut frame_bytes = vec![0u8; aligned_frame_len];
        let frame_buf = AtomicBuffer::new(&mut frame_bytes);
        let data_header = DataHeaderFlyweight::new(frame_buf);
        data_header.set_header_type(crate::protocol::HDR_TYPE_DATA);
        data_header.set_frame_length(msg_len as i32);
        data_header.set_session_id(SESSION_ID);
        data_header.set_term_id(INITIAL_TERM_ID);
        data_header.set_term_offset(0);

        terms[0].put_bytes(0, &frame_bytes);

        let cnc_path = std::env::temp_dir().join(format!(
            "aeron-client-image-cnc-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&cnc_path);
        let cnc = CncFile::create_new_with_regions(
            &cnc_path,
            DEFAULT_TO_DRIVER_LENGTH,
            DEFAULT_TO_CLIENTS_LENGTH,
            COUNTER_METADATA_LENGTH,
            DEFAULT_COUNTER_VALUES_LENGTH,
            DEFAULT_ERROR_LOG_LENGTH,
            10_000_000_000,
        )
        .unwrap();
        cnc.write_counter_metadata(0, 1, 0, initial_position, "subscriber")
            .unwrap();

        let mut image = Image::new(
            1,
            SESSION_ID,
            INITIAL_TERM_ID,
            initial_position,
            subscriber_pos.clone(),
            log_buffers,
            "ipc".to_string(),
            CORRELATION_ID,
        );
        image.set_cnc_subscriber_position(AtomicCounter::new(cnc.counters_values_buffer(), 0));

        let mut called = false;
        let read = image.poll(
            |_buf, offset, len, hdr| {
                called = true;
                assert_eq!(offset, HEADER_LENGTH as usize);
                assert_eq!(len, data.len());
                assert_eq!(hdr.session_id(), SESSION_ID);
            },
            usize::MAX,
        );

        assert_eq!(read, 1);
        assert!(called);
        assert_eq!(
            subscriber_pos.load(Ordering::Acquire),
            initial_position + aligned_frame_len as i64
        );
        assert_eq!(
            cnc.counters_values_buffer().get_i64_volatile(0),
            initial_position + aligned_frame_len as i64
        );

        drop(cnc);
        let _ = std::fs::remove_file(cnc_path);
    }

    /// Write one aligned data frame at `offset` in `term`, returning the aligned frame length.
    fn write_frame_at(
        term: &AtomicBuffer,
        offset: usize,
        term_id: i32,
        payload_len: usize,
    ) -> usize {
        let msg_len = HEADER_LENGTH as usize + payload_len;
        let aligned_len = align(msg_len as i32, FRAME_ALIGNMENT) as usize;

        let mut frame_bytes = vec![0u8; aligned_len];
        let frame_buf = AtomicBuffer::new(&mut frame_bytes);
        let data_header = DataHeaderFlyweight::new(frame_buf);
        data_header.set_header_type(crate::protocol::HDR_TYPE_DATA);
        data_header.set_frame_length(msg_len as i32);
        data_header.set_session_id(SESSION_ID);
        data_header.set_term_id(term_id);
        data_header.set_term_offset(offset as i32);

        term.put_bytes(offset, &frame_bytes);
        aligned_len
    }

    fn image_with_three_frames() -> (Image, Arc<AtomicI64>, i64, usize) {
        let position_bits_to_shift = (TERM_MIN_LENGTH as usize).trailing_zeros() as i32;
        let initial_position =
            compute_position(INITIAL_TERM_ID, 0, position_bits_to_shift, INITIAL_TERM_ID);
        let subscriber_pos = Arc::new(AtomicI64::new(initial_position));
        let log_buffers = create_test_log_buffers();
        let terms = log_buffers.duplicate_term_buffers();

        let mut offset = 0usize;
        let mut frame_len = 0usize;
        for _ in 0..3 {
            frame_len = write_frame_at(&terms[0], offset, INITIAL_TERM_ID, 8);
            offset += frame_len;
        }

        let image = Image::new(
            1,
            SESSION_ID,
            INITIAL_TERM_ID,
            initial_position,
            subscriber_pos.clone(),
            log_buffers,
            "ipc".to_string(),
            CORRELATION_ID,
        );

        (image, subscriber_pos, initial_position, frame_len)
    }

    #[test]
    fn controlled_poll_abort_leaves_position_and_fragment_uncounted() {
        let (mut image, _subscriber_pos, initial_position, _frame_len) = image_with_three_frames();

        let mut calls = 0;
        let read = image.controlled_poll(
            |_buf, _offset, _len, _hdr| {
                calls += 1;
                ControlledPollAction::Abort
            },
            usize::MAX,
        );

        assert_eq!(read, 0);
        assert_eq!(calls, 1, "abort must stop the scan at the first fragment");
        assert_eq!(image.position(), initial_position);
    }

    #[test]
    fn controlled_poll_break_commits_only_up_to_the_break_point() {
        let (mut image, _subscriber_pos, initial_position, frame_len) = image_with_three_frames();

        let mut calls = 0;
        let read = image.controlled_poll(
            |_buf, _offset, _len, _hdr| {
                calls += 1;
                if calls == 1 {
                    ControlledPollAction::Continue
                } else {
                    ControlledPollAction::Break
                }
            },
            usize::MAX,
        );

        assert_eq!(read, 2);
        assert_eq!(calls, 2);
        assert_eq!(image.position(), initial_position + (frame_len as i64) * 2);
    }

    #[test]
    fn controlled_poll_commit_applies_position_before_batch_completes() {
        // Prove COMMIT is applied immediately, not just batched into the final store: read the
        // shared position counter from inside the handler for fragment 3, after fragment 2
        // already returned Commit but before controlled_poll itself has returned.
        let (mut image, subscriber_pos, initial_position, frame_len) = image_with_three_frames();

        let mut calls = 0;
        let mut position_seen_before_third_fragment = -1i64;
        let read = image.controlled_poll(
            |_buf, _offset, _len, _hdr| {
                calls += 1;
                match calls {
                    2 => ControlledPollAction::Commit,
                    3 => {
                        position_seen_before_third_fragment =
                            subscriber_pos.load(Ordering::Acquire);
                        ControlledPollAction::Continue
                    }
                    _ => ControlledPollAction::Continue,
                }
            },
            usize::MAX,
        );

        assert_eq!(read, 3);
        assert_eq!(
            position_seen_before_third_fragment,
            initial_position + (frame_len as i64) * 2
        );
        assert_eq!(image.position(), initial_position + (frame_len as i64) * 3);
    }
}
