//! Reassembles fragmented messages so that delegates see complete messages.

use crate::buffer_builder::BufferBuilder;
use crate::logbuffer::{
    BEGIN_FRAG_FLAG, ControlledPollAction, END_FRAG_FLAG, Header, UNFRAGMENTED,
};
use agrona::AtomicBuffer;
use std::collections::HashMap;

/// Adapter that reassembles fragmented messages and delegates on whole messages.
#[derive(Debug, Default)]
pub struct FragmentAssembler {
    initial_buffer_length: usize,
    builders_by_session_id: HashMap<i32, BufferBuilder>,
}

impl FragmentAssembler {
    pub fn new(initial_buffer_length: usize) -> Self {
        Self {
            initial_buffer_length,
            builders_by_session_id: HashMap::new(),
        }
    }

    pub fn on_fragment<F>(
        &mut self,
        buffer: &AtomicBuffer,
        offset: usize,
        length: usize,
        header: &Header,
        mut delegate: F,
    ) where
        F: FnMut(&AtomicBuffer, usize, usize, &Header),
    {
        let flags = header.flags();
        if (flags & UNFRAGMENTED) == UNFRAGMENTED {
            delegate(buffer, offset, length, header);
        } else {
            self.handle_fragment(buffer, offset, length, header, flags, delegate);
        }
    }

    fn handle_fragment<F>(
        &mut self,
        buffer: &AtomicBuffer,
        offset: usize,
        length: usize,
        header: &Header,
        flags: u8,
        mut delegate: F,
    ) where
        F: FnMut(&AtomicBuffer, usize, usize, &Header),
    {
        let session_id = header.session_id();
        if (flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG {
            let initial_len = self.initial_buffer_length;
            let builder = self
                .builders_by_session_id
                .entry(session_id)
                .or_insert_with(|| BufferBuilder::new(initial_len));

            builder
                .reset()
                .capture_header(header)
                .append(buffer, offset, length)
                .set_next_term_offset(header.next_term_offset());
        } else if let Some(builder) = self.builders_by_session_id.get_mut(&session_id) {
            if header.term_offset() == builder.next_term_offset() {
                builder.append(buffer, offset, length);

                if (flags & END_FRAG_FLAG) == END_FRAG_FLAG {
                    let limit = builder.limit();
                    let mut assembled_bytes = builder.buffer_bytes().to_vec();
                    let complete_hdr = builder.complete_header(header);
                    let assembled_buf = AtomicBuffer::new(&mut assembled_bytes);
                    delegate(&assembled_buf, 0, limit, &complete_hdr);
                    builder.reset();
                } else {
                    builder.set_next_term_offset(header.next_term_offset());
                }
            } else {
                builder.reset();
            }
        }
    }

    pub fn free_session_buffer(&mut self, session_id: i32) -> bool {
        self.builders_by_session_id.remove(&session_id).is_some()
    }

    pub fn clear(&mut self) {
        self.builders_by_session_id.clear();
    }
}

/// Adapter that reassembles fragmented messages and delegates on whole messages using a controlled handler.
#[derive(Debug, Default)]
pub struct ControlledFragmentAssembler {
    initial_buffer_length: usize,
    builders_by_session_id: HashMap<i32, BufferBuilder>,
}

impl ControlledFragmentAssembler {
    pub fn new(initial_buffer_length: usize) -> Self {
        Self {
            initial_buffer_length,
            builders_by_session_id: HashMap::new(),
        }
    }

    pub fn on_fragment<F>(
        &mut self,
        buffer: &AtomicBuffer,
        offset: usize,
        length: usize,
        header: &Header,
        delegate: F,
    ) -> ControlledPollAction
    where
        F: FnMut(&AtomicBuffer, usize, usize, &Header) -> ControlledPollAction,
    {
        let flags = header.flags();
        if (flags & UNFRAGMENTED) == UNFRAGMENTED {
            let mut delegate = delegate;
            delegate(buffer, offset, length, header)
        } else {
            self.handle_fragment(buffer, offset, length, header, flags, delegate)
        }
    }

    fn handle_fragment<F>(
        &mut self,
        buffer: &AtomicBuffer,
        offset: usize,
        length: usize,
        header: &Header,
        flags: u8,
        mut delegate: F,
    ) -> ControlledPollAction
    where
        F: FnMut(&AtomicBuffer, usize, usize, &Header) -> ControlledPollAction,
    {
        let session_id = header.session_id();
        if (flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG {
            let initial_len = self.initial_buffer_length;
            let builder = self
                .builders_by_session_id
                .entry(session_id)
                .or_insert_with(|| BufferBuilder::new(initial_len));

            builder
                .reset()
                .capture_header(header)
                .append(buffer, offset, length)
                .set_next_term_offset(header.next_term_offset());
            ControlledPollAction::Continue
        } else if let Some(builder) = self.builders_by_session_id.get_mut(&session_id) {
            if header.term_offset() == builder.next_term_offset() {
                let limit_before = builder.limit();
                builder.append(buffer, offset, length);

                if (flags & END_FRAG_FLAG) == END_FRAG_FLAG {
                    let limit = builder.limit();
                    let mut assembled_bytes = builder.buffer_bytes().to_vec();
                    let complete_hdr = builder.complete_header(header);
                    let assembled_buf = AtomicBuffer::new(&mut assembled_bytes);
                    let action = delegate(&assembled_buf, 0, limit, &complete_hdr);
                    if action == ControlledPollAction::Abort {
                        builder.set_limit(limit_before);
                    } else {
                        builder.reset();
                    }
                    action
                } else {
                    builder.set_next_term_offset(header.next_term_offset());
                    ControlledPollAction::Continue
                }
            } else {
                builder.reset();
                ControlledPollAction::Continue
            }
        } else {
            ControlledPollAction::Continue
        }
    }

    pub fn free_session_buffer(&mut self, session_id: i32) -> bool {
        self.builders_by_session_id.remove(&session_id).is_some()
    }

    pub fn clear(&mut self) {
        self.builders_by_session_id.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logbuffer::{FRAME_ALIGNMENT, HEADER_LENGTH, align};
    use crate::protocol::DataHeaderFlyweight;

    const SESSION_ID: i32 = 777;
    const INITIAL_TERM_ID: i32 = 3;

    #[test]
    fn should_pass_through_unfragmented_message() {
        let mut header_bytes = vec![0u8; 64];
        let header_buf = AtomicBuffer::new(&mut header_bytes);
        let data_header = DataHeaderFlyweight::new(header_buf.clone());
        data_header.set_flags(UNFRAGMENTED);
        data_header.set_session_id(SESSION_ID);

        let mut header = Header::new(INITIAL_TERM_ID, 16);
        header.set_buffer(header_buf);
        header.set_offset(0);

        let mut assembler = FragmentAssembler::default();
        let mut src_bytes = vec![0u8; 128];
        let src_buf = AtomicBuffer::new(&mut src_bytes);

        let mut called = false;
        assembler.on_fragment(&src_buf, 8, 32, &header, |_buf, offset, length, hdr| {
            called = true;
            assert_eq!(offset, 8);
            assert_eq!(length, 32);
            assert_eq!(hdr.session_id(), SESSION_ID);
        });

        assert!(called);
    }

    #[test]
    fn should_assemble_two_part_message() {
        let mut assembler = FragmentAssembler::default();
        let mut src_bytes = vec![0u8; 2048];
        let src_buf = AtomicBuffer::new(&mut src_bytes);
        let length = 512;

        let mut header_bytes_1 = vec![0u8; 64];
        let header_buf_1 = AtomicBuffer::new(&mut header_bytes_1);
        let data_hdr_1 = DataHeaderFlyweight::new(header_buf_1.clone());
        data_hdr_1.set_flags(BEGIN_FRAG_FLAG);
        data_hdr_1.set_session_id(SESSION_ID);
        data_hdr_1.set_term_offset(HEADER_LENGTH);
        data_hdr_1.set_frame_length(HEADER_LENGTH + length as i32);

        let mut header_1 = Header::new(INITIAL_TERM_ID, 16);
        header_1.set_buffer(header_buf_1);

        assembler.on_fragment(
            &src_buf,
            HEADER_LENGTH as usize,
            length,
            &header_1,
            |_, _, _, _| {},
        );

        let next_offset = align(
            HEADER_LENGTH + length as i32 + HEADER_LENGTH,
            FRAME_ALIGNMENT,
        );

        let mut header_bytes_2 = vec![0u8; 64];
        let header_buf_2 = AtomicBuffer::new(&mut header_bytes_2);
        let data_hdr_2 = DataHeaderFlyweight::new(header_buf_2.clone());
        data_hdr_2.set_flags(END_FRAG_FLAG);
        data_hdr_2.set_session_id(SESSION_ID);
        data_hdr_2.set_term_offset(next_offset);
        data_hdr_2.set_frame_length(HEADER_LENGTH + length as i32);

        let mut header_2 = Header::new(INITIAL_TERM_ID, 16);
        header_2.set_buffer(header_buf_2);

        let mut called = false;
        assembler.on_fragment(
            &src_buf,
            next_offset as usize,
            length,
            &header_2,
            |_buf, offset, len, hdr| {
                called = true;
                assert_eq!(offset, 0);
                assert_eq!(len, length * 2);
                assert_eq!(hdr.session_id(), SESSION_ID);
                assert_eq!(hdr.flags(), UNFRAGMENTED);
            },
        );

        assert!(called);
    }

    #[test]
    fn should_free_session_buffer() {
        let mut assembler = FragmentAssembler::default();
        let mut src_bytes = vec![0u8; 256];
        let src_buf = AtomicBuffer::new(&mut src_bytes);

        let mut header_bytes = vec![0u8; 64];
        let header_buf = AtomicBuffer::new(&mut header_bytes);
        let data_header = DataHeaderFlyweight::new(header_buf.clone());
        data_header.set_flags(BEGIN_FRAG_FLAG);
        data_header.set_session_id(SESSION_ID);
        data_header.set_term_offset(HEADER_LENGTH);
        data_header.set_frame_length(HEADER_LENGTH + 16);

        let mut header = Header::new(INITIAL_TERM_ID, 16);
        header.set_buffer(header_buf);
        assembler.on_fragment(
            &src_buf,
            HEADER_LENGTH as usize,
            16,
            &header,
            |_, _, _, _| {},
        );

        assert!(assembler.free_session_buffer(SESSION_ID));
        assert!(!assembler.free_session_buffer(SESSION_ID));
    }

    #[test]
    fn should_ignore_fragments_without_a_begin() {
        let mut assembler = FragmentAssembler::default();
        let mut src_bytes = vec![0u8; 256];
        let src_buf = AtomicBuffer::new(&mut src_bytes);

        let mut header_bytes = vec![0u8; 64];
        let header_buf = AtomicBuffer::new(&mut header_bytes);
        let data_header = DataHeaderFlyweight::new(header_buf.clone());
        data_header.set_flags(END_FRAG_FLAG);
        data_header.set_session_id(SESSION_ID);
        data_header.set_term_offset(HEADER_LENGTH);

        let mut header = Header::new(INITIAL_TERM_ID, 16);
        header.set_buffer(header_buf);

        let mut called = false;
        assembler.on_fragment(
            &src_buf,
            HEADER_LENGTH as usize,
            16,
            &header,
            |_, _, _, _| called = true,
        );

        assert!(!called);
    }

    #[test]
    fn should_reset_session_after_a_fragment_gap() {
        let mut assembler = FragmentAssembler::default();
        let mut src_bytes = vec![0u8; 512];
        let src_buf = AtomicBuffer::new(&mut src_bytes);

        let mut begin_bytes = vec![0u8; 64];
        let begin_buf = AtomicBuffer::new(&mut begin_bytes);
        let begin_data = DataHeaderFlyweight::new(begin_buf.clone());
        begin_data.set_flags(BEGIN_FRAG_FLAG);
        begin_data.set_session_id(SESSION_ID);
        begin_data.set_term_offset(HEADER_LENGTH);
        begin_data.set_frame_length(HEADER_LENGTH + 16);
        let mut begin_header = Header::new(INITIAL_TERM_ID, 16);
        begin_header.set_buffer(begin_buf);
        assembler.on_fragment(
            &src_buf,
            HEADER_LENGTH as usize,
            16,
            &begin_header,
            |_, _, _, _| {},
        );

        let mut middle_bytes = vec![0u8; 64];
        let middle_buf = AtomicBuffer::new(&mut middle_bytes);
        let middle_data = DataHeaderFlyweight::new(middle_buf.clone());
        middle_data.set_flags(0);
        middle_data.set_session_id(SESSION_ID);
        middle_data.set_term_offset(HEADER_LENGTH + 64);
        let mut middle_header = Header::new(INITIAL_TERM_ID, 16);
        middle_header.set_buffer(middle_buf);
        assembler.on_fragment(
            &src_buf,
            (HEADER_LENGTH + 64) as usize,
            16,
            &middle_header,
            |_, _, _, _| {},
        );

        let mut end_bytes = vec![0u8; 64];
        let end_buf = AtomicBuffer::new(&mut end_bytes);
        let end_data = DataHeaderFlyweight::new(end_buf.clone());
        end_data.set_flags(END_FRAG_FLAG);
        end_data.set_session_id(SESSION_ID);
        end_data.set_term_offset(HEADER_LENGTH + 16);
        let mut end_header = Header::new(INITIAL_TERM_ID, 16);
        end_header.set_buffer(end_buf);

        let mut called = false;
        assembler.on_fragment(
            &src_buf,
            (HEADER_LENGTH + 16) as usize,
            16,
            &end_header,
            |_, _, _, _| called = true,
        );

        assert!(!called);
    }

    #[test]
    fn should_pass_through_unfragmented_controlled_message() {
        let mut header_bytes = vec![0u8; 64];
        let header_buf = AtomicBuffer::new(&mut header_bytes);
        let data_header = DataHeaderFlyweight::new(header_buf.clone());
        data_header.set_flags(UNFRAGMENTED);
        data_header.set_session_id(SESSION_ID);

        let mut header = Header::new(INITIAL_TERM_ID, 16);
        header.set_buffer(header_buf);
        header.set_offset(0);

        let mut assembler = ControlledFragmentAssembler::default();
        let mut src_bytes = vec![0u8; 128];
        let src_buf = AtomicBuffer::new(&mut src_bytes);

        let mut call_count = 0;
        let action =
            assembler.on_fragment(&src_buf, 8, 32, &header, |_buf, offset, length, hdr| {
                call_count += 1;
                assert_eq!(offset, 8);
                assert_eq!(length, 32);
                assert_eq!(hdr.session_id(), SESSION_ID);
                ControlledPollAction::Abort
            });

        assert_eq!(call_count, 1);
        assert_eq!(action, ControlledPollAction::Abort);
    }

    #[test]
    fn should_assemble_two_part_controlled_message_and_restore_limit_on_abort() {
        let mut assembler = ControlledFragmentAssembler::default();
        let mut src_bytes = vec![0u8; 2048];
        for (i, b) in src_bytes.iter_mut().enumerate() {
            *b = (i % 256) as u8;
        }
        let src_buf = AtomicBuffer::new(&mut src_bytes);
        let length = 512;

        let mut header_bytes_1 = vec![0u8; 64];
        let header_buf_1 = AtomicBuffer::new(&mut header_bytes_1);
        let data_hdr_1 = DataHeaderFlyweight::new(header_buf_1.clone());
        data_hdr_1.set_flags(BEGIN_FRAG_FLAG);
        data_hdr_1.set_session_id(SESSION_ID);
        data_hdr_1.set_term_offset(HEADER_LENGTH);
        data_hdr_1.set_frame_length(HEADER_LENGTH + length as i32);

        let mut header_1 = Header::new(INITIAL_TERM_ID, 16);
        header_1.set_buffer(header_buf_1);

        let action1 = assembler.on_fragment(
            &src_buf,
            HEADER_LENGTH as usize,
            length,
            &header_1,
            |_, _, _, _| ControlledPollAction::Continue,
        );
        assert_eq!(action1, ControlledPollAction::Continue);

        let second_frag_offset = align(
            HEADER_LENGTH + length as i32 + HEADER_LENGTH,
            FRAME_ALIGNMENT,
        );
        let mut header_bytes_2 = vec![0u8; 64];
        let header_buf_2 = AtomicBuffer::new(&mut header_bytes_2);
        let data_hdr_2 = DataHeaderFlyweight::new(header_buf_2.clone());
        data_hdr_2.set_flags(END_FRAG_FLAG);
        data_hdr_2.set_session_id(SESSION_ID);
        data_hdr_2.set_term_offset(second_frag_offset);
        data_hdr_2.set_frame_length(HEADER_LENGTH + length as i32);

        let mut header_2 = Header::new(INITIAL_TERM_ID, 16);
        header_2.set_buffer(header_buf_2);

        // First delivery: handler returns Abort
        let mut attempts = 0;
        let action2 = assembler.on_fragment(
            &src_buf,
            second_frag_offset as usize,
            length,
            &header_2,
            |buf, offset, len, _hdr| {
                attempts += 1;
                assert_eq!(offset, 0);
                assert_eq!(len, length * 2);
                assert_eq!(buf.get_u8(0), src_buf.get_u8(HEADER_LENGTH as usize));
                ControlledPollAction::Abort
            },
        );
        assert_eq!(action2, ControlledPollAction::Abort);
        assert_eq!(attempts, 1);

        // Second delivery (retry on subsequent poll): handler returns Continue
        let action3 = assembler.on_fragment(
            &src_buf,
            second_frag_offset as usize,
            length,
            &header_2,
            |_buf, offset, len, _hdr| {
                attempts += 1;
                assert_eq!(offset, 0);
                assert_eq!(len, length * 2);
                ControlledPollAction::Continue
            },
        );
        assert_eq!(action3, ControlledPollAction::Continue);
        assert_eq!(attempts, 2);

        // Session buffer free check
        assert!(assembler.free_session_buffer(SESSION_ID));
        assert!(!assembler.free_session_buffer(SESSION_ID));
    }
}
