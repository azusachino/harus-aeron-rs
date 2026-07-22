//! Learner stub for log buffer operations.
//! Reference implementation: `crates/client/src/logbuffer.rs`.

use tutorial_agrona::AtomicBuffer;

pub struct Header<'a> {
    buffer: Option<AtomicBuffer<'a>>,
    offset: usize,
}

impl<'a> Header<'a> {
    pub fn new(_initial_term_id: i32, _position_bits_to_shift: i32) -> Self {
        todo!("implement Header::new")
    }

    pub fn buffer(&self) -> Option<&AtomicBuffer<'a>> {
        self.buffer.as_ref()
    }

    pub fn offset(&self) -> usize {
        self.offset
    }
}

pub struct TermScanner;

impl TermScanner {
    pub fn scan_for_availability(
        _term_buffer: &AtomicBuffer,
        _offset: usize,
        _max_length: usize,
    ) -> i64 {
        todo!("implement scan_for_availability")
    }
}

pub struct TermReader;

impl TermReader {
    pub fn pack(_offset: i32, _fragments_read: i32) -> i64 {
        todo!("implement TermReader::pack")
    }
}
