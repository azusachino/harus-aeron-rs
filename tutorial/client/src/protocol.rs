//! Learner stub for wire protocol frame codecs.
//! Reference implementation: `crates/client/src/protocol.rs`.

use tutorial_agrona::AtomicBuffer;

#[derive(Copy, Clone)]
pub struct HeaderFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
}

impl<'a> HeaderFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>) -> Self {
        Self { buffer }
    }

    pub fn frame_length(&self) -> i32 {
        todo!("implement frame_length")
    }

    pub fn set_frame_length(&self, _length: i32) -> &Self {
        todo!("implement set_frame_length")
    }
}

#[derive(Copy, Clone)]
pub struct DataHeaderFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
}

impl<'a> DataHeaderFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>) -> Self {
        Self { buffer }
    }

    pub fn term_offset(&self) -> i32 {
        todo!("implement term_offset")
    }
}
