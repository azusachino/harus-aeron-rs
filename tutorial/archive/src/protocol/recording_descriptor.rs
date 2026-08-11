//! Learner stub for RecordingDescriptor.
//!
//! Reference implementation: `crates/archive/src/protocol/recording_descriptor.rs`.

#[allow(dead_code)]
pub struct RecordingDescriptor;

impl RecordingDescriptor {
    pub fn new() {
        todo!("implement RecordingDescriptor")
    }

    pub fn encode() -> Vec<u8> {
        todo!("encode the v13 template-22 fixed block and three ASCII fields")
    }

    pub fn decode() -> Self {
        todo!("validate and decode a complete RecordingDescriptor frame")
    }
}
