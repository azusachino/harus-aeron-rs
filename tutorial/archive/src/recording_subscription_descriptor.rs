//! Learner stub for the recording-subscription descriptor response.
//!
//! Reference implementation: `crates/archive/src/recording_subscription_descriptor.rs`.

#[allow(dead_code)]
pub struct RecordingSubscriptionDescriptor;

impl RecordingSubscriptionDescriptor {
    pub fn encode() -> Vec<u8> {
        todo!("encode the template-23 fixed block and stripped channel")
    }

    pub fn decode() -> Self {
        todo!("validate and decode a complete recording-subscription descriptor")
    }
}
