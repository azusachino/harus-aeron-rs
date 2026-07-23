//! Learner stub for the client-to-driver response protocol.

pub const ON_ERROR: i32 = 0x0F01;
pub const ON_AVAILABLE_IMAGE: i32 = 0x0F02;
pub const ON_PUBLICATION_READY: i32 = 0x0F03;
pub const ON_SUBSCRIPTION_READY: i32 = 0x0F07;
pub const ON_CLIENT_TIMEOUT: i32 = 0x0F0A;

#[derive(Debug)]
pub enum DriverEvent {
    PublicationReady,
    SubscriptionReady,
    AvailableImage,
    Error,
    ClientTimeout,
}

pub fn decode(_message_type: i32, _buffer: &[u8]) -> DriverEvent {
    todo!("decode driver response flyweights")
}
