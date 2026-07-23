//! Client error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AeronError {
    #[error("failed to write command to driver: {0}")]
    CommandWriteFailed(String),

    #[error("driver error for correlation {correlation_id}: {message} (code {code})")]
    DriverResponse {
        correlation_id: i64,
        code: i32,
        message: String,
    },

    #[error("media driver timed out client {0}")]
    ClientTimeout(i64),

    #[error("media driver did not respond within {0} ms")]
    DriverTimeout(u64),

    #[error("registration {registration_id} is a {actual}, not a {expected}")]
    WrongResourceType {
        registration_id: i64,
        expected: &'static str,
        actual: &'static str,
    },

    #[error("{0}")]
    IllegalState(String),

    #[error("{0}")]
    IllegalArgument(String),

    #[error("context was concluded more than once")]
    ConcurrentConclude,

    #[error("{0}")]
    Configuration(String),

    #[error("channel URI error: {0}")]
    ChannelUri(#[from] crate::uri::ChannelUriError),

    #[error("agrona error: {0}")]
    Agrona(#[from] agrona::AgronaError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, AeronError>;
