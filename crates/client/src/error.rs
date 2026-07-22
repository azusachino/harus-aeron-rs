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
