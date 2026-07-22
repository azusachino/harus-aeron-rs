//! Client error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AeronError {
    #[error("failed to write command to driver: {0}")]
    CommandWriteFailed(String),

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
