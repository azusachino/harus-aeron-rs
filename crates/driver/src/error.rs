//! Media Driver error types.

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DriverError {
    #[error("{0}")]
    InvalidChannel(String),

    #[error("{0}")]
    IllegalState(String),

    #[error("{0}")]
    IllegalArgument(String),
}

impl From<aeron_client::error::AeronError> for DriverError {
    fn from(err: aeron_client::error::AeronError) -> Self {
        DriverError::IllegalState(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, DriverError>;
