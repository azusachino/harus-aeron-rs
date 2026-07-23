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

pub type Result<T> = std::result::Result<T, DriverError>;
