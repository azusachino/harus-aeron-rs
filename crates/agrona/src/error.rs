//! Error types for the Agrona crate.

/// The result type for Agrona operations.
pub type Result<T> = std::result::Result<T, AgronaError>;

/// Errors that can occur when using Agrona primitives.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum AgronaError {
    /// Memory access is out of bounds.
    #[error("Out of bounds: offset {offset}, length {length}, buffer capacity {capacity}")]
    OutOfBounds {
        /// The starting offset of the access.
        offset: usize,
        /// The length of the access.
        length: usize,
        /// The capacity of the buffer.
        capacity: usize,
    },

    /// Memory access does not meet the alignment requirement.
    #[error("Invalid alignment: offset {offset} is not aligned to {required_alignment}")]
    Alignment {
        /// The starting offset of the access.
        offset: usize,
        /// The required alignment in bytes.
        required_alignment: usize,
    },

    /// String decoding failed due to invalid UTF-8.
    #[error("Invalid UTF-8 string: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    /// Invalid capacity (e.g. not a power of two or too small).
    #[error("Invalid capacity: {0}. Must be a power of two.")]
    InvalidCapacity(usize),

    /// Receiver was lapped (overtaken by transmitter).
    #[error("Receiver lapped: cursor {cursor}, tail {tail}")]
    Lapped {
        /// The receiver's cursor position before reset.
        cursor: i64,
        /// The transmitter's tail position.
        tail: i64,
    },
}
