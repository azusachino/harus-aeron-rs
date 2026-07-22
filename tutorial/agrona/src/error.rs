//! Error types for the Agrona tutorial crate.

pub type Result<T> = std::result::Result<T, AgronaError>;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum AgronaError {
    #[error("Out of bounds: offset {offset}, length {length}, buffer capacity {capacity}")]
    OutOfBounds {
        offset: usize,
        length: usize,
        capacity: usize,
    },

    #[error("Invalid alignment: offset {offset} is not aligned to {required_alignment}")]
    Alignment {
        offset: usize,
        required_alignment: usize,
    },

    #[error("Invalid UTF-8 string: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    #[error("Invalid capacity: {0}. Must be a power of two.")]
    InvalidCapacity(usize),

    #[error("Receiver lapped: cursor {cursor}, tail {tail}")]
    Lapped {
        cursor: i64,
        tail: i64,
    },
}
