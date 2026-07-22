//! Learner stub for client errors.

#[derive(Debug, thiserror::Error)]
pub enum AeronError {
    #[error("not implemented")]
    NotImplemented,
}
