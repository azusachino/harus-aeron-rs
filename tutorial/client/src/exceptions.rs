//! Learner stub for client exception helpers.

#[derive(Debug, thiserror::Error)]
#[error("storage space exhausted")]
pub struct StorageSpaceException;

pub fn is_storage_space_error() -> bool { todo!("implement is_storage_space_error") }
