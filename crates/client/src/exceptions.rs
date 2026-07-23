//! Client exception helpers mirrored from Aeron's exception package.

use std::error::Error;

const NO_SPACE_LEFT_ON_DEVICE: &str = "No space left on device";
const NOT_ENOUGH_SPACE_ON_DISK: &str = "There is not enough space on the disk";

#[derive(Debug, thiserror::Error)]
#[error("storage space exhausted: {0}")]
pub struct StorageSpaceException(pub String);

/// Returns whether an error or any source in its chain indicates exhausted storage.
pub fn is_storage_space_error(error: &(dyn Error + 'static)) -> bool {
    if error.downcast_ref::<StorageSpaceException>().is_some() {
        return true;
    }

    let message = error.to_string();
    message.contains(NO_SPACE_LEFT_ON_DEVICE)
        || message.contains(NOT_ENOUGH_SPACE_ON_DISK)
        || error.source().is_some_and(is_storage_space_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn recognizes_storage_space_errors() {
        assert!(!is_storage_space_error(&io::Error::other("other")));
        assert!(is_storage_space_error(&StorageSpaceException(
            "test".to_string()
        )));
        assert!(is_storage_space_error(&io::Error::other(
            NO_SPACE_LEFT_ON_DEVICE
        )));
        assert!(is_storage_space_error(&io::Error::other(
            NOT_ENOUGH_SPACE_ON_DISK
        )));
    }
}
