//! Driver log allocation boundary for publications and images.

use std::path::{Path, PathBuf};

use aeron_client::log_buffers::LogBuffers;
use aeron_client::logbuffer::PAGE_MIN_SIZE;

use crate::error::{DriverError, Result};

pub trait LogFactory {
    fn new_publication(
        &self,
        correlation_id: i64,
        term_buffer_length: i32,
        use_sparse_files: bool,
    ) -> Result<LogBuffers>;

    fn new_image(
        &self,
        correlation_id: i64,
        term_buffer_length: i32,
        use_sparse_files: bool,
    ) -> Result<LogBuffers>;
}

/// In-memory factory for deterministic unit tests and components without a filesystem.
pub struct TestLogFactory;

impl TestLogFactory {
    pub fn new_log_buffers(term_length: i32) -> Result<LogBuffers> {
        Ok(LogBuffers::from_zeroed(term_length, PAGE_MIN_SIZE)?)
    }
}

impl LogFactory for TestLogFactory {
    fn new_publication(
        &self,
        _correlation_id: i64,
        term_buffer_length: i32,
        _use_sparse_files: bool,
    ) -> Result<LogBuffers> {
        Self::new_log_buffers(term_buffer_length)
    }

    fn new_image(
        &self,
        _correlation_id: i64,
        term_buffer_length: i32,
        _use_sparse_files: bool,
    ) -> Result<LogBuffers> {
        Self::new_log_buffers(term_buffer_length)
    }
}

/// File-backed factory used by the driver when allocating publication or image logs.
pub struct FileStoreLogFactory {
    data_dir: PathBuf,
    page_size: i32,
}

impl FileStoreLogFactory {
    pub fn new(data_dir: impl Into<PathBuf>, page_size: i32) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)
            .map_err(|error| DriverError::IllegalState(format!("create log directory: {error}")))?;
        Ok(Self {
            data_dir,
            page_size,
        })
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn publication_path(&self, correlation_id: i64) -> PathBuf {
        self.data_dir
            .join(format!("publication-{correlation_id}.logbuffer"))
    }

    pub fn image_path(&self, correlation_id: i64) -> PathBuf {
        self.data_dir
            .join(format!("image-{correlation_id}.logbuffer"))
    }

    fn new_log(
        &self,
        prefix: &str,
        correlation_id: i64,
        term_buffer_length: i32,
        _use_sparse_files: bool,
    ) -> Result<LogBuffers> {
        let path = match prefix {
            "publication" => self.publication_path(correlation_id),
            "image" => self.image_path(correlation_id),
            other => self
                .data_dir
                .join(format!("{other}-{correlation_id}.logbuffer")),
        };
        LogBuffers::map_new(path, term_buffer_length, self.page_size).map_err(Into::into)
    }
}

impl LogFactory for FileStoreLogFactory {
    fn new_publication(
        &self,
        correlation_id: i64,
        term_buffer_length: i32,
        use_sparse_files: bool,
    ) -> Result<LogBuffers> {
        self.new_log(
            "publication",
            correlation_id,
            term_buffer_length,
            use_sparse_files,
        )
    }

    fn new_image(
        &self,
        correlation_id: i64,
        term_buffer_length: i32,
        use_sparse_files: bool,
    ) -> Result<LogBuffers> {
        self.new_log(
            "image",
            correlation_id,
            term_buffer_length,
            use_sparse_files,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aeron_client::logbuffer::TERM_MIN_LENGTH;

    #[test]
    fn test_factory_creates_three_zeroed_terms_and_metadata() {
        let factory = TestLogFactory;
        let buffers = factory.new_publication(102, TERM_MIN_LENGTH, true).unwrap();
        assert_eq!(buffers.term_length(), TERM_MIN_LENGTH as usize);
        for term in buffers.duplicate_term_buffers() {
            assert_eq!(term.get_i8(0), 0);
            assert_eq!(term.get_i8(TERM_MIN_LENGTH as usize - 1), 0);
        }
        let metadata = buffers.meta_data_buffer();
        assert_eq!(metadata.get_i8(0), 0);
        assert_eq!(
            metadata.get_i8(aeron_client::logbuffer::LOG_META_DATA_LENGTH - 1),
            0
        );
    }

    #[test]
    fn file_factory_creates_distinct_publication_and_image_logs() {
        let directory =
            std::env::temp_dir().join(format!("aeron-log-factory-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        let factory = FileStoreLogFactory::new(&directory, PAGE_MIN_SIZE).unwrap();
        let publication = factory.new_publication(1, TERM_MIN_LENGTH, true).unwrap();
        let image = factory.new_image(1, TERM_MIN_LENGTH, false).unwrap();
        assert_eq!(publication.term_length(), TERM_MIN_LENGTH as usize);
        assert_eq!(image.term_length(), TERM_MIN_LENGTH as usize);
        assert!(directory.join("publication-1.logbuffer").exists());
        assert!(directory.join("image-1.logbuffer").exists());
        drop(publication);
        drop(image);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
