//! Driver log allocation boundary for publications and images.

use std::path::{Path, PathBuf};

use aeron_client::log_buffers::LogBuffers;
use aeron_client::logbuffer::{
    LOG_META_DATA_LENGTH, PAGE_MIN_SIZE, PARTITION_COUNT, check_page_size, check_term_length,
};

use crate::configuration::{Configuration, LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT};
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
    perform_storage_checks: bool,
    low_storage_warning_threshold: u64,
}

impl FileStoreLogFactory {
    pub fn new(data_dir: impl Into<PathBuf>, page_size: i32) -> Result<Self> {
        Self::new_with_storage_checks(
            data_dir,
            page_size,
            true,
            LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT,
        )
    }

    pub fn from_configuration(
        data_dir: impl Into<PathBuf>,
        page_size: i32,
        configuration: &Configuration,
    ) -> Result<Self> {
        Self::new_with_storage_checks(
            data_dir,
            page_size,
            configuration.perform_storage_checks,
            configuration.low_file_store_warning_threshold,
        )
    }

    pub fn new_with_storage_checks(
        data_dir: impl Into<PathBuf>,
        page_size: i32,
        perform_storage_checks: bool,
        low_storage_warning_threshold: u64,
    ) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)
            .map_err(|error| DriverError::IllegalState(format!("create log directory: {error}")))?;
        Ok(Self {
            data_dir,
            page_size,
            perform_storage_checks,
            low_storage_warning_threshold,
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

    pub fn image_path_named(&self, file_name: &str) -> PathBuf {
        self.data_dir.join(file_name)
    }

    /// Allocate an image log with a conductor-selected stable filename.
    pub fn new_image_at(
        &self,
        file_name: &str,
        term_buffer_length: i32,
        use_sparse_files: bool,
    ) -> Result<LogBuffers> {
        self.new_log_at(
            self.image_path_named(file_name),
            term_buffer_length,
            use_sparse_files,
        )
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
        self.new_log_at(path, term_buffer_length, _use_sparse_files)
    }

    fn new_log_at(
        &self,
        path: PathBuf,
        term_buffer_length: i32,
        _use_sparse_files: bool,
    ) -> Result<LogBuffers> {
        check_term_length(term_buffer_length)
            .map_err(|message| DriverError::IllegalArgument(message.to_string()))?;
        check_page_size(self.page_size)
            .map_err(|message| DriverError::IllegalArgument(message.to_string()))?;
        let log_length = (term_buffer_length as u64)
            .checked_mul(PARTITION_COUNT as u64)
            .and_then(|length| length.checked_add(LOG_META_DATA_LENGTH as u64))
            .ok_or_else(|| DriverError::IllegalArgument("log length overflow".to_string()))?;
        self.check_storage(log_length)?;
        LogBuffers::map_new(path, term_buffer_length, self.page_size).map_err(Into::into)
    }

    fn check_storage(&self, log_length: u64) -> Result<()> {
        if !self.perform_storage_checks {
            return Ok(());
        }

        let Some(usable_space) = usable_space(&self.data_dir)? else {
            tracing::debug!(path = %self.data_dir.display(), "file-store capacity checks unavailable");
            return Ok(());
        };

        if let Some(warning) = storage_capacity_check(
            usable_space,
            log_length,
            self.low_storage_warning_threshold,
            &self.data_dir,
        )? {
            tracing::warn!(message = %warning, "file-store space is running low");
        }
        Ok(())
    }
}

/// Return a warning message when capacity is low, or an error when a new log cannot fit.
pub fn storage_capacity_check(
    usable_space: u64,
    log_length: u64,
    low_storage_warning_threshold: u64,
    file_store: &Path,
) -> Result<Option<String>> {
    if usable_space < log_length {
        return Err(DriverError::StorageSpace(format!(
            "insufficient usable storage for new log of length={log_length} usable={usable_space} in {}",
            file_store.display()
        )));
    }

    if usable_space <= low_storage_warning_threshold {
        return Ok(Some(format!(
            "space is running low: threshold={low_storage_warning_threshold} usable={usable_space} in {}",
            file_store.display()
        )));
    }

    Ok(None)
}

#[cfg(unix)]
fn usable_space(path: &Path) -> Result<Option<u64>> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path_bytes = path.as_os_str().as_bytes();
    let path = CString::new(path_bytes)
        .map_err(|_| DriverError::IllegalArgument("log directory contains NUL".to_string()))?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stats` points to writable storage of the expected
    // platform type. libc initialises it when the call succeeds.
    let result = unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) };
    if result != 0 {
        return Err(DriverError::IllegalState(format!(
            "stat file-store capacity: {}",
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: statvfs returned success and therefore initialised every field.
    let stats = unsafe { stats.assume_init() };
    Ok(Some(
        u64::from(stats.f_bavail).saturating_mul(stats.f_frsize),
    ))
}

#[cfg(not(unix))]
fn usable_space(_path: &Path) -> Result<Option<u64>> {
    Ok(None)
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

    #[test]
    fn storage_capacity_rejects_a_log_that_does_not_fit() {
        let error = storage_capacity_check(117, 118, 1_000, Path::new("test-fs")).unwrap_err();
        assert_eq!(
            error,
            DriverError::StorageSpace(
                "insufficient usable storage for new log of length=118 usable=117 in test-fs"
                    .to_string()
            )
        );
    }

    #[test]
    fn storage_capacity_reports_low_space_at_the_warning_threshold() {
        let warning = storage_capacity_check(117, 64, 117, Path::new("test-fs"))
            .unwrap()
            .unwrap();
        assert_eq!(
            warning,
            "space is running low: threshold=117 usable=117 in test-fs"
        );
    }
}
