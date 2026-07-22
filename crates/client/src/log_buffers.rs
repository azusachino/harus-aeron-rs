//! LogBuffers: memory-mapped or in-memory abstraction over Aeron 3-term log layout.

use crate::error::{AeronError, Result};
use crate::logbuffer::{
    LOG_META_DATA_LENGTH, PARTITION_COUNT, check_page_size, check_term_length, page_size,
    term_length,
};
use agrona::AtomicBuffer;
use std::fs::File;
use std::path::Path;

#[allow(dead_code)]
enum BufferStorage {
    File(std::fs::File),
    Bytes(Vec<u8>),
}

/// Represents an Aeron log file containing 3 term buffers and a log metadata buffer.
pub struct LogBuffers {
    _storage: BufferStorage,
    raw_buffer: Vec<u8>,
    term_length: usize,
}

impl LogBuffers {
    /// Load and validate an Aeron log buffer file from a path on disk.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let p = path.as_ref();
        let file = File::open(p)?;
        let metadata = file.metadata()?;
        let log_length = metadata.len() as usize;

        if log_length < LOG_META_DATA_LENGTH {
            return Err(AeronError::IllegalState(format!(
                "Log file length less than min length of {}: length={}",
                LOG_META_DATA_LENGTH, log_length
            )));
        }

        let bytes = std::fs::read(p)?;
        Self::from_storage(BufferStorage::File(file), bytes)
    }

    /// Construct `LogBuffers` from an in-memory byte buffer.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        Self::from_storage(BufferStorage::Bytes(vec![]), bytes)
    }

    fn from_storage(storage: BufferStorage, raw_buffer: Vec<u8>) -> Result<Self> {
        let log_length = raw_buffer.len();
        if log_length < LOG_META_DATA_LENGTH {
            return Err(AeronError::IllegalState(format!(
                "Log file length less than min length of {}: length={}",
                LOG_META_DATA_LENGTH, log_length
            )));
        }

        let meta_offset = log_length - LOG_META_DATA_LENGTH;
        let mut meta_slice = raw_buffer[meta_offset..].to_vec();
        let meta_buf = AtomicBuffer::new(&mut meta_slice);

        let term_len = term_length(&meta_buf);
        let page_sz = page_size(&meta_buf);

        if let Err(msg) = check_term_length(term_len) {
            return Err(AeronError::IllegalState(msg));
        }

        if let Err(msg) = check_page_size(page_sz) {
            return Err(AeronError::IllegalState(msg));
        }

        Ok(Self {
            _storage: storage,
            raw_buffer,
            term_length: term_len as usize,
        })
    }

    /// Returns the length of each term in bytes.
    pub fn term_length(&self) -> usize {
        self.term_length
    }

    /// Duplicate the term buffers for thread-local or concurrent access.
    pub fn duplicate_term_buffers(&mut self) -> Vec<AtomicBuffer<'_>> {
        let term_len = self.term_length;
        let total_terms_len = term_len * PARTITION_COUNT as usize;
        let mut buffers = Vec::with_capacity(PARTITION_COUNT as usize);

        if self.raw_buffer.len() >= total_terms_len {
            let mut remaining = &mut self.raw_buffer[..total_terms_len];
            for _ in 0..PARTITION_COUNT as usize {
                let (first, rest) = remaining.split_at_mut(term_len);
                buffers.push(AtomicBuffer::new(first));
                remaining = rest;
            }
        }

        buffers
    }

    /// Get atomic buffer view over the metadata section.
    pub fn meta_data_buffer(&mut self) -> AtomicBuffer<'_> {
        let meta_offset = self.raw_buffer.len() - LOG_META_DATA_LENGTH;
        let slice = &mut self.raw_buffer[meta_offset..];
        AtomicBuffer::new(slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logbuffer::{PAGE_MIN_SIZE, TERM_MIN_LENGTH, set_page_size, set_term_length};

    #[test]
    fn throws_illegal_state_exception_if_log_file_size_is_less_than_log_meta_data_length() {
        let file_length = LOG_META_DATA_LENGTH - 5;
        let contents = vec![0u8; file_length];

        let res = LogBuffers::from_bytes(contents);
        assert!(res.is_err());
        let err = res.err().unwrap().to_string();
        assert_eq!(
            err,
            format!(
                "Log file length less than min length of {}: length={}",
                LOG_META_DATA_LENGTH, file_length
            )
        );
    }

    #[test]
    fn throws_illegal_state_exception_if_term_length_is_invalid() {
        let mut contents = vec![0u8; LOG_META_DATA_LENGTH];
        let meta_buf = AtomicBuffer::new(&mut contents);
        set_term_length(&meta_buf, 100);

        let res = LogBuffers::from_bytes(contents);
        assert!(res.is_err());
        let err = res.err().unwrap().to_string();
        assert!(err.starts_with("Term length"));
        assert!(err.ends_with("length=100"));
    }

    #[test]
    fn throws_illegal_state_exception_if_page_size_is_invalid() {
        let mut contents = vec![0u8; LOG_META_DATA_LENGTH];
        let meta_buf = AtomicBuffer::new(&mut contents);
        set_term_length(&meta_buf, TERM_MIN_LENGTH);
        set_page_size(&meta_buf, 100);

        let res = LogBuffers::from_bytes(contents);
        assert!(res.is_err());
        let err = res.err().unwrap().to_string();
        assert!(err.starts_with("Page size"));
        assert!(err.ends_with("page size=100"));
    }

    #[test]
    fn should_load_valid_log_buffer() {
        let term_len = TERM_MIN_LENGTH as usize;
        let total_len = term_len * 3 + LOG_META_DATA_LENGTH;
        let mut contents = vec![0u8; total_len];

        let meta_offset = total_len - LOG_META_DATA_LENGTH;
        let mut meta_slice = contents[meta_offset..].to_vec();
        let meta_buf = AtomicBuffer::new(&mut meta_slice);
        set_term_length(&meta_buf, TERM_MIN_LENGTH);
        set_page_size(&meta_buf, PAGE_MIN_SIZE);
        contents[meta_offset..].copy_from_slice(&meta_slice);

        let mut log_buffers = LogBuffers::from_bytes(contents).unwrap();
        assert_eq!(log_buffers.term_length(), term_len);
        let terms = log_buffers.duplicate_term_buffers();
        assert_eq!(terms.len(), 3);
        assert_eq!(terms[0].capacity(), term_len);
    }
}
