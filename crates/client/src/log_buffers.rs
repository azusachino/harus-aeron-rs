//! LogBuffers: memory-mapped or in-memory abstraction over Aeron 3-term log layout.
//!
//! The raw bytes live behind an `Arc` so that every `LogBuffers` clone — and every
//! `AtomicBuffer` handed out by [`LogBuffers::duplicate_term_buffers`] /
//! [`LogBuffers::meta_data_buffer`] — refers to the *same* underlying memory. This mirrors
//! upstream's memory-mapped log file being shared (zero-copy) across the driver, publishers,
//! and subscribers; mutation happens exclusively through `AtomicBuffer`'s atomic operations,
//! the same way `UnsafeBuffer` mutation works concurrently across threads/processes upstream.

use crate::error::{AeronError, Result};
use crate::logbuffer::{
    LOG_META_DATA_LENGTH, PARTITION_COUNT, check_page_size, check_term_length, page_size,
    set_page_size, set_term_length, term_length,
};
use agrona::AtomicBuffer;
use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::Arc;

enum BufferStorage {
    /// Shared, file-backed `MAP_SHARED` mapping — writes are visible to every other
    /// mapping of the same log file, across threads and processes (real Aeron semantics).
    Mmap(MmapMut),
    /// Private in-memory buffer for tests and IPC-less scenarios.
    Bytes(Vec<u8>),
}

impl BufferStorage {
    #[inline]
    fn as_ptr(&self) -> *const u8 {
        match self {
            BufferStorage::Mmap(m) => m.as_ptr(),
            BufferStorage::Bytes(v) => v.as_ptr(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            BufferStorage::Mmap(m) => m.len(),
            BufferStorage::Bytes(v) => v.len(),
        }
    }
}

struct LogBuffersInner {
    storage: BufferStorage,
    term_length: usize,
}

/// Represents an Aeron log file containing 3 term buffers and a log metadata buffer.
///
/// Cheap to clone: clones share the same underlying byte storage via `Arc`.
#[derive(Clone)]
pub struct LogBuffers(Arc<LogBuffersInner>);

impl LogBuffers {
    /// Load and validate an Aeron log buffer file into a private in-memory copy.
    ///
    /// This does **not** share memory with other mappers of the file; use
    /// [`LogBuffers::map_existing`] for the shared, cross-process log buffer.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let p = path.as_ref();
        let log_length = File::open(p)?.metadata()?.len() as usize;
        if log_length < LOG_META_DATA_LENGTH {
            return Err(AeronError::IllegalState(format!(
                "Log file length less than min length of {}: length={}",
                LOG_META_DATA_LENGTH, log_length
            )));
        }
        Self::from_storage(BufferStorage::Bytes(std::fs::read(p)?))
    }

    /// Create a new log file of `term_length * 3 + metadata` bytes, memory-map it
    /// `MAP_SHARED`, initialize its metadata (term length + page size), and return a shared
    /// handle. Writes are visible to every other [`LogBuffers::map_existing`] handle for the
    /// same path, across threads and processes — the driver creates the file, publishers and
    /// subscribers map it.
    pub fn map_new<P: AsRef<Path>>(path: P, term_length: i32, page_size: i32) -> Result<Self> {
        if let Err(msg) = check_term_length(term_length) {
            return Err(AeronError::IllegalState(msg));
        }
        if let Err(msg) = check_page_size(page_size) {
            return Err(AeronError::IllegalState(msg));
        }

        let total_len = term_length as usize * PARTITION_COUNT as usize + LOG_META_DATA_LENGTH;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(total_len as u64)?;

        // Safety: the file was just sized to `total_len` and is mapped writable.
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        // Initialize the metadata region so `from_storage` validation passes and every
        // mapper sees the correct term/page geometry.
        let meta_offset = total_len - LOG_META_DATA_LENGTH;
        // Safety: `meta_offset..total_len` lies within the freshly mapped region.
        let meta_buf = unsafe {
            AtomicBuffer::from_raw_parts(
                (mmap.as_ptr() as *mut u8).add(meta_offset),
                LOG_META_DATA_LENGTH,
            )
        };
        set_term_length(&meta_buf, term_length);
        set_page_size(&meta_buf, page_size);

        Self::from_storage(BufferStorage::Mmap(mmap))
    }

    /// Memory-map an existing log file `MAP_SHARED` and validate its metadata. Shares memory
    /// with the [`LogBuffers::map_new`] creator and any other mapper of the same path.
    pub fn map_existing<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let log_length = file.metadata()?.len() as usize;
        if log_length < LOG_META_DATA_LENGTH {
            return Err(AeronError::IllegalState(format!(
                "Log file length less than min length of {}: length={}",
                LOG_META_DATA_LENGTH, log_length
            )));
        }
        // Safety: file length validated above; mapped writable for shared pub/sub access.
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Self::from_storage(BufferStorage::Mmap(mmap))
    }

    /// Construct `LogBuffers` from an in-memory byte buffer (tests / IPC-less use).
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        Self::from_storage(BufferStorage::Bytes(bytes))
    }

    fn from_storage(storage: BufferStorage) -> Result<Self> {
        let log_length = storage.len();
        if log_length < LOG_META_DATA_LENGTH {
            return Err(AeronError::IllegalState(format!(
                "Log file length less than min length of {}: length={}",
                LOG_META_DATA_LENGTH, log_length
            )));
        }

        let meta_offset = log_length - LOG_META_DATA_LENGTH;
        // Safety: `meta_offset..log_length` is within `storage`; used read-only for validation.
        let meta_buf = unsafe {
            AtomicBuffer::from_raw_parts(
                (storage.as_ptr() as *mut u8).add(meta_offset),
                LOG_META_DATA_LENGTH,
            )
        };

        let term_len = term_length(&meta_buf);
        let page_sz = page_size(&meta_buf);

        if let Err(msg) = check_term_length(term_len) {
            return Err(AeronError::IllegalState(msg));
        }
        if let Err(msg) = check_page_size(page_sz) {
            return Err(AeronError::IllegalState(msg));
        }

        Ok(Self(Arc::new(LogBuffersInner {
            storage,
            term_length: term_len as usize,
        })))
    }

    /// Returns the length of each term in bytes.
    pub fn term_length(&self) -> usize {
        self.0.term_length
    }

    /// Duplicate the term buffers for thread-local or concurrent access.
    ///
    /// Every returned [`AtomicBuffer`] aliases the same underlying storage as `self` and any
    /// clone of `self` — writes through one are visible through all of them, matching
    /// upstream's shared memory-mapped log semantics.
    pub fn duplicate_term_buffers(&self) -> Vec<AtomicBuffer<'static>> {
        let term_len = self.0.term_length;
        let total_terms_len = term_len * PARTITION_COUNT as usize;
        let mut buffers = Vec::with_capacity(PARTITION_COUNT as usize);

        if self.0.storage.len() >= total_terms_len {
            let base_ptr = self.0.storage.as_ptr() as *mut u8;
            for i in 0..PARTITION_COUNT as usize {
                // Safety: `base_ptr` points into `self.0.raw_buffer`, which is kept alive for
                // as long as any clone of this `Arc` (including `self`) is alive, and is never
                // resized/reallocated after construction — only mutated in place via
                // `AtomicBuffer`'s atomic operations.
                let ptr = unsafe { base_ptr.add(i * term_len) };
                buffers.push(unsafe { AtomicBuffer::from_raw_parts(ptr, term_len) });
            }
        }

        buffers
    }

    /// Get an atomic buffer view over the metadata section.
    ///
    /// Aliases the same underlying storage as `self` and any clone of `self`; see
    /// [`LogBuffers::duplicate_term_buffers`].
    pub fn meta_data_buffer(&self) -> AtomicBuffer<'static> {
        let meta_offset = self.0.storage.len() - LOG_META_DATA_LENGTH;
        // Safety: see `duplicate_term_buffers`.
        let ptr = unsafe { (self.0.storage.as_ptr() as *mut u8).add(meta_offset) };
        unsafe { AtomicBuffer::from_raw_parts(ptr, LOG_META_DATA_LENGTH) }
    }

    /// Touch one byte per page in every mapped region, matching Aeron's optional pre-touch
    /// startup behavior. The buffer remains shared and is not copied.
    pub fn pre_touch(&self) {
        for buffer in self.duplicate_term_buffers() {
            for offset in (0..buffer.capacity()).step_by(4096) {
                let _ = buffer.get_u8(offset);
            }
        }
        for offset in (0..LOG_META_DATA_LENGTH).step_by(4096) {
            let _ = self.meta_data_buffer().get_u8(offset);
        }
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

        let log_buffers = LogBuffers::from_bytes(contents).unwrap();
        assert_eq!(log_buffers.term_length(), term_len);
        let terms = log_buffers.duplicate_term_buffers();
        assert_eq!(terms.len(), 3);
        assert_eq!(terms[0].capacity(), term_len);
    }

    #[test]
    fn map_new_then_map_existing_share_memory_across_independent_mappings() {
        // Two independent `MAP_SHARED` mappings of the same file must observe each other's
        // writes — the property the driver relies on to hand a log buffer to publishers and
        // subscribers (and, across processes, to separate client instances).
        let path = std::env::temp_dir().join(format!(
            "aeron-logbuffers-test-{}-{:?}.dat",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);

        let writer = LogBuffers::map_new(&path, TERM_MIN_LENGTH, PAGE_MIN_SIZE).unwrap();
        let reader = LogBuffers::map_existing(&path).unwrap();

        assert_eq!(writer.term_length(), TERM_MIN_LENGTH as usize);
        assert_eq!(reader.term_length(), TERM_MIN_LENGTH as usize);

        // Write through the creator's term buffer, read through the independent mapping.
        writer.duplicate_term_buffers()[0].put_i32(64, 0x0BAD_F00D);
        assert_eq!(reader.duplicate_term_buffers()[0].get_i32(64), 0x0BAD_F00D);

        // And the reverse direction, through the metadata region.
        reader
            .meta_data_buffer()
            .put_i64(128, 0x0102_0304_0506_0708);
        assert_eq!(
            writer.meta_data_buffer().get_i64(128),
            0x0102_0304_0506_0708
        );

        drop(writer);
        drop(reader);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn duplicate_term_buffers_share_the_same_underlying_memory() {
        let term_len = TERM_MIN_LENGTH as usize;
        let total_len = term_len * 3 + LOG_META_DATA_LENGTH;
        let mut contents = vec![0u8; total_len];

        let meta_offset = total_len - LOG_META_DATA_LENGTH;
        let mut meta_slice = contents[meta_offset..].to_vec();
        let meta_buf = AtomicBuffer::new(&mut meta_slice);
        set_term_length(&meta_buf, TERM_MIN_LENGTH);
        set_page_size(&meta_buf, PAGE_MIN_SIZE);
        contents[meta_offset..].copy_from_slice(&meta_slice);

        let log_buffers = LogBuffers::from_bytes(contents).unwrap();

        // A write through one handle's term-buffer view must be visible through a
        // freshly-duplicated view from a clone of the same LogBuffers.
        let writer_terms = log_buffers.duplicate_term_buffers();
        writer_terms[0].put_i32(0, 0x1234_5678);

        let cloned = log_buffers.clone();
        let reader_terms = cloned.duplicate_term_buffers();
        assert_eq!(reader_terms[0].get_i32(0), 0x1234_5678);

        let meta_writer = log_buffers.meta_data_buffer();
        meta_writer.put_i32(0, 42);
        let meta_reader = cloned.meta_data_buffer();
        assert_eq!(meta_reader.get_i32(0), 42);
    }
}
