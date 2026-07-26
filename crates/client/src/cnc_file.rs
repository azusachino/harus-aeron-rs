//! CnC (command-and-control) file: the shared, memory-mapped `cnc.dat` control file that
//! the media driver and every client map to exchange commands and events.
//!
//! Mirrors `io.aeron.CncFileDescriptor`. Layout:
//!
//! ```text
//! +-----------------------------+  0
//! | metadata header (version,   |
//! |  buffer lengths, liveness,  |
//! |  start ts, pid)             |
//! +-----------------------------+  META_DATA_LENGTH
//! | to-driver buffer            |  (ManyToOneRingBuffer: clients -> driver commands)
//! +-----------------------------+  + to_driver_length
//! | to-clients buffer           |  (Broadcast: driver -> clients events)
//! +-----------------------------+  + to_clients_length
//! | counter metadata            |
//! +-----------------------------+
//! | counter values              |
//! +-----------------------------+
//! | error log                   |
//! +-----------------------------+
//! ```
//!
//! Both buffer regions include their ring/broadcast trailer, so they can be wrapped directly
//! by [`agrona::ring_buffer::mpsc::ManyToOneRingBuffer`] and
//! [`agrona::BroadcastTransmitter`]/[`agrona::BroadcastReceiver`]. Because the file is mapped
//! `MAP_SHARED`, the driver and clients see each other's writes across processes.
//!
//! The metadata field offsets, region ordering, `META_DATA_LENGTH` (CACHE_LINE * 2 = 128), and
//! `CNC_VERSION` (SemanticVersion 0.2.0 = 512) match the checked-in upstream source:
//! `vendor/aeron/aeron-client/src/main/java/io/aeron/CncFileDescriptor.java`.

use crate::error::{AeronError, Result};
use agrona::AtomicBuffer;
use agrona::broadcast::TRAILER_LENGTH as BROADCAST_TRAILER_LENGTH;
use agrona::ring_buffer::TRAILER_LENGTH as RING_TRAILER_LENGTH;
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Current CnC file format version negotiated between driver and clients.
///
/// Matches `vendor/aeron/aeron-client/src/main/java/io/aeron/CncFileDescriptor.java`:
/// `CNC_VERSION = SemanticVersion.compose(0, 2, 0)` = `(0 << 16) | (2 << 8) | 0` = 512.
pub const CNC_VERSION: i32 = 512;
/// Conventional CnC file name inside the Aeron directory.
pub const CNC_FILE: &str = "cnc.dat";

/// Default file page size recorded in the CnC metadata.
pub const DEFAULT_FILE_PAGE_SIZE: i32 = 4096;

const CACHE_LINE_LENGTH: usize = 64;

const CNC_VERSION_OFFSET: usize = 0;
const TO_DRIVER_LENGTH_OFFSET: usize = 4;
const TO_CLIENTS_LENGTH_OFFSET: usize = 8;
const COUNTER_METADATA_LENGTH_OFFSET: usize = 12;
const COUNTER_VALUES_LENGTH_OFFSET: usize = 16;
const ERROR_LOG_LENGTH_OFFSET: usize = 20;
const CLIENT_LIVENESS_TIMEOUT_OFFSET: usize = 24;
const START_TIMESTAMP_OFFSET: usize = 32;
const PID_OFFSET: usize = 40;
const FILE_PAGE_SIZE_OFFSET: usize = 48;

const fn align(value: usize, alignment: usize) -> usize {
    (value + (alignment - 1)) & !(alignment - 1)
}

/// Length of the fixed metadata header, padded to a double cache line so the following
/// ring/broadcast buffers start cache-line aligned.
pub const META_DATA_LENGTH: usize = align(48, CACHE_LINE_LENGTH * 2);

/// Default to-driver buffer length (1 MiB of data plus the ring-buffer trailer).
pub const DEFAULT_TO_DRIVER_LENGTH: usize = (1 << 20) + RING_TRAILER_LENGTH;
/// Default to-clients buffer length (1 MiB of data plus the broadcast trailer).
pub const DEFAULT_TO_CLIENTS_LENGTH: usize = (1 << 20) + BROADCAST_TRAILER_LENGTH;
/// A compact but valid counter-values region for the standalone Rust driver.
pub const DEFAULT_COUNTER_VALUES_LENGTH: usize = 1 << 20;
/// Upstream's ratio is four metadata bytes for every value byte.
pub const DEFAULT_COUNTER_METADATA_LENGTH: usize = DEFAULT_COUNTER_VALUES_LENGTH * 4;
/// Error-log region; the Java driver defaults to 4 MiB, while the layout accepts any positive
/// region and the Rust driver does not yet write distinct error records.
pub const DEFAULT_ERROR_LOG_LENGTH: usize = 1 << 20;

/// Upstream Agrona counter layout constants used by Java `CountersReader`.
pub const COUNTER_METADATA_LENGTH: usize = 512;
pub const COUNTER_VALUE_LENGTH: usize = 128;
pub const COUNTER_TYPE_ID_OFFSET: usize = 4;
pub const COUNTER_REGISTRATION_ID_OFFSET: usize = 8;
pub const COUNTER_OWNER_ID_OFFSET: usize = 16;
pub const COUNTER_REFERENCE_ID_OFFSET: usize = 24;
pub const COUNTER_KEY_OFFSET: usize = 16;
pub const COUNTER_LABEL_OFFSET: usize = 128;
pub const COUNTER_KEY_MAX_LENGTH: usize = COUNTER_LABEL_OFFSET - COUNTER_KEY_OFFSET;
pub const COUNTER_LABEL_MAX_LENGTH: usize = 380;
pub const COUNTER_RECORD_ALLOCATED: i32 = 1;

enum CncStorage {
    Mmap(MmapMut),
    /// In-memory backing, only used by tests via [`CncFile::from_bytes`].
    #[allow(dead_code)]
    Bytes(Vec<u8>),
}

impl CncStorage {
    #[inline]
    fn as_ptr(&self) -> *const u8 {
        match self {
            CncStorage::Mmap(m) => m.as_ptr(),
            CncStorage::Bytes(v) => v.as_ptr(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            CncStorage::Mmap(m) => m.len(),
            CncStorage::Bytes(v) => v.len(),
        }
    }
}

struct CncFileInner {
    storage: CncStorage,
    to_driver_offset: usize,
    to_driver_length: usize,
    to_clients_offset: usize,
    to_clients_length: usize,
    counters_metadata_offset: usize,
    counters_metadata_length: usize,
    counters_values_offset: usize,
    counters_values_length: usize,
    error_log_offset: usize,
    error_log_length: usize,
}

/// A handle to a mapped `cnc.dat`. Cheap to clone; clones share the same mapping.
#[derive(Clone)]
pub struct CncFile(Arc<CncFileInner>);

impl CncFile {
    /// Create and map a new `cnc.dat`, sizing the to-driver and to-clients regions and
    /// writing the metadata header. The version field is published last (ordered) so a
    /// concurrent mapper never observes a half-initialized file.
    pub fn create_new<P: AsRef<Path>>(
        path: P,
        to_driver_length: usize,
        to_clients_length: usize,
        client_liveness_timeout_ns: i64,
    ) -> Result<Self> {
        Self::create_new_with_regions(
            path,
            to_driver_length,
            to_clients_length,
            0,
            0,
            0,
            client_liveness_timeout_ns,
        )
    }

    /// Create a CnC file with the complete upstream region ordering.
    pub fn create_new_with_regions<P: AsRef<Path>>(
        path: P,
        to_driver_length: usize,
        to_clients_length: usize,
        counters_metadata_length: usize,
        counters_values_length: usize,
        error_log_length: usize,
        client_liveness_timeout_ns: i64,
    ) -> Result<Self> {
        let total_len = META_DATA_LENGTH
            + to_driver_length
            + to_clients_length
            + counters_metadata_length
            + counters_values_length
            + error_log_length;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(total_len as u64)?;

        // Safety: the file was just sized to `total_len` and is mapped writable.
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        // Safety: `0..META_DATA_LENGTH` lies within the freshly mapped region.
        let meta =
            unsafe { AtomicBuffer::from_raw_parts(mmap.as_ptr() as *mut u8, META_DATA_LENGTH) };
        meta.put_i32(TO_DRIVER_LENGTH_OFFSET, to_driver_length as i32);
        meta.put_i32(TO_CLIENTS_LENGTH_OFFSET, to_clients_length as i32);
        meta.put_i32(
            COUNTER_METADATA_LENGTH_OFFSET,
            counters_metadata_length as i32,
        );
        meta.put_i32(COUNTER_VALUES_LENGTH_OFFSET, counters_values_length as i32);
        meta.put_i32(ERROR_LOG_LENGTH_OFFSET, error_log_length as i32);
        meta.put_i64(CLIENT_LIVENESS_TIMEOUT_OFFSET, client_liveness_timeout_ns);
        meta.put_i64(START_TIMESTAMP_OFFSET, now_ms());
        meta.put_i64(PID_OFFSET, std::process::id() as i64);
        meta.put_i32(FILE_PAGE_SIZE_OFFSET, DEFAULT_FILE_PAGE_SIZE);
        // Publish version last so readers that see CNC_VERSION also see the fields above.
        meta.put_i32_ordered(CNC_VERSION_OFFSET, CNC_VERSION);

        Self::from_storage(CncStorage::Mmap(mmap))
    }

    /// Map an existing `cnc.dat`, validating the version and buffer geometry.
    pub fn map_existing<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        // Safety: mapped writable so clients can write the to-driver ring buffer.
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Self::from_storage(CncStorage::Mmap(mmap))
    }

    /// Construct from an in-memory buffer (tests). Expects an already-initialized header.
    #[cfg(test)]
    fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        Self::from_storage(CncStorage::Bytes(bytes))
    }

    fn from_storage(storage: CncStorage) -> Result<Self> {
        if storage.len() < META_DATA_LENGTH {
            return Err(AeronError::IllegalState(format!(
                "CnC file length {} less than metadata length {}",
                storage.len(),
                META_DATA_LENGTH
            )));
        }

        // Safety: `0..META_DATA_LENGTH` is within `storage`.
        let meta =
            unsafe { AtomicBuffer::from_raw_parts(storage.as_ptr() as *mut u8, META_DATA_LENGTH) };

        let version = meta.get_i32_volatile(CNC_VERSION_OFFSET);
        if version != CNC_VERSION {
            return Err(AeronError::IllegalState(format!(
                "CnC version mismatch: expected {CNC_VERSION}, got {version}"
            )));
        }

        let to_driver_length = meta.get_i32(TO_DRIVER_LENGTH_OFFSET) as usize;
        let to_clients_length = meta.get_i32(TO_CLIENTS_LENGTH_OFFSET) as usize;
        let counters_metadata_length = meta.get_i32(COUNTER_METADATA_LENGTH_OFFSET) as usize;
        let counters_values_length = meta.get_i32(COUNTER_VALUES_LENGTH_OFFSET) as usize;
        let error_log_length = meta.get_i32(ERROR_LOG_LENGTH_OFFSET) as usize;
        let to_driver_offset = META_DATA_LENGTH;
        let to_clients_offset = to_driver_offset + to_driver_length;
        let counters_metadata_offset = to_clients_offset + to_clients_length;
        let counters_values_offset = counters_metadata_offset + counters_metadata_length;
        let error_log_offset = counters_values_offset + counters_values_length;

        let required_length = error_log_offset + error_log_length;
        if storage.len() < required_length {
            return Err(AeronError::IllegalState(format!(
                "CnC file length {} too small for declared buffers (to_driver={}, to_clients={}, counters_metadata={}, counters_values={}, error_log={})",
                storage.len(),
                to_driver_length,
                to_clients_length,
                counters_metadata_length,
                counters_values_length,
                error_log_length
            )));
        }

        Ok(Self(Arc::new(CncFileInner {
            storage,
            to_driver_offset,
            to_driver_length,
            to_clients_offset,
            to_clients_length,
            counters_metadata_offset,
            counters_metadata_length,
            counters_values_offset,
            counters_values_length,
            error_log_offset,
            error_log_length,
        })))
    }

    fn view(&self, offset: usize, length: usize) -> AtomicBuffer<'static> {
        // Safety: `offset..offset+length` was validated to lie within `storage` at
        // construction, and the mapping is kept alive by the `Arc` for as long as any
        // returned buffer is used.
        let ptr = unsafe { (self.0.storage.as_ptr() as *mut u8).add(offset) };
        unsafe { AtomicBuffer::from_raw_parts(ptr, length) }
    }

    /// The to-driver region — wrap with `ManyToOneRingBuffer` to send/receive client commands.
    pub fn to_driver_buffer(&self) -> AtomicBuffer<'static> {
        self.view(self.0.to_driver_offset, self.0.to_driver_length)
    }

    /// The to-clients region — wrap with `BroadcastTransmitter`/`BroadcastReceiver` for events.
    pub fn to_clients_buffer(&self) -> AtomicBuffer<'static> {
        self.view(self.0.to_clients_offset, self.0.to_clients_length)
    }

    pub fn counters_metadata_buffer(&self) -> AtomicBuffer<'static> {
        self.view(
            self.0.counters_metadata_offset,
            self.0.counters_metadata_length,
        )
    }

    pub fn counters_values_buffer(&self) -> AtomicBuffer<'static> {
        self.view(self.0.counters_values_offset, self.0.counters_values_length)
    }

    pub fn error_log_buffer(&self) -> AtomicBuffer<'static> {
        self.view(self.0.error_log_offset, self.0.error_log_length)
    }

    pub fn counters_metadata_length(&self) -> usize {
        self.0.counters_metadata_length
    }

    pub fn counters_values_length(&self) -> usize {
        self.0.counters_values_length
    }

    pub fn error_log_length(&self) -> usize {
        self.0.error_log_length
    }

    /// Publish one counter record in the format consumed by Agrona's Java `CountersReader`.
    pub fn write_counter_metadata(
        &self,
        counter_id: i32,
        type_id: i32,
        registration_id: i64,
        value: i64,
        label: &str,
    ) -> Result<()> {
        self.write_counter_metadata_with_key(
            counter_id,
            type_id,
            registration_id,
            value,
            &[],
            label,
        )
    }

    /// Publish a counter record with its optional application key and label.
    pub fn write_counter_metadata_with_key(
        &self,
        counter_id: i32,
        type_id: i32,
        registration_id: i64,
        value: i64,
        key: &[u8],
        label: &str,
    ) -> Result<()> {
        if counter_id < 0 {
            return Err(AeronError::IllegalArgument(format!(
                "negative counter id: {counter_id}"
            )));
        }
        let counter_id = counter_id as usize;
        let metadata_offset = counter_id.saturating_mul(COUNTER_METADATA_LENGTH);
        let value_offset = counter_id.saturating_mul(COUNTER_VALUE_LENGTH);
        if metadata_offset + COUNTER_METADATA_LENGTH > self.0.counters_metadata_length
            || value_offset + COUNTER_REFERENCE_ID_OFFSET + 8 > self.0.counters_values_length
        {
            return Err(AeronError::IllegalState(format!(
                "counter id {counter_id} does not fit CnC counter regions"
            )));
        }

        let label_bytes = label.as_bytes();
        if key.len() > COUNTER_KEY_MAX_LENGTH {
            return Err(AeronError::IllegalArgument(format!(
                "counter key too long: {} > {COUNTER_KEY_MAX_LENGTH}",
                key.len()
            )));
        }
        if label_bytes.len() > COUNTER_LABEL_MAX_LENGTH {
            return Err(AeronError::IllegalArgument(format!(
                "counter label too long: {} > {COUNTER_LABEL_MAX_LENGTH}",
                label_bytes.len()
            )));
        }

        let metadata = self.counters_metadata_buffer();
        metadata.put_i32(metadata_offset + COUNTER_TYPE_ID_OFFSET, type_id);
        metadata.put_bytes(metadata_offset + COUNTER_KEY_OFFSET, key);
        metadata.put_i32(
            metadata_offset + COUNTER_LABEL_OFFSET,
            label_bytes.len() as i32,
        );
        metadata.put_bytes(metadata_offset + COUNTER_LABEL_OFFSET + 4, label_bytes);
        self.counters_values_buffer()
            .put_i64_ordered(value_offset, value);
        let values = self.counters_values_buffer();
        values.put_i64(
            value_offset + COUNTER_REGISTRATION_ID_OFFSET,
            registration_id,
        );
        values.put_i64(value_offset + COUNTER_OWNER_ID_OFFSET, 0);
        values.put_i64(value_offset + COUNTER_REFERENCE_ID_OFFSET, 0);
        // Publish the state last, matching CountersManager's allocation visibility contract.
        metadata.put_i32_ordered(metadata_offset, COUNTER_RECORD_ALLOCATED);
        Ok(())
    }

    pub fn set_counter_value(&self, counter_id: i32, value: i64) -> Result<()> {
        if counter_id < 0 {
            return Err(AeronError::IllegalArgument(format!(
                "negative counter id: {counter_id}"
            )));
        }
        let offset = (counter_id as usize).saturating_mul(COUNTER_VALUE_LENGTH);
        if offset + 8 > self.0.counters_values_length {
            return Err(AeronError::IllegalState(format!(
                "counter id {counter_id} does not fit CnC counter values"
            )));
        }
        self.counters_values_buffer().put_i64_ordered(offset, value);
        Ok(())
    }

    /// Release a counter record so shared readers stop reporting it as allocated.
    pub fn free_counter(&self, counter_id: i32) -> Result<()> {
        if counter_id < 0 {
            return Err(AeronError::IllegalArgument(format!(
                "negative counter id: {counter_id}"
            )));
        }
        let counter_id = counter_id as usize;
        let metadata_offset = counter_id.saturating_mul(COUNTER_METADATA_LENGTH);
        let value_offset = counter_id.saturating_mul(COUNTER_VALUE_LENGTH);
        if metadata_offset + COUNTER_METADATA_LENGTH > self.0.counters_metadata_length
            || value_offset + COUNTER_REFERENCE_ID_OFFSET + 8 > self.0.counters_values_length
        {
            return Err(AeronError::IllegalState(format!(
                "counter id {counter_id} does not fit CnC counter regions"
            )));
        }
        self.counters_metadata_buffer()
            .put_i32_ordered(metadata_offset, 0);
        self.counters_values_buffer()
            .put_i64_ordered(value_offset, 0);
        Ok(())
    }

    /// CnC format version recorded in the file.
    pub fn cnc_version(&self) -> i32 {
        self.meta().get_i32_volatile(CNC_VERSION_OFFSET)
    }

    /// Client liveness timeout in nanoseconds.
    pub fn client_liveness_timeout_ns(&self) -> i64 {
        self.meta().get_i64(CLIENT_LIVENESS_TIMEOUT_OFFSET)
    }

    /// Driver start timestamp in epoch milliseconds.
    pub fn start_timestamp_ms(&self) -> i64 {
        self.meta().get_i64(START_TIMESTAMP_OFFSET)
    }

    /// PID of the process that created the file.
    pub fn pid(&self) -> i64 {
        self.meta().get_i64(PID_OFFSET)
    }

    fn meta(&self) -> AtomicBuffer<'static> {
        self.view(0, META_DATA_LENGTH)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agrona::BroadcastReceiver;
    use agrona::BroadcastTransmitter;
    use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "aeron-cnc-{}-{}-{:?}.dat",
            tag,
            std::process::id(),
            std::thread::current().id()
        ))
    }

    #[test]
    fn rejects_short_file_and_bad_version() {
        // Too short for metadata.
        assert!(CncFile::from_bytes(vec![0u8; META_DATA_LENGTH - 1]).is_err());
        // Long enough but version field is zero (uninitialized).
        assert!(CncFile::from_bytes(vec![0u8; META_DATA_LENGTH + 8]).is_err());
    }

    #[test]
    fn metadata_survives_create_then_map_existing() {
        let path = temp_path("meta");
        let _ = std::fs::remove_file(&path);

        let created = CncFile::create_new(
            &path,
            DEFAULT_TO_DRIVER_LENGTH,
            DEFAULT_TO_CLIENTS_LENGTH,
            10_000_000_000,
        )
        .unwrap();
        let mapped = CncFile::map_existing(&path).unwrap();

        assert_eq!(mapped.cnc_version(), CNC_VERSION);
        assert_eq!(mapped.client_liveness_timeout_ns(), 10_000_000_000);
        assert_eq!(mapped.pid(), created.pid());
        assert_eq!(mapped.pid(), std::process::id() as i64);

        drop(created);
        drop(mapped);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn complete_regions_follow_upstream_cnc_order() {
        let path = temp_path("complete-regions");
        let _ = std::fs::remove_file(&path);
        let cnc =
            CncFile::create_new_with_regions(&path, 128, 256, 512, 1024, 2048, 10_000_000_000)
                .unwrap();
        let mapped = CncFile::map_existing(&path).unwrap();

        assert_eq!(mapped.counters_metadata_length(), 512);
        assert_eq!(mapped.counters_values_length(), 1024);
        assert_eq!(mapped.error_log_length(), 2048);
        mapped.counters_metadata_buffer().put_i32(0, 0x1122_3344);
        mapped.counters_values_buffer().put_i64(0, 0x5566_7788);
        mapped.error_log_buffer().put_i32(0, 0x99AA_BBCC_u32 as i32);
        assert_eq!(cnc.counters_metadata_buffer().get_i32(0), 0x1122_3344);
        assert_eq!(cnc.counters_values_buffer().get_i64(0), 0x5566_7788);
        assert_eq!(cnc.error_log_buffer().get_i32(0), 0x99AA_BBCC_u32 as i32);

        drop(mapped);
        drop(cnc);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn to_driver_command_written_in_one_handle_is_read_in_another() {
        let path = temp_path("todriver");
        let _ = std::fs::remove_file(&path);

        let driver_side = CncFile::create_new(
            &path,
            DEFAULT_TO_DRIVER_LENGTH,
            DEFAULT_TO_CLIENTS_LENGTH,
            10_000_000_000,
        )
        .unwrap();
        let client_side = CncFile::map_existing(&path).unwrap();

        // Client writes a command into the to-driver ring buffer.
        let producer = ManyToOneRingBuffer::new(client_side.to_driver_buffer()).unwrap();
        let mut msg = [0u8; 8];
        let src = AtomicBuffer::new(&mut msg);
        src.put_i64(0, 0x00C0_FFEE_i64);
        assert!(producer.write(7, &src, 0, 8).unwrap());

        // Driver reads it from an independent mapping of the same file.
        let consumer = ManyToOneRingBuffer::new(driver_side.to_driver_buffer()).unwrap();
        let mut seen_type = 0i32;
        let mut seen_value = 0i64;
        let read = consumer.read(|msg_type, buf, offset, _len| {
            seen_type = msg_type;
            seen_value = buf.get_i64(offset);
        });

        assert_eq!(read, 1);
        assert_eq!(seen_type, 7);
        assert_eq!(seen_value, 0x00C0_FFEE);

        drop(driver_side);
        drop(client_side);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn to_clients_event_broadcast_in_one_handle_is_received_in_another() {
        let path = temp_path("toclients");
        let _ = std::fs::remove_file(&path);

        let driver_side = CncFile::create_new(
            &path,
            DEFAULT_TO_DRIVER_LENGTH,
            DEFAULT_TO_CLIENTS_LENGTH,
            10_000_000_000,
        )
        .unwrap();
        let client_side = CncFile::map_existing(&path).unwrap();

        // Client subscribes first (broadcast delivers only messages sent after the receiver
        // starts listening), from its own independent mapping.
        let mut receiver = BroadcastReceiver::new(client_side.to_clients_buffer()).unwrap();

        // Driver then broadcasts an event on the to-clients buffer.
        let transmitter = BroadcastTransmitter::new(driver_side.to_clients_buffer()).unwrap();
        let mut msg = [0u8; 8];
        let src = AtomicBuffer::new(&mut msg);
        src.put_i64(0, 0x1234_5678_9ABC_DEF0_u64 as i64);
        transmitter.transmit(9, &src, 0, 8).unwrap();

        let mut seen_type = 0i32;
        let mut seen_value = 0i64;
        let got = receiver
            .receive_next(|msg_type, buf, offset, _len| {
                seen_type = msg_type;
                seen_value = buf.get_i64(offset);
            })
            .unwrap();

        assert!(got);
        assert_eq!(seen_type, 9);
        assert_eq!(seen_value, 0x1234_5678_9ABC_DEF0_u64 as i64);

        drop(driver_side);
        drop(client_side);
        let _ = std::fs::remove_file(&path);
    }
}
