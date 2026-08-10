//! File-backed Archive liveness and metadata marker.
//!
//! The layout follows `aeron-archive-mark-codecs.xml`: an eight-byte SBE message
//! header, the 128-byte `MarkFileHeader` block, variable ASCII channels, and an
//! error buffer beginning at the 8 KiB header boundary.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use memmap2::MmapMut;

pub const MAJOR_VERSION: i32 = 3;
pub const MINOR_VERSION: i32 = 1;
pub const PATCH_VERSION: i32 = 0;
pub const SEMANTIC_VERSION: i32 = (MAJOR_VERSION << 16) | (MINOR_VERSION << 8) | PATCH_VERSION;
pub const HEADER_LENGTH: usize = 8 * 1024;
pub const FILENAME: &str = "archive-mark.dat";
pub const LINK_FILENAME: &str = "archive-mark.lnk";
pub const MESSAGE_HEADER_LENGTH: usize = 8;
pub const BLOCK_LENGTH: usize = 128;
pub const SCHEMA_ID: u16 = 100;
pub const TEMPLATE_ID: u16 = 200;
pub const SCHEMA_VERSION: u16 = 2;
pub const NULL_VALUE: i64 = -1;
const FIXED_BLOCK_OFFSET: usize = MESSAGE_HEADER_LENGTH;
const VARIABLE_DATA_OFFSET: usize = FIXED_BLOCK_OFFSET + BLOCK_LENGTH;
const PAGE_MIN_SIZE: usize = 4 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ArchiveMarkFileError {
    #[error("Archive mark file I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid Archive mark file: {0}")]
    Invalid(String),
    #[error("Archive mark file is closed")]
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveMarkFileConfig {
    pub start_timestamp: i64,
    pub pid: i64,
    pub control_stream_id: i32,
    pub local_control_stream_id: i32,
    pub events_stream_id: i32,
    pub header_length: i32,
    pub error_buffer_length: usize,
    pub archive_id: i64,
    pub control_channel: String,
    pub local_control_channel: String,
    pub events_channel: String,
    pub aeron_directory: String,
    pub file_page_size: usize,
}

impl Default for ArchiveMarkFileConfig {
    fn default() -> Self {
        Self {
            start_timestamp: 0,
            pid: i64::from(std::process::id()),
            control_stream_id: 10,
            local_control_stream_id: 0,
            events_stream_id: 0,
            header_length: HEADER_LENGTH as i32,
            error_buffer_length: 4096,
            archive_id: 0,
            control_channel: String::new(),
            local_control_channel: String::new(),
            events_channel: String::new(),
            aeron_directory: String::new(),
            file_page_size: PAGE_MIN_SIZE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveMarkFileHeader {
    pub version: i32,
    pub activity_timestamp: i64,
    pub start_timestamp: i64,
    pub pid: i64,
    pub control_stream_id: i32,
    pub local_control_stream_id: i32,
    pub events_stream_id: i32,
    pub header_length: i32,
    pub error_buffer_length: i32,
    pub archive_id: i64,
    pub control_channel: String,
    pub local_control_channel: String,
    pub events_channel: String,
    pub aeron_directory: String,
}

pub struct ArchiveMarkFile {
    path: PathBuf,
    file: Option<File>,
    map: Option<MmapMut>,
}

impl ArchiveMarkFile {
    pub fn create<P: AsRef<Path>>(
        path: P,
        config: &ArchiveMarkFileConfig,
    ) -> Result<Self, ArchiveMarkFileError> {
        validate_config(config)?;
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file_length = align(
            HEADER_LENGTH
                .checked_add(config.error_buffer_length)
                .ok_or_else(|| ArchiveMarkFileError::Invalid("file length overflow".into()))?,
            config.file_page_size,
        );
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path.as_ref())?;
        file.set_len(file_length as u64)?;
        let map = unsafe { MmapMut::map_mut(&file)? };
        let mut mark_file = Self {
            path: path.as_ref().to_path_buf(),
            file: Some(file),
            map: Some(map),
        };
        mark_file.encode(config)?;
        Ok(mark_file)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, ArchiveMarkFileError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())?;
        let file_length = file.metadata()?.len() as usize;
        if file_length < HEADER_LENGTH {
            return Err(ArchiveMarkFileError::Invalid(
                "file is shorter than the header boundary".into(),
            ));
        }
        let map = unsafe { MmapMut::map_mut(&file)? };
        let mark_file = Self {
            path: path.as_ref().to_path_buf(),
            file: Some(file),
            map: Some(map),
        };
        mark_file.validate_layout()?;
        Ok(mark_file)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_closed(&self) -> bool {
        self.map.is_none()
    }

    pub fn close(&mut self) -> Result<(), ArchiveMarkFileError> {
        if let Some(map) = self.map.as_mut() {
            map.flush()?
        }
        self.map.take();
        self.file.take();
        Ok(())
    }

    pub fn force(&mut self) -> Result<(), ArchiveMarkFileError> {
        if let Some(map) = self.map.as_mut() {
            map.flush()?
        }
        Ok(())
    }

    pub fn header(&self) -> Result<ArchiveMarkFileHeader, ArchiveMarkFileError> {
        let map = self.map.as_ref().ok_or(ArchiveMarkFileError::Closed)?;
        decode_header(map)
    }

    pub fn activity_timestamp(&self) -> i64 {
        self.map
            .as_ref()
            .map_or(NULL_VALUE, |map| read_i64(map, FIXED_BLOCK_OFFSET + 8))
    }

    pub fn archive_id(&self) -> i64 {
        self.map
            .as_ref()
            .map_or(NULL_VALUE, |map| read_i64(map, FIXED_BLOCK_OFFSET + 56))
    }

    pub fn update_activity_timestamp(&mut self, timestamp: i64) {
        if let Some(map) = self.map.as_mut() {
            write_i64(map, FIXED_BLOCK_OFFSET + 8, timestamp);
        }
    }

    pub fn signal_ready(&mut self, activity_timestamp: i64) -> Result<(), ArchiveMarkFileError> {
        if let Some(map) = self.map.as_mut() {
            write_i64(map, FIXED_BLOCK_OFFSET + 8, activity_timestamp);
            write_i32(map, FIXED_BLOCK_OFFSET, SEMANTIC_VERSION);
            self.force()?;
        }
        Ok(())
    }

    pub fn signal_terminated(&mut self) -> Result<(), ArchiveMarkFileError> {
        self.signal_ready(NULL_VALUE)
    }

    pub fn error_buffer(&self) -> Result<&[u8], ArchiveMarkFileError> {
        let map = self.map.as_ref().ok_or(ArchiveMarkFileError::Closed)?;
        let length = read_i32(map, FIXED_BLOCK_OFFSET + 48);
        if length < 0 {
            return Err(ArchiveMarkFileError::Invalid(
                "negative error buffer length".into(),
            ));
        }
        let end = HEADER_LENGTH
            .checked_add(length as usize)
            .ok_or_else(|| ArchiveMarkFileError::Invalid("error buffer length overflow".into()))?;
        map.get(HEADER_LENGTH..end)
            .ok_or_else(|| ArchiveMarkFileError::Invalid("error buffer exceeds file".into()))
    }

    pub fn error_buffer_mut(&mut self) -> Result<&mut [u8], ArchiveMarkFileError> {
        let map = self.map.as_mut().ok_or(ArchiveMarkFileError::Closed)?;
        let length = read_i32(map, FIXED_BLOCK_OFFSET + 48);
        if length < 0 {
            return Err(ArchiveMarkFileError::Invalid(
                "negative error buffer length".into(),
            ));
        }
        let end = HEADER_LENGTH
            .checked_add(length as usize)
            .ok_or_else(|| ArchiveMarkFileError::Invalid("error buffer length overflow".into()))?;
        map.get_mut(HEADER_LENGTH..end)
            .ok_or_else(|| ArchiveMarkFileError::Invalid("error buffer exceeds file".into()))
    }

    fn encode(&mut self, config: &ArchiveMarkFileConfig) -> Result<(), ArchiveMarkFileError> {
        let map = self.map.as_mut().ok_or(ArchiveMarkFileError::Closed)?;
        map.fill(0);
        write_u16(map, 0, BLOCK_LENGTH as u16);
        write_u16(map, 2, TEMPLATE_ID);
        write_u16(map, 4, SCHEMA_ID);
        write_u16(map, 6, SCHEMA_VERSION);
        write_i32(map, FIXED_BLOCK_OFFSET, 0);
        write_i64(map, FIXED_BLOCK_OFFSET + 8, 0);
        write_i64(map, FIXED_BLOCK_OFFSET + 16, config.start_timestamp);
        write_i64(map, FIXED_BLOCK_OFFSET + 24, config.pid);
        write_i32(map, FIXED_BLOCK_OFFSET + 32, config.control_stream_id);
        write_i32(map, FIXED_BLOCK_OFFSET + 36, config.local_control_stream_id);
        write_i32(map, FIXED_BLOCK_OFFSET + 40, config.events_stream_id);
        write_i32(map, FIXED_BLOCK_OFFSET + 44, config.header_length);
        write_i32(
            map,
            FIXED_BLOCK_OFFSET + 48,
            config.error_buffer_length as i32,
        );
        write_i64(map, FIXED_BLOCK_OFFSET + 56, config.archive_id);

        let mut offset = VARIABLE_DATA_OFFSET;
        for channel in [
            &config.control_channel,
            &config.local_control_channel,
            &config.events_channel,
            &config.aeron_directory,
        ] {
            offset = write_ascii(map, offset, channel)?;
        }
        Ok(())
    }

    fn validate_layout(&self) -> Result<(), ArchiveMarkFileError> {
        let map = self.map.as_ref().ok_or(ArchiveMarkFileError::Closed)?;
        if read_u16(map, 0) != BLOCK_LENGTH as u16
            || read_u16(map, 2) != TEMPLATE_ID
            || read_u16(map, 4) != SCHEMA_ID
        {
            return Err(ArchiveMarkFileError::Invalid(
                "unexpected SBE mark-file header".into(),
            ));
        }
        let error_buffer_length = read_i32(map, FIXED_BLOCK_OFFSET + 48);
        if error_buffer_length < 0 || HEADER_LENGTH + error_buffer_length as usize > map.len() {
            return Err(ArchiveMarkFileError::Invalid(
                "error buffer exceeds mapped file".into(),
            ));
        }
        let mut offset = VARIABLE_DATA_OFFSET;
        for _ in 0..4 {
            offset = skip_ascii(map, offset)?;
        }
        Ok(())
    }
}

impl Drop for ArchiveMarkFile {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn validate_config(config: &ArchiveMarkFileConfig) -> Result<(), ArchiveMarkFileError> {
    if config.file_page_size < PAGE_MIN_SIZE || !config.file_page_size.is_power_of_two() {
        return Err(ArchiveMarkFileError::Invalid(
            "file page size must be a power of two at least 4096".into(),
        ));
    }
    if config.error_buffer_length > i32::MAX as usize {
        return Err(ArchiveMarkFileError::Invalid(
            "error buffer length exceeds i32".into(),
        ));
    }
    for channel in [
        &config.control_channel,
        &config.local_control_channel,
        &config.events_channel,
        &config.aeron_directory,
    ] {
        if !channel.is_ascii() {
            return Err(ArchiveMarkFileError::Invalid(
                "mark-file channels must contain ASCII".into(),
            ));
        }
    }
    Ok(())
}

fn align(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

fn write_ascii(map: &mut [u8], offset: usize, value: &str) -> Result<usize, ArchiveMarkFileError> {
    let end = offset
        .checked_add(4)
        .and_then(|end| end.checked_add(value.len()))
        .ok_or_else(|| ArchiveMarkFileError::Invalid("variable field overflow".into()))?;
    if end > HEADER_LENGTH || value.len() > u32::MAX as usize {
        return Err(ArchiveMarkFileError::Invalid(
            "variable fields exceed the header boundary".into(),
        ));
    }
    map[offset..offset + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
    map[offset + 4..end].copy_from_slice(value.as_bytes());
    Ok(end)
}

fn skip_ascii(map: &[u8], offset: usize) -> Result<usize, ArchiveMarkFileError> {
    let length = read_u32(map, offset) as usize;
    let end = offset
        .checked_add(4)
        .and_then(|end| end.checked_add(length))
        .ok_or_else(|| ArchiveMarkFileError::Invalid("variable field overflow".into()))?;
    if end > HEADER_LENGTH {
        return Err(ArchiveMarkFileError::Invalid(
            "variable field exceeds the header boundary".into(),
        ));
    }
    Ok(end)
}

fn decode_ascii(map: &[u8], offset: &mut usize) -> Result<String, ArchiveMarkFileError> {
    let length = read_u32(map, *offset) as usize;
    let start = *offset + 4;
    let end = start
        .checked_add(length)
        .ok_or_else(|| ArchiveMarkFileError::Invalid("variable field overflow".into()))?;
    let bytes = map
        .get(start..end)
        .ok_or_else(|| ArchiveMarkFileError::Invalid("variable field exceeds file".into()))?;
    *offset = end;
    if !bytes.is_ascii() {
        return Err(ArchiveMarkFileError::Invalid(
            "variable field is not ASCII".into(),
        ));
    }
    Ok(String::from_utf8(bytes.to_vec()).expect("ASCII is valid UTF-8"))
}

fn decode_header(map: &[u8]) -> Result<ArchiveMarkFileHeader, ArchiveMarkFileError> {
    let mut offset = VARIABLE_DATA_OFFSET;
    Ok(ArchiveMarkFileHeader {
        version: read_i32(map, FIXED_BLOCK_OFFSET),
        activity_timestamp: read_i64(map, FIXED_BLOCK_OFFSET + 8),
        start_timestamp: read_i64(map, FIXED_BLOCK_OFFSET + 16),
        pid: read_i64(map, FIXED_BLOCK_OFFSET + 24),
        control_stream_id: read_i32(map, FIXED_BLOCK_OFFSET + 32),
        local_control_stream_id: read_i32(map, FIXED_BLOCK_OFFSET + 36),
        events_stream_id: read_i32(map, FIXED_BLOCK_OFFSET + 40),
        header_length: read_i32(map, FIXED_BLOCK_OFFSET + 44),
        error_buffer_length: read_i32(map, FIXED_BLOCK_OFFSET + 48),
        archive_id: read_i64(map, FIXED_BLOCK_OFFSET + 56),
        control_channel: decode_ascii(map, &mut offset)?,
        local_control_channel: decode_ascii(map, &mut offset)?,
        events_channel: decode_ascii(map, &mut offset)?,
        aeron_directory: decode_ascii(map, &mut offset)?,
    })
}

fn read_u16(map: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        map[offset..offset + 2]
            .try_into()
            .expect("fixed mark-file field"),
    )
}

fn read_u32(map: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        map[offset..offset + 4]
            .try_into()
            .expect("fixed mark-file field"),
    )
}

fn read_i32(map: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes(
        map[offset..offset + 4]
            .try_into()
            .expect("fixed mark-file field"),
    )
}

fn read_i64(map: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(
        map[offset..offset + 8]
            .try_into()
            .expect("fixed mark-file field"),
    )
}

fn write_u16(map: &mut [u8], offset: usize, value: u16) {
    map[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_i32(map: &mut [u8], offset: usize, value: i32) {
    map[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_i64(map: &mut [u8], offset: usize, value: i64) {
    map[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(label: &str) -> (PathBuf, PathBuf) {
        let directory = std::env::temp_dir().join(format!(
            "aeron-archive-mark-{}-{}-{}",
            std::process::id(),
            label,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join(FILENAME);
        (directory, path)
    }

    fn remove_temp(directory: &Path) {
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn config() -> ArchiveMarkFileConfig {
        ArchiveMarkFileConfig {
            start_timestamp: 123_456,
            pid: 987,
            control_stream_id: 42,
            local_control_stream_id: 19,
            events_stream_id: 77,
            header_length: HEADER_LENGTH as i32,
            error_buffer_length: 4096,
            archive_id: 888,
            control_channel: "aeron:udp?endpoint=localhost:8010".into(),
            local_control_channel: "aeron:ipc?alias=local-control".into(),
            events_channel: "aeron:ipc?alias=events".into(),
            aeron_directory: "/tmp/aeron".into(),
            file_page_size: 16 * 1024,
        }
    }

    #[test]
    fn creates_aligned_sbe_mark_file_and_round_trips_header() {
        let (directory, path) = temp_path("create");
        let config = config();
        let mark_file = ArchiveMarkFile::create(&path, &config).unwrap();

        assert_eq!(std::fs::metadata(&path).unwrap().len(), 16 * 1024);
        assert_eq!(
            mark_file.header().unwrap().start_timestamp,
            config.start_timestamp
        );
        assert_eq!(mark_file.header().unwrap().archive_id, config.archive_id);
        assert_eq!(
            mark_file.header().unwrap().control_channel,
            config.control_channel
        );
        assert_eq!(
            mark_file.error_buffer().unwrap().len(),
            config.error_buffer_length
        );
        drop(mark_file);
        remove_temp(&directory);
    }

    #[test]
    fn signals_ready_updates_version_and_activity_timestamp() {
        let (directory, path) = temp_path("ready");
        let mut mark_file = ArchiveMarkFile::create(&path, &config()).unwrap();

        assert_eq!(mark_file.activity_timestamp(), 0);
        assert_eq!(mark_file.header().unwrap().version, 0);
        mark_file.signal_ready(5555).unwrap();
        assert_eq!(mark_file.activity_timestamp(), 5555);
        assert_eq!(mark_file.header().unwrap().version, SEMANTIC_VERSION);
        drop(mark_file);
        remove_temp(&directory);
    }

    #[test]
    fn signal_terminated_and_close_expose_null_values() {
        let (directory, path) = temp_path("terminated");
        let mut mark_file = ArchiveMarkFile::create(&path, &config()).unwrap();

        mark_file.signal_terminated().unwrap();
        assert_eq!(mark_file.activity_timestamp(), NULL_VALUE);
        mark_file.close().unwrap();
        assert!(mark_file.is_closed());
        assert_eq!(mark_file.activity_timestamp(), NULL_VALUE);
        assert_eq!(mark_file.archive_id(), NULL_VALUE);
        mark_file.close().unwrap();
        drop(mark_file);
        remove_temp(&directory);
    }

    #[test]
    fn reopens_persisted_header_and_error_buffer() {
        let (directory, path) = temp_path("reopen");
        let config = config();
        {
            let mut mark_file = ArchiveMarkFile::create(&path, &config).unwrap();
            mark_file.error_buffer_mut().unwrap()[0] = 42;
            mark_file.signal_ready(777).unwrap();
        }

        let mark_file = ArchiveMarkFile::open(&path).unwrap();
        assert_eq!(mark_file.header().unwrap().version, SEMANTIC_VERSION);
        assert_eq!(mark_file.activity_timestamp(), 777);
        assert_eq!(mark_file.error_buffer().unwrap()[0], 42);
        drop(mark_file);
        remove_temp(&directory);
    }

    #[test]
    fn rejects_invalid_page_size_and_non_ascii_channels() {
        let (directory, path) = temp_path("invalid");
        let mut invalid_page = config();
        invalid_page.file_page_size = 5000;
        assert!(matches!(
            ArchiveMarkFile::create(&path, &invalid_page),
            Err(ArchiveMarkFileError::Invalid(_))
        ));

        let mut invalid_channel = config();
        invalid_channel.events_channel = "イベント".into();
        assert!(matches!(
            ArchiveMarkFile::create(&path, &invalid_channel),
            Err(ArchiveMarkFileError::Invalid(_))
        ));
        remove_temp(&directory);
    }
}
