//! Archive catalog and mark-file migration helpers.
//!
//! This handles the v2.1.0 on-disk shape used by the upstream
//! `ArchiveMigration_2_3` tests and rewrites it into the current Rust storage
//! model at the v3.0.0 migration boundary.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::archive_mark_file::{
    ArchiveMarkFile, ArchiveMarkFileConfig, ArchiveMarkFileError, HEADER_LENGTH,
};
use crate::catalog::{Catalog, RecordingState};
use crate::recording_descriptor::RecordingDescriptor;

pub const VERSION_2_1_0: i32 = (2 << 16) | (1 << 8);
pub const MINIMUM_VERSION: i32 = 3 << 16;
pub const CATALOG_FILE_NAME: &str = "catalog.dat";
pub const MARK_FILE_NAME: &str = "archive-mark.dat";
pub const RECORDING_FRAME_LENGTH_V2: usize = 1024;
const MARK_FILE_HEADER_LENGTH_V2: usize = 8 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ArchiveMigrationError {
    #[error("Archive migration I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error(transparent)]
    MarkFile(#[from] ArchiveMarkFileError),
    #[error("invalid Archive v2 data: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    pub from_version: i32,
    pub to_version: i32,
    pub recording_count: usize,
    pub next_recording_id: i64,
    pub timestamp_file: PathBuf,
}

pub fn migration_timestamp_file_name(from_version: i32, to_version: i32) -> String {
    format!("migration-{from_version}-to-{to_version}.dat")
}

pub fn migrate_v2_archive(
    archive_dir: impl AsRef<Path>,
) -> Result<MigrationReport, ArchiveMigrationError> {
    let archive_dir = archive_dir.as_ref();
    fs::create_dir_all(archive_dir)?;
    let mark_path = archive_dir.join(MARK_FILE_NAME);
    let catalog_path = archive_dir.join(CATALOG_FILE_NAME);
    let legacy_mark = read_legacy_mark_file(&mark_path)?;
    if legacy_mark.version != VERSION_2_1_0 {
        return Err(ArchiveMigrationError::Invalid(format!(
            "expected v2.1.0 mark file, found {}",
            legacy_mark.version
        )));
    }
    let recordings = read_legacy_catalog(&catalog_path)?;
    let timestamp_file = archive_dir.join(migration_timestamp_file_name(
        legacy_mark.version,
        MINIMUM_VERSION,
    ));
    let mut timestamp = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&timestamp_file)?;
    timestamp.write_all(&legacy_mark.activity_timestamp.to_le_bytes())?;
    timestamp.sync_all()?;

    let mut catalog = Catalog::new();
    catalog.set_version(MINIMUM_VERSION);
    for recording in recordings {
        let state = recording.valid;
        let recording_id = recording.descriptor.recording_id;
        catalog.add_recording(recording.descriptor);
        if state == 0 {
            catalog.change_state(recording_id, RecordingState::Invalid);
        }
    }
    let next_recording_id = catalog
        .recordings()
        .iter()
        .map(|recording| recording.recording_id)
        .max()
        .and_then(|recording_id| recording_id.checked_add(1))
        .unwrap_or(0);
    catalog.set_next_recording_id(next_recording_id);

    let updated_catalog_path = archive_dir.join(format!("{CATALOG_FILE_NAME}.updated"));
    let _ = fs::remove_file(&updated_catalog_path);
    catalog.save_to_file(&updated_catalog_path)?;
    fs::remove_file(&catalog_path)?;
    fs::rename(&updated_catalog_path, &catalog_path)?;

    let mark_config = ArchiveMarkFileConfig {
        start_timestamp: legacy_mark.start_timestamp,
        pid: legacy_mark.pid,
        control_stream_id: legacy_mark.control_stream_id,
        local_control_stream_id: legacy_mark.local_control_stream_id,
        events_stream_id: legacy_mark.events_stream_id,
        header_length: HEADER_LENGTH as i32,
        error_buffer_length: legacy_mark.error_buffer_length,
        archive_id: 0,
        control_channel: legacy_mark.control_channel,
        local_control_channel: legacy_mark.local_control_channel,
        events_channel: legacy_mark.events_channel,
        aeron_directory: legacy_mark.aeron_directory,
        file_page_size: 4 * 1024,
    };
    let mut mark_file = ArchiveMarkFile::create(&mark_path, &mark_config)?;
    mark_file.signal_ready_with_version(legacy_mark.activity_timestamp, MINIMUM_VERSION)?;

    Ok(MigrationReport {
        from_version: legacy_mark.version,
        to_version: MINIMUM_VERSION,
        recording_count: catalog.recordings().len(),
        next_recording_id,
        timestamp_file,
    })
}

struct LegacyMarkFile {
    version: i32,
    activity_timestamp: i64,
    start_timestamp: i64,
    pid: i64,
    control_stream_id: i32,
    local_control_stream_id: i32,
    events_stream_id: i32,
    error_buffer_length: usize,
    control_channel: String,
    local_control_channel: String,
    events_channel: String,
    aeron_directory: String,
}

struct LegacyRecording {
    valid: u8,
    descriptor: RecordingDescriptor,
}

fn read_legacy_mark_file(path: &Path) -> Result<LegacyMarkFile, ArchiveMigrationError> {
    let bytes = fs::read(path)?;
    if bytes.len() < MARK_FILE_HEADER_LENGTH_V2 {
        return Err(ArchiveMigrationError::Invalid(
            "v2 mark file is shorter than 8 KiB".into(),
        ));
    }
    let mut offset = 0;
    let version = read_i32(&bytes, &mut offset)?;
    let activity_timestamp = read_i64(&bytes, &mut offset)?;
    let start_timestamp = read_i64(&bytes, &mut offset)?;
    let pid = read_i64(&bytes, &mut offset)?;
    let control_stream_id = read_i32(&bytes, &mut offset)?;
    let local_control_stream_id = read_i32(&bytes, &mut offset)?;
    let events_stream_id = read_i32(&bytes, &mut offset)?;
    let header_length = read_i32(&bytes, &mut offset)?;
    let error_buffer_length = read_i32(&bytes, &mut offset)?;
    if header_length != MARK_FILE_HEADER_LENGTH_V2 as i32 || error_buffer_length < 0 {
        return Err(ArchiveMigrationError::Invalid(
            "invalid v2 mark-file geometry".into(),
        ));
    }
    let control_channel = read_ascii(&bytes, &mut offset, MARK_FILE_HEADER_LENGTH_V2)?;
    let local_control_channel = read_ascii(&bytes, &mut offset, MARK_FILE_HEADER_LENGTH_V2)?;
    let events_channel = read_ascii(&bytes, &mut offset, MARK_FILE_HEADER_LENGTH_V2)?;
    let aeron_directory = read_ascii(&bytes, &mut offset, MARK_FILE_HEADER_LENGTH_V2)?;
    Ok(LegacyMarkFile {
        version,
        activity_timestamp,
        start_timestamp,
        pid,
        control_stream_id,
        local_control_stream_id,
        events_stream_id,
        error_buffer_length: error_buffer_length as usize,
        control_channel,
        local_control_channel,
        events_channel,
        aeron_directory,
    })
}

fn read_legacy_catalog(path: &Path) -> Result<Vec<LegacyRecording>, ArchiveMigrationError> {
    let bytes = fs::read(path)?;
    if bytes.len() < RECORDING_FRAME_LENGTH_V2
        || (bytes.len() - RECORDING_FRAME_LENGTH_V2) % RECORDING_FRAME_LENGTH_V2 != 0
    {
        return Err(ArchiveMigrationError::Invalid(
            "v2 catalog is not aligned to 1024-byte frames".into(),
        ));
    }
    let version = i32::from_le_bytes(
        bytes[0..4]
            .try_into()
            .map_err(|_| ArchiveMigrationError::Invalid("missing v2 catalog version".into()))?,
    );
    let frame_length =
        i32::from_le_bytes(bytes[4..8].try_into().map_err(|_| {
            ArchiveMigrationError::Invalid("missing v2 catalog frame length".into())
        })?);
    if version != VERSION_2_1_0 || frame_length != RECORDING_FRAME_LENGTH_V2 as i32 {
        return Err(ArchiveMigrationError::Invalid(
            "unexpected v2 catalog header".into(),
        ));
    }
    let mut recordings = Vec::new();
    for index in (RECORDING_FRAME_LENGTH_V2..bytes.len()).step_by(RECORDING_FRAME_LENGTH_V2) {
        let frame_end = index + RECORDING_FRAME_LENGTH_V2;
        let descriptor_length = read_i32_at(&bytes, index)?;
        if descriptor_length < 0 || descriptor_length as usize > RECORDING_FRAME_LENGTH_V2 - 32 {
            return Err(ArchiveMigrationError::Invalid(
                "invalid v2 catalog descriptor length".into(),
            ));
        }
        let valid = *bytes.get(index + 4).ok_or_else(|| {
            ArchiveMigrationError::Invalid("missing v2 catalog validity byte".into())
        })?;
        if valid > 1 {
            return Err(ArchiveMigrationError::Invalid(
                "invalid v2 catalog recording state".into(),
            ));
        }
        let mut offset = index + 32;
        let control_session_id = read_i64(&bytes, &mut offset)?;
        let correlation_id = read_i64(&bytes, &mut offset)?;
        let recording_id = read_i64(&bytes, &mut offset)?;
        let start_timestamp = read_i64(&bytes, &mut offset)?;
        let stop_timestamp = read_i64(&bytes, &mut offset)?;
        let start_position = read_i64(&bytes, &mut offset)?;
        let stop_position = read_i64(&bytes, &mut offset)?;
        let initial_term_id = read_i32(&bytes, &mut offset)?;
        let segment_file_length = read_i32(&bytes, &mut offset)?;
        let term_buffer_length = read_i32(&bytes, &mut offset)?;
        let mtu_length = read_i32(&bytes, &mut offset)?;
        let session_id = read_i32(&bytes, &mut offset)?;
        let stream_id = read_i32(&bytes, &mut offset)?;
        let stripped_channel = read_ascii(&bytes, &mut offset, frame_end)?;
        let original_channel = read_ascii(&bytes, &mut offset, frame_end)?;
        let source_identity = read_ascii(&bytes, &mut offset, frame_end)?;
        recordings.push(LegacyRecording {
            valid,
            descriptor: RecordingDescriptor::new(
                control_session_id,
                correlation_id,
                recording_id,
                start_position,
                stop_position,
                start_timestamp,
                stop_timestamp,
                initial_term_id,
                segment_file_length,
                term_buffer_length,
                mtu_length,
                session_id,
                stream_id,
                stripped_channel,
                original_channel,
                source_identity,
            ),
        });
    }
    Ok(recordings)
}

fn read_ascii(
    bytes: &[u8],
    offset: &mut usize,
    limit: usize,
) -> Result<String, ArchiveMigrationError> {
    let length = read_u32(bytes, offset)? as usize;
    let start = *offset;
    let end = start
        .checked_add(length)
        .ok_or_else(|| ArchiveMigrationError::Invalid("legacy string overflow".into()))?;
    if end > limit {
        return Err(ArchiveMigrationError::Invalid(
            "legacy string exceeds its frame".into(),
        ));
    }
    let value = bytes
        .get(start..end)
        .ok_or_else(|| ArchiveMigrationError::Invalid("legacy string exceeds file".into()))?;
    if !value.is_ascii() {
        return Err(ArchiveMigrationError::Invalid(
            "legacy string is not ASCII".into(),
        ));
    }
    *offset = end;
    String::from_utf8(value.to_vec())
        .map_err(|_| ArchiveMigrationError::Invalid("legacy ASCII string is not UTF-8".into()))
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, ArchiveMigrationError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| ArchiveMigrationError::Invalid("legacy integer overflow".into()))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| ArchiveMigrationError::Invalid("legacy integer exceeds file".into()))?;
    *offset = end;
    Ok(u32::from_le_bytes(value.try_into().map_err(|_| {
        ArchiveMigrationError::Invalid("legacy integer width mismatch".into())
    })?))
}

fn read_i32(bytes: &[u8], offset: &mut usize) -> Result<i32, ArchiveMigrationError> {
    Ok(read_u32(bytes, offset)? as i32)
}

fn read_i32_at(bytes: &[u8], offset: usize) -> Result<i32, ArchiveMigrationError> {
    let mut offset = offset;
    read_i32(bytes, &mut offset)
}

fn read_i64(bytes: &[u8], offset: &mut usize) -> Result<i64, ArchiveMigrationError> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| ArchiveMigrationError::Invalid("legacy integer overflow".into()))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| ArchiveMigrationError::Invalid("legacy integer exceeds file".into()))?;
    *offset = end;
    Ok(i64::from_le_bytes(value.try_into().map_err(|_| {
        ArchiveMigrationError::Invalid("legacy integer width mismatch".into())
    })?))
}
