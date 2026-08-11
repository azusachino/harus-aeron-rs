//! Legacy Archive v2 fixture builders used by migration tests.
//!
//! The byte layout mirrors `vendor/aeron/aeron-archive/src/test/java/io/aeron/archive/
//! ArchiveMigrationUtils.java`, including its 1024-byte catalog frames and
//! pre-SBE mark-file header.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

pub const VERSION_2_1_0: i32 = (2 << 16) | (1 << 8);
pub const MARK_FILE_HEADER_LENGTH_V2: usize = 8 * 1024;
pub const RECORDING_FRAME_LENGTH_V2: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingDescriptorV2 {
    pub valid: u8,
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub recording_id: i64,
    pub start_timestamp: i64,
    pub stop_timestamp: i64,
    pub start_position: i64,
    pub stop_position: i64,
    pub initial_term_id: i32,
    pub segment_file_length: i32,
    pub term_buffer_length: i32,
    pub mtu_length: i32,
    pub session_id: i32,
    pub stream_id: i32,
    pub stripped_channel: String,
    pub original_channel: String,
    pub source_identity: String,
}

pub fn create_archive_mark_file_in_version2(
    archive_dir: &Path,
    activity_timestamp: i64,
) -> io::Result<std::path::PathBuf> {
    std::fs::create_dir_all(archive_dir)?;
    let path = archive_dir.join("archive-mark.dat");
    let control_channel = "ctx.controlChannel()";
    let local_control_channel = "ctx.localControlChannel()";
    let events_channel = "ctx.recordingEventsChannel()";
    let aeron_directory = "ctx.aeronDirectoryName()";
    let error_buffer_length = 100usize;
    let header_content_length = 128
        + 4 * std::mem::size_of::<u32>()
        + control_channel.len()
        + local_control_channel.len()
        + events_channel.len()
        + aeron_directory.len();
    if header_content_length > MARK_FILE_HEADER_LENGTH_V2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "v2 mark-file header exceeds 8 KiB",
        ));
    }

    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    file.set_len((MARK_FILE_HEADER_LENGTH_V2 + error_buffer_length) as u64)?;
    let mut buffer = vec![0u8; MARK_FILE_HEADER_LENGTH_V2 + error_buffer_length];
    let mut offset = 0;
    put_i32(&mut buffer, &mut offset, VERSION_2_1_0)?;
    put_i64(&mut buffer, &mut offset, activity_timestamp)?;
    put_i64(&mut buffer, &mut offset, activity_timestamp)?;
    put_i64(&mut buffer, &mut offset, 1)?;
    put_i32(&mut buffer, &mut offset, 777)?;
    put_i32(&mut buffer, &mut offset, 42)?;
    put_i32(&mut buffer, &mut offset, 13)?;
    put_i32(&mut buffer, &mut offset, MARK_FILE_HEADER_LENGTH_V2 as i32)?;
    put_i32(&mut buffer, &mut offset, error_buffer_length as i32)?;
    for value in [
        control_channel,
        local_control_channel,
        events_channel,
        aeron_directory,
    ] {
        put_ascii(&mut buffer, &mut offset, value)?;
    }
    file.write_all(&buffer)?;
    file.sync_all()?;
    Ok(path)
}

pub fn create_catalog_in_version2(
    archive_dir: &Path,
    recordings: &[RecordingDescriptorV2],
) -> io::Result<std::path::PathBuf> {
    std::fs::create_dir_all(archive_dir)?;
    let path = archive_dir.join("catalog.dat");
    let mut file = File::create(&path)?;
    let length = RECORDING_FRAME_LENGTH_V2
        .checked_add(
            recordings
                .len()
                .checked_mul(RECORDING_FRAME_LENGTH_V2)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "catalog length overflow")
                })?,
        )
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "catalog length overflow"))?;
    file.set_len(length as u64)?;
    let mut buffer = vec![0u8; length];
    buffer[0..4].copy_from_slice(&VERSION_2_1_0.to_le_bytes());
    buffer[4..8].copy_from_slice(&(RECORDING_FRAME_LENGTH_V2 as i32).to_le_bytes());
    for (index, recording) in recordings.iter().enumerate() {
        write_recording(
            &mut buffer,
            RECORDING_FRAME_LENGTH_V2 * (index + 1),
            recording,
        )?;
    }
    file.write_all(&buffer)?;
    file.sync_all()?;
    Ok(path)
}

fn write_recording(
    buffer: &mut [u8],
    index: usize,
    recording: &RecordingDescriptorV2,
) -> io::Result<()> {
    let mut offset = index + 32;
    for value in [
        recording.control_session_id,
        recording.correlation_id,
        recording.recording_id,
        recording.start_timestamp,
        recording.stop_timestamp,
        recording.start_position,
        recording.stop_position,
    ] {
        put_i64(buffer, &mut offset, value)?;
    }
    for value in [
        recording.initial_term_id,
        recording.segment_file_length,
        recording.term_buffer_length,
        recording.mtu_length,
        recording.session_id,
        recording.stream_id,
    ] {
        put_i32(buffer, &mut offset, value)?;
    }
    for value in [
        &recording.stripped_channel,
        &recording.original_channel,
        &recording.source_identity,
    ] {
        put_ascii(buffer, &mut offset, value)?;
    }
    let total_length = offset - index;
    if total_length > RECORDING_FRAME_LENGTH_V2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "recordingId={} encoded length is {} bytes which exceeds {} bytes",
                recording.recording_id, total_length, RECORDING_FRAME_LENGTH_V2
            ),
        ));
    }
    let mut header_offset = index;
    put_i32(buffer, &mut header_offset, (total_length - 32) as i32)?;
    let valid_offset = index
        .checked_add(std::mem::size_of::<i32>())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "catalog offset overflow"))?;
    if valid_offset >= buffer.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "catalog validity byte exceeds buffer",
        ));
    }
    buffer[valid_offset] = recording.valid;
    Ok(())
}

fn put_ascii(buffer: &mut [u8], offset: &mut usize, value: &str) -> io::Result<()> {
    if !value.is_ascii() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "legacy Archive fields must be ASCII",
        ));
    }
    let end = offset
        .checked_add(4)
        .and_then(|start| start.checked_add(value.len()))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "ASCII field overflow"))?;
    if end > buffer.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ASCII field exceeds buffer",
        ));
    }
    buffer[*offset..*offset + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
    buffer[*offset + 4..end].copy_from_slice(value.as_bytes());
    *offset = end;
    Ok(())
}

fn put_i32(buffer: &mut [u8], offset: &mut usize, value: i32) -> io::Result<()> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "i32 field overflow"))?;
    if end > buffer.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "i32 field exceeds buffer",
        ));
    }
    buffer[*offset..end].copy_from_slice(&value.to_le_bytes());
    *offset = end;
    Ok(())
}

fn put_i64(buffer: &mut [u8], offset: &mut usize, value: i64) -> io::Result<()> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "i64 field overflow"))?;
    if end > buffer.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "i64 field exceeds buffer",
        ));
    }
    buffer[*offset..end].copy_from_slice(&value.to_le_bytes());
    *offset = end;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(source_identity: &str) -> RecordingDescriptorV2 {
        RecordingDescriptorV2 {
            valid: 1,
            control_session_id: 42,
            correlation_id: 21,
            recording_id: 0,
            start_timestamp: 123,
            stop_timestamp: 456,
            start_position: 0,
            stop_position: 1024,
            initial_term_id: 3,
            segment_file_length: 500,
            term_buffer_length: 1024,
            mtu_length: 1408,
            session_id: 8,
            stream_id: 55,
            stripped_channel: "strCh".into(),
            original_channel: "aeron:udp?endpoint=localhost:9090".into(),
            source_identity: source_identity.into(),
        }
    }

    fn directory(label: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "aeron-archive-migration-{}-{}-{}",
            std::process::id(),
            label,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn writes_v2_mark_file_with_legacy_header_and_strings() {
        let directory = directory("mark");
        let path = create_archive_mark_file_in_version2(&directory, 1234).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), MARK_FILE_HEADER_LENGTH_V2 + 100);
        assert_eq!(
            i32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            VERSION_2_1_0
        );
        assert_eq!(i64::from_le_bytes(bytes[4..12].try_into().unwrap()), 1234);
        assert_eq!(i32::from_le_bytes(bytes[28..32].try_into().unwrap()), 777);
        assert_eq!(u32::from_le_bytes(bytes[48..52].try_into().unwrap()), 20);
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn writes_fixed_size_v2_catalog_frames_and_validity() {
        let directory = directory("catalog");
        let recordings = vec![
            descriptor("sourceA"),
            RecordingDescriptorV2 {
                valid: 0,
                recording_id: 4,
                ..descriptor("sourceB")
            },
        ];
        let path = create_catalog_in_version2(&directory, &recordings).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), RECORDING_FRAME_LENGTH_V2 * 3);
        assert_eq!(
            i32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            VERSION_2_1_0
        );
        assert_eq!(i32::from_le_bytes(bytes[4..8].try_into().unwrap()), 1024);
        assert_eq!(bytes[1024 + 4], 1);
        assert_eq!(bytes[2048 + 4], 0);
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn rejects_a_v2_descriptor_that_does_not_fit_a_frame() {
        let directory = directory("oversize");
        let recording = descriptor(&"x".repeat(1024));
        assert!(matches!(
            create_catalog_in_version2(&directory, &[recording]),
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
        let _ = std::fs::remove_dir_all(directory);
    }
}
