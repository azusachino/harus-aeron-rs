//! Read-only command-line inspection for a persisted Archive catalog.

use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};

use aeron_client::logbuffer::FRAME_ALIGNMENT;

use crate::archive_mark_file::{ArchiveMarkFile, ArchiveMarkFileError, FILENAME, LINK_FILENAME};
use crate::catalog::{Catalog, RecordingState};
use crate::recording_descriptor::RecordingDescriptor;

const CATALOG_FILE_NAME: &str = "catalog.dat";
const SEGMENT_FILE_SUFFIX: &str = ".rec";

#[derive(Debug)]
pub enum ArchiveToolError {
    InvalidArguments(String),
    Io(std::io::Error),
    MarkFile(ArchiveMarkFileError),
    RecordingNotFound(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationReport {
    pub checked_recordings: usize,
    pub invalid_recordings: Vec<i64>,
}

impl fmt::Display for ArchiveToolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArguments(message) => formatter.write_str(message),
            Self::Io(error) => error.fmt(formatter),
            Self::MarkFile(error) => error.fmt(formatter),
            Self::RecordingNotFound(recording_id) => {
                write!(
                    formatter,
                    "no recording found with recordingId: {recording_id}"
                )
            }
        }
    }
}

impl std::error::Error for ArchiveToolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidArguments(_) | Self::RecordingNotFound(_) => None,
            Self::MarkFile(error) => Some(error),
        }
    }
}

impl From<std::io::Error> for ArchiveToolError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ArchiveMarkFileError> for ArchiveToolError {
    fn from(error: ArchiveMarkFileError) -> Self {
        Self::MarkFile(error)
    }
}

/// Run the bounded read-only Archive tool commands.
///
/// Arguments follow the upstream order: `<archive-directory> <command> [recording-id]`.
/// The supported commands are `describe`, `describe <recording-id>`, and `describe-all`.
pub fn run(args: &[&str], output: &mut impl Write) -> Result<(), ArchiveToolError> {
    let archive_dir = args.first().map(PathBuf::from).ok_or_else(usage)?;
    if !archive_dir.is_dir() {
        return Err(ArchiveToolError::InvalidArguments(format!(
            "archive directory not found: {}",
            archive_dir.display()
        )));
    }

    match args.get(1..) {
        Some([command]) if *command == "describe" => describe(&archive_dir, output, false),
        Some([command, recording_id]) if *command == "describe" => {
            let recording_id = recording_id.parse().map_err(|_| {
                ArchiveToolError::InvalidArguments(format!("invalid recording id: {recording_id}"))
            })?;
            describe_recording(&archive_dir, output, recording_id)
        }
        Some([command]) if *command == "describe-all" => describe(&archive_dir, output, true),
        _ => Err(usage()),
    }
}

fn describe(
    archive_dir: &Path,
    output: &mut impl Write,
    include_invalid: bool,
) -> Result<(), ArchiveToolError> {
    let _mark_file = open_mark_file(archive_dir)?;
    let catalog = load_catalog(archive_dir)?;
    for descriptor in catalog.recordings() {
        if include_invalid
            || catalog
                .get_valid_recording(descriptor.recording_id)
                .is_some()
        {
            write_descriptor(
                output,
                descriptor,
                catalog.recording_state(descriptor.recording_id),
            )?;
        }
    }
    Ok(())
}

fn describe_recording(
    archive_dir: &Path,
    output: &mut impl Write,
    recording_id: i64,
) -> Result<(), ArchiveToolError> {
    let _mark_file = open_mark_file(archive_dir)?;
    let catalog = load_catalog(archive_dir)?;
    let descriptor = catalog
        .get_recording(recording_id)
        .ok_or(ArchiveToolError::RecordingNotFound(recording_id))?;
    write_descriptor(output, descriptor, catalog.recording_state(recording_id))?;
    Ok(())
}

fn load_catalog(archive_dir: &Path) -> Result<Catalog, ArchiveToolError> {
    Ok(Catalog::load_from_file(
        &archive_dir.join(CATALOG_FILE_NAME),
    )?)
}

pub fn resolve_mark_file(archive_dir: &Path) -> Result<Option<PathBuf>, ArchiveToolError> {
    let link_path = archive_dir.join(LINK_FILENAME);
    if link_path.exists() {
        let target = std::fs::read_to_string(&link_path)?.trim().to_owned();
        if target.is_empty() {
            return Err(ArchiveToolError::InvalidArguments(format!(
                "empty Archive mark-file link: {}",
                link_path.display()
            )));
        }
        return Ok(Some(PathBuf::from(target).join(FILENAME)));
    }

    let default_path = archive_dir.join(FILENAME);
    Ok(default_path.exists().then_some(default_path))
}

pub fn catalog_capacity(archive_dir: &Path) -> Result<u64, ArchiveToolError> {
    Ok(std::fs::metadata(archive_dir.join(CATALOG_FILE_NAME))?.len())
}

pub fn mark_recording_invalid(
    archive_dir: &Path,
    recording_id: i64,
) -> Result<(), ArchiveToolError> {
    change_recording_state(
        archive_dir,
        recording_id,
        RecordingState::Valid,
        RecordingState::Invalid,
    )
}

pub fn mark_recording_valid(archive_dir: &Path, recording_id: i64) -> Result<(), ArchiveToolError> {
    change_recording_state(
        archive_dir,
        recording_id,
        RecordingState::Invalid,
        RecordingState::Valid,
    )
}

fn change_recording_state(
    archive_dir: &Path,
    recording_id: i64,
    expected: RecordingState,
    target: RecordingState,
) -> Result<(), ArchiveToolError> {
    let catalog = load_catalog(archive_dir)?;
    match catalog.recording_state(recording_id) {
        None => return Err(ArchiveToolError::RecordingNotFound(recording_id)),
        Some(state) if state != expected => {
            return Err(ArchiveToolError::InvalidArguments(format!(
                "recording {recording_id} is {}, expected {}",
                state_name(Some(state)),
                state_name(Some(expected)),
            )));
        }
        Some(_) => {}
    }
    if !catalog.change_state(recording_id, target) {
        return Err(ArchiveToolError::InvalidArguments(format!(
            "failed to change state for recording {recording_id}"
        )));
    }
    catalog.save_to_file(&archive_dir.join(CATALOG_FILE_NAME))?;
    Ok(())
}

pub fn compact(archive_dir: &Path) -> Result<usize, ArchiveToolError> {
    let mut catalog = load_catalog(archive_dir)?;
    let invalid_recordings = catalog
        .recordings()
        .iter()
        .filter(|descriptor| {
            catalog.recording_state(descriptor.recording_id) == Some(RecordingState::Invalid)
        })
        .map(|descriptor| descriptor.recording_id)
        .collect::<Vec<_>>();

    for recording_id in &invalid_recordings {
        catalog.remove_recording(*recording_id);
        remove_segment_files(archive_dir, *recording_id)?;
    }
    if !invalid_recordings.is_empty() {
        catalog.save_to_file(&archive_dir.join(CATALOG_FILE_NAME))?;
    }
    Ok(invalid_recordings.len())
}

pub fn delete_orphaned_segments(
    archive_dir: &Path,
    target_recording_id: Option<i64>,
) -> Result<usize, ArchiveToolError> {
    let catalog = load_catalog(archive_dir)?;
    let mut deleted = 0;
    for entry in std::fs::read_dir(archive_dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        let Some(recording_id) = parse_segment_recording_id(&path) else {
            continue;
        };
        if target_recording_id.is_some_and(|target| target != recording_id)
            || catalog.get_recording(recording_id).is_some()
        {
            continue;
        }
        std::fs::remove_file(path)?;
        deleted += 1;
    }
    Ok(deleted)
}

pub fn verify(archive_dir: &Path) -> Result<VerificationReport, ArchiveToolError> {
    let catalog = load_catalog(archive_dir)?;
    let invalid_recordings = catalog
        .recordings()
        .iter()
        .filter(|descriptor| !descriptor_is_valid(descriptor))
        .map(|descriptor| descriptor.recording_id)
        .collect::<Vec<_>>();
    Ok(VerificationReport {
        checked_recordings: catalog.recordings().len(),
        invalid_recordings,
    })
}

pub fn verify_and_mark_invalid(archive_dir: &Path) -> Result<VerificationReport, ArchiveToolError> {
    let report = verify(archive_dir)?;
    if report.invalid_recordings.is_empty() {
        return Ok(report);
    }

    let catalog = load_catalog(archive_dir)?;
    for recording_id in &report.invalid_recordings {
        if catalog.recording_state(*recording_id) == Some(RecordingState::Valid) {
            catalog.change_state(*recording_id, RecordingState::Invalid);
        }
    }
    catalog.save_to_file(&archive_dir.join(CATALOG_FILE_NAME))?;
    Ok(report)
}

fn descriptor_is_valid(descriptor: &RecordingDescriptor) -> bool {
    descriptor.start_position >= 0
        && descriptor.start_position % i64::from(FRAME_ALIGNMENT) == 0
        && (descriptor.stop_position == -1
            || (descriptor.stop_position >= descriptor.start_position
                && descriptor.stop_position % i64::from(FRAME_ALIGNMENT) == 0))
        && descriptor.segment_file_length > 0
        && descriptor.term_buffer_length > 0
        && descriptor.mtu_length > 0
        && descriptor.segment_file_length % descriptor.term_buffer_length == 0
}

fn remove_segment_files(archive_dir: &Path, recording_id: i64) -> Result<usize, ArchiveToolError> {
    let mut removed = 0;
    for entry in std::fs::read_dir(archive_dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        if parse_segment_recording_id(&path) == Some(recording_id) {
            std::fs::remove_file(path)?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn parse_segment_recording_id(path: &Path) -> Option<i64> {
    let name = path.file_name()?.to_str()?;
    let name = name.strip_suffix(SEGMENT_FILE_SUFFIX)?;
    let (recording_id, _) = name.split_once('-')?;
    recording_id.parse().ok()
}

fn open_mark_file(archive_dir: &Path) -> Result<Option<ArchiveMarkFile>, ArchiveToolError> {
    resolve_mark_file(archive_dir)?
        .map(ArchiveMarkFile::open)
        .transpose()
        .map_err(ArchiveToolError::from)
}

fn write_descriptor(
    output: &mut impl Write,
    descriptor: &RecordingDescriptor,
    state: Option<RecordingState>,
) -> Result<(), ArchiveToolError> {
    writeln!(
        output,
        "|recordingId={}|startPosition={}|stopPosition={}|startTimestamp={}|stopTimestamp={}|initialTermId={}|segmentFileLength={}|termBufferLength={}|mtuLength={}|sessionId={}|streamId={}|strippedChannel={}|originalChannel={}|sourceIdentity={}|state={}|",
        descriptor.recording_id,
        descriptor.start_position,
        descriptor.stop_position,
        descriptor.start_timestamp,
        descriptor.stop_timestamp,
        descriptor.initial_term_id,
        descriptor.segment_file_length,
        descriptor.term_buffer_length,
        descriptor.mtu_length,
        descriptor.session_id,
        descriptor.stream_id,
        descriptor.stripped_channel,
        descriptor.original_channel,
        descriptor.source_identity,
        state_name(state),
    )?;
    Ok(())
}

fn state_name(state: Option<RecordingState>) -> &'static str {
    match state {
        Some(RecordingState::Valid) => "VALID",
        Some(RecordingState::Invalid) => "INVALID",
        None => "UNKNOWN",
    }
}

fn usage() -> ArchiveToolError {
    ArchiveToolError::InvalidArguments(
        "usage: aeron-archive-tool <archive-directory> <describe|describe-all> [recording-id]"
            .into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_recording_state_names() {
        assert_eq!(state_name(Some(RecordingState::Valid)), "VALID");
        assert_eq!(state_name(Some(RecordingState::Invalid)), "INVALID");
        assert_eq!(state_name(None), "UNKNOWN");
    }
}
