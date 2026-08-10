//! Read-only command-line inspection for a persisted Archive catalog.

use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::catalog::{Catalog, RecordingState};
use crate::recording_descriptor::RecordingDescriptor;

const CATALOG_FILE_NAME: &str = "catalog.dat";

#[derive(Debug)]
pub enum ArchiveToolError {
    InvalidArguments(String),
    Io(std::io::Error),
    RecordingNotFound(i64),
}

impl fmt::Display for ArchiveToolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArguments(message) => formatter.write_str(message),
            Self::Io(error) => error.fmt(formatter),
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
        }
    }
}

impl From<std::io::Error> for ArchiveToolError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
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
