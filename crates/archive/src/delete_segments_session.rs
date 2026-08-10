//! Session for deleting a recording's segment files.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use crate::codecs::RecordingSignal;

/// Sentinel used for the unused signal fields in a delete notification.
pub const NULL_VALUE: i64 = -1;
/// Suffix used when a segment cannot be removed immediately.
pub const DELETE_SUFFIX: &str = ".del";

/// Errors found while constructing or executing a delete-segments session.
#[derive(Debug, thiserror::Error)]
pub enum DeleteSegmentsError {
    #[error("invalid segment filename for recording {recording_id}: {path}")]
    InvalidSegmentFilename { recording_id: i64, path: PathBuf },
    #[error("could not delete segment {path}: {source}")]
    Delete {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// The control-plane operations required when a delete session closes.
pub trait DeleteSegmentsControl {
    fn remove_delete_segments_session(&mut self, recording_id: i64);

    fn send_signal(
        &mut self,
        correlation_id: i64,
        recording_id: i64,
        position: i64,
        subscription_id: i64,
        signal: RecordingSignal,
    );
}

/// Incrementally deletes the files belonging to one recording.
#[derive(Debug)]
pub struct DeleteSegmentsSession {
    recording_id: i64,
    correlation_id: i64,
    max_delete_position: i64,
    files: VecDeque<PathBuf>,
}

impl DeleteSegmentsSession {
    pub fn new(
        recording_id: i64,
        correlation_id: i64,
        files: VecDeque<PathBuf>,
    ) -> Result<Self, DeleteSegmentsError> {
        let max_delete_position = files
            .iter()
            .map(|path| parse_segment_position(path, recording_id))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max()
            .unwrap_or(i64::MIN);

        Ok(Self {
            recording_id,
            correlation_id,
            max_delete_position,
            files,
        })
    }

    pub fn max_delete_position(&self) -> i64 {
        self.max_delete_position
    }

    pub fn is_done(&self) -> bool {
        self.files.is_empty()
    }

    pub fn session_id(&self) -> i64 {
        self.recording_id
    }

    /// Delete one queued segment, returning the amount of work performed.
    pub fn do_work(&mut self) -> Result<usize, DeleteSegmentsError> {
        let Some(path) = self.files.pop_front() else {
            return Ok(0);
        };

        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if path.extension().and_then(|extension| extension.to_str()) != Some("del") {
                    let renamed = PathBuf::from(format!("{}{}", path.display(), DELETE_SUFFIX));
                    match std::fs::remove_file(&renamed) {
                        Ok(()) => {}
                        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                        Err(source) => {
                            return Err(DeleteSegmentsError::Delete {
                                path: renamed,
                                source,
                            });
                        }
                    }
                }
            }
            Err(source) => return Err(DeleteSegmentsError::Delete { path, source }),
        }

        Ok(1)
    }

    /// Remove the session and notify the control plane in upstream order.
    pub fn close<C: DeleteSegmentsControl>(&self, control: &mut C) {
        control.remove_delete_segments_session(self.recording_id);
        control.send_signal(
            self.correlation_id,
            self.recording_id,
            NULL_VALUE,
            NULL_VALUE,
            RecordingSignal::Delete,
        );
    }
}

fn parse_segment_position(path: &Path, recording_id: i64) -> Result<i64, DeleteSegmentsError> {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| DeleteSegmentsError::InvalidSegmentFilename {
            recording_id,
            path: path.to_path_buf(),
        })?;
    let prefix = format!("{recording_id}-");
    let position = filename
        .strip_prefix(&prefix)
        .and_then(|name| name.split_once('.'))
        .and_then(|(position, _)| position.parse().ok())
        .ok_or_else(|| DeleteSegmentsError::InvalidSegmentFilename {
            recording_id,
            path: path.to_path_buf(),
        })?;
    Ok(position)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn computes_max_delete_position_from_recording_segments() {
        let recording_id = 18;
        let correlation_id = 42;
        let files = VecDeque::from([
            Path::new("18-1048576.rec").to_path_buf(),
            Path::new("18-0.rec.del").to_path_buf(),
            Path::new("18-42.rec").to_path_buf(),
            Path::new("18--9999999999.rec").to_path_buf(),
            Path::new("18-12345000000.rec.del").to_path_buf(),
            Path::new("18-56678.rec").to_path_buf(),
        ]);

        let session = DeleteSegmentsSession::new(recording_id, correlation_id, files).unwrap();

        assert_eq!(12_345_000_000, session.max_delete_position());
        assert_eq!(recording_id, session.session_id());
        assert!(!session.is_done());
    }

    #[test]
    fn close_removes_session_before_sending_delete_signal() {
        #[derive(Debug, PartialEq, Eq)]
        enum Event {
            Remove(i64),
            Signal(i64, i64, i64, i64, RecordingSignal),
        }

        #[derive(Default)]
        struct Control {
            events: Vec<Event>,
        }

        impl DeleteSegmentsControl for Control {
            fn remove_delete_segments_session(&mut self, recording_id: i64) {
                self.events.push(Event::Remove(recording_id));
            }

            fn send_signal(
                &mut self,
                correlation_id: i64,
                recording_id: i64,
                position: i64,
                subscription_id: i64,
                signal: RecordingSignal,
            ) {
                self.events.push(Event::Signal(
                    correlation_id,
                    recording_id,
                    position,
                    subscription_id,
                    signal,
                ));
            }
        }

        let session = DeleteSegmentsSession::new(0, -4_732_948_723, VecDeque::new()).unwrap();
        let mut control = Control::default();

        session.close(&mut control);

        assert_eq!(
            vec![
                Event::Remove(0),
                Event::Signal(
                    -4_732_948_723,
                    0,
                    NULL_VALUE,
                    NULL_VALUE,
                    RecordingSignal::Delete,
                ),
            ],
            control.events
        );
        assert!(session.is_done());
    }

    #[test]
    fn rejects_a_segment_from_another_recording() {
        let error = DeleteSegmentsSession::new(
            18,
            42,
            VecDeque::from([Path::new("19-0.rec").to_path_buf()]),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DeleteSegmentsError::InvalidSegmentFilename { .. }
        ));
    }

    #[test]
    fn empty_session_does_no_work() {
        let mut session = DeleteSegmentsSession::new(18, 42, VecDeque::new()).unwrap();

        assert_eq!(0, session.do_work().unwrap());
        assert!(session.is_done());
    }
}
