//! Persistent recording and replay runtime over real Aeron client subscriptions/publications.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use aeron_client::logbuffer::{FRAME_ALIGNMENT, HEADER_LENGTH};
use aeron_client::publication::{CLOSED, ConcurrentPublication, NOT_CONNECTED};
use aeron_client::subscription::Subscription;

use crate::catalog::Catalog;
use crate::recording_descriptor::RecordingDescriptor;

const CATALOG_FILE_NAME: &str = "catalog.dat";
const DEFAULT_SEGMENT_LENGTH: i32 = 128 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ArchiveRuntimeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("subscription has no active image")]
    NoImage,
    #[error("unknown recording id: {0}")]
    UnknownRecording(i64),
    #[error("recording is already active: {0}")]
    RecordingActive(i64),
    #[error(
        "invalid replay position {position} for recording {recording_id}: range is {start}..={stop}"
    )]
    InvalidReplayPosition {
        recording_id: i64,
        position: i64,
        start: i64,
        stop: i64,
    },
    #[error("invalid replay length: {0}")]
    InvalidReplayLength(i64),
    #[error("invalid recorded frame at position {position}: length {length}")]
    InvalidFrame { position: i64, length: i32 },
    #[error("replay publication rejected payload at position {position}: result {result}")]
    ReplayOffer { position: i64, result: i64 },
}

pub struct ArchiveRuntime {
    archive_dir: PathBuf,
    catalog_path: PathBuf,
    catalog: Catalog,
    active: HashMap<i64, RecordingWriter>,
    segment_length: i32,
}

pub struct ReplayCursor {
    archive_dir: PathBuf,
    descriptor: RecordingDescriptor,
    position: i64,
    limit: i64,
}

impl ReplayCursor {
    pub fn position(&self) -> i64 {
        self.position
    }

    pub fn limit(&self) -> i64 {
        self.limit
    }

    pub fn is_complete(&self) -> bool {
        self.position >= self.limit
    }

    pub fn poll(
        &mut self,
        publication: &ConcurrentPublication,
        fragment_limit: usize,
    ) -> Result<usize, ArchiveRuntimeError> {
        let mut messages = 0;
        while !self.is_complete() && messages < fragment_limit {
            let position = self.position;
            let (mut payload, aligned_length) =
                read_recorded_payload(&self.archive_dir, &self.descriptor, position)?;
            let payload_length = payload.len();
            let buffer = agrona::AtomicBuffer::new(&mut payload);
            let result = publication.offer(&buffer, 0, payload_length).map_err(|_| {
                ArchiveRuntimeError::ReplayOffer {
                    position,
                    result: i64::MIN,
                }
            })?;
            if result == CLOSED || result == NOT_CONNECTED {
                return Err(ArchiveRuntimeError::ReplayOffer { position, result });
            }
            if result < 0 {
                break;
            }
            self.position += i64::from(aligned_length);
            messages += 1;
        }
        Ok(messages)
    }
}

impl ArchiveRuntime {
    pub fn open(archive_dir: impl AsRef<Path>) -> Result<Self, ArchiveRuntimeError> {
        let archive_dir = archive_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&archive_dir)?;
        let catalog_path = archive_dir.join(CATALOG_FILE_NAME);
        let catalog = if catalog_path.exists() {
            Catalog::load_from_file(&catalog_path)?
        } else {
            Catalog::new()
        };
        Ok(Self {
            archive_dir,
            catalog_path,
            catalog,
            active: HashMap::new(),
            segment_length: DEFAULT_SEGMENT_LENGTH,
        })
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn start_recording(
        &mut self,
        subscription: &Subscription,
    ) -> Result<i64, ArchiveRuntimeError> {
        let image = subscription
            .images()
            .first()
            .ok_or(ArchiveRuntimeError::NoImage)?;
        let recording_id = self.catalog.allocate_recording_id();
        let start_position = image.position();
        let descriptor = RecordingDescriptor::new(
            0,
            0,
            recording_id,
            start_position,
            start_position,
            now_ms(),
            0,
            image.initial_term_id(),
            self.segment_length,
            image.term_length().min(i32::MAX as usize) as i32,
            1408,
            image.session_id(),
            subscription.stream_id(),
            subscription.channel().to_string(),
            subscription.channel().to_string(),
            image.source_identity().to_string(),
        );
        let writer = RecordingWriter::open(&self.archive_dir, &descriptor)?;
        self.catalog.add_recording(descriptor);
        self.catalog.save_to_file(&self.catalog_path)?;
        self.active.insert(recording_id, writer);
        Ok(recording_id)
    }

    pub fn poll_recording(
        &mut self,
        recording_id: i64,
        subscription: &mut Subscription,
        fragment_limit: usize,
    ) -> Result<usize, ArchiveRuntimeError> {
        let writer = self
            .active
            .get_mut(&recording_id)
            .ok_or(ArchiveRuntimeError::UnknownRecording(recording_id))?;
        let mut write_error = None;
        let fragments = subscription.poll(
            |buffer, _offset, _length, header| {
                if write_error.is_none() {
                    write_error = writer.write_frame(buffer, header.offset()).err();
                }
            },
            fragment_limit,
        );
        if let Some(error) = write_error {
            return Err(error);
        }
        Ok(fragments)
    }

    pub fn stop_recording(&mut self, recording_id: i64) -> Result<i64, ArchiveRuntimeError> {
        let mut writer = self
            .active
            .remove(&recording_id)
            .ok_or(ArchiveRuntimeError::UnknownRecording(recording_id))?;
        writer.sync()?;
        let stop_position = writer.position;
        self.catalog
            .update_stop_position(recording_id, stop_position, now_ms());
        self.catalog.save_to_file(&self.catalog_path)?;
        Ok(stop_position)
    }

    pub fn replay(
        &self,
        recording_id: i64,
        publication: &ConcurrentPublication,
    ) -> Result<usize, ArchiveRuntimeError> {
        let mut cursor = self.start_replay(recording_id, -1, -1)?;
        let mut messages = 0;
        while !cursor.is_complete() {
            let work = cursor.poll(publication, usize::MAX)?;
            if work == 0 {
                return Err(ArchiveRuntimeError::ReplayOffer {
                    position: cursor.position(),
                    result: aeron_client::publication::BACK_PRESSURED,
                });
            }
            messages += work;
        }
        Ok(messages)
    }

    pub fn start_replay(
        &self,
        recording_id: i64,
        requested_position: i64,
        requested_length: i64,
    ) -> Result<ReplayCursor, ArchiveRuntimeError> {
        if self.active.contains_key(&recording_id) {
            return Err(ArchiveRuntimeError::RecordingActive(recording_id));
        }
        let descriptor = self
            .catalog
            .get_recording(recording_id)
            .ok_or(ArchiveRuntimeError::UnknownRecording(recording_id))?
            .clone();
        let position = if requested_position == -1 {
            descriptor.start_position
        } else {
            requested_position
        };
        if position < descriptor.start_position || position > descriptor.stop_position {
            return Err(ArchiveRuntimeError::InvalidReplayPosition {
                recording_id,
                position,
                start: descriptor.start_position,
                stop: descriptor.stop_position,
            });
        }
        let available = descriptor.stop_position - position;
        let length = match requested_length {
            -1 | -2 => available,
            value if value >= 0 => value.min(available),
            value => return Err(ArchiveRuntimeError::InvalidReplayLength(value)),
        };
        let limit = position.saturating_add(length);
        if position < descriptor.stop_position {
            let _ = read_recorded_payload(&self.archive_dir, &descriptor, position)?;
        }
        Ok(ReplayCursor {
            archive_dir: self.archive_dir.clone(),
            descriptor,
            position,
            limit,
        })
    }
}

fn read_recorded_payload(
    archive_dir: &Path,
    descriptor: &RecordingDescriptor,
    position: i64,
) -> Result<(Vec<u8>, i32), ArchiveRuntimeError> {
    let segment_base = descriptor.segment_position(position);
    let segment_offset = (position - segment_base) as u64;
    let mut file = File::open(archive_dir.join(descriptor.segment_file_name(position)))?;
    file.seek(SeekFrom::Start(segment_offset))?;
    let mut length_bytes = [0; 4];
    file.read_exact(&mut length_bytes)?;
    let frame_length = i32::from_le_bytes(length_bytes);
    if frame_length < HEADER_LENGTH {
        return Err(ArchiveRuntimeError::InvalidFrame {
            position,
            length: frame_length,
        });
    }
    let payload_length = (frame_length - HEADER_LENGTH) as usize;
    file.seek(SeekFrom::Current((HEADER_LENGTH - 4) as i64))?;
    let mut payload = vec![0; payload_length];
    file.read_exact(&mut payload)?;
    Ok((payload, align(frame_length, FRAME_ALIGNMENT)))
}

struct RecordingWriter {
    archive_dir: PathBuf,
    descriptor: RecordingDescriptor,
    file: File,
    segment_base: i64,
    position: i64,
}

impl RecordingWriter {
    fn open(
        archive_dir: &Path,
        descriptor: &RecordingDescriptor,
    ) -> Result<Self, ArchiveRuntimeError> {
        let segment_base = descriptor.segment_position(descriptor.start_position);
        let file = open_segment(archive_dir, descriptor, segment_base)?;
        Ok(Self {
            archive_dir: archive_dir.to_path_buf(),
            descriptor: descriptor.clone(),
            file,
            segment_base,
            position: descriptor.start_position,
        })
    }

    fn write_frame(
        &mut self,
        buffer: &agrona::AtomicBuffer,
        frame_offset: usize,
    ) -> Result<(), ArchiveRuntimeError> {
        let frame_length = buffer.get_i32(frame_offset);
        if frame_length < HEADER_LENGTH {
            return Err(ArchiveRuntimeError::InvalidFrame {
                position: self.position,
                length: frame_length,
            });
        }
        let aligned_length = align(frame_length, FRAME_ALIGNMENT);
        if self.position - self.segment_base + i64::from(aligned_length)
            > i64::from(self.descriptor.segment_file_length)
        {
            self.segment_base += i64::from(self.descriptor.segment_file_length);
            self.file = open_segment(&self.archive_dir, &self.descriptor, self.segment_base)?;
        }
        let mut bytes = vec![0; aligned_length as usize];
        buffer.get_bytes(frame_offset, &mut bytes);
        self.file
            .seek(SeekFrom::Start((self.position - self.segment_base) as u64))?;
        self.file.write_all(&bytes)?;
        self.position += i64::from(aligned_length);
        Ok(())
    }

    fn sync(&mut self) -> Result<(), std::io::Error> {
        self.file.sync_all()
    }
}

fn open_segment(
    archive_dir: &Path,
    descriptor: &RecordingDescriptor,
    segment_base: i64,
) -> Result<File, std::io::Error> {
    let path = archive_dir.join(format!("{}-{segment_base}.rec", descriptor.recording_id));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.set_len(descriptor.segment_file_length as u64)?;
    Ok(file)
}

fn align(value: i32, alignment: i32) -> i32 {
    (value + alignment - 1) & -alignment
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as i64)
}
