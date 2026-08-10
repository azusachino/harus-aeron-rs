//! Persistent recording and replay runtime over real Aeron client subscriptions/publications.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use aeron_client::logbuffer::{FRAME_ALIGNMENT, HEADER_LENGTH, mtu_length};
use aeron_client::publication::{CLOSED, ConcurrentPublication, NOT_CONNECTED};
use aeron_client::subscription::Subscription;

use crate::catalog::Catalog;
use crate::recording_descriptor::RecordingDescriptor;

const CATALOG_FILE_NAME: &str = "catalog.dat";
const DEFAULT_SEGMENT_LENGTH: i32 = 128 * 1024 * 1024;
pub const NULL_POSITION: i64 = -1;

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
    #[error("recording extension does not match descriptor: {0}")]
    InvalidExtension(String),
    #[error(
        "invalid truncate position {position} for recording {recording_id}: range is {start}..={stop} and alignment is {alignment}"
    )]
    InvalidTruncatePosition {
        recording_id: i64,
        position: i64,
        start: i64,
        stop: i64,
        alignment: i32,
    },
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
    pub fn recording_id(&self) -> i64 {
        self.descriptor.recording_id
    }

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
        image_correlation_id: i64,
    ) -> Result<i64, ArchiveRuntimeError> {
        let image = subscription
            .image_by_correlation_id(image_correlation_id)
            .ok_or(ArchiveRuntimeError::NoImage)?;
        let recording_id = self.catalog.allocate_recording_id();
        let start_position = image.position();
        let descriptor = RecordingDescriptor::new(
            0,
            0,
            recording_id,
            start_position,
            NULL_POSITION,
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
        image_correlation_id: i64,
        fragment_limit: usize,
    ) -> Result<usize, ArchiveRuntimeError> {
        let writer = self
            .active
            .get_mut(&recording_id)
            .ok_or(ArchiveRuntimeError::UnknownRecording(recording_id))?;
        let mut write_error = None;
        let fragments = subscription.poll_image(
            image_correlation_id,
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

    pub fn extend_recording(
        &mut self,
        recording_id: i64,
        control_session_id: i64,
        correlation_id: i64,
        subscription: &Subscription,
        image_correlation_id: i64,
    ) -> Result<(), ArchiveRuntimeError> {
        if self.active.contains_key(&recording_id) {
            return Err(ArchiveRuntimeError::RecordingActive(recording_id));
        }
        let image = subscription
            .image_by_correlation_id(image_correlation_id)
            .ok_or(ArchiveRuntimeError::NoImage)?;
        let descriptor = self
            .catalog
            .get_recording(recording_id)
            .ok_or(ArchiveRuntimeError::UnknownRecording(recording_id))?
            .clone();
        let image_mtu_length = mtu_length(&image.log_buffers().meta_data_buffer());
        if image.position() != descriptor.stop_position
            || image.initial_term_id() != descriptor.initial_term_id
            || image.term_length() != descriptor.term_buffer_length as usize
            || image_mtu_length != descriptor.mtu_length
        {
            return Err(ArchiveRuntimeError::InvalidExtension(format!(
                "join={} stop={} initial-term={} expected-initial-term={} term-length={} expected-term-length={} mtu={} expected-mtu={}",
                image.position(),
                descriptor.stop_position,
                image.initial_term_id(),
                descriptor.initial_term_id,
                image.term_length(),
                descriptor.term_buffer_length,
                image_mtu_length,
                descriptor.mtu_length
            )));
        }
        let writer =
            RecordingWriter::open_at(&self.archive_dir, &descriptor, descriptor.stop_position)?;
        self.catalog.extend_recording(
            recording_id,
            control_session_id,
            correlation_id,
            image.session_id(),
        );
        self.catalog.save_to_file(&self.catalog_path)?;
        self.active.insert(recording_id, writer);
        Ok(())
    }

    pub fn truncate_recording(
        &mut self,
        recording_id: i64,
        position: i64,
    ) -> Result<(), ArchiveRuntimeError> {
        if self.active.contains_key(&recording_id) {
            return Err(ArchiveRuntimeError::RecordingActive(recording_id));
        }
        let descriptor = self
            .catalog
            .get_recording(recording_id)
            .ok_or(ArchiveRuntimeError::UnknownRecording(recording_id))?
            .clone();
        if position < descriptor.start_position
            || position > descriptor.stop_position
            || position & i64::from(FRAME_ALIGNMENT - 1) != 0
        {
            return Err(ArchiveRuntimeError::InvalidTruncatePosition {
                recording_id,
                position,
                start: descriptor.start_position,
                stop: descriptor.stop_position,
                alignment: FRAME_ALIGNMENT,
            });
        }
        truncate_segments(&self.archive_dir, &descriptor, position)?;
        self.catalog
            .update_stop_position(recording_id, position, now_ms());
        self.catalog.save_to_file(&self.catalog_path)?;
        Ok(())
    }

    pub fn purge_recording(&mut self, recording_id: i64) -> Result<(), ArchiveRuntimeError> {
        if self.active.contains_key(&recording_id) {
            return Err(ArchiveRuntimeError::RecordingActive(recording_id));
        }
        if self.catalog.get_recording(recording_id).is_none() {
            return Err(ArchiveRuntimeError::UnknownRecording(recording_id));
        }
        delete_recording_segments(&self.archive_dir, recording_id)?;
        self.catalog.remove_recording(recording_id);
        self.catalog.save_to_file(&self.catalog_path)?;
        Ok(())
    }

    pub fn start_position(&self, recording_id: i64) -> Option<i64> {
        self.catalog
            .get_recording(recording_id)
            .map(|descriptor| descriptor.start_position)
    }

    pub fn stop_position(&self, recording_id: i64) -> Option<i64> {
        self.catalog
            .get_recording(recording_id)
            .map(|descriptor| descriptor.stop_position)
    }

    pub fn recording_position(&self, recording_id: i64) -> Option<i64> {
        self.catalog.get_recording(recording_id)?;
        Some(
            self.active
                .get(&recording_id)
                .map_or(NULL_POSITION, |writer| writer.position),
        )
    }

    pub fn max_recorded_position(&self, recording_id: i64) -> Option<i64> {
        self.active.get(&recording_id).map_or_else(
            || self.stop_position(recording_id),
            |writer| Some(writer.position),
        )
    }

    pub fn find_last_matching_recording(
        &self,
        min_recording_id: i64,
        session_id: i32,
        stream_id: i32,
        channel_fragment: &str,
    ) -> Option<i64> {
        self.catalog
            .find_last_matching(min_recording_id, session_id, stream_id, channel_fragment)
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
        let active_position = self.active.get(&recording_id).map(|writer| writer.position);
        let descriptor = self
            .catalog
            .get_recording(recording_id)
            .ok_or(ArchiveRuntimeError::UnknownRecording(recording_id))?
            .clone();
        let available_stop = active_position.unwrap_or(descriptor.stop_position);
        let position = if requested_position == -1 {
            descriptor.start_position
        } else {
            requested_position
        };
        if position < descriptor.start_position || position > available_stop {
            return Err(ArchiveRuntimeError::InvalidReplayPosition {
                recording_id,
                position,
                start: descriptor.start_position,
                stop: available_stop,
            });
        }
        let available = available_stop - position;
        let length = match requested_length {
            -1 | -2 => available,
            value if value >= 0 => value.min(available),
            value => return Err(ArchiveRuntimeError::InvalidReplayLength(value)),
        };
        let limit = position.saturating_add(length);
        if position < available_stop {
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

    fn open_at(
        archive_dir: &Path,
        descriptor: &RecordingDescriptor,
        position: i64,
    ) -> Result<Self, ArchiveRuntimeError> {
        let segment_base = descriptor.segment_position(position);
        let file = open_segment(archive_dir, descriptor, segment_base)?;
        Ok(Self {
            archive_dir: archive_dir.to_path_buf(),
            descriptor: descriptor.clone(),
            file,
            segment_base,
            position,
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

fn truncate_segments(
    archive_dir: &Path,
    descriptor: &RecordingDescriptor,
    position: i64,
) -> Result<(), std::io::Error> {
    let retained_segment_base = descriptor.segment_position(position);
    if position == descriptor.start_position {
        delete_recording_segments(archive_dir, descriptor.recording_id)?;
        return Ok(());
    }

    let retained_path = archive_dir.join(format!(
        "{}-{retained_segment_base}.rec",
        descriptor.recording_id
    ));
    let retained_offset = position - retained_segment_base;
    if retained_offset > 0 && position < descriptor.stop_position {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(retained_path)?;
        let offset = retained_offset as u64;
        file.seek(SeekFrom::Start(offset))?;
        let zeroes = [0_u8; 8192];
        let mut remaining = u64::from(descriptor.segment_file_length as u32) - offset;
        while remaining > 0 {
            let length = remaining.min(zeroes.len() as u64) as usize;
            file.write_all(&zeroes[..length])?;
            remaining -= length as u64;
        }
        file.sync_all()?;
    }

    for entry in std::fs::read_dir(archive_dir)? {
        let entry = entry?;
        if segment_base_from_name(
            &entry.file_name().to_string_lossy(),
            descriptor.recording_id,
        )
        .is_some_and(|segment_base| {
            segment_base > retained_segment_base
                || (retained_offset == 0 && segment_base == retained_segment_base)
        }) {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn delete_recording_segments(archive_dir: &Path, recording_id: i64) -> Result<(), std::io::Error> {
    for entry in std::fs::read_dir(archive_dir)? {
        let entry = entry?;
        if segment_base_from_name(&entry.file_name().to_string_lossy(), recording_id).is_some() {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn segment_base_from_name(name: &str, recording_id: i64) -> Option<i64> {
    name.strip_prefix(&format!("{recording_id}-"))?
        .strip_suffix(".rec")?
        .parse()
        .ok()
}

fn align(value: i32, alignment: i32) -> i32 {
    (value + alignment - 1) & -alignment
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor() -> RecordingDescriptor {
        RecordingDescriptor::new(
            1,
            2,
            7,
            0,
            320,
            3,
            4,
            5,
            128,
            64,
            32,
            6,
            8,
            "aeron:ipc".into(),
            "aeron:ipc".into(),
            "source".into(),
        )
    }

    fn segment_test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "aeron-archive-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    fn write_segments(path: &Path) {
        std::fs::create_dir_all(path).unwrap();
        for base in [0, 128, 256] {
            std::fs::write(path.join(format!("7-{base}.rec")), vec![1_u8; 128]).unwrap();
        }
    }

    #[test]
    fn truncate_zeroes_the_retained_tail_and_deletes_later_segments() {
        let path = segment_test_dir("truncate-tail");
        let _ = std::fs::remove_dir_all(&path);
        write_segments(&path);

        truncate_segments(&path, &descriptor(), 64).unwrap();

        let retained = std::fs::read(path.join("7-0.rec")).unwrap();
        assert_eq!(&retained[..64], &[1_u8; 64]);
        assert_eq!(&retained[64..], &[0_u8; 64]);
        assert!(!path.join("7-128.rec").exists());
        assert!(!path.join("7-256.rec").exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn truncate_at_a_segment_boundary_deletes_that_segment_and_later_segments() {
        let path = segment_test_dir("truncate-boundary");
        let _ = std::fs::remove_dir_all(&path);
        write_segments(&path);

        truncate_segments(&path, &descriptor(), 128).unwrap();

        assert!(path.join("7-0.rec").exists());
        assert!(!path.join("7-128.rec").exists());
        assert!(!path.join("7-256.rec").exists());
        std::fs::remove_dir_all(path).unwrap();
    }
}
