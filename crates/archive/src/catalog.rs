//! Catalog manager for indexing and querying recording descriptors in Aeron Archive.
//!
//! Mirrors `io.aeron.archive.Catalog.java`.

use std::collections::HashMap;

use crate::recording_descriptor::RecordingDescriptor;

#[derive(Debug, Clone)]
pub struct CatalogHeader {
    pub version: i32,
    pub length: i32,
    pub next_recording_id: i64,
}

pub struct Catalog {
    header: CatalogHeader,
    descriptors: HashMap<i64, RecordingDescriptor>,
    ordered_descriptors: Vec<RecordingDescriptor>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            header: CatalogHeader {
                version: 1,
                length: 0,
                next_recording_id: 1,
            },
            descriptors: HashMap::new(),
            ordered_descriptors: Vec::new(),
        }
    }

    pub fn allocate_recording_id(&mut self) -> i64 {
        let rec_id = self.header.next_recording_id;
        self.header.next_recording_id += 1;
        rec_id
    }

    pub fn add_recording(&mut self, descriptor: RecordingDescriptor) -> i64 {
        let rec_id = descriptor.recording_id;
        self.descriptors.insert(rec_id, descriptor.clone());
        self.ordered_descriptors.push(descriptor);
        rec_id
    }

    pub fn get_recording(&self, recording_id: i64) -> Option<&RecordingDescriptor> {
        self.descriptors.get(&recording_id)
    }

    pub fn recordings(&self) -> &[RecordingDescriptor] {
        &self.ordered_descriptors
    }

    pub fn header(&self) -> &CatalogHeader {
        &self.header
    }

    pub fn update_stop_position(
        &mut self,
        recording_id: i64,
        stop_position: i64,
        stop_timestamp: i64,
    ) -> bool {
        if let Some(descriptor) = self.descriptors.get_mut(&recording_id) {
            descriptor.stop_position = stop_position;
            descriptor.stop_timestamp = stop_timestamp;

            if let Some(ordered) = self
                .ordered_descriptors
                .iter_mut()
                .find(|d| d.recording_id == recording_id)
            {
                ordered.stop_position = stop_position;
                ordered.stop_timestamp = stop_timestamp;
            }
            return true;
        }
        false
    }

    pub fn extend_recording(
        &mut self,
        recording_id: i64,
        control_session_id: i64,
        correlation_id: i64,
        session_id: i32,
    ) -> bool {
        let update = |descriptor: &mut RecordingDescriptor| {
            descriptor.control_session_id = control_session_id;
            descriptor.correlation_id = correlation_id;
            descriptor.session_id = session_id;
            descriptor.stop_position = -1;
            descriptor.stop_timestamp = 0;
        };
        if let Some(descriptor) = self.descriptors.get_mut(&recording_id) {
            update(descriptor);
            if let Some(ordered) = self
                .ordered_descriptors
                .iter_mut()
                .find(|descriptor| descriptor.recording_id == recording_id)
            {
                update(ordered);
            }
            return true;
        }
        false
    }

    pub fn remove_recording(&mut self, recording_id: i64) -> bool {
        if self.descriptors.remove(&recording_id).is_none() {
            return false;
        }
        self.ordered_descriptors
            .retain(|descriptor| descriptor.recording_id != recording_id);
        true
    }

    pub fn find_recording_for_stream(
        &self,
        stream_id: i32,
        channel: &str,
    ) -> Option<&RecordingDescriptor> {
        self.ordered_descriptors
            .iter()
            .rev()
            .find(|d| d.stream_id == stream_id && d.original_channel.contains(channel))
    }

    pub fn find_last_matching(
        &self,
        min_recording_id: i64,
        session_id: i32,
        stream_id: i32,
        channel_fragment: &str,
    ) -> Option<i64> {
        self.ordered_descriptors
            .iter()
            .rev()
            .find_map(|descriptor| {
                (descriptor.recording_id >= min_recording_id
                    && descriptor.session_id == session_id
                    && descriptor.stream_id == stream_id
                    && descriptor.original_channel.contains(channel_fragment))
                .then_some(descriptor.recording_id)
            })
    }

    pub fn save_to_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        use std::io::Write;
        let mut file = std::fs::File::create(path)?;
        file.write_all(&self.header.version.to_le_bytes())?;
        let count = u32::try_from(self.ordered_descriptors.len()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "too many catalog entries")
        })?;
        file.write_all(&count.to_le_bytes())?;
        file.write_all(&self.header.next_recording_id.to_le_bytes())?;
        for descriptor in &self.ordered_descriptors {
            for value in [
                descriptor.control_session_id,
                descriptor.correlation_id,
                descriptor.recording_id,
                descriptor.start_position,
                descriptor.stop_position,
                descriptor.start_timestamp,
                descriptor.stop_timestamp,
            ] {
                file.write_all(&value.to_le_bytes())?;
            }
            for value in [
                descriptor.initial_term_id,
                descriptor.segment_file_length,
                descriptor.term_buffer_length,
                descriptor.mtu_length,
                descriptor.session_id,
                descriptor.stream_id,
            ] {
                file.write_all(&value.to_le_bytes())?;
            }
            for value in [
                &descriptor.stripped_channel,
                &descriptor.original_channel,
                &descriptor.source_identity,
            ] {
                let bytes = value.as_bytes();
                let length = u32::try_from(bytes.len()).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "catalog string exceeds u32",
                    )
                })?;
                file.write_all(&length.to_le_bytes())?;
                file.write_all(bytes)?;
            }
        }
        file.sync_all()?;
        Ok(())
    }

    pub fn load_from_file(path: &std::path::Path) -> std::io::Result<Self> {
        use std::io::Read;
        let mut file = std::fs::File::open(path)?;
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];

        file.read_exact(&mut buf4)?;
        let version = i32::from_le_bytes(buf4);

        file.read_exact(&mut buf4)?;
        let descriptor_count = u32::from_le_bytes(buf4);

        file.read_exact(&mut buf8)?;
        let next_recording_id = i64::from_le_bytes(buf8);

        let mut catalog = Self {
            header: CatalogHeader {
                version,
                length: descriptor_count.min(i32::MAX as u32) as i32,
                next_recording_id,
            },
            descriptors: HashMap::new(),
            ordered_descriptors: Vec::new(),
        };
        for _ in 0..descriptor_count {
            let mut read_i64 = || -> std::io::Result<i64> {
                let mut bytes = [0u8; 8];
                file.read_exact(&mut bytes)?;
                Ok(i64::from_le_bytes(bytes))
            };
            let control_session_id = read_i64()?;
            let correlation_id = read_i64()?;
            let recording_id = read_i64()?;
            let start_position = read_i64()?;
            let stop_position = read_i64()?;
            let start_timestamp = read_i64()?;
            let stop_timestamp = read_i64()?;
            let mut read_i32 = || -> std::io::Result<i32> {
                let mut bytes = [0u8; 4];
                file.read_exact(&mut bytes)?;
                Ok(i32::from_le_bytes(bytes))
            };
            let initial_term_id = read_i32()?;
            let segment_file_length = read_i32()?;
            let term_buffer_length = read_i32()?;
            let mtu_length = read_i32()?;
            let session_id = read_i32()?;
            let stream_id = read_i32()?;
            let mut read_string = || -> std::io::Result<String> {
                let mut length_bytes = [0u8; 4];
                file.read_exact(&mut length_bytes)?;
                let length = u32::from_le_bytes(length_bytes) as usize;
                if length > 16 * 1024 * 1024 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "catalog string is unreasonably large",
                    ));
                }
                let mut bytes = vec![0u8; length];
                file.read_exact(&mut bytes)?;
                String::from_utf8(bytes)
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
            };
            let descriptor = RecordingDescriptor::new(
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
                read_string()?,
                read_string()?,
                read_string()?,
            );
            catalog.add_recording(descriptor);
        }
        let expected_next_recording_id = catalog
            .ordered_descriptors
            .iter()
            .map(|descriptor| descriptor.recording_id)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "catalog recording id overflow",
                )
            })?;
        if catalog.header.next_recording_id == 0 {
            catalog.header.next_recording_id = expected_next_recording_id;
        } else if catalog.header.next_recording_id < expected_next_recording_id {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "catalog next recording id {} is below expected {}",
                    catalog.header.next_recording_id, expected_next_recording_id
                ),
            ));
        }
        Ok(catalog)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};

    fn descriptor(recording_id: i64) -> RecordingDescriptor {
        RecordingDescriptor::new(
            1,
            2,
            recording_id,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            "stripped".into(),
            "original".into(),
            "source".into(),
        )
    }

    fn rewrite_next_recording_id(path: &std::path::Path, next_recording_id: i64) {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open catalog");
        file.seek(SeekFrom::Start(8))
            .expect("seek next recording id");
        file.write_all(&next_recording_id.to_le_bytes())
            .expect("rewrite next recording id");
    }

    #[test]
    fn should_allocate_recording_id_and_store_descriptor() {
        let mut catalog = Catalog::new();
        let rec_id = catalog.allocate_recording_id();
        assert_eq!(rec_id, 1);

        let desc = RecordingDescriptor::new(
            1,
            100,
            rec_id,
            0,
            1024,
            1000,
            2000,
            0,
            128 * 1024 * 1024,
            64 * 1024,
            1408,
            10,
            1001,
            "aeron:ipc".into(),
            "aeron:ipc".into(),
            "test_source".into(),
        );

        catalog.add_recording(desc.clone());
        assert_eq!(catalog.get_recording(rec_id), Some(&desc));
        assert_eq!(catalog.recordings().len(), 1);
    }

    #[test]
    fn should_persist_complete_recording_descriptors() {
        let path = std::env::temp_dir().join(format!(
            "aeron-catalog-{}-{:?}.dat",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut catalog = Catalog::new();
        let recording_id = catalog.allocate_recording_id();
        let descriptor = descriptor(recording_id);
        catalog.add_recording(descriptor.clone());
        catalog.save_to_file(&path).unwrap();

        let loaded = Catalog::load_from_file(&path).unwrap();
        assert_eq!(loaded.header().next_recording_id, 2);
        assert_eq!(loaded.recordings(), &[descriptor]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn should_recover_zero_next_recording_id_from_descriptors() {
        let path = std::env::temp_dir().join(format!(
            "aeron-catalog-recover-{}-{:?}.dat",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut catalog = Catalog::new();
        catalog.add_recording(descriptor(7));
        catalog.save_to_file(&path).expect("save catalog");
        rewrite_next_recording_id(&path, 0);

        let loaded = Catalog::load_from_file(&path).expect("recover catalog");
        assert_eq!(loaded.header().next_recording_id, 8);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn should_reject_next_recording_id_before_descriptors() {
        let path = std::env::temp_dir().join(format!(
            "aeron-catalog-invalid-next-{}-{:?}.dat",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut catalog = Catalog::new();
        catalog.add_recording(descriptor(7));
        catalog.save_to_file(&path).expect("save catalog");
        rewrite_next_recording_id(&path, 7);

        let error = match Catalog::load_from_file(&path) {
            Ok(_) => panic!("reject stale next id"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("below expected 8"));
        let _ = std::fs::remove_file(path);
    }
}
