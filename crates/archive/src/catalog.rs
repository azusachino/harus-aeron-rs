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

    pub fn find_recording_for_stream(
        &self,
        stream_id: i32,
        channel: &str,
    ) -> Option<&RecordingDescriptor> {
        self.ordered_descriptors
            .iter()
            .rev()
            .find(|d| d.stream_id == stream_id && d.original_channel == channel)
    }

    pub fn save_to_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        use std::io::Write;
        let mut file = std::fs::File::create(path)?;
        file.write_all(&self.header.version.to_le_bytes())?;
        file.write_all(&self.header.length.to_le_bytes())?;
        file.write_all(&self.header.next_recording_id.to_le_bytes())?;
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
        let length = i32::from_le_bytes(buf4);

        file.read_exact(&mut buf8)?;
        let next_recording_id = i64::from_le_bytes(buf8);

        Ok(Self {
            header: CatalogHeader {
                version,
                length,
                next_recording_id,
            },
            descriptors: HashMap::new(),
            ordered_descriptors: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
