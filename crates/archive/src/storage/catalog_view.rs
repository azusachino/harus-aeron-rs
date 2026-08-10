//! Read-only views over a persisted Archive catalog.

use std::path::Path;

use crate::{catalog::Catalog, recording_descriptor::RecordingDescriptor};

pub const NULL_VALUE: i64 = -1;

/// Visit every recording descriptor in a persisted catalog.
pub fn list_recordings<F>(path: &Path, mut consumer: F) -> std::io::Result<usize>
where
    F: FnMut(&RecordingDescriptor),
{
    let catalog = Catalog::load_from_file(path)?;
    for descriptor in catalog.recordings() {
        consumer(&view_descriptor(descriptor));
    }
    Ok(catalog.recordings().len())
}

/// Visit one recording descriptor if it exists in a persisted catalog.
pub fn list_recording<F>(path: &Path, recording_id: i64, mut consumer: F) -> std::io::Result<bool>
where
    F: FnMut(&RecordingDescriptor),
{
    let catalog = Catalog::load_from_file(path)?;
    if let Some(descriptor) = catalog.get_recording(recording_id) {
        consumer(&view_descriptor(descriptor));
        Ok(true)
    } else {
        Ok(false)
    }
}

fn view_descriptor(descriptor: &RecordingDescriptor) -> RecordingDescriptor {
    let mut descriptor = descriptor.clone();
    descriptor.control_session_id = NULL_VALUE;
    descriptor.correlation_id = NULL_VALUE;
    descriptor
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(recording_id: i64, session_id: i32, stream_id: i32) -> RecordingDescriptor {
        RecordingDescriptor::new(
            1,
            2,
            recording_id,
            4,
            NULL_VALUE,
            10,
            NULL_VALUE,
            0,
            2 * 2 * 64 * 1024,
            2 * 64 * 1024,
            1024,
            session_id,
            stream_id,
            format!("channel-{recording_id}"),
            format!("channel-{recording_id}?tag=f"),
            format!("source-{recording_id}"),
        )
    }

    fn catalog_path(test_name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "aeron-catalog-view-{test_name}-{}-{:?}.dat",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    #[test]
    fn should_list_all_recordings_in_catalog() {
        let path = catalog_path("all");
        let _ = std::fs::remove_file(&path);
        let mut catalog = Catalog::new();
        let descriptors = [
            descriptor(1, 7, 1),
            descriptor(2, 8, 2),
            descriptor(3, 9, 3),
        ];
        for descriptor in &descriptors {
            assert_eq!(catalog.allocate_recording_id(), descriptor.recording_id);
            catalog.add_recording(descriptor.clone());
        }
        catalog.save_to_file(&path).unwrap();

        let mut listed = Vec::new();
        assert_eq!(
            list_recordings(&path, |descriptor| listed.push(descriptor.clone())).unwrap(),
            3
        );
        assert_eq!(
            listed,
            descriptors
                .into_iter()
                .map(|mut descriptor| {
                    descriptor.control_session_id = NULL_VALUE;
                    descriptor.correlation_id = NULL_VALUE;
                    descriptor
                })
                .collect::<Vec<_>>()
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn should_list_recording_by_recording_id() {
        let path = catalog_path("single");
        let _ = std::fs::remove_file(&path);
        let mut catalog = Catalog::new();
        let first = descriptor(1, 7, 1);
        let second = descriptor(2, 8, 2);
        assert_eq!(catalog.allocate_recording_id(), first.recording_id);
        catalog.add_recording(first);
        assert_eq!(catalog.allocate_recording_id(), second.recording_id);
        catalog.add_recording(second.clone());
        catalog.save_to_file(&path).unwrap();

        let mut listed = Vec::new();
        assert!(list_recording(&path, 2, |descriptor| listed.push(descriptor.clone())).unwrap());
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].recording_id, second.recording_id);
        assert_eq!(listed[0].session_id, second.session_id);
        assert!(!list_recording(&path, 99, |_| {}).unwrap());
        let _ = std::fs::remove_file(path);
    }
}
