use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use aeron_archive::archive_tool::{ArchiveToolError, run};
use aeron_archive::catalog::{Catalog, RecordingState};
use aeron_archive::recording_descriptor::RecordingDescriptor;

fn archive_dir(test_name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "aeron-archive-tool-{test_name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ))
}

fn create_catalog(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
    std::fs::create_dir_all(path).expect("create archive directory");
    let mut catalog = Catalog::new();
    for _ in 1..=4 {
        let recording_id = catalog.allocate_recording_id();
        catalog.add_recording(RecordingDescriptor::new(
            0,
            0,
            recording_id,
            0,
            -1,
            0,
            -1,
            15,
            1024,
            256,
            128,
            2,
            2,
            "ch2".into(),
            "ch2?tag=OK".into(),
            "src2".into(),
        ));
    }
    catalog.change_state(2, RecordingState::Invalid);
    catalog
        .save_to_file(&path.join("catalog.dat"))
        .expect("save catalog");
}

fn output_for(path: &Path, command: &[&str]) -> Result<String, ArchiveToolError> {
    let mut output = String::new();
    let mut args = vec![path.to_str().expect("archive path")];
    args.extend_from_slice(command);
    run(&args, &mut StringWriter(&mut output))?;
    Ok(output)
}

struct StringWriter<'a>(&'a mut String);

impl std::io::Write for StringWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let text = std::str::from_utf8(bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        self.0.write_str(text).map_err(std::io::Error::other)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn describe_recording_prints_existing_valid_and_invalid_entries() {
    let path = archive_dir("single");
    create_catalog(&path);

    let valid = output_for(&path, &["describe", "1"]).expect("describe valid");
    let invalid = output_for(&path, &["describe", "2"]).expect("describe invalid");
    assert!(valid.contains("|recordingId=1|"));
    assert!(valid.contains("|state=VALID|"));
    assert!(invalid.contains("|recordingId=2|"));
    assert!(invalid.contains("|state=INVALID|"));

    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn describe_rejects_unknown_recording() {
    let path = archive_dir("unknown");
    create_catalog(&path);

    let error = output_for(&path, &["describe", "10"]).expect_err("unknown recording");
    assert_eq!(error.to_string(), "no recording found with recordingId: 10");

    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn describe_filters_invalid_recordings_and_describe_all_keeps_them() {
    let path = archive_dir("all");
    create_catalog(&path);

    let valid = output_for(&path, &["describe"]).expect("describe valid entries");
    assert!(valid.contains("|recordingId=1|"));
    assert!(!valid.contains("|recordingId=2|"));

    let all = output_for(&path, &["describe-all"]).expect("describe all entries");
    assert!(all.contains("|recordingId=1|"));
    assert!(all.contains("|recordingId=2|"));
    assert!(all.contains("|state=INVALID|"));

    let _ = std::fs::remove_dir_all(path);
}
