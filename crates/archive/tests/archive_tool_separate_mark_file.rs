use std::path::{Path, PathBuf};

use aeron_archive::archive_mark_file::{
    ArchiveMarkFile, ArchiveMarkFileConfig, FILENAME, LINK_FILENAME,
};
use aeron_archive::archive_tool::{resolve_mark_file, run};
use aeron_archive::catalog::Catalog;

fn directory(test_name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "aeron-archive-tool-separate-mark-{test_name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ))
}

fn setup(archive_dir: &Path, mark_file_dir: &Path) {
    let _ = std::fs::remove_dir_all(archive_dir);
    let _ = std::fs::remove_dir_all(mark_file_dir);
    std::fs::create_dir_all(archive_dir).expect("create archive directory");
    std::fs::create_dir_all(mark_file_dir).expect("create mark-file directory");
    Catalog::new()
        .save_to_file(&archive_dir.join("catalog.dat"))
        .expect("save empty catalog");
    ArchiveMarkFile::create(
        mark_file_dir.join(FILENAME),
        &ArchiveMarkFileConfig::default(),
    )
    .expect("create mark file");
    std::fs::write(
        archive_dir.join(LINK_FILENAME),
        mark_file_dir.to_string_lossy().as_bytes(),
    )
    .expect("write mark-file link");
}

#[test]
fn describes_an_archive_with_a_separate_mark_file_directory() {
    let archive_dir = directory("describe");
    let mark_file_dir = directory("describe-mark");
    setup(&archive_dir, &mark_file_dir);

    assert_eq!(
        resolve_mark_file(&archive_dir).expect("resolve mark file"),
        Some(mark_file_dir.join(FILENAME))
    );
    let mut output = Vec::new();
    run(&[archive_dir.to_str().unwrap(), "describe"], &mut output).expect("describe archive");

    let _ = std::fs::remove_dir_all(archive_dir);
    let _ = std::fs::remove_dir_all(mark_file_dir);
}

#[test]
fn describes_all_recordings_with_a_separate_mark_file_directory() {
    let archive_dir = directory("describe-all");
    let mark_file_dir = directory("describe-all-mark");
    setup(&archive_dir, &mark_file_dir);

    let mut output = Vec::new();
    run(
        &[archive_dir.to_str().unwrap(), "describe-all"],
        &mut output,
    )
    .expect("describe all archive");

    let _ = std::fs::remove_dir_all(archive_dir);
    let _ = std::fs::remove_dir_all(mark_file_dir);
}
