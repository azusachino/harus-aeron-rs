//! Learner stub for the Archive mark file.
//!
//! Reference implementation: `crates/archive/src/storage/archive_mark_file.rs`.

#[allow(dead_code)]
pub struct ArchiveMarkFile;

impl ArchiveMarkFile {
    pub fn create() {
        todo!("map archive-mark.dat, encode the SBE header, and publish liveness")
    }

    pub fn signal_ready() {
        todo!("publish the semantic version and activity timestamp")
    }
}
