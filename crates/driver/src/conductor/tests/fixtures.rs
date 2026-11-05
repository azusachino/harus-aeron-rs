use super::super::*;
use aeron_client::cnc_file::{
    CncFile, DEFAULT_COUNTER_METADATA_LENGTH, DEFAULT_COUNTER_VALUES_LENGTH,
    DEFAULT_ERROR_LOG_LENGTH, DEFAULT_TO_CLIENTS_LENGTH, DEFAULT_TO_DRIVER_LENGTH,
};
use aeron_client::driver_proxy::DriverProxy;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

pub const CLIENT_ID: i64 = 42;
pub const LIVENESS_TIMEOUT_NS: i64 = 10_000_000_000;

/// A tiny IPC term length keeps test log files fast to map (64 KiB terms, not 64 MiB).
pub fn small_config() -> Configuration {
    Configuration {
        ipc_term_buffer_length: aeron_client::logbuffer::TERM_MIN_LENGTH as usize,
        ..Configuration::default()
    }
}

/// A per-test scratch directory for the driver's log files, removed on drop.
pub struct TempDir(pub std::path::PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn temp_dir(tag: &str) -> TempDir {
    let path = std::env::temp_dir().join(format!(
        "aeron-conductor-dir-{}-{}-{:?}",
        tag,
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    TempDir(path)
}

pub fn temp_cnc(tag: &str) -> (CncFile, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "aeron-conductor-{}-{}-{:?}.dat",
        tag,
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_file(&path);
    let cnc = CncFile::create_new_with_regions(
        &path,
        DEFAULT_TO_DRIVER_LENGTH,
        DEFAULT_TO_CLIENTS_LENGTH,
        DEFAULT_COUNTER_METADATA_LENGTH,
        DEFAULT_COUNTER_VALUES_LENGTH,
        DEFAULT_ERROR_LOG_LENGTH,
        LIVENESS_TIMEOUT_NS,
    )
    .unwrap();
    (cnc, path)
}

pub fn proxy(cnc: &CncFile) -> DriverProxy<'static> {
    let ring = ManyToOneRingBuffer::new(cnc.to_driver_buffer()).unwrap();
    DriverProxy::new(ring, CLIENT_ID)
}

pub fn receive_event_type(events: &mut agrona::BroadcastReceiver<'_>) -> i32 {
    let mut seen_type = 0;
    assert!(
        events
            .receive_next(|msg_type, _, _, _| seen_type = msg_type)
            .unwrap()
    );
    seen_type
}
