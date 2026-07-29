//! Learner stub for the persistent Archive recording and replay runtime.
//!
//! Reference implementation: `crates/archive/src/runtime.rs`.

#[allow(dead_code)]
pub struct ArchiveRuntime;

#[allow(dead_code)]
pub struct ReplayCursor;

impl ArchiveRuntime {
    pub fn extend_recording() {
        todo!("validate the extending image term geometry and reopen at the stop position")
    }

    pub fn truncate_recording() {
        todo!("validate alignment, clear the retained segment tail, and delete later segments")
    }

    pub fn purge_recording() {
        todo!("remove every recording segment and invalidate the persisted catalog entry")
    }

    pub fn start_replay() -> ReplayCursor {
        todo!("validate replay bounds and open an incremental replay cursor")
    }
}

impl ReplayCursor {
    pub fn poll() {
        todo!("offer bounded recorded fragments and advance only on success")
    }
}
