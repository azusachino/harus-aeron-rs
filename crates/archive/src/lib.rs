//! Archive: recording, replay, and catalog persistence.
//!
//! Mirrors upstream `aeron-archive`, which depends on `aeron-client` the same way this
//! crate depends on [`aeron_client`].
pub mod admin;
pub mod protocol;
pub mod service;
pub mod storage;

// Keep the existing crate-level module paths stable while the source tree is grouped
// by domain.
pub use admin::{archive_tool, migration};
pub use protocol::{
    archive_exception, codecs, recording_descriptor, recording_subscription_descriptor,
};
pub use service::{
    client_context, conductor, control_session, control_session_adapter, delete_segments_session,
    list_recording_by_id_session, list_recordings_for_uri_session, list_recordings_session,
    runtime,
};
pub use storage::{
    archive_counters, archive_mark_file, catalog, catalog_index, catalog_view, checksum,
    recording_pos,
};
