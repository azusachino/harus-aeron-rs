//! Archive: recording, replay, and catalog persistence.
//!
//! Mirrors upstream `aeron-archive`, which depends on `aeron-client` the same way this
//! crate depends on [`aeron_client`].
pub mod archive_counters;
pub mod archive_exception;
pub mod catalog;
pub mod catalog_index;
pub mod catalog_view;
pub mod checksum;
pub mod codecs;
pub mod conductor;
pub mod control_session;
pub mod control_session_adapter;
pub mod delete_segments_session;
pub mod list_recording_by_id_session;
pub mod list_recordings_for_uri_session;
pub mod list_recordings_session;
pub mod recording_descriptor;
pub mod recording_pos;
pub mod recording_subscription_descriptor;
pub mod runtime;
pub mod service;
