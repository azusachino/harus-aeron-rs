//! Archive: recording, replay, and catalog persistence.
//!
//! Mirrors upstream `aeron-archive`, which depends on `aeron-client` the same way this
//! crate depends on [`aeron_client`]. Future phase — not started. Placeholder crate to
//! reserve the module boundary.
pub mod catalog;
pub mod codecs;
pub mod conductor;
pub mod control_session;
pub mod recording_descriptor;
