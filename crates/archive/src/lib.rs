//! Archive: recording, replay, and catalog persistence.
//!
//! Mirrors upstream `aeron-archive`, which depends on `aeron-client` the same way this
//! crate depends on [`aeron_client`]. Future phase — not started. Placeholder crate to
//! reserve the module boundary.
pub mod catalog;
pub mod catalog_index;
pub mod catalog_view;
pub mod codecs;
pub mod conductor;
pub mod control_session;
pub mod control_session_adapter;
pub mod recording_descriptor;
pub mod recording_subscription_descriptor;
pub mod runtime;
pub mod service;
