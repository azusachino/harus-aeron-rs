//! Aeron client: wire protocol, log buffer, `Publication`/`Subscription` API.
//!
//! Mirrors upstream `aeron-client`'s package layout (`io.aeron.protocol`,
//! `io.aeron.logbuffer`, plus the client API and `ClientConductor` at the crate root) —
//! depends only on [`agrona`], same as upstream. `aeron-driver`, `aeron-archive`, and
//! `aeron-cluster` all depend on this crate rather than reimplementing the wire types.
//!
//! `protocol` (`HeaderFlyweight`/`ErrorFlyweight`/`DataHeaderFlyweight`) and `logbuffer`
//! (`LogBufferDescriptor` layout, `Header`/`HeaderWriter`, term scan/read/rebuild/unblock
//! utilities), plus the initial Publication/Subscription/ClientConductor and context APIs
//! from the v0.0.4 client milestone.

pub mod buffer_builder;
pub mod cnc_file;
pub mod command;
pub mod conductor;
pub mod context;
pub mod counters;
pub mod driver_events;
pub mod driver_proxy;
pub mod error;
pub mod error_log;
pub mod exceptions;
pub mod exclusive_publication;
pub mod fragment_assembler;
pub mod image;
pub mod log_buffers;
pub mod logbuffer;
pub mod protocol;
pub mod publication;
pub mod security;
pub mod subscription;
pub mod timestamp_util;
pub mod uri;
