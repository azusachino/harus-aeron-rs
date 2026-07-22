//! Learner stub mirroring `crates/client`. Implement the bodies in each module.
//!
//! Reference implementation: `crates/client`. Course chapter: `docs/tutorial/02-client-wire-protocol.md`.

pub mod logbuffer;
pub mod protocol;
pub mod buffer_builder;
pub mod command;
pub mod conductor;
pub mod context;
pub mod counters;
pub mod driver_proxy;
pub mod error;
pub mod exceptions;
pub mod fragment_assembler;
pub mod image;
pub mod log_buffers;
pub mod publication;
pub mod security;
pub mod subscription;
pub mod timestamp_util;
pub mod uri;

pub use logbuffer::{Header, TermReader, TermScanner};
pub use protocol::{DataHeaderFlyweight, HeaderFlyweight};
