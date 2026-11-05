//! Three-term log buffer, mmap-backed layout descriptors and utilities.
//!
//! Mirrors upstream `io.aeron.logbuffer.LogBufferDescriptor`, `Header`, and `HeaderWriter`.

pub mod buffer_claim;
pub mod descriptor;
pub mod frame_descriptor;
pub mod header;
pub mod term_reader;
pub mod term_rebuilder;
pub mod unblocker;

pub use buffer_claim::*;
pub use descriptor::*;
pub use frame_descriptor::*;
pub use header::*;
pub use term_reader::*;
pub use term_rebuilder::*;
pub use unblocker::*;

pub use crate::protocol::{BEGIN_FRAG_FLAG, END_FRAG_FLAG, UNFRAGMENTED};
