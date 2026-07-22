//! Learner stub mirroring `crates/client`. Implement the bodies in each module.
//!
//! Reference implementation: `crates/client`. Course chapter: `docs/tutorial/02-client-wire-protocol.md`.

pub mod logbuffer;
pub mod protocol;

pub use logbuffer::{Header, TermReader, TermScanner};
pub use protocol::{DataHeaderFlyweight, HeaderFlyweight};
