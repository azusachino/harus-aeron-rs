//! Learner stub mirroring `crates/agrona`. Implement the bodies in each module.
//!
//! Reference implementation: `crates/agrona`. Course chapter: `docs/tutorial/01-agrona-core.md`.

pub mod atomic_buffer;
pub mod broadcast;
pub mod counters;
pub mod error;
pub mod idle_strategy;
pub mod ring_buffer;

pub use atomic_buffer::AtomicBuffer;
pub use broadcast::{BroadcastReceiver, BroadcastTransmitter};
pub use counters::{AtomicCounter, CountersManager};
pub use error::{AgronaError, Result};
pub use idle_strategy::IdleStrategy;
