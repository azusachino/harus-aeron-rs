//! Generic concurrent primitives, independent of Aeron itself.
//!
//! Mirrors [org.agrona](https://github.com/aeron-io/agrona) — Aeron depends on this,
//! it isn't part of Aeron. Ring buffer, broadcast transmitter/receiver, atomic buffer,
//! idle strategies, counters manager.
//!
//! Lock-free structures here are dual-tested: a normal `#[test]` for the fast path,
//! plus a `#[test]` under `#[cfg(loom)]` using `loom::sync`/`loom::thread` in place of
//! `std::sync`/`std::thread` (see `atomic_buffer.rs`'s `#[cfg(all(test, loom))]`
//! variant) to exhaustively check interleavings rather than hoping a flaky test
//! catches a race. Run with `RUSTFLAGS="--cfg loom" cargo test --release --lib`.
//! Loom tests use `loom::model::Builder` with a bounded `preemption_bound` and
//! `max_duration` rather than the bare `loom::model()` default — an unbounded search
//! over more than two threads can run for hours; see the ring buffer/broadcast loom
//! tests for the pattern.

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
