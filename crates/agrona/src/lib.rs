//! Generic concurrent primitives, independent of Aeron itself.
//!
//! Mirrors [org.agrona](https://github.com/aeron-io/agrona) — Aeron depends on this,
//! it isn't part of Aeron. Ring buffer, broadcast transmitter/receiver, atomic buffer,
//! idle strategies, counters manager. Skeleton crate — not yet implemented.
//!
//! Lock-free structures here should be dual-tested: a normal `#[test]` for the fast
//! path, plus a `#[test]` under `#[cfg(loom)]` using `loom::sync`/`loom::thread` in
//! place of `std::sync`/`std::thread` (gate the real code with
//! `#[cfg(loom)] use loom::sync::atomic::AtomicUsize;` /
//! `#[cfg(not(loom))] use std::sync::atomic::AtomicUsize;`) to exhaustively check
//! interleavings rather than hoping a flaky test catches a race. Run with
//! `RUSTFLAGS="--cfg loom" cargo test --release --test loom_<name>`.
