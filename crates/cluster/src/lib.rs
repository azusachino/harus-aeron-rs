//! Cluster: Raft-style consensus, replicated log, `ClusteredService` lifecycle.
//!
//! Mirrors upstream `aeron-cluster`, which depends on both `aeron-client` and
//! `aeron-archive` (snapshots persist through the archive). The current code is an
//! in-memory election model, not a wire-compatible or durable cluster implementation.
//! The zig sibling project hit an unresolved stuck-failover-under-load bug in its
//! cluster implementation (see `docs/investigations/`); this crate exists so the same
//! failover scenario can be tried on Rust/tokio for comparison once the driver/client
//! phases are solid.
pub mod cluster_node;
pub mod codecs;
pub mod consensus_module;
pub mod state_machine;
