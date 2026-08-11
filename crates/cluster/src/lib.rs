//! Cluster: Raft-style consensus, replicated log, `ClusteredService` lifecycle.
//!
//! Mirrors upstream `aeron-cluster`, which depends on both `aeron-client` and
//! `aeron-archive` (snapshots persist through the archive). The cluster core is
//! bounded rather than wire-compatible with Aeron. The `udp_runtime` module
//! provides a real Tokio UDP process harness for comparing leader failover under
//! load; it is intentionally not an Aeron wire or Archive replication implementation.
pub mod cluster_clock;
pub mod cluster_node;
pub mod codecs;
pub mod consensus_module;
pub mod log_source_validator;
pub mod node_state;
pub mod recording_log;
pub mod state_machine;
pub mod udp_runtime;
