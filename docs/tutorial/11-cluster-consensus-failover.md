# Chapter 11: Aeron Cluster Raft Consensus & Leader Failover

This chapter covers `aeron-cluster` Raft consensus engine, protocol codecs, `ConsensusModule` election mechanics, and leader failover recovery.

## Cluster Protocol Codecs & State Machine

- `crates/cluster/src/codecs.rs`: `ClusterRole` enum (`Follower`, `Candidate`, `Leader`), `ClusterMessageType`, and 32-byte `ClusterHeader` flyweight layout.
- `crates/cluster/src/state_machine.rs`: `ClusterMember` struct and `ClusterStateMachine` transitions.

## ConsensusModule Duty Cycle & Quorum

- `crates/cluster/src/consensus_module.rs`: `ConsensusModule` agent managing election timers, heartbeats, vote requests/responses, and strict majority quorum (`(members.len() / 2) + 1`).

## Leader Failover Recovery Test

- `crates/cluster/tests/failover_repro_tests.rs`: Integration tests for a 3-node in-memory
  leader transition and a deterministic 3,000-message failover comparison.
- `scripts/cluster_failover_repro.py`: Real Tokio UDP node/client process comparison. It
  demonstrates recovery across a 100-order, 1-second load with a leader killed at 10 seconds,
  but uses a custom protocol and is not Aeron wire/Archive replication.

## Verification

Run all tests and workspace checks with:

```bash
make check
```
