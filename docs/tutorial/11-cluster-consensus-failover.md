# Chapter 11: Aeron Cluster Raft Consensus & Leader Failover

This chapter covers `aeron-cluster` Raft consensus engine, protocol codecs, `ConsensusModule` election mechanics, and leader failover recovery.

## Cluster Protocol Codecs & State Machine

- `crates/cluster/src/codecs.rs`: `ClusterRole` enum (`Follower`, `Candidate`, `Leader`), `ClusterMessageType`, and 32-byte `ClusterHeader` flyweight layout.
- `crates/cluster/src/state_machine.rs`: `ClusterMember` struct and `ClusterStateMachine` transitions.

## ConsensusModule Duty Cycle & Quorum

- `crates/cluster/src/consensus_module.rs`: `ConsensusModule` agent managing election timers, heartbeats, vote requests/responses, and strict majority quorum (`(members.len() / 2) + 1`).

## Leader Failover Recovery Test

- `crates/cluster/tests/failover_repro_tests.rs`: Integration test simulating a 3-node cluster leader failover scenario under load, demonstrating automatic election recovery for Term 2.

## Verification

Run all tests and workspace checks with:

```bash
make check
```
