# Chapter 11: Aeron Cluster Raft Consensus & Leader Failover

This chapter covers `aeron-cluster`: Raft-style consensus primitives, the durable
node-local election state, the replicated recording log, and both an in-memory and a
real Tokio UDP leader-failover comparison.

Cluster failover is the scenario where "just read the logs" stops being a platitude and becomes
the actual recovery procedure, so — same as Archive — the two files a node writes to disk are the
chapter's real subject matter, more than the election state machine that produces them.

`crates/cluster/src/node_state.rs` persists exactly three numbers per node in `node-state.dat`:
`candidate_term_id`, `log_position`, and `timestamp_ms`, behind an 8-byte magic (`AERONNS1`) and a
version tag so a corrupt or foreign file fails fast on reload instead of silently misleading you.
`NodeStateFile::propose_max_candidate_term_id` only ever moves `candidate_term_id` forward — it
writes the new triple exactly when the proposed term is strictly greater than what's on disk — so
the invariant you can rely on when reading this file after a crash is monotonicity: whatever
`candidate_term_id` you see is the highest term this node has ever voted for or campaigned on, full
stop.

`crates/cluster/src/recording_log.rs` is the append-only ledger next to it, `recording.log`. Each
`RecordingLogEntry` row carries `recording_id`, `leadership_term_id`, `term_base_log_position`,
`log_position`, `timestamp`, `service_id`, an `entry_type` byte (`ENTRY_TYPE_TERM` = 0,
`ENTRY_TYPE_SNAPSHOT` = 1, `ENTRY_TYPE_STANDBY_SNAPSHOT` = 2), an optional `archive_endpoint`, and
an `is_valid` flag that lets an entry be invalidated in place rather than physically removed. Read
this file top to bottom after a leader failover and you're reading the exact sequence of terms and
snapshots the cluster committed to, in commit order — which is the whole point of keeping it as a
flat, append-only, human-greppable log rather than folding it into some in-memory-only structure
that a crash could take with it.

## Cluster Protocol Codecs & State Machine

- `crates/cluster/src/codecs.rs`: `ClusterRole` enum (`Follower`, `Candidate`, `Leader`), `ClusterMessageType`, and the fixed-size `ClusterHeader` flyweight layout.
- `crates/cluster/src/state_machine.rs`: `ClusterMember` identity and `ClusterStateMachine` deterministic role/term transitions.

## ConsensusModule Duty Cycle & Quorum

- `crates/cluster/src/consensus_module.rs`: `ConsensusModule` agent managing election timers, heartbeats, vote requests/responses, appointed leadership, and strict majority quorum (`(members.len() / 2) + 1`).
- `crates/cluster/src/cluster_node.rs`: `ClusterNode`/`ClusterNodeContext` launcher wrapping a `ConsensusModule` in a step loop, mirroring upstream `io.aeron.cluster.ClusterNode`.
- `crates/cluster/src/cluster_clock.rs`: `OffsetMillisecondClusterClock`, a pure offsettable time source used by deterministic tests, mirroring upstream `OffsetMillisecondClusterClock`.
- `crates/cluster/src/log_source_validator.rs`: `LogSourceValidator`/`LogSourceType` acceptance rules for whether a backup/catchup log source (leader, follower, or any) is valid for a given leader/member pair.

## Durable Node State & Recording Log

- `crates/cluster/src/node_state.rs`: `NodeStateFile` — versioned, magic-tagged `node-state.dat` persistence of the candidate term (`candidate_term_id`, `log_position`, `timestamp`), with monotonic `propose_max_candidate_term_id` and crash-safe reload/version validation.
- `crates/cluster/src/recording_log.rs`: `RecordingLog` — bounded local metadata equivalent of upstream `RecordingLog`: an append-only `recording.log` file recording term and snapshot (including standby-snapshot) entries, log-position commits, entry invalidation/removal, latest-snapshot lookup, term lookup, and truncate-after-position pruning. This is metadata only; Archive recording, replay, and multi-node recovery remain a separate boundary.

## Real Tokio UDP Failover Harness

- `crates/cluster/src/udp_runtime.rs`: `NodeRuntime`, `NodeConfig`, `ClientConfig`, and `WireMessage` — a real Tokio UDP process runtime built for the issue #20 transport comparison: leader heartbeats, quorum voting, and client order load across independent OS processes. It is deliberately a comparison harness, not Aeron wire or Archive compatibility.
- `crates/cluster/src/bin/aeron-cluster-node.rs` and `crates/cluster/src/bin/aeron-cluster-client.rs`: standalone binaries that drive `udp_runtime::run_node`/`run_client` from `NodeConfig`/`ClientConfig` environment configuration, used by `scripts/cluster_failover_repro.py`.

## Leader Failover Recovery Tests

- `crates/cluster/tests/failover_repro_tests.rs`: Integration tests for a 3-node in-memory
  leader transition and a deterministic 3,000-message failover comparison against the
  `ConsensusModule` state machine.
- `crates/cluster/tests/properties.rs`: `proptest` coverage proving quorum size is always
  a strict majority (`(member_count / 2) + 1`) across arbitrary member counts.
- `scripts/cluster_failover_repro.py`: Drives the real `aeron-cluster-node`/`aeron-cluster-client`
  binaries as independent OS processes across a 100-order, 1-second load with the leader
  killed at 10 seconds, and proves 100 acknowledgements with no repeated survivor elections.
  It uses a custom UDP protocol and is not Aeron wire or Archive replication; see the
  Cluster Parity Boundaries and Cluster Failover Comparison documents in the verification
  section for the full boundary matrix.

## Verification

Run all tests and workspace checks with:

```bash
make check
make test-parity
```
