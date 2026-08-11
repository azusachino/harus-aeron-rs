# Cluster parity boundaries

This document is the starting matrix for the `v0.0.8 — aeron-cluster` milestone.
The authoritative source is the pinned `vendor/aeron` checkout; the row-level ledger
is [`test/upstream_map.jsonl`](../test/upstream_map.jsonl).

## Baseline

The upstream scope is 38 `aeron-cluster` module test files plus 23
`aeron-system-tests/cluster` files: 61 rows in total. The bounded
`ClusterMemberTest` equivalent is resolved. The ledger also records justified
skips for Java-only harness files or boundaries whose required runtime is not in
the Rust crate; the remaining 28 module rows and all 23 system rows remain
`todo`.

The Rust crate currently contains four small in-memory building blocks. The
bounded `ClusterMemberTest` equivalent covers endpoint parsing, active-member
quorum position, log-ranking, vote suitability, election-result, reset, and
active-quorum primitives. Runtime membership integration remains open in the
other cluster boundaries:

- `codecs.rs`: cluster role/message enums and a fixed-size cluster header.
- `state_machine.rs`: member identity and deterministic role/term transitions.
- `consensus_module.rs`: vote collection, majority quorum, and heartbeat handling.
- `cluster_node.rs`: a minimal node context and step loop.

The existing failover test proves only an in-memory election model. It does not prove
UDP transport, durable replicated logs, Archive recovery, cluster client traffic,
clustered services, or concurrent load behavior.

### Skip policy

`skip` is reserved for a boundary that is either a Java test fixture with no
executable test of its own, or an integration boundary requiring an upstream
runtime that this crate does not claim to implement. Each skip row names the
missing boundary in `test/upstream_map.jsonl`; skips do not count as Rust parity.

## Module-test matrix

| Boundary | Rows | Upstream files | Rust target |
| --- | ---: | --- | --- |
| Authentication | 1 | `AuthenticationTest` | Document as a later boundary unless a Rust authentication contract is introduced. |
| Membership, election, and consensus configuration | 8 | `ClusterMemberTest`, `ClusterTestConstants`, `ConsensusModuleAgentTest`, `ConsensusModuleConfigurationTest`, `ConsensusModuleContextCloseTests`, `ConsensusModuleContextTest`, `ConsensusModuleSnapshotTakerTest`, `ElectionTest` | Extend the deterministic core first, then add persistence and snapshot contracts. |
| Node lifecycle, timers, and state files | 9 | `ClusterNodeRestartTest`, `ClusterNodeTest`, `ClusterTimerTest`, `ClusterWithNoServicesTest`, `NameResolutionClusterNodeTest`, `NodeStateFileTest`, `PriorityHeapTimerServiceClusterTimeTest`, `PriorityHeapTimerServiceTest`, `WheelTimerServiceClusterTimeTest` | Add explicit clock, node lifecycle, and restartable state boundaries. |
| Logs, replication, snapshots, and backup | 7 | `ClusterBackupAgentTest`, `ClusterBackupContextTest`, `LogSourceValidatorTest`, `RecordingLogTest`, `RecordingReplicationTest`, `SnapshotReplicationTest`, `StandbySnapshotReplicatorTest` | Depends on durable log and Archive integration; do not model these as election-only tests. |
| Ingress, service, and protocol adapters | 8 | `IngressAdapterTest`, `PendingServiceMessageTrackerTest`, `PublicationGroupTest`, `SessionEventCodecCompatibilityTest`, `service/ClusterMarkFileTest`, `service/ClusteredServiceAgentTest`, `service/ClusteredServiceContainerContextTest`, `service/ServiceSnapshotTakerTest` | Add protocol/service contracts after the core state and log boundaries exist. |
| Cluster client | 5 | `client/AeronClusterAsyncConnectTest`, `client/AeronClusterContextTest`, `client/AeronClusterTest`, `client/EgressAdapterTest`, `client/EgressPollerTest` | Add an explicit client surface; current Rust has no cluster client API. |

## System-test matrix

| Boundary | Rows | Upstream files | Rust target |
| --- | ---: | --- | --- |
| Election, topology, recovery, and multi-node lifecycle | 13 | `AppointedLeaderTest`, `ClusterBackupTest`, `ClusterNetworkPartitionTest`, `ClusterNetworkTopologyTest`, `FailedFirstElectionClusterTest`, `InitiateShutdownThenImmediatelyCloseLeaderTest`, `MultiNodeTest`, `RacingCatchupClusterTest`, `RecoverAfterFailedCatchupClusterTest`, `SingleNodeTest`, `StalledLeaderLogReplicationClusterTest`, `StartFromTruncatedRecordingLogTest`, `TestClusterTest` | Build real multi-node runtime probes only after transport, log, and recovery contracts are present. |
| Sessions, services, and ingress | 5 | `ClusterSessionReliabilityTest`, `ClusterTest`, `MultiClusteredServicesTest`, `MultiModuleSharedDriverTest`, `ServiceIpcIngressMessageTest` | Separate client/session/service evidence from election-model tests. |
| Tooling, clocks, and test infrastructure | 5 | `ClusterInstrumentor`, `ClusterToolTest`, `MethodCallBlocker`, `OffsetMillisecondClusterClock`, `OffsetMillisecondClusterClockTest` | Port behavioral tests; record Java instrumentation helpers as justified skips when no Rust equivalent exists. |

## Acceptance rules

1. A ledger row becomes `done` only when a Rust test or an explicit justified
   Java-specific skip is recorded in `rust_equivalent`.
2. A bounded equivalent must state its ceiling in the ledger or this document; an
   in-memory election test must not be reported as cluster or system-test parity.
3. The milestone must include the issue #20 comparison: kill the leader during
   approximately 100 messages over at least 90 seconds of traffic and observe
   whether failover permanently stalls.
4. `make check` and `make test-parity` remain the baseline gates. Passing either gate
   does not prove real multi-node, Archive, or Java interoperability.

## Implementation order

1. Deterministic membership, election, clock, and configuration contracts.
2. Durable node state, recording log, snapshots, and replay/recovery contracts.
3. Ingress/service/client adapters and protocol compatibility.
4. Real multi-node system tests, then the stuck-failover-under-load comparison.
