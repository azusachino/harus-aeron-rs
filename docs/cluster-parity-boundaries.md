# Cluster parity boundaries

This document is the starting matrix for the `v0.0.8 — aeron-cluster` milestone.
The authoritative source is the pinned `vendor/aeron` checkout; the row-level ledger
is [`test/upstream_map.jsonl`](../test/upstream_map.jsonl).

## Baseline

The upstream scope is 38 `aeron-cluster` module test files plus 23
`aeron-system-tests/cluster` files: 61 rows in total. All 61 rows are now classified:
bounded Rust equivalents are marked `done`, and Java-only or unavailable runtime
boundaries are documented as `skip`. The bounded rows state their ceiling in the
ledger; the full Java media-driver, client, service, and Archive runtime remains
outside this crate.

The Rust crate currently contains bounded cluster-core building blocks. The
membership equivalent covers endpoint parsing, active-member quorum position,
log-ranking, vote suitability, election-result, reset, and active-quorum primitives;
the node-state, recording-log, clock, appointed-leader, and single-node slices add
their corresponding local contracts. Runtime membership integration remains open in
the other cluster boundaries:

- `codecs.rs`: cluster role/message enums and a fixed-size cluster header.
- `state_machine.rs`: member identity and deterministic role/term transitions.
- `consensus_module.rs`: vote collection, majority quorum, appointed leadership, and heartbeat handling.
- `cluster_node.rs`: a minimal node context and step loop.
- `node_state.rs`: versioned local candidate-term persistence.
- `recording_log.rs`: bounded local term/snapshot metadata persistence.
- `cluster_clock.rs` and `log_source_validator.rs`: pure clock and source-acceptance contracts.

The failover tests prove only an in-memory election model. The sustained-load
comparison processes 3,000 deterministic messages and fails the leader halfway
through, but it does not prove UDP transport, durable replicated logs, Archive
recovery, cluster client traffic, clustered services, or real concurrent load behavior.

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
3. The milestone must include a bounded issue #20 comparison, and must state whether
   the exact sibling reproduction was run. The current bounded comparison kills the
   in-memory leader halfway through 3,000 messages and observes continued progress;
   the exact approximately-100-message, at-least-90-second UDP/Archive reproduction
   remains unrun until a real multi-process Rust cluster runtime exists.
4. `make check` and `make test-parity` remain the baseline gates. Passing either gate
   does not prove real multi-node, Archive, or Java interoperability.

## Implementation order

1. Deterministic membership, election, clock, and configuration contracts.
2. Durable node state, recording log, snapshots, and replay/recovery contracts.
3. Ingress/service/client adapters and protocol compatibility.
4. Real multi-node system tests, then the stuck-failover-under-load comparison.
