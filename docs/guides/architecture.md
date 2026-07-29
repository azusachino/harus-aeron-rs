# Harus Aeron Rust System Architecture

`harus-aeron-rs` is a high-performance Rust reimplementation of the Aeron UDP messaging protocol, wire-compatible with upstream `aeron-io/aeron`.

## Component Architecture & Dependencies

```
[ crates/agrona ]  <--- Lock-free primitives, AtomicBuffer, RingBuffers, Counters
       ^
       |
[ crates/client ]  <--- Aeron wire codecs, Header flyweights, LogBuffers, Pub/Sub
       ^
       +--------------------+
       |                    |
[ crates/driver ]   [ crates/archive ]  <--- Catalog persistent index, ControlSession
                            ^
                            |
                    [ crates/cluster ]  <--- Raft ConsensusModule, ClusterNode failover
```

### Crate Boundaries

1. **`agrona` (`crates/agrona`)**:
   - Generic concurrent primitives: `AtomicBuffer`, `OneToOneRingBuffer`, `ManyToOneRingBuffer`, `BroadcastTransmitter`, `BroadcastReceiver`, `CountersManager`, `IdleStrategy`.
2. **`aeron-client` (`crates/client`)**:
   - Wire frame codecs: `HeaderFlyweight`, `DataHeaderFlyweight`, `SetupFlyweight`, `StatusMessageFlyweight`, `NakFlyweight`.
   - Client structures: `LogBuffers`, `Publication`, `Subscription`.
3. **`aeron-driver` (`crates/driver`)**:
   - Transport layers: `UdpChannel`, `UdpChannelTransport`, `SendChannelEndpoint`, `ReceiveChannelEndpoint`, `DataTransportPoller`, `ControlTransportPoller`.
   - Driver agents: `Sender`, `Receiver`, `DriverConductor`, `MediaDriver`.
4. **`aeron-archive` (`crates/archive`)**:
   - Archive v13 SBE control codecs and bounded `ControlSessionAdapter` dispatch.
   - Persistent aligned-frame recording/replay through `ArchiveRuntime`.
   - `ArchiveContext`, managed duty-cycle service, and standalone `aeron-archive` process.
5. **`aeron-cluster` (`crates/cluster`)**:
   - Raft protocol: `ClusterHeader`, `ClusterRole`, `ClusterMember`, `ClusterStateMachine`.
   - Consensus agent: `ConsensusModule`, `ClusterNode`.

## Agent & Duty Cycle Runtime Model

Aeron uses explicit single-threaded agent duty cycles (`do_work()`) rather than thread-per-connection.

- **`Sender`**: Transmits pending data frames from log buffers to UDP channels.
- **`Receiver`**: Polls UDP data transport sockets and processes incoming frames.
- **`DriverConductor`**: Coordinates client commands, image/publication allocations, and counter cleanup.
- **`ConsensusModule`**: Drives Raft election timeouts, vote collections, and leader heartbeats.

## Leader Failover & Parity Verification

The `aeron-cluster` implementation resolves the stuck-failover scenario identified in `harus-aeron-zig` by maintaining strict majority quorum checks (`(len / 2) + 1`) and deterministic state transitions.
