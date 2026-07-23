# Aeron Rust Course & Tutorial Index

Welcome to the `harus-aeron-rs` structured course. This course mirrors the architecture of Aeron UDP protocol implementation in Rust across Agrona primitives, Client IPC/UDP protocol codecs, MediaDriver duty cycles, Archive stream recording, and Cluster Raft consensus.

## Course Structure & Chapters

| Chapter | Title | Module / Topic |
|---|---|---|
| 01 | [Agrona AtomicBuffer & Endian Primitives](01-agrona-atomic-buffer.md) | Concurrent memory views & endianness |
| 02 | [Agrona RingBuffers (SPSC & MPSC)](02-agrona-ring-buffers.md) | Lock-free queueing structures |
| 03 | [Agrona Broadcast Transmitters & Receivers](03-agrona-broadcast.md) | Multi-receiver broadcast buffers |
| 04 | [Aeron Wire Protocol Codecs](04-client-protocol-codecs.md) | Header flyweights & SBE frame layouts |
| 05 | [Aeron Client LogBuffers, Publications & Subscriptions](05-client-log-buffers.md) | IPC shared memory & term rotations |
| 06 | [MediaDriver UDP Channel & Interface Parsing](06-driver-udp-channel.md) | Unicast, Multicast, & Control Mode URIs |
| 07 | [MediaDriver Transport Poller & Channel Endpoints](07-driver-transport-endpoints.md) | Non-blocking UDP sockets & socket options |
| 08 | [MediaDriver Agents, Duty Cycles & Conductor](08-driver-agents-conductor.md) | Sender, Receiver & DriverConductor threads |
| 09 | [IPC & UDP System Integration Tests](09-system-tests.md) | End-to-end pub/sub & flow control tests |
| 10 | [Aeron Archive Codecs, Catalog & Conductor](10-archive-codecs-catalog.md) | Persistent stream recording & catalog index |
| 11 | [Aeron Cluster Raft Consensus & Leader Failover](11-cluster-consensus-failover.md) | ConsensusModule election mechanics & failover |

## Hands-on Stubs

Learner stubs with `todo!(...)` placeholders mirror each crate in `tutorial/`:

- `tutorial/agrona/`
- `tutorial/client/`
- `tutorial/driver/`
- `tutorial/archive/`
- `tutorial/cluster/`

Verify stubs compilation with:

```bash
make tutorial-check
```
