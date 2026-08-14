# Aeron Rust Course & Tutorial Index

Welcome to the `harus-aeron-rs` structured course. This course mirrors the architecture of Aeron UDP protocol implementation in Rust across Agrona primitives, Client IPC/UDP protocol codecs, MediaDriver duty cycles, Archive stream recording, and Cluster Raft consensus.

## Course Structure & Chapters

| Chapter | Title | Module / Topic |
|---|---|---|
| 01 | [Agrona Core](01-agrona-core.md) | Atomic buffers, ring buffers, and broadcast primitives |
| 02 | [Client Wire Protocol](02-client-wire-protocol.md) | Header flyweights and SBE frame layouts |
| 03 | [Client Publication, Subscription, and Conductor](03-client-pub-sub.md) | Publications, subscriptions, and client data paths |
| 04 | [Client Conductor and Driver Events](04-client-conductor.md) | Asynchronous registration and resource lifecycle |
| 05 | [Driver Configuration](05-driver-configuration.md) | Configuration defaults and publication parameters |
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
