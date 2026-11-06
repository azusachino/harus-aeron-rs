# Java Interop Smoke Status

This document tracks interop smoke verification between `harus-aeron-rs` (Rust MediaDriver)
and upstream Java Aeron `1.54.0`. It is a smoke gate, not full protocol or parity proof.

## Interop Architecture

- **Driver Process**: `cargo run --bin aeron-driver` (Rust MediaDriver)
- **Protocol**: UDP unicast and shared-memory IPC. Multicast remains outside this verified boundary.
- **Client SDKs**: Rust `aeron-client` and `io.aeron:aeron-client` (Java 17+ JVM runtime).

## Verification Matrix

| Target Scenario | Transport | Status | Notes |
| :--- | :--- | :--- | :--- |
| **Driver Startup** | Local IPC | **PASS** | Rust MediaDriver process launches cleanly |
| **CnC metadata mapping** | `cnc.dat` | **PASS** | Aeron Java `CncFileDescriptor` and `CountersReader` map Rust output |
| **Publication Reg** | `aeron:ipc`, `aeron:udp` | **PASS** | Java client registration completed through the Rust CnC command path in the smoke harness |
| **Subscription Reg** | `aeron:ipc`, `aeron:udp` | **PASS** | Java client received Rust driver readiness/image events in the smoke harness |
| **Pub/Sub Loop** | IPC + UDP Loopback | **PASS** | Java publication payloads were delivered through the Rust driver to Java subscriptions |
| **Reverse Pub/Sub Loop** | IPC | **PASS** | Rust client mapped Java CnC/counters and exchanged a payload through Java MediaDriver |

The harness does not yet prove 268-file parity, multicast parity, cluster/archive behavior,
or a long-running soak under packet loss.

## Execution

Run the interop smoke harness:

```bash
make interop-smoke
```

Run the independent Rust-process evidence:

```bash
make interop-rust-smoke
```

That harness starts separate Rust driver processes and client processes for IPC, then
separate publisher/subscriber driver and client processes for UDP. Each process has a
bounded timeout and is released or killed during cleanup.

## Aeron 1.54.0 Compatibility Checkpoint

Completed across S0 through S13 of the 1.54 catch-up plan ([issue #44](https://github.com/azusachino/harus-aeron-rs/issues/44)), verified on macOS arm64 (`Darwin 25.1.0 arm64`) with Amazon Corretto 25.0.5+10-LTS.

- **Exact Reference**: `454da1e4e6c7edf3dd6d34878512d3243384992a` (Aeron release tag `1.54.0`).
- **Target JAR**: `aeron-all-1.54.0.jar` built from that exact commit via `./gradlew clean :aeron-all:shadowJar`.
- **Smoke & Soak Results**:
  - `make interop-smoke`: Java CnC readers, Java IPC and UDP loopback through the Rust driver, and Rust-client IPC through the Java driver pass cleanly.
  - `make interop-soak`: 1,000 ordered sequence-tagged UDP datagrams delivered without loss or reordering.
  - `make interop-rust-smoke` and `make interop-rust-soak`: independent multi-process Rust driver/client IPC and cross-driver UDP probes pass with 1,000 messages.
  - `RUSTFLAGS="--cfg loom" cargo test -p agrona --lib`: 10 concurrency tests pass, exhaustively proving SPSC, MPSC (reservation, unblock, wrap), and broadcast buffer safety.
- **Archive Compatibility**:
  - Modern `AuthConnectRequest` (template 58, schema 14, semantic version 1.12.0) implemented; legacy template 2 retired.
  - Source-image identity binding enforces session ownership on requests and closing.
  - Active replays stopped before recording truncation.
- **Client & Driver Lifecycle**:
  - Buffer claim commit/abort, 64-bit reserved values, controlled polling, and `ControlledFragmentAssembler` retry semantics verified end-to-end.
  - Concurrent publication term rotation race verified with driver IPC term buffer zeroing (`cleanBufferTo`).
  - Min flow control receiver liveness tracking, MDC destination cleanup/re-add, and untethered subscriber draining verified under live traffic.
- **Explicit Follow-Up Boundaries**:
  - Persistent subscriptions (orchestration layer) kept open under [#8](https://github.com/azusachino/harus-aeron-rs/issues/8).
  - Driver UDP name resolution gossip kept open under [#12](https://github.com/azusachino/harus-aeron-rs/issues/12).
  - NativeResourceAgent scheduling kept open under [#10](https://github.com/azusachino/harus-aeron-rs/issues/10).
  - Replicated cluster consensus, service container, and Archive recovery kept open under [#18](https://github.com/azusachino/harus-aeron-rs/issues/18) and [#19](https://github.com/azusachino/harus-aeron-rs/issues/19).
  - Release acceptance remains governed by [#23](https://github.com/azusachino/harus-aeron-rs/issues/23).
