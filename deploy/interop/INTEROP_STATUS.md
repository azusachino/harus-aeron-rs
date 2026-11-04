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

## Aeron 1.54.0 checkpoint

Verified during PR #45 validation on macOS arm64 with Corretto 17.0.21, after a
fresh `gradlew clean :aeron-all:shadowJar` under that JDK. Earlier validation also
passed with Temurin 21.0.13; the project now selects Corretto's 17.0 minor line. The selected source is
`454da1e4e6c7edf3dd6d34878512d3243384992a`, tag `1.54.0`, and the harness uses
`aeron-all-1.54.0.jar` built from that checkout.

- `make interop-smoke` passed: Java CnC readers, Java IPC and UDP through the Rust
  driver, and Rust-client IPC through the Java driver.
- `make interop-soak` passed the same probes with 1,000 ordered UDP messages.
- Rerunning `make setup-upstream-aeron` preserved the exact reference rather than
  following the newer `release/1.54.x` branch head.

The older 1.50.x evidence remains in Git history. These new runs do not establish
Archive authentication/control compatibility, full system-test parity, or Aeron
Cluster wire/recovery support; issue #44 tracks that work.
