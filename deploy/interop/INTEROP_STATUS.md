# Java Interop Smoke Status

This document tracks interop smoke verification between `harus-aeron-rs` (Rust MediaDriver) and upstream Java Aeron `1.50.x`.

## Interop Architecture

- **Driver Process**: `cargo run --bin aeron-driver` (Rust MediaDriver)
- **Protocol**: UDP unicast & multicast wire-compatibility per Aeron UDP protocol specification.
- **Client SDKs**: Rust `aeron-client` and `io.aeron:aeron-client` (Java 17+ JVM runtime).

## Verification Matrix

| Target Scenario | Transport | Status | Notes |
| :--- | :--- | :--- | :--- |
| **Driver Startup** | Local IPC | **PASS** | Rust MediaDriver process launches cleanly |
| **CnC metadata mapping** | `cnc.dat` | **PASS** | Aeron Java `CncFileDescriptor` and `CountersReader` map Rust output |
| **Publication Reg** | `aeron:ipc`, `aeron:udp` | **PASS** | Java client registration completed through the Rust CnC command path |
| **Subscription Reg** | `aeron:ipc`, `aeron:udp` | **PASS** | Java client received Rust driver readiness/image events |
| **Pub/Sub Loop** | IPC + UDP Loopback | **PASS** | Java publication payloads were delivered through the Rust driver to Java subscriptions |
| **Reverse Pub/Sub Loop** | IPC | **PASS** | Rust client mapped Java CnC/counters and exchanged a payload through Java MediaDriver |

## Execution

Run the interop smoke harness:

```bash
make interop-smoke
```
