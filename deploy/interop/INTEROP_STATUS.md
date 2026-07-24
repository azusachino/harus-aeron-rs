# Java Interop Smoke Status

This document tracks interop smoke verification between `harus-aeron-rs` (Rust MediaDriver) and upstream Java Aeron `1.50.x`.

## Interop Architecture

- **Driver Process**: `cargo run --bin aeron-driver` (Rust MediaDriver)
- **Protocol**: UDP unicast & multicast wire-compatibility per Aeron UDP protocol specification.
- **Client SDK**: `io.aeron:aeron-client` (Java 17+ JVM runtime).

## Verification Matrix

| Target Scenario | Transport | Status | Notes |
| :--- | :--- | :--- | :--- |
| **Driver Startup** | Local IPC | **PASS** | Rust MediaDriver process launches cleanly |
| **CnC metadata mapping** | `cnc.dat` | **PASS** | Aeron Java `CncFileDescriptor` and `CountersReader` map Rust output |
| **Publication Reg** | `aeron:udp` | **UNVERIFIED** | Rust-side registration is tested; Java client mixing is not yet proven |
| **Subscription Reg** | `aeron:udp` | **UNVERIFIED** | Rust-side registration is tested; Java client mixing is not yet proven |
| **Pub/Sub Loop** | UDP Loopback | **UNVERIFIED** | Rust↔Rust UDP loopback passes; Java endpoint roundtrip remains |

## Execution

Run the interop smoke harness:

```bash
make interop-smoke
```
