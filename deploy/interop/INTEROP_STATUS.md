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
| **Publication Reg** | `aeron:udp` | **PASS** | Channel URI parsing and endpoint binding match Java upstream |
| **Subscription Reg** | `aeron:udp` | **PASS** | Multicast/Unicast endpoint demuxing green |
| **Pub/Sub Loop** | UDP Loopback | **PASS** | Datagram frame transmission verified |

## Execution

Run the interop smoke harness:

```bash
uv run scripts/interop_smoke.py
```
