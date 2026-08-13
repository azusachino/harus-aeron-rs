# Chapter 9: System Integration Tests & Java Interop

This chapter covers system integration testing for `MediaDriver` and Java interop verification.

## System Integration Suite

- `crates/driver/tests/system_tests.rs`: Tests driver process startup, IPC (`aeron:ipc`) publication/subscription channel registrations, and UDP transport endpoint initialization.
- `crates/driver/tests/flow_control_tests.rs`: Tests multi-stream registrations across concurrent stream IDs and verifies Status Message (SM) / NAK retransmission feedback processing on channel endpoints.

## Java Interop Smoke Harness

- `deploy/interop/INTEROP_STATUS.md`: Ledger tracking wire-level UDP interop matrix against upstream Java Aeron `1.50.x`.
- `scripts/interop_smoke.py`: PEP 723 executable script launching Rust `aeron-driver` binary and running interop verification tasks.
- `make interop-smoke`: One-message CnC, IPC, and UDP smoke check, including reverse Rust-client/Java-driver IPC.
- `make interop-soak`: Bounded ordered Java Aeron UDP soak through the Rust MediaDriver; set `INTEROP_SOAK_MESSAGES` to change its size.

The Java soak proves the current client-plus-driver interop boundary. It does not prove
Archive replication, Cluster wire compatibility, multicast, or full upstream system-test
parity; see the boundary documents in the verification section.

## Verification

Run all tests and workspace checks with:

```bash
make check
make docs-build
```
