# Chapter 9: System Integration Tests & Java Interop

This chapter covers system integration testing for `MediaDriver` and Java interop verification.

## System Integration Suite

- `crates/driver/tests/system_tests.rs`: Tests driver process startup, IPC (`aeron:ipc`) publication/subscription channel registrations, and UDP transport endpoint initialization.
- `crates/driver/tests/flow_control_tests.rs`: Tests multi-stream registrations across concurrent stream IDs and verifies Status Message (SM) / NAK retransmission feedback processing on channel endpoints.

## Java Interop Smoke Harness

- `deploy/interop/INTEROP_STATUS.md`: Ledger tracking wire-level UDP interop matrix against upstream Java Aeron `1.50.x`.
- `scripts/interop_smoke.py`: PEP 723 executable script launching Rust `aeron-driver` binary and running interop verification tasks.

## Verification

Run all tests and workspace checks with:

```bash
make check
```
