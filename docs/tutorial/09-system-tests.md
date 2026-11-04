# Chapter 9: System Integration Tests & Java Interop

This chapter covers system integration testing for `MediaDriver` and Java interop verification.

This chapter is different from the rest of the tutorial in one important way: its "table" isn't a
byte layout in a buffer, it's a table of *trust*. `deploy/interop/INTEROP_STATUS.md` is a literal
markdown table — Target Scenario / Transport / Status / Notes — and it is the single most honest
document in this repository, because every row is a claim you could go and re-run yourself with
`make interop-smoke`. As of this writing it says **PASS** for driver startup, CnC metadata mapping
against Java's own `CncFileDescriptor`/`CountersReader`, publication/subscription registration in
both directions, and a Java-publishes/Rust-driver/Java-subscribes loop plus its reverse
(Rust-client through a real Java `MediaDriver`). It explicitly does *not* claim multicast, a long
packet-loss soak, or — critically for the chapters ahead — anything about Archive or Cluster wire
compatibility. If you're debugging something that crosses the Rust/Java boundary, this table is
where you check what's actually been proven before you assume the two implementations agree.

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
make astro-build
```
