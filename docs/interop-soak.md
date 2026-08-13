# Bounded process soak

The Rust multi-process harness provides a repeatable bounded soak over the real
MediaDriver and UDP publication path. It starts independent publisher and
subscriber driver/client processes, sends sequence-tagged payloads, and checks
that the subscriber receives every payload in order.

Run the default 1,000-message soak with:

```bash
make interop-rust-soak
```

The one-message form remains the fast smoke gate:

```bash
make interop-rust-smoke
```

The message count is configurable for local runs:

```bash
INTEROP_SOAK_MESSAGES=5000 make interop-rust-soak
```

`make stress` runs this soak and the existing three-process Tokio UDP leader-kill
comparison. The cluster comparison uses its documented custom protocol and does
not provide Aeron wire, Archive replication, packet-loss, or Java interoperability
evidence. The Rust process soak likewise does not prove Java interop or full
upstream system-test parity; it is a bounded process-lifecycle and transport gate.
