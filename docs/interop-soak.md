# Interop soaks

The Java-backed soak is the v0.1.0 release gate. It uses the vendored Aeron `1.50.x`
`aeron-all` JAR, a real Java Aeron client, and the Rust MediaDriver, then checks ordered
sequence-tagged UDP messages end to end.

Run the default 1,000-message Java interop soak with:

```bash
make interop-soak
```

The one-message form remains the fast Java interop smoke gate:

```bash
make interop-smoke
```

The Java soak count is configurable for local runs:

```bash
INTEROP_SOAK_MESSAGES=5000 make interop-soak
```

The Rust-only process soak remains available separately:

```bash
make interop-rust-smoke
make interop-rust-soak
```

It starts independent Rust publisher, subscriber, and driver processes. This proves a
bounded Rust process-lifecycle and transport path, but is not Java interoperability evidence.

`make stress` runs the Rust process soak and the existing three-process Tokio UDP
leader-kill comparison. Neither is Java Aeron, Archive replication, packet-loss, or
full upstream system-test parity evidence; those boundaries remain documented.
