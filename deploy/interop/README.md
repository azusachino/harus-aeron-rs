# Interop harness

Mirrors `harus-aeron-zig`'s `deploy/interop/`: a Java<->Rust interop smoke/soak harness
driven via `podman-compose`/`docker-compose`, validating the wire path against a real
Aeron Java client/driver.

The current smoke test verifies four boundaries: `CncFileDescriptor`/`CountersReader` mapping,
Java-client ↔ Rust-driver IPC and UDP round-trips, and a Rust-client ↔ Java-driver IPC
round-trip. `make interop-soak` reuses the Java UDP probe for an ordered, configurable
message run against the Rust driver; `make interop-smoke` fixes that run at one message.
The Rust driver is serviced continuously during the first set of probes; the Java driver is
held separately for the reverse-direction probe.

Probe sources:

- `CncLayoutProbe.java` — CnC metadata and Agrona ring-layout reader
- `JavaPubSubProbe.java` — Java IPC client round-trip through Rust MediaDriver
- `JavaUdpPubSubProbe.java` — Java UDP client round-trip or ordered soak through Rust MediaDriver
- `JavaDriverHost.java` — holds a vendored Java MediaDriver for reverse mixing
- `crates/client/src/bin/rust_client_probe.rs` — Rust IPC client round-trip through Java MediaDriver

The built `aeron-all` JAR is only a Java runtime/compile dependency for these probes. It is
not used as the authority for offsets or protocol behavior. Those references are checked-in
under `vendor/aeron/aeron-client/src/main/java/` (for example, `CncFileDescriptor.java` and
`LogBufferDescriptor.java`) and the pinned Agrona source reference in
`crates/agrona/src/ring_buffer/mod.rs`.

This proves Java-client ↔ Rust-driver mixing for these IPC and UDP paths plus Rust-client ↔
Java-driver IPC mixing. It does not prove the full upstream driver test suite, archive/cluster
parity, or all multicast and advanced channel modes.
