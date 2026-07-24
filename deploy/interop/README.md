# Interop harness

Mirrors `harus-aeron-zig`'s `deploy/interop/`: a Java<->Rust interop smoke/soak harness
driven via `podman-compose`/`docker-compose`, validating the wire path against a real
Aeron Java client/driver.

The current smoke test launches the Rust driver, then uses the vendored Aeron Java client to
verify three boundaries: `CncFileDescriptor`/`CountersReader` mapping, an IPC publication and
subscription round-trip, and a UDP loopback publication and subscription round-trip. The Rust
driver is serviced continuously during the probes.

Probe sources:
- `CncLayoutProbe.java` — CnC metadata and Agrona ring-layout reader
- `JavaPubSubProbe.java` — Java IPC client round-trip through Rust MediaDriver
- `JavaUdpPubSubProbe.java` — Java UDP client round-trip through Rust MediaDriver

The built `aeron-all` JAR is only a Java runtime/compile dependency for these probes. It is
not used as the authority for offsets or protocol behavior. Those references are checked-in
under `vendor/aeron/aeron-client/src/main/java/` (for example, `CncFileDescriptor.java` and
`LogBufferDescriptor.java`) and the pinned Agrona source reference in
`crates/agrona/src/ring_buffer/mod.rs`.

This proves Java-client ↔ Rust-driver mixing for these IPC and UDP paths. It does not prove the
full upstream driver test suite, Rust-client ↔ Java-driver mixing, archive/cluster parity, or
all multicast and advanced channel modes.
