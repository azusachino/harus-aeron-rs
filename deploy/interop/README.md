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

This proves Java-client ↔ Rust-driver mixing for these IPC and UDP paths. It does not prove the
full upstream driver test suite, Rust-client ↔ Java-driver mixing, archive/cluster parity, or
all multicast and advanced channel modes.
