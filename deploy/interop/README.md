# Interop harness

A Java<->Rust smoke/soak harness using local processes and the mise-selected JDK,
validating the wire path against a real Aeron Java client/driver.

The current smoke test verifies four boundaries: `CncFileDescriptor`/`CountersReader` mapping,
Java-client ↔ Rust-driver IPC and UDP round-trips, and a Rust-client ↔ Java-driver IPC
round-trip. `make interop-soak` reuses the Java UDP probe for an ordered, configurable
message run against the Rust driver; `make interop-smoke` fixes that run at one message.
The Rust driver is serviced continuously during the first set of probes; the Java driver is
held separately for the reverse-direction probe.

Probe sources:

- `AeronArtifactProbe.java` — verify the built JAR's embedded version and Git revision
- `CncLayoutProbe.java` — CnC metadata and Agrona ring-layout reader
- `JavaPubSubProbe.java` — Java IPC client round-trip through Rust MediaDriver
- `JavaUdpPubSubProbe.java` — Java UDP client round-trip or ordered soak through Rust MediaDriver
- `JavaDriverHost.java` — holds a vendored Java MediaDriver for reverse mixing
- `crates/client/src/bin/rust_client_probe.rs` — Rust IPC client round-trip through Java MediaDriver

## Source and artifact identity

Before compiling the traffic probes, the harness checks that `vendor/aeron` matches
the recorded Git index gitlink. This also supports validating a deliberately staged
reference upgrade before committing it. An uninitialized or mismatched checkout
blocks the run; initialize it with `make setup-upstream-aeron`.

Gradle checks source/output freshness on every run. The harness selects only
`aeron-all-<version.txt>.jar`, not a lexicographically selected cached version.
`AeronArtifactProbe` compares the JAR's embedded `AeronVersion.VERSION` and
`AeronVersion.GIT_SHA` against the selected source and prints the source revision,
artifact identity, and JDK runtime/vendor. A same-filename JAR with the wrong version
or Git revision fails before it can become interoperability evidence. These checks
catch accidental stale or mislabelled local outputs; they are not a cryptographic
build attestation.

The built `aeron-all` JAR is only a Java runtime/compile dependency for these probes. It is
not used as the authority for offsets or protocol behavior. Those references are checked-in
under `vendor/aeron/aeron-client/src/main/java/` (for example, `CncFileDescriptor.java` and
`LogBufferDescriptor.java`) and the pinned Agrona source reference in
`crates/agrona/src/ring_buffer/mod.rs`.

This proves Java-client ↔ Rust-driver mixing for these IPC and UDP paths plus Rust-client ↔
Java-driver IPC mixing. It does not prove the full upstream driver test suite, archive/cluster
parity, or all multicast and advanced channel modes.
