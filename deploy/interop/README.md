# Interop harness

Mirrors `harus-aeron-zig`'s `deploy/interop/`: a Java<->Rust interop smoke/soak harness
driven via `podman-compose`/`docker-compose`, validating the wire path against a real
Aeron Java client/driver.

The current smoke test launches the Rust driver, then maps its generated `cnc.dat` with
vendored Aeron Java `CncFileDescriptor` and `CountersReader` code. It verifies the shared
metadata geometry and control-protocol counter used by Java clients.

Planned shape, matching the zig repo's layout:
- `CncLayoutProbe.java` — Java reader used by `make interop-smoke`

This is a CnC compatibility probe, not yet a full Java Aeron client publication/subscription
roundtrip. The Rust CnC command/event plane and counter values still need the remaining
control-protocol and log-buffer parity before that stronger mixing claim is valid.
