# Interop harness (planned)

Mirrors `harus-aeron-zig`'s `deploy/interop/`: a Java<->Rust interop smoke/soak harness
driven via `podman-compose`/`docker-compose`, validating the wire path against a real
Aeron Java client/driver.

Not implemented yet — there is no Rust driver or client to test against. Build this
once `crates/driver` and `crates/client` pass their own unit tests and
`make interop-smoke` has something real to exercise.

Planned shape, matching the zig repo's layout:
- `Containerfile` — Rust driver image
- `Containerfile.java-aeron` — Java client/driver image (vendored `aeron-all` jar)
- `docker-compose.ci.yml` — CI-friendly smoke topology
- `InteropSmoke.java` — finite Java client used by `make interop-smoke`
