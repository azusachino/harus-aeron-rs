# Changelog

## 0.1.0 (unreleased)

- Implemented the bounded driver, client, Archive, and cluster foundations described
  in the roadmap, including shared CnC/log buffers, IPC and UDP transport, Archive
  control/runtime services, and deterministic cluster failover primitives.
- Completed the upstream Aeron `1.50.x` test-file ledger: 175 bounded Rust
  equivalents and 93 documented skips, resolving all 268 tracked files without
  claiming full Java method or system-test parity.
- Added real Java Aeron `1.50.x` CnC, IPC, and UDP interop probes plus a bounded,
  ordered Java UDP soak through the Rust MediaDriver.
- Added fuzz coverage and bounded process/transport gates, with the remaining
  Java-specific, Archive, cluster, and protocol boundaries documented.

## 0.0.1 (unreleased)

- Initial skeleton: Cargo workspace with stub crates `agrona`, `aeron-client`,
  `aeron-driver`, `aeron-archive`, `aeron-cluster` — mirroring upstream
  [`aeron-io/aeron`](https://github.com/aeron-io/aeron)'s actual Gradle module
  boundaries (agrona as a separate dependency; protocol/logbuffer/media as packages
  inside `aeron-client`/`aeron-driver`, not separate crates) rather than
  [`harus-aeron-zig`](https://github.com/azusachino/harus-aeron-zig)'s single-build-unit
  file layout. No subsystem implemented yet.
