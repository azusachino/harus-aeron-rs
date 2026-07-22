# Changelog

## 0.0.1 (unreleased)

- Initial skeleton: Cargo workspace with stub crates `agrona`, `aeron-client`,
  `aeron-driver`, `aeron-archive`, `aeron-cluster` — mirroring upstream
  [`aeron-io/aeron`](https://github.com/aeron-io/aeron)'s actual Gradle module
  boundaries (agrona as a separate dependency; protocol/logbuffer/media as packages
  inside `aeron-client`/`aeron-driver`, not separate crates) rather than
  [`harus-aeron-zig`](https://github.com/azusachino/harus-aeron-zig)'s single-build-unit
  file layout. No subsystem implemented yet.
