# harus-aeron-rs

Aeron reimplemented in Rust — wire-compatible with the real Aeron UDP protocol
(https://github.com/aeron-io/aeron). Sibling project to
[`harus-aeron-zig`](https://github.com/azusachino/harus-aeron-zig); see
[`docs/plan.md`](docs/plan.md) for why both exist.

## Project Status

Cargo workspace mirroring upstream Aeron's actual module boundaries: `agrona`
(generic concurrent primitives, a dependency of Aeron rather than part of it),
`aeron-client` (protocol + logbuffer + Publication/Subscription), `aeron-driver`
(Conductor/Sender/Receiver + UDP transport, also home to the `aeron-driver` binary),
`aeron-archive`, and `aeron-cluster`.

The current branch has working driver and Archive slices plus a bounded cluster core:
shared mmap CnC/log buffers, real `DriverConductor` command processing, IPC and UDP
publication paths, Archive control/runtime services, deterministic cluster election,
node state, recording-log metadata, and failover comparison tests. The latest parity
ledger is 268/268 files resolved: 175 bounded equivalents and 93 documented skips.
The 83 driver/client system-test rows are specifically 43 bounded equivalents and 40
skips; this is a complete audit, not a claim of full Java method parity. See
[`docs/plan.md`](docs/plan.md), [`docs/system-test-parity-boundaries.md`](docs/system-test-parity-boundaries.md),
[`docs/cluster-parity-boundaries.md`](docs/cluster-parity-boundaries.md), and
[`docs/interop-soak.md`](docs/interop-soak.md).

## Getting Started

```bash
# Enter dev shell (provides rustc, cargo, clippy, rustfmt, rust-analyzer)
nix develop

# Build
make build

# Run tests
make test

# Check everything (fmt + lint + build + test)
make check
```

## Stack

- **Language**: Rust (stable, edition 2024)
- **Async runtime**: tokio
- **Dev tooling**: Nix devShell (`nix develop`)
- **Task runner**: `make`
- **Interop pin**: Aeron `1.50.x`
- **Reference**: [aeron-io/aeron](https://github.com/aeron-io/aeron)

## Upstream Source

- Preferred source of truth for protocol/spec checks: local `vendor/aeron` when present.
- If `vendor/aeron` is missing or stale, refresh it with `make setup-upstream-aeron`.
- Preferred source of truth for Tokio API checks: local `vendor/tokio` when present.
- If `vendor/tokio` is missing or stale, refresh it with `make setup-upstream-tokio`.
- For Rust stdlib API/source checks, use the `rust-src` rustup component (pinned by
  `rust-toolchain.toml`, so it always matches the active toolchain exactly) rather than
  a vendored clone: `rustup component add rust-src` (already listed in
  `rust-toolchain.toml`'s `components`), then browse
  `$(rustc --print sysroot)/lib/rustlib/src/rust/library`.

## Test parity

`0.1.0` ships once near-100% of upstream Aeron's 268 test files have a Rust
equivalent (see [`docs/plan.md`](docs/plan.md)). Track progress with:

```bash
make test-parity   # wraps: uv run scripts/upstream_tests.py report
```

`test/upstream_map.jsonl` is the source of truth for per-file status. Regenerate it
after pulling new upstream commits with `uv run scripts/upstream_tests.py init`
(additive only — never overwrites an existing entry's status).

See [`AGENTS.md`](AGENTS.md) for the full architecture and conventions.
