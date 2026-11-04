# harus-aeron-rs

Aeron reimplemented in Rust — wire-compatible with the real Aeron UDP protocol
(<https://github.com/aeron-io/aeron>). Sibling project to
[`harus-aeron-zig`](https://github.com/azusachino/harus-aeron-zig); see
[`docs/plan.md`](docs/plan.md) for why both exist.

## Project Status

Cargo workspace mirroring upstream Aeron's actual module boundaries: `agrona`
(generic concurrent primitives, a dependency of Aeron rather than part of it),
`aeron-client` (protocol + logbuffer + Publication/Subscription), `aeron-driver`
(Conductor/Sender/Receiver + UDP transport, also home to the `aeron-driver` binary),
`aeron-archive`, and `aeron-cluster`.

The upstream reference is Aeron 1.54.0. The existing 268-row parity ledger and
older interoperability records describe the previous 1.50.x scope; reference selection
alone does not establish 1.54 behavioral parity. [Issue #44](https://github.com/azusachino/harus-aeron-rs/issues/44)
tracks the remaining catch-up work.

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
# Install mise first, then install project tools and locked dependencies
make setup

# Build
make build

# Run tests
make test

# Check everything (fmt + lint + build + test)
make check

# Check and preview the Astro documentation site
make astro-check
make astro-dev
make astro-build
make astro-e2e
```

## Stack

- **Language**: Rust (stable, edition 2024)
- **Async runtime**: tokio
- **Dev tooling**: mise manages Rust stable, Bun, uv, and the interop JDK;
  tool CLIs use major.minor selectors in `.mise.toml`. uv manages Python through
  `.python-version`; Bun and uv lockfiles pin project dependencies.
- **Task runner**: `make`
- **Upstream reference**: Aeron `1.54.0` (exact gitlink; `release/1.54.x` family)
- **Reference**: [aeron-io/aeron](https://github.com/aeron-io/aeron)

## Upstream Source

- Preferred source of truth for protocol/spec checks: local `vendor/aeron` when present.
- Initialize `vendor/aeron` at the recorded revision with `make setup-upstream-aeron`.
  This target does not advance the reference; upgrades require a reviewed gitlink change.
- Preferred source of truth for Tokio API checks: local `vendor/tokio` when present.
- If `vendor/tokio` is missing or stale, refresh it with `make setup-upstream-tokio`.
- For Rust stdlib API/source checks, use the `rust-src` component installed by
  `make setup`, matching the selected stable compiler, rather than a vendored clone.
  Find the source under `lib/rustlib/src/rust/library` in the directory printed by
  `mise exec -- rustc --print sysroot`. `rust-toolchain.toml` keeps direct rustup
  invocations on stable with the same components.

## Test parity

`0.1.0` ships once near-100% of upstream Aeron's 268 test files have a Rust
equivalent (see [`docs/plan.md`](docs/plan.md)). Track progress with:

```bash
make test-parity   # wraps: uv run scripts/upstream_tests.py report
```

`test/upstream_map.jsonl` is the source of truth for per-file status. Regenerate it
after pulling new upstream commits with `mise exec -- uv run scripts/upstream_tests.py init`
(additive only — never overwrites an existing entry's status).

See [`AGENTS.md`](AGENTS.md) for the full architecture and conventions.
