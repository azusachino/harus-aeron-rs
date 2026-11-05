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

The upstream reference is Aeron 1.54.0. The previous 268-row ledger described the
1.50.x scope. The reconciled 1.54.0 inventory contains 335 current paths plus one
retained historical path; 163 added or changed sources remain queued for review.
Reference selection and file counts alone do not establish behavioral parity. See
[the inventory audit](docs/aeron-1.54.0-test-inventory.md) and [issue #44](https://github.com/azusachino/harus-aeron-rs/issues/44).

The current branch has working driver and Archive slices plus a bounded cluster core:
shared mmap CnC/log buffers, real `DriverConductor` command processing, IPC and UDP
publication paths, Archive control/runtime services, deterministic cluster election,
node state, recording-log metadata, and failover comparison tests. The previous
ledger recorded 175 bounded equivalents and 93 skips across 268 paths. The current
inventory preserves those statuses as earlier audit evidence and queues changed/new
source review; skips do not count as implemented parity. See [`docs/plan.md`](docs/plan.md),
[`docs/aeron-1.54.0-test-inventory.md`](docs/aeron-1.54.0-test-inventory.md),
[`docs/system-test-parity-boundaries.md`](docs/system-test-parity-boundaries.md),
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

The Astro site implementation lives in `scripts/astro/`; authored documentation
and tutorial chapters remain in `docs/`.

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

`0.1.0` ships once near-100% of the current upstream test-file inventory has a Rust
equivalent (see [`docs/plan.md`](docs/plan.md) and the [inventory audit](docs/aeron-1.54.0-test-inventory.md)). Track progress with:

```bash
make test-parity   # wraps: uv run scripts/upstream_tests.py report
```

`test/upstream_map.jsonl` is the source of truth for per-file status. Regenerate it
after pulling new upstream commits with `mise exec -- uv run scripts/upstream_tests.py init`
(additive only — never overwrites an existing entry's status).

See [`AGENTS.md`](AGENTS.md) for the full architecture and conventions.
