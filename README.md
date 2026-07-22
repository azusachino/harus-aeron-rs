# harus-aeron-rs

Aeron reimplemented in Rust — wire-compatible with the real Aeron UDP protocol
(https://github.com/aeron-io/aeron). Sibling project to
[`harus-aeron-zig`](https://github.com/azusachino/harus-aeron-zig); see
[`docs/plan.md`](docs/plan.md) for why both exist.

## Project Status

Skeleton only. Cargo workspace mirroring upstream Aeron's actual module boundaries:
`agrona` (generic concurrent primitives, a dependency of Aeron rather than part of it),
`aeron-client` (protocol + logbuffer + Publication/Subscription), `aeron-driver`
(Conductor/Sender/Receiver + UDP transport, also home to the `aeron-driver` binary),
`aeron-archive`, and `aeron-cluster`. Nothing is implemented yet.

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
- Preferred source of truth for Rust stdlib API checks: local `vendor/rust` when present.
- If `vendor/rust` is missing or stale, refresh it with `make setup-upstream-rust`.

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
