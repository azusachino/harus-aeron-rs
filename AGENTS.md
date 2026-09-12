# harus-aeron-rs

Aeron protocol reimplementation in Rust. Wire-compatible with the real Aeron UDP protocol
(<https://github.com/aeron-io/aeron>). Targets: Media Driver first, then Archive, then Cluster.
Sibling project: `harus-aeron-zig` (same protocol, different language) — see
[`docs/plan.md`](docs/plan.md) for why both exist.

## Tech Stack & Architecture

- **Language**: Rust (stable, edition 2024)
- **Async runtime**: tokio, used for UDP transport in the driver's duty-cycle agents
- **Build**: Cargo workspace via Makefile
- **Dev tooling**: Nix devShell (`nix develop`); `scripts/` are `uv`-run Python (PEP 723
  inline deps, `#!/usr/bin/env -S uv run --script`) for orchestration/reporting that
  doesn't belong in Cargo — no reimplementing what `cargo`/`clippy` already do. The
  Makefile stays a thin dispatcher: prefer delegating logic into a `uv`-run script
  over growing Makefile recipes, same as `test-parity` -> `scripts/upstream_tests.py`.
  `pytest`/`ruff` (via `uv run --group dev`) cover `scripts/` itself and are wired into
  `make fmt`/`make fmt-check`/`make test`.
- **Components** (crate boundaries mirror upstream `aeron-io/aeron`'s real Gradle
  modules — see `vendor/aeron/build.gradle` — not the zig repo's file layout):
  - `crates/agrona` — generic concurrent primitives (ring buffer, broadcast, atomic
    buffer, idle strategies, counters). Mirrors `org.agrona`, a dependency of Aeron
    rather than part of it. No other crate depends on it except through `aeron-client`.
  - `crates/client` (`aeron-client`) — protocol frame codecs (`src/protocol.rs`) +
    log buffer (`src/logbuffer.rs`) + Publication/Subscription API. Depends only on
    `agrona`, matching upstream. Every other crate depends on this one for wire types.
  - `crates/driver` (`aeron-driver`) — MediaDriver: Conductor, Sender, Receiver agents
    - UDP transport (`src/media.rs`). Depends on `aeron-client` + `agrona`. Also hosts
    the driver process binary at `src/bin/aeron-driver.rs`.
  - `crates/archive` (`aeron-archive`) — future phase, depends on `aeron-client`.
  - `crates/cluster` (`aeron-cluster`) — future phase, depends on `aeron-client` +
    `aeron-archive` (snapshots persist through the archive).

## Build, Run & Test

Enter dev shell: `nix develop`
Run any tool without entering: `nix develop --command <cmd>`
All daily operations go through `make <target>`:

```bash
make build            # cargo build --workspace
make build-driver     # build driver binary only
make test             # run all tests
make test-unit        # unit tests only
make test-integration # integration tests only
make lint             # cargo clippy -D warnings
make fmt              # cargo fmt + prettier
make check            # fmt-check + lint + build + test
make clean            # cargo clean
make run              # run media driver
make tutorial-check   # build tutorial/ learner-stub workspace
make docs-serve       # preview the MkDocs Material tutorial site
make docs-build       # strict documentation build
```

## Coding Conventions

- Standard Rust community conventions: hyphenated `aeron-*` crate names, workspace
  inheritance for shared metadata/deps (`version.workspace = true`, etc.), `rustfmt`
  defaults, `clippy -D warnings` clean.
- **Error handling**: `thiserror` for library error types, no `unwrap()`/`expect()` in
  receiver/conductor paths — Aeron receives untrusted UDP.
- **No global state**: pass config/handles down explicitly.
- **Wire types**: `#[repr(C)]` structs for protocol frames, matching Aeron's wire layout.
- **Naming**: snake_case for functions/variables, PascalCase for types, SCREAMING_SNAKE
  for constants — standard Rust, no deviation.
- **Tests**: `#[cfg(test)]` modules per file for unit tests; `tests/` per crate for
  integration tests once they exist.

## Key Files & Entry Points

- `crates/driver/src/bin/aeron-driver.rs` — driver binary entry
- `crates/client/src/lib.rs` — client library root
- `crates/client/src/protocol.rs` — Aeron wire frame types (once implemented)
- `crates/driver/src/lib.rs` — DriverConductor equivalent (command/control core)
- `docs/plan.md` — phased implementation roadmap
- `docs/guides/architecture.md` — system architecture
- `scripts/upstream_tests.py` — test-parity tracker/reporter (`make test-parity`);
  `test/upstream_map.jsonl` is the per-upstream-test-file status ledger it reads/writes

## Tutorial Layer

This repo is also a structured course, mirroring `harus-aeron-zig`. Two parallel trees:

- **`crates/`** — full reference implementation (always builds, `make check` must pass)
- **`tutorial/`** — learner stubs mirroring `crates/`, bodies are `todo!(...)`

**Key conventions for agents writing code:**

- After implementing a chapter in `crates/`, write the matching stub in `tutorial/`
  and the chapter doc in `docs/tutorial/`.
- Full design reference (zig repo): `docs/specs/2026-03-17-tutorial-course-design.md`
  equivalent — not yet ported here.

## Quality Standards

- `make check` must pass before every commit; CI (`.github/workflows/ci.yml`) enforces
  it on every push/PR.
- All frame types must have `const _: () = assert!(size_of::<T>() == N);` size checks
  once implemented, matching the spec.
- No `unwrap()`/`expect()`/`panic!()` in receiver/conductor paths.
- Unit tests for every codec function; integration tests for pub/sub round-trip.
- `proptest` for codec/buffer roundtrip properties (`crates/client`, `crates/agrona`
  dev-dependency).
- `loom` for lock-free structures in `crates/agrona` — see that crate's `lib.rs` doc
  comment for the `#[cfg(loom)]` convention. Exhaustive interleaving checks matter here
  specifically because `harus-aeron-zig`'s headline unresolved bug was a concurrency
  race (see `docs/plan.md`); don't skip this for ring buffer/broadcast code.
- `cargo +nightly miri test -p agrona` (`make miri`) once `crates/agrona` has real
  `unsafe` — requires a nightly toolchain + `miri` component, separate from the pinned
  stable `rust-toolchain.toml`.
- `criterion` (`make bench`) for anything perf-sensitive; add `[[bench]]` targets as
  real implementations land (see the commented-out stanza in `crates/agrona/Cargo.toml`).
