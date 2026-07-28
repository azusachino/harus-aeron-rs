# Implementation Plan & Roadmap

Crate boundaries mirror upstream `aeron-io/aeron`'s actual Gradle modules (`agrona` as
an external dependency; `io.aeron.protocol`/`io.aeron.logbuffer`/`io.aeron.driver.media`
as packages inside `aeron-client`/`aeron-driver`, not separate modules) rather than the
zig repo's single-build-unit file layout — see `vendor/aeron/build.gradle`.

## Release gate for 0.1.0

`0.1.0` is the first release. The gate is near-100% parity with upstream Aeron's own
test suite, ported to Rust equivalents and green — not just "the happy path works."
Counts below are upstream `1.50.x` Java test *files* (one file is usually many
`@Test` cases), re-surveyed from `vendor/aeron` on 2026-07-22:

| Upstream module | Test files | Rust target |
|---|---:|---|
| `aeron-client` (`protocol` 2, `logbuffer` 11, `command` 2, `exceptions` 1, `security` 1, root 16) | 33 | `crates/client` |
| `aeron-driver` (`driver` package) | 41 | `crates/driver` |
| `aeron-archive` | 30 | `crates/archive` |
| `aeron-cluster` | 38 | `crates/cluster` |
| `aeron-system-tests` (`driver` 13, `security` 2, `test` 1, root 67 — driver/client integration) | 83 | integration tests across `crates/client` + `crates/driver` |
| `aeron-system-tests` (`archive` 20) | 20 | integration tests, `crates/archive` |
| `aeron-system-tests` (`cluster` 23) | 23 | integration tests, `crates/cluster` |
| **Total** | **268** | |

Re-run the survey before trusting these numbers as current:
```bash
find vendor/aeron/aeron-{client,driver,archive,cluster,system-tests}/src/test -name '*.java' | wc -l
```

Or use the tracked, per-file version: `make test-parity` (wraps
`scripts/upstream_tests.py report`, reading `test/upstream_map.jsonl`). Run
`scripts/upstream_tests.py init` after `make setup-upstream-aeron` picks up new
upstream commits — it adds newly-appeared upstream test files as `todo` entries
without touching existing status. Flip an entry's `status` to `done` (with
`rust_equivalent` pointing at the ported test) or `skip` (with a reason) as work lands.

"Near-100%" means: every upstream test file has a Rust equivalent covering the same
behavior, and it passes. A handful of upstream tests that are Java-specific (JMX,
annotation-processor checks, JUnit extension plumbing) are expected exceptions —
document each skip explicitly rather than silently dropping coverage.

## Milestones (0.0.1 → 0.1.0)

Each milestone is a version bump, gated on the stated exit criteria. Track these as
GitHub milestones with one issue per exit-criteria bullet (see "Workflow" below).

### v0.0.1 — Foundations (current)
Cargo workspace mirroring upstream's module graph, Nix/Make tooling, `vendor/aeron` +
`vendor/tokio` pinned as submodules, `rust-toolchain.toml` pinned (including the
`rust-src` component for stdlib API/source checks — no need to vendor rust-lang/rust
itself), `uv`-managed `scripts/` (see `scripts/upstream_tests.py`,
which generated `test/upstream_map.jsonl` — the 268-entry parity tracker this roadmap
is gated on), this roadmap. No subsystem implemented.

### v0.0.2 — agrona core
Ring buffer (SPSC/MPSC), broadcast transmitter/receiver, atomic buffer wrapper, idle
strategies (busy-spin/back-off/yielding), counters manager. No direct upstream test
count (agrona ships as its own repo/artifact) — gate is unit-test coverage sufficient
for `aeron-client` to build on top of it in the next milestone.

### v0.0.3 — aeron-client: wire protocol + log buffer
Port `client/protocol` (2 files) + `client/logbuffer` (11 files) test equivalents.
**Exit: 13/13 green.**

### v0.0.4 — aeron-client: Publication/Subscription/ClientConductor
Port `client/command` (2) + `client/exceptions` (1) + `client/security` (1) + client
root (16). **Exit: 33/33 client parity.**

### v0.0.5 — aeron-driver: Conductor/Sender/Receiver + media
Port the driver runtime boundary: CnC command processing, IPC and UDP data paths,
flow control, loss recovery, and managed lifecycle. **Current: 41/41 driver test
files resolved.** This is driver-module parity only; it does not imply Archive, Cluster,
or full system-test parity.

### v0.0.6 — driver/client system tests
Real pub/sub round-trip, reconnect, multi-stream, flow control — the scenarios upstream
puts in `aeron-system-tests` rather than the per-module unit tests. Port
`system-tests/driver` (13) + `system-tests/security` (2) + `system-tests/test` (1) +
the driver/client-relevant subset of system-tests root (67). **Exit: 83/83 driver/client
system-test parity.** This is also where Java interop-smoke goes green (mirrors the zig
repo's `INTEROP_STATUS.md` gate) — build out `deploy/interop/` here.

**Current state (2026-07-27, honest re-audit):** the exit criterion is not met. A full
method-level audit of vendor `aeron-system-tests` (319 `@Test` methods across 65 files,
not the 83-file estimate above — see `.agents/system-test-audit.md` for the complete
mapping) found only 15 genuine Rust system tests in `crates/driver/tests/system_tests.rs`,
covering: IPC/UDP pub/sub roundtrip, network spy, fragmented-message reassembly, bounded
resource cleanup across repeated churn, manual multi-destination cast (MDC), subscription
reconnect, silent-client liveness reaping, one client running two concurrent streams, and
single- and multi-gap NAK/retransmit loss recovery. `tutorial/driver/tests/system_tests.rs`
and `tutorial/driver/tests/flow_control_tests.rs` are learner stubs with zero assertions
and do not count as coverage.

That work also surfaced and fixed one real correctness bug (not just a test gap): a
subscriber joining an already-active IPC/spy/network image had its read position
hardcoded to 0 on both the driver and client side, so it replayed data the publisher had
already written instead of starting at the live position. Fixed in `on_available_image`
(driver) and `ClientConductor::on_available_image` (client) to thread the real join
position through.

Explicit boundaries found during this pass (not silently dropped — see the
`harus-aeron-rs:pitfall:*` entries in the project's Asobi graph for full detail):
- `crates/client` has no `ExclusivePublication` type, `try_claim`/`BufferClaim`, or
  controlled-poll API, so ~15 vendor methods in that area cannot get a system test until
  the client exposes them.
- `multicast_flow_control.rs` (891 lines, min/max/tagged strategies) and
  `congestion_control.rs` are unit-tested in isolation but never called from
  `network_publication.rs`/`sender.rs`/`conductor.rs` — every publication behaves as
  `fc=max` regardless of the channel's `fc=` parameter, so strategy *selection* has
  nothing to system-test yet (~15 vendor methods).
- Untethered-subscriber timeout requires 0.75x term lag, but real IPC backpressure caps
  reachable lag at 0.5x term, so the Linger transition is unreachable via genuine
  `offer()` calls — only the existing unit test (which pokes `raw_tail` directly) covers
  it.
- True UDP multicast, `control-mode=response`, and publication revoke/reject-image are
  recognized in parsing but have no driver-side transport/handling at all (39 methods).

Remaining in-scope gaps (~220 methods, largest first): channel/URI validation (51),
MDC/MDS depth beyond the one manual-add-destination test (43), reconnection/lifecycle
depth (37), counters (17), session-specific pub/sub (11), name re-resolution (11,
depth unconfirmed), timestamping (9, depth unconfirmed), misc/internal (10).

### v0.0.7 — aeron-archive
Port `aeron-archive` module (30) + `system-tests/archive` (20). **Exit: 50/50.**

### v0.0.8 — aeron-cluster
Port `aeron-cluster` module (38) + `system-tests/cluster` (23). **Exit: 61/61.** This is
the milestone that matters most for the sibling-project comparison: run the same
stuck-failover-under-load repro from `harus-aeron-zig` (kill the leader mid-load, watch
for permanent stall) against this Rust/tokio implementation. If it reproduces here too,
that's strong evidence for the zig repo's macOS/XNU kernel hypothesis rather than a
zig-specific defect. If it doesn't, the zig repo has a real bug to go find.

### v0.0.9 — Hardening
Soak tests, fuzz targets for codecs/ring buffer/log buffer, full `make check` green,
close any parity gaps found during v0.0.2–v0.0.8, document intentional Java-specific
test skips.

### v0.1.0 — Release
268-test-file parity (minus documented Java-specific exceptions), interop-smoke +
interop-soak green against real Java Aeron `1.50.x`, `CHANGELOG.md` cut, tag.

## Workflow

Tracked via GitHub milestones + issues on `azusachino/harus-aeron-rs`, one milestone
per version above, issues scoped per exit-criteria bullet. See repo issues/milestones
for current status rather than trusting this document's checkboxes (it doesn't have
any — state lives in GitHub, not here).

## Why this repo exists alongside harus-aeron-zig

The zig implementation hit an unresolved, suspected macOS/XNU kernel-level UDP
receive-queue bug (`stuck-failover-under-load`) that permanently hangs cluster
failover under sustained load — reproduced on bare host, ruled out as an
application-layer bug across five separate investigations, pending confirmation on
Linux. v0.0.8 above is where this repo reaches the same test scenario on Rust/tokio.
