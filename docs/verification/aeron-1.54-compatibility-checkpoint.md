# Aeron 1.54.0 Compatibility Checkpoint (S13)

Compared against Aeron tag `1.54.0`, exact commit `454da1e4e6c7edf3dd6d34878512d3243384992a`.
Tracking issue: [#44](https://github.com/azusachino/harus-aeron-rs/issues/44).
Governing release issue: [#23](https://github.com/azusachino/harus-aeron-rs/issues/23).

---

## 1. Checkpoint Summary

This document records the completion of the Aeron 1.54.0 catch-up plan ([`docs/plans/2026-11-04-aeron-1.54-catch-up.md`](../plans/2026-11-04-aeron-1.54-catch-up.md)) across slices S0 through S13.

The catch-up plan moved `harus-aeron-rs` from the historical `1.50.x` snapshot (`8e0cdb2f92800a047c0066028444b790a80fe593`) to the released Aeron `1.54.0` reference (`454da1e4e6c7edf3dd6d34878512d3243384992a`), closed compatibility gaps in protocol wire contracts, packet validation, flow control, MPSC concurrency, modern Archive connection, and driver lifecycle, and established clear, honest boundaries for deferred runtime capabilities.

---

## 2. Milestone Journey (S0 – S13)

| Slice | Focus | PR / Commit | Key Verification & Evidence |
| :--- | :--- | :--- | :--- |
| **S0** | Upstream Reference Pin | PR #45 | Submodule pinned to `454da1e4e6c7edf3dd6d34878512d3243384992a`; `make setup` made reproducible; Java 1.54 shadow JAR built under Corretto 25.0.5; smoke + 1,000-message soak passing. |
| **S1** | Test Inventory Reconciliation | PR #46 | Reconciled 335 upstream test files with added, absent, and changed classifications in `test/upstream_map.jsonl`; documented in `docs/aeron-1.54.0-test-inventory.md`. |
| **S2** | Wire & Frame Layout Contracts | PR #47 | Wire contracts verified; CnC v0.2.0, command structures, data headers, and layout compatibility validated against Java golden vectors; documented in `docs/verification/aeron-1.54-wire-contracts.md`. |
| **S3** | Complete Packet Validation | PR #48 | Validated every frame in multi-frame UDP packets prior to driver state mutation; malformed trailing frames reject whole packet without side effects; documented in `docs/verification/aeron-1.54-udp-packet-validation.md`. |
| **S4** | Status Message Bounds | PR #49 | Enforced sender-relative bounds `[-0.5 term, +1.5 terms]` on Status Messages in network publications; documented in `docs/verification/aeron-1.54-status-message-bounds.md`. |
| **S5** | MPSC Ordering & Concurrency Proof | PR #56 | Ported upstream Aeron 1.54 MPSC protocol with record-header zeroing; verified exhaustively under Loom models (`test_mpsc_loom`, `test_mpsc_loom_try_claim`, `test_mpsc_loom_unblock`) and full normal test suite; documented in `docs/verification/aeron-1.54-mpsc-proof.md`. |
| **S6a** | Modern Archive Codecs (v14) | PR #50 | Ported template 58 `AuthConnectRequest` with credentials/token framing; updated XML schema version to 14 while retaining runtime semantic version 1.12.0; isolated legacy template 2. |
| **S6b** | Real Java 1.54 Archive Client | PR #50 | Official Java 1.54 client connects via template 58, records, stops, restarts, lists, and replays against Rust `ArchiveService`. |
| **S7** | Control Session Image Binding | PR #50 | Enforced source-image identity binding on Archive control sessions; reject unauthorized mutation or closing from secondary images. |
| **S8** | Replay Cancellation on Truncate | PR #50 | Active replays stopped before recording segments are truncated or deleted; unrelated recordings preserved. |
| **S9** | Archive Admission & Error Paths | PR #50 | Empty replays rejected cleanly without allocating recording resources; session timeouts and catalog sync verified. |
| **S10a** | Min Flow Control Liveness | PR #52 | Receiver activity tracking ensures slow/timed-out receivers do not stall sender limits indefinitely; documented in `docs/verification/aeron-1.54-min-flow-control-liveness.md`. |
| **S10b** | MDC Destination Lifecycle | PR #53 | Removing dynamic/manual MDC destinations cleans up state; verified re-addition and live casting; documented in `docs/verification/aeron-1.54-mdc-destination-lifecycle.md`. |
| **S10c** | Untethered Subscriber Draining | PR #55 | Lingering untethered subscribers resting behind publication tail advance to draining and close cleanly; documented in `docs/verification/aeron-1.54-untethered-draining-cleanup.md`. |
| **S11** | Client APIs & Concurrency End-to-End | PR #57 | BufferClaim commit/abort, 64-bit reserved value transfer, controlled polling (`Abort`, `Break`, `Continue`), `ControlledFragmentAssembler` retry semantics, and concurrent publication term rotation race verified across IPC and UDP; driver IPC term buffer zeroing (`cleanBufferTo`) implemented; documented in `docs/verification/aeron-1.54-client-apis-end-to-end.md`. |
| **S12** | Bounded Features & Cluster Scope | PR #58 | Verified cluster snapshot ordering by `(term, position, service)` in `RecordingLog`, multi-archive counter isolation in `RecordingPos`, UDP `so-tos` and `rcv-wnd` options, and resolver lifecycle; preserved larger gaps visibly open; documented in `docs/verification/aeron-1.54-bounded-feature-scope.md`. |
| **S13** | Compatibility Checkpoint | PR #59 | Consolidate parity boundaries, interop status, and release alignment; run full stable gate suite. |

---

## 3. Explicit Boundaries and Follow-Up Ownership

The completion of this compatibility checkpoint does **not** claim full upstream behavioral parity. The following capabilities remain explicitly bounded and tracked under their owning issues:

1. **Persistent Subscriptions** ([#8](https://github.com/azusachino/harus-aeron-rs/issues/8)): Client-orchestrated replay-to-live stream stitching remains a future client feature.
2. **Driver Name Resolution Gossip** ([#12](https://github.com/azusachino/harus-aeron-rs/issues/12)): Multi-node neighbor discovery and dynamic UDP gossip protocol remain an unsupported transport protocol. Primitives (`DriverNameResolverCache`, `DefaultNameResolver`, `TimeTrackingNameResolver`) are fully verified.
3. **NativeResourceAgent Scheduling** ([#10](https://github.com/azusachino/harus-aeron-rs/issues/10)): Conductor allocates log buffers synchronously via `mmap`. An asynchronous background worker thread requires an evidence-backed throughput/latency bottleneck before introduction.
4. **Replicated Cluster Wire Parity** ([#18](https://github.com/azusachino/harus-aeron-rs/issues/18), [#19](https://github.com/azusachino/harus-aeron-rs/issues/19)): Core snapshot ordering, term tracking, election logic, and custom-protocol failover comparisons are verified; wire-compatible Aeron Cluster client (`AeronCluster`), `ClusterBackup`, and clustered service container remain open work.
5. **Release Governance** ([#23](https://github.com/azusachino/harus-aeron-rs/issues/23)): This compatibility checkpoint fulfills tracking issue [#44](https://github.com/azusachino/harus-aeron-rs/issues/44). Issue #23 retains exclusive authority over release `0.1.0` acceptance criteria and release approval.

---

## 4. Stable Gate Verification Record

All required stable gates have been executed on `main` / `feat/aeron-1.54-s13-compatibility-checkpoint`:

| Gate Command | Result | Scope Covered |
| :--- | :--- | :--- |
| `make check` | **PASS** | `cargo fmt --check`, prettier (Astro/TS/JSON/CSS), ruff (Python scripts), `cargo clippy -D warnings`, `cargo build --workspace`, `cargo test --workspace` (30 cluster, 110 archive, 167 client, 23 driver unit + 37 system tests), pytest suite. |
| `make test-parity` | **PASS** | Ledger audit of 335 current paths: 121 reviewed current-source equivalents (36.1%), 180 done, 88 skip, 67 todo. |
| `make astro-build` | **PASS** | 35 documentation pages built cleanly with zero broken links or type errors. |
| `make interop-rust-smoke` | **PASS** | Multi-process Rust driver/client IPC and cross-driver UDP roundtrip. |
| `make interop-rust-soak` | **PASS** | Multi-process Rust driver/client 1,000 ordered UDP messages. |
| `make interop-smoke` | **PASS** | Java CnC mapping, Java IPC through Rust driver, Java UDP through Rust driver, Rust client IPC through Java driver. |
| `make interop-soak` | **PASS** | 1,000 ordered sequence-tagged UDP messages exchanged between Java client and Rust driver. |
| `RUSTFLAGS="--cfg loom" cargo test -p agrona --lib` | **PASS** | 10 exhaustive concurrency exploration tests (SPSC, MPSC claim/unblock/wrap, broadcast buffer). |

---

## 5. Environment & Provenance

- **Operating System**: macOS 25.1.0 (`Darwin Kernel Version 25.1.0: arm64`)
- **JDK**: Amazon Corretto 25.0.5+10-LTS (`/Users/azusachino/.local/share/mise/installs/java/corretto-25.0.5.10.1`)
- **Rust Toolchain**: `rustc 1.91.0 (dev 2026-09-01)` / stable edition 2024
- **Aeron Target Tag**: `1.54.0`
- **Aeron Target Commit**: `454da1e4e6c7edf3dd6d34878512d3243384992a`
