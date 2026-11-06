# Aeron 1.54 Bounded Feature & Cluster Follow-Up Scope (S12)

Compared against tag `1.54.0`, commit `454da1e4e6c7edf3dd6d34878512d3243384992a`.
Tracking issue: [#44](https://github.com/azusachino/harus-aeron-rs/issues/44), coordinated with [#8](https://github.com/azusachino/harus-aeron-rs/issues/8), [#10](https://github.com/azusachino/harus-aeron-rs/issues/10), [#12](https://github.com/azusachino/harus-aeron-rs/issues/12), [#18](https://github.com/azusachino/harus-aeron-rs/issues/18), and [#19](https://github.com/azusachino/harus-aeron-rs/issues/19).

## Outcome and Purpose

Slice S12 establishes source-backed implementation evidence and concrete follow-up boundaries for features introduced or modified in Aeron 1.51 through 1.54. It ensures:
1. Implemented features and algorithms have targeted regression tests in Rust.
2. Verified equivalent upstream tests are reconciled in `test/upstream_map.jsonl`.
3. Larger runtime capabilities (persistent subscriptions, live driver gossip, NativeResourceAgent, full Aeron Cluster wire parity) remain visibly open as `status: "todo"` with honest dispositions rather than converted into parity skips.

---

## Family Dispositions

### 1. Persistent Subscriptions (`PersistentSubscriptionContextTest.java`, `PersistentSubscriptionTest.java`)

- **Upstream semantics**: Aeron 1.51 introduced `AeronArchive.addPersistentSubscription` and `PersistentSubscriptionContext` to orchestrate automatic transition from historic archive playback to a live stream without gaps.
- **Rust boundary**:
  - The Aeron Archive control protocol schema (version 14, semantic version 1.12.0) contains no dedicated message templates for persistent subscriptions; orchestration is entirely handled within the client SDK.
  - Rust `harus-aeron-rs` implements basic Archive client recording and replay control, but does not provide the persistent subscription state machine.
- **Disposition**: Kept visibly open as `status: "todo"` with `audit_disposition: "unsupported-runtime-behavior"`. Follow-up is tracked under [#8](https://github.com/azusachino/harus-aeron-rs/issues/8) (Client) and Archive client roadmap.

### 2. Live Name Resolution & Driver Gossip

- **Upstream semantics**:
  - `DefaultNameResolverTest`: Basic hostname to IP resolution.
  - `DriverNameResolverCacheTest`: Expiring cache of name-to-address resolutions.
  - `TimeTrackingNameResolverTest`: Wrapper recording resolver duty-cycle durations and lifecycle forwarding (`init`, `do_work`, `close`).
  - `DriverNameResolverTest` & `DriverNameResolverSystemTest`: Multi-driver neighbor discovery and name gossip protocol over UDP (`ResolutionEntryFlyweight`).
- **Rust boundary**:
  - `crates/driver/src/media/name_resolver.rs` implements `DriverNameResolverCache`, `DefaultNameResolver`, and `TimeTrackingNameResolver`.
  - `TimeTrackingNameResolver` forwards lifecycle calls and measures duration via `NanoClock` and `DutyCycleTracker`. Verified by `time_tracking_resolver_forwards_lifecycle_and_measures_duration`. Updated to `verified-equivalent`.
  - The live UDP gossip protocol (`DriverNameResolverAgent`) is not implemented.
- **Disposition**: Unit-tested primitives marked `done` / `verified-equivalent`; driver gossip protocol kept open as `status: "todo"` under [#12](https://github.com/azusachino/harus-aeron-rs/issues/12).

### 3. UDP Channel Configuration & Timestamp Writes (`UdpChannelTest.java`)

- **Upstream semantics**:
  - Added `so-tos` URI parameter to configure IP TOS / DiffServ byte (`0..255`), rejecting out-of-range values.
  - Added receiver window length `rcv-wnd`.
  - Added timestamp write offsets (`channel-snd-ts-offset`, `channel-rcv-ts-offset`).
- **Rust boundary**:
  - In `crates/driver/src/media/udp_channel.rs`:
    - Added `SOCKET_TOS_PARAM_NAME` (`"so-tos"`), `SOCKET_TOS_DEFAULT` (`0`), and `socket_tos: u8` field.
    - Added `parse_socket_tos` validating `0..255` range and rejecting invalid inputs.
    - Added `socket_tos()` and `receiver_window_length()` accessors.
    - Added unit tests `should_parse_socket_tos`, `should_reject_invalid_socket_tos`, and `should_parse_receiver_window`.
  - Updated `UdpChannelTest.java` in `test/upstream_map.jsonl` to `verified-equivalent`.
- **Disposition**: Implemented and verified in `crates/driver/src/media/udp_channel.rs`. Low-level OS socket option setting and hardware timestamping remain platform-specific driver options.

### 4. CPU Topology & Thread Affinity

- **Upstream semantics**: 9 test suites under `io.aeron.cpu.topology.*` validate Linux `/sys/devices/system/cpu` sysfs parsing (NUMA die locality, L3 cache groups, cpuset v2) and thread affinity pinning.
- **Rust boundary**:
  - Diagnostic inspection of Linux sysfs topology and JVM thread naming conventions are specific to Java harness environments.
  - Rust driver and client provide cross-platform abstractions; core message delivery does not depend on sysfs parser assertions.
- **Disposition**: Retained as `java-diagnostics-boundary` and `unsupported-runtime-behavior` in `test/upstream_map.jsonl`.

### 5. NativeResourceAgent Scheduling (`NativeResourceAgentTest.java`, `MediaDriverContextTest.java`)

- **Upstream semantics**: In 1.51–1.52, upstream moved log buffer file allocation (`allocate_log_buffers`) and unmapping out of the `DriverConductor` thread into an asynchronous `NativeResourceAgent` with an internal command queue to prevent conductor duty cycle stalls.
- **Rust boundary**:
  - In `harus-aeron-rs`, `DriverConductor` allocates log buffers synchronously via `allocate_log_buffers` using `mmap` or in-memory buffers.
  - No measurable latency spike or conductor stall has been observed under current workload benchmarks.
  - Upstream plan requires a demonstrated stall/throughput bottleneck before introducing thread/agent complexity.
- **Disposition**: Kept open under [#10](https://github.com/azusachino/harus-aeron-rs/issues/10) (`driver: DriverConductor + MediaDriver lifecycle`).

### 6. Cluster Recovery, Service Lifecycle & State Machine

- **Upstream semantics**:
  - `RecordingLog`: Tracks leadership terms and snapshots. `snapshot_order` sorts snapshots by `(leadership_term_id, log_position, service_id)` while filtering invalid entries.
  - Multi-archive isolation: Counter allocations and status lookups (`RecordingPos`) take `archive_id` to isolate co-located Archive instances while preserving `NULL_VALUE` (-1) as legacy wildcard.
  - Subscription image alternation: Round-robin starting image index advances across exhausted poll budgets.
  - Cluster runtime: `EgressPublisher`, `ClusterUncommittedState`, `RecordingLogValidator`, `SessionManager`, `ServiceProxy`.
- **Rust boundary**:
  - `crates/cluster/src/recording_log.rs`: `snapshot_order` verified by `should_order_snapshots_by_term_position_and_service_id_and_filter_invalids`.
  - `crates/archive/src/storage/recording_pos.rs`: Multi-archive isolation and wildcard lookups verified by `multi_archive_isolation_and_wildcard_lookups`.
  - `crates/client/src/subscription.rs`: Fair modulo starting image round-robin verified by `should_round_robin_starting_image_when_polling_with_exhausted_fragment_limit`.
  - Wire-compatible cluster client (`AeronCluster`), service container, and distributed log recovery remain open follow-up scope.
- **Disposition**: Core algorithms verified; full cluster wire protocol remains open under [#18](https://github.com/azusachino/harus-aeron-rs/issues/18) and [#19](https://github.com/azusachino/harus-aeron-rs/issues/19).

---

## Verification Evidence

1. **Cluster Snapshot Ordering**:
   - `cargo test --manifest-path vendor/harus-aeron-rs/Cargo.toml -p aeron-cluster` (30 unit tests pass, including `should_order_snapshots_by_term_position_and_service_id_and_filter_invalids`).
2. **Multi-Archive Isolation & Wildcard Lookups**:
   - `cargo test --manifest-path vendor/harus-aeron-rs/Cargo.toml -p aeron-archive recording_pos` (passes, including `multi_archive_isolation_and_wildcard_lookups`).
3. **UDP Channel `so-tos` & `rcv-wnd` Validation**:
   - `cargo test --manifest-path vendor/harus-aeron-rs/Cargo.toml -p aeron-driver udp_channel` (23 unit tests pass, including `should_parse_socket_tos`, `should_reject_invalid_socket_tos`, and `should_parse_receiver_window`).
4. **TimeTrackingNameResolver Lifecycle**:
   - `crates/driver/src/media/name_resolver.rs::tests::time_tracking_resolver_forwards_lifecycle_and_measures_duration` verifies `init`, `do_work`, and `close`.
5. **Parity Ledger**:
   - `make -C vendor/harus-aeron-rs test-parity` passes with 121 reviewed current-source equivalents (36.1%).
