# Driver/Client System-Test Scope Audit

Task: `harus-aeron-rs:driver-client-system-tests:task-1`.
Scope: map vendor `aeron-system-tests` Java behavioral tests to Rust coverage or an explicit
boundary. This file records the original research baseline; the v0.0.9 row-level outcome is
maintained in [`docs/system-test-parity-boundaries.md`](../docs/system-test-parity-boundaries.md)
and `test/upstream_map.jsonl`.

## 1. Vendor file inventory

`vendor/aeron/aeron-system-tests/src/test/java/io/aeron/*.java`, counted via
`grep -cE '@Test|@ParameterizedTest|@RepeatedTest'` per file.

67 `.java` files total; 2 have zero test methods (`SystemTests.java`, `FlowControlTests.java` —
both are shared helper/base classes, not test classes), leaving **65 files with tests**. The
epic description says "64 files" — off by one from this recount; flagged as a minor discrepancy,
not investigated further since it doesn't change the method-count total.

Method count sums to **319**, matching the epic's stated total exactly.

| Count | File |
|---|---|
| 33 | ChannelValidationTest.java |
| 28 | PubAndSubTest.java |
| 25 | MultiDestinationSubscriptionTest.java |
| 17 | ResponseChannelsTest.java |
| 14 | RejectImageTest.java |
| 14 | MultiDestinationCastTest.java |
| 14 | CounterTest.java |
| 11 | NameReResolutionTest.java |
| 9 | TimestampingSystemTest.java |
| 9 | ExclusivePublicationTest.java |
| 7 | SpySimulatedConnectionTest.java |
| 6 | UntetheredSubscriptionTest.java |
| 6 | StopStartSecondSubscriberTest.java |
| 6 | SessionSpecificPublicationTest.java |
| 6 | ClientContextTest.java |
| 5 | TaggedFlowControlSystemTest.java |
| 5 | SubscriberEndOfStreamTest.java |
| 5 | SessionSpecificSubscriptionTest.java |
| 5 | ResolvedEndpointSystemTest.java |
| 5 | PublicationRevokeTest.java |
| 5 | MinFlowControlSystemTest.java |
| 5 | DataLossAndRecoverySystemTest.java |
| 5 | AsyncResourceTest.java |
| 4 | ChannelEndpointStatusTest.java |
| 4 | BusySocketTest.java |
| 3 | StreamSessionLimitsTest.java |
| 3 | SpecifiedPositionPublicationTest.java |
| 3 | MultipleMulticastsSubscriptionsTest.java |
| 3 | MultiDriverTest.java |
| 3 | MinPositionSubscriptionTest.java |
| 3 | ImageAvailabilityTest.java |
| 3 | CounterReferencesTest.java |
| 3 | ChannelInterfaceTest.java |
| 2 | TwoBufferOfferMessageTest.java |
| 2 | SpySubscriptionTest.java |
| 2 | RemoteEchoTest.java |
| 2 | PongTest.java |
| 2 | MultiSubscriberTest.java |
| 2 | MemoryOrderingTest.java |
| 2 | MdsAndMdcInteractionTest.java |
| 2 | MaxFlowControlStrategySystemTest.java |
| 2 | LifecycleTest.java |
| 2 | BufferClaimMessageTest.java |
| 1 | WildcardPortManagerSystemTest.java, UriValidationTest.java, TermBufferLengthTest.java, SubscriptionReconnectTest.java, RegistrationAndOwnerTest.java, ReentrantClientTest.java, PublishFromArbitraryPositionTest.java, PublicationUnblockTest.java, PrintEnvInfoTest.java, PathologicallySlowConsumerTest.java, MultipathTest.java, MultiGapLossAndRecoverySystemTest.java, MdsEosPositionTest.java, MaxPositionPublicationTest.java, ImageRangeTest.java, GapFillLossTest.java, FragmentedMessageTest.java, ErrorHandlerTest.java, ControlledMessageTest.java, ControlledAssemblyTest.java, ConcurrentPublicationTermRotationRaceTest.java, ClientErrorHandlerTest.java |
| 0 | SystemTests.java, FlowControlTests.java (helper base classes, no `@Test`s) |

## 2. Rust test inventory checked

- `crates/driver/tests/system_tests.rs` (about 1,540 lines, 20 `#[test]`s) — confirmed real: launches an
  actual `MediaDriver`, drives real `ClientConductor`/`DriverProxy` over CnC ring buffers, real
  IPC/UDP roundtrips, a real captured SETUP/DATA exchange, and a real relay-socket NAK/retransmit
  test.
- `crates/driver/tests/flow_control_tests.rs` (63 lines, 2 `#[test]`s) — confirmed real: registers
  multiple streams from one client via the CnC ring, and drives `SendChannelEndpoint`/
  `ReceiveChannelEndpoint` status-message/NAK framing directly (not full driver duty-cycle, but
  real object calls, not stub asserts).
- `crates/driver/tests/properties.rs` (~230 lines, proptest) — real property tests of
  `DriverConductor`/`DriverProxy` command handling (malformed commands, session-id conflicts,
  reliability conflicts, etc.) at the conductor unit level. Not cross-process system tests; noted
  as adjacent coverage where relevant below, not counted as a system-test equivalent.
- `crates/client/tests/properties.rs` — proptest for logbuffer/protocol/URI primitives (unit
  level). Not system-test scope. No `crates/client/tests/system_tests.rs` exists.
- `tutorial/driver/tests/system_tests.rs` and `tutorial/driver/tests/flow_control_tests.rs` —
  **confirmed stubs**, exactly the `tests-assert-noop-as-green` pitfall pattern: each file is one
  `#[test]` whose body is a comment (`// Learner stub for ... tests`) with zero assertions. These
  are explicitly labeled "Learner stub" pointing at the `crates/driver/tests/` files as the real
  reference implementation. **Not counted as coverage.**

**Original baseline total:** 9 Rust system-test methods doing real work (7 + 2). The current
branch has 20 tests in `crates/driver/tests/system_tests.rs` plus 2 endpoint tests in
`flow_control_tests.rs`; the extra bounded scenarios and their ceilings are recorded in the
v0.0.9 boundary matrix.

## 3. Category mapping (sums to 319)

| # | Category | Vendor files (count) | Sum | Status | Rust evidence / boundary reasoning |
|---|---|---|---|---|---|
| A | Basic pub/sub & messaging (roundtrip, exclusive pub, buffer-claim, fragmented/controlled assembly, term geometry, position/ordering edge cases) | PubAndSubTest(28), ExclusivePublicationTest(9), SpecifiedPositionPublicationTest(3), TwoBufferOfferMessageTest(2), BufferClaimMessageTest(2), MemoryOrderingTest(2), FragmentedMessageTest(1), ControlledMessageTest(1), ControlledAssemblyTest(1), PublishFromArbitraryPositionTest(1), TermBufferLengthTest(1), MaxPositionPublicationTest(1), ConcurrentPublicationTermRotationRaceTest(1), PathologicallySlowConsumerTest(1) | 54 | **PARTIAL — core COVERED, depth GAP** | Core roundtrip real in `ipc_publication_to_subscription_roundtrip_through_the_driver`, `udp_publication_to_subscription_roundtrip_through_two_drivers`, `client_registers_ipc_pub_and_sub_through_the_cnc`. No Rust system test for exclusive publications, buffer-claim (try-claim), fragmented/controlled assembly, or term-rotation races → GAP subset ~26 methods. |
| B | Reconnection / client lifecycle / async resources | StopStartSecondSubscriberTest(6), ClientContextTest(6), AsyncResourceTest(5), ImageAvailabilityTest(3), SubscriberEndOfStreamTest(5), StreamSessionLimitsTest(3), MultiSubscriberTest(2), LifecycleTest(2), SubscriptionReconnectTest(1), RegistrationAndOwnerTest(1), ReentrantClientTest(1), PublicationUnblockTest(1), ImageRangeTest(1) | 37 | **GAP** | No Rust system test exercises subscription reconnect, image lifecycle/end-of-stream, client context edge cases, or publication unblock. |
| C | Multi-destination cast/subscription (MDC/MDS, unicast-based) | MultiDestinationSubscriptionTest(25), MultiDestinationCastTest(14), MdsAndMdcInteractionTest(2), MdsEosPositionTest(1), MultipathTest(1) | 43 | **GAP (not out-of-scope — feature exists)** | Driver has real add/remove-destination plumbing: `ADD_DESTINATION`/`REMOVE_DESTINATION`/`ADD_RCV_DESTINATION`/`REMOVE_RCV_DESTINATION` commands wired conductor↔client (`crates/client/src/driver_proxy.rs`, `crates/driver/src/conductor.rs`), `NetworkPublication::add_destination`/`remove_destination` with its own unit test (`crates/driver/src/network_publication.rs:183,193,569`), and `ControlMode::{Dynamic,Manual}` parsing (`crates/driver/src/media/control_mode.rs`). No system-level test exercises any of it. **This corrects the epic background's assumption that MDC/MDS are a boundary — they are an implemented-but-untested GAP.** |
| C2 | True multicast (UDP multicast group) | MultipleMulticastsSubscriptionsTest(3) | 3 | **OUT-OF-SCOPE** | No multicast socket join/`set_multicast_*` calls found anywhere in `crates/driver/src` — only a NAK backoff constant is *named* "multicast". Real multicast transport isn't implemented. |
| D | Flow control strategies (max/min/tagged) | TaggedFlowControlSystemTest(5), MinFlowControlSystemTest(5), MaxFlowControlStrategySystemTest(2), MinPositionSubscriptionTest(3) | 15 | **GAP** | `crates/driver/src/flow_control.rs`, `multicast_flow_control.rs` (891 lines), `congestion_control.rs` implement strategy logic with unit tests, but no system test drives strategy selection end-to-end through a running driver. `flow_control_tests.rs` (despite the name) only covers raw SM/NAK framing at the channel-endpoint level, not strategy selection. |
| E | Loss / NAK / retransmit / recovery | DataLossAndRecoverySystemTest(5), MultiGapLossAndRecoverySystemTest(1), GapFillLossTest(1) | 7 | **PARTIAL — single-gap COVERED, multi-gap GAP** | `udp_receiver_gap_generates_nak_and_sender_retransmits` is a real single-gap NAK→retransmit test via relay socket. No Rust test for multiple simultaneous gaps or gap-fill-specific behavior (~2 methods GAP). |
| F | Spy subscriptions | SpySimulatedConnectionTest(7), SpySubscriptionTest(2) | 9 | **PARTIAL — basic COVERED, edge cases GAP** | `network_spy_subscription_reads_the_publication_log` is real. No test for simulated-connection spy edge cases (~7 methods GAP). |
| G | Session-specific pub/sub | SessionSpecificPublicationTest(6), SessionSpecificSubscriptionTest(5) | 11 | **GAP** | No Rust system test filters/targets by explicit session id end-to-end (conductor has session-conflict unit tests in `properties.rs`/`conductor.rs`, but not a system-level session-specific pub/sub roundtrip). |
| H | Untethered subscriptions | UntetheredSubscriptionTest(6) | 6 | **GAP (feature implemented + unit-tested, no system test)** | Real state machine at `crates/driver/src/conductor.rs:2082` ("mirrors `IpcPublication.checkUntetheredSubscriptions`") with its own passing unit test `untethered_ipc_subscription_leaves_flow_control_and_rejoins` (line 4617). No `crates/driver/tests/` system test exercises it. |
| I | Channel / URI validation | ChannelValidationTest(33), ChannelInterfaceTest(3), ChannelEndpointStatusTest(4), ResolvedEndpointSystemTest(5), WildcardPortManagerSystemTest(1), BusySocketTest(4), UriValidationTest(1) | 51 | **GAP** | `crates/client/tests/properties.rs` has proptests for `ChannelUri`/`ChannelUriStringBuilder` syntax (unit-level parse/roundtrip only). No system-level test proves the driver rejects/accepts channels the way vendor's `ChannelValidationTest` (33 methods — the single largest file) does. This is the single biggest GAP file. |
| J | Counters | CounterTest(14), CounterReferencesTest(3) | 17 | **GAP** | Ties to the known parity-register finding that the counters layout is under-padded/not byte-compatible with vendor. No Rust system test exercises counter registration/lookup/reference-counting end-to-end. |
| K | Name resolution / re-resolution | NameReResolutionTest(11) | 11 | **GAP, ambiguous depth** | `crates/driver/src/media/name_resolver.rs` (383 lines) implements `DriverNameResolverCache` and `TimeTrackingNameResolver` — real infra. Did not trace whether re-resolution is actually triggered on a live connection (vs. just cache lookup); flagging as ambiguous rather than asserting full wiring. No system test either way. |
| L | Response channels | ResponseChannelsTest(17) | 17 | **OUT-OF-SCOPE** | `ControlMode::Response` exists only as an enum variant; the one place it's referenced in `conductor.rs:3022` actively **rejects** `control-mode=response` on destinations (`"destinations may not specify control-mode=response"`). The feature is recognized but explicitly unimplemented/refused, not partially built. |
| M | Timestamping | TimestampingSystemTest(9) | 9 | **GAP, ambiguous depth** | `CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME` is parsed and stored on `UdpChannel` (`media/udp_channel.rs`), and `client/src/timestamp_util.rs` exists, but did not trace whether the reserved-value timestamp is actually written into frames at receive time. Flagging as ambiguous; no system test either way. |
| N | Reject image / publication revoke | RejectImageTest(14), PublicationRevokeTest(5) | 19 | **OUT-OF-SCOPE** | Client-side `revoke` flag exists on the remove-publication command (`crates/client/src/command.rs:302`, `driver_proxy.rs:103`) but there is zero handling of "revoke" anywhere in `crates/driver/src/*.rs` — the flag is threaded through and dropped. `RejectImageTest`'s corrupt/invalid-image-rejection behavior has no matching driver feature at all. |
| O | Misc / internal (echo, pong, env info, multi-driver, error handlers) | RemoteEchoTest(2), PongTest(2), MultiDriverTest(3), PrintEnvInfoTest(1), ErrorHandlerTest(1), ClientErrorHandlerTest(1) | 10 | **GAP (low priority)** | No Rust equivalents. `PrintEnvInfoTest` is a diagnostic dump, not really behavioral — lowest priority in this bucket. |

**Total check:** 54+37+43+3+15+7+9+11+6+51+17+11+17+9+19+10 = **319**. ✓

## 4. Status tallies

- COVERED (fully): 0 categories at full-category granularity — the two "PARTIAL" categories (A,
  E, F) have real coverage of their core/common-case methods but not their full method count.
  Counting only methods with a direct Rust equivalent: **basic roundtrip (~28 of 54), single-gap
  NAK/retransmit (~5 of 7), basic spy (~2 of 9) ≈ 35 methods genuinely covered.**
- GAP (in-scope, no test yet): B(37) + C(43) + D(15) + G(11) + H(6) + I(51) + J(17) + K(11) + O(10)
  + A-depth(~26) + E-depth(~2) + F-depth(~7) ≈ **236 methods**
- OUT-OF-SCOPE (explicit boundary, feature not implemented): C2(3) + L(17) + N(19) = **39 methods**
