# Aeron 1.54 Untethered Subscription DRAINING Cleanup

Compared against tag `1.54.0`, commit `454da1e4e6c7edf3dd6d34878512d3243384992a`.

## Upstream Aeron 1.54.0 Semantics

In Aeron 1.54.0:
1. `UntetheredSubscriptionTest.java`:
   - Adds `imageShouldCompleteDrainStateEvenIfThereAreNonActiveUntetheredSubscriptions`: when a publication enters `DRAINING` state (e.g. publisher client calls `close()` / sends `REMOVE_PUBLICATION`), non-active untethered subscriptions (`LINGER` or `RESTING`) do not prevent the publication/image from completing drain state.
   - Specifically asserts `awaitCounterClosed(untetheredSubPosCounterId)`: the untethered subscriber position counter is closed and freed once the drain/linger state machine finishes, rather than lingering indefinitely.
2. In `IpcPublication.onTimeEvent` and `PublicationImage.onTimeEvent`:
   - In `ACTIVE` state: calls `checkUntetheredSubscriptions(timeNs, conductor, true)` (`allowRejoin = true`). Lagging untethered subscribers fall behind `untetheredWindowLimit` into `LINGER`, and after `untetheredLingerTimeoutNs` transition to `RESTING` to eventually rejoin as `ACTIVE`.
   - In `DRAINING` state: calls `checkUntetheredSubscriptions(timeNs, conductor, false)` (`allowRejoin = false`).
     - Any untethered subscription timing out in `LINGER` does *not* transition to `RESTING`; its position counter is closed (`position.close()`) and it is immediately removed from untethered tracking.
     - Any untethered subscription in `RESTING` does *not* wait or rejoin; its position counter is closed and it is immediately removed.
   - Once all tethered subscribers have caught up to `producerPosition` (or `rebuildPosition`), `isDrained()` evaluates to `true`, allowing the resource to complete drain, transition to `LINGER`/`DONE`, and be released.

## Rust Implementation

1. **Untethered State Machine**:
   - `crates/driver/src/publication_image.rs`:
     - Added `UntetheredSubscriptionStateKind` (`Active`, `Linger`, `Resting`).
     - Added `ImageLink` tracking `subscriber_position_id`, `tethered`, `rejoin`, timeouts, and state.
     - Added `all_tethered_drained` tracking to `is_drained()`.
   - `crates/driver/src/conductor/entries.rs`:
     - Added `PublicationState` (`Active`, `Draining`, `Linger`, `Closed`) and timestamps to `PublicationEntry`.
     - Parsed `rejoin`, `untethered-window-limit-timeout`, `untethered-linger-timeout`, and `untethered-resting-timeout` parameters in `SubscriptionOptions`.
     - Added `IpcSubscriptionState` tracking per-link timeouts and states.
2. **Conductor Lifecycle & Draining Sweeps**:
   - `crates/driver/src/conductor/mod.rs`:
     - `Command::RemovePublication`: when removing an IPC publication with attached subscribers, sets `PublicationState::Draining` and publishes end-of-stream position rather than dropping the shared log buffer abruptly.
     - `sweep_ipc_subscriptions`: computes `window_limit = (max_consumer_position - window_length) + (window_length / 4)`. When the publication is `Draining`, enforces `allow_rejoin = false`. When `LINGER` expires or for subscriptions in `RESTING`, closes the untethered subscriber position counter and removes the subscription link.
     - `sweep_draining_publications`: evaluates whether tethered subscribers have caught up to `producer_position`. Once drained, cleans up the publication, frees the publisher limit counter, and cleans up log buffers.
     - `sweep_inactive_images`: performs untethered checks on network images, enforcing `allow_rejoin = false` while `PublicationImageState::Draining`, releasing untethered position counters on expiration, and gating `is_drained()` on tethered subscribers.
3. **Client Counter Visibility**:
   - `crates/client/src/image.rs` & `crates/client/src/conductor.rs`:
     - Added `subscriber_position_id` getter and storage on `Image`, matching upstream `Image.subscriberPositionId()`.

## Verification

1. **Unit & Conductor Tests**:
   - `crates/driver/src/conductor/tests/ipc.rs::untethered_subscription_closed_on_draining_cleanup`: unit-level conductor test verifying that an untethered subscriber falling behind into `Linger` has its subscriber position counter freed when the publication is removed, and the publication completes draining once the tethered subscriber catches up.
2. **Live Integration Tests**:
   - `crates/driver/tests/system_tests.rs::image_should_complete_drain_state_even_if_there_are_non_active_untethered_subscriptions`: live client/driver system test matching upstream's `imageShouldCompleteDrainStateEvenIfThereAreNonActiveUntetheredSubscriptions`. A live publisher offers 32k messages, untethered subscriber falls behind into Linger, tethered subscriber consumes to EOS, publication is closed, and the test asserts `RECORD_ALLOCATED != counter_state` and counter removal from driver.
3. **Parity Ledger**:
   - Updated `test/upstream_map.jsonl` Line 263 (`UntetheredSubscriptionTest.java`) from `behavior-unverified` to `verified-equivalent` (114/335 reviewed equivalents, 34.0%).
