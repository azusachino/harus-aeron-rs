# Aeron 1.54 Min flow control receiver liveness and timeout

Compared against tag `1.54.0`, commit `454da1e4e6c7edf3dd6d34878512d3243384992a`.
In Aeron 1.54.0:
1. `aeron-driver/src/main/java/io/aeron/driver/MinMulticastFlowControl.java` and `AbstractMinMulticastFlowControl.java` track all remote receivers by receiver ID, refreshing the active window and timestamp on every received Status Message.
2. In `AbstractMinMulticastFlowControl.onIdle`, receivers whose last Status Message is older than `receiverTimeoutNs` or that explicitly flagged `END_OF_STREAM_FLAG` are evicted. When the slowest receiver times out, `onIdle` recalculates the minimum window across the surviving active receivers, unblocking the sender from being indefinitely constrained by a dead or partitioned receiver.
3. Upstream test `shouldRemoveOnlyTimedOutReceiversWhenTheLastReceiverHasAlsoTimedOut` verifies that when multiple receivers time out concurrently, the swap-and-truncate eviction pass removes only the timed-out receivers without corrupting surviving active receiver state.
4. On `NetworkPublication`, `livenessTracker` tracks active remote receiver IDs and updates `isConnected` in the shared log metadata buffer based on `hasSubscribers() = hasReceivers && flowControl.hasRequiredReceivers()`. Client publications observe real connection state changes dynamically without polling.

In Rust:
1. `MinMulticastFlowControl` and `TaggedMulticastFlowControl` in `crates/driver/src/multicast_flow_control.rs` implement `has_required_receivers()` and `receiver_timeout_ns()`.
2. Ported unit test `should_remove_only_timed_out_receivers_when_the_last_receiver_has_also_timed_out` to `crates/driver/src/multicast_flow_control.rs`.
3. Integrated `ReceiverLivenessTracker` into `NetworkPublication`:
   - Records live receiver IDs on incoming Status Messages and immediately drops on `END_OF_STREAM_FLAG`.
   - Sweeps timed-out receivers on idle and updates `set_is_connected(&meta_data_buffer, has_subscribers)`.
4. Live integration tests in `crates/driver/tests/system_tests.rs`:
   - `min_flow_control_expires_timed_out_slow_receiver_and_advances_sender_limit`: verifies that under live UDP traffic, a slow receiver times out past its configured timeout (`t:50ms`), unblocking the sender limit to the faster active receiver's window.
   - `network_publication_connection_status_tracks_receiver_liveness_and_required_group_size`: verifies that `publication.is_connected()` starts false, transitions to true on first Status Message, drops to false when the receiver times out or sends EOS, and reconnects when active traffic resumes.
