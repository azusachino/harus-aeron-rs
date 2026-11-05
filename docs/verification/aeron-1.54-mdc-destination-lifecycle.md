# Aeron 1.54 Multi-Destination Cast (MDC) Lifecycle and Socket Lifecycle

Compared against tag `1.54.0`, commit `454da1e4e6c7edf3dd6d34878512d3243384992a`.

In Aeron 1.54.0:
1. `MultiDestinationCastTest.java` asserts that manual and dynamic MDC destinations can be added, removed, and re-added during active streaming without disrupting surviving destinations or leaving leaked state.
2. When a destination is added to a publication, setup frames are immediately re-enabled so newly joined receivers learn stream parameters without waiting for idle heartbeat intervals.
3. When a destination is removed from a publication, outbound traffic (data, setup, heartbeats, and retransmits) immediately ceases to that endpoint address. Re-adding the destination resumes traffic delivery.
4. Publications initialized in manual MDC mode (`control-mode=manual`) with endpoint port 0 do not transmit to invalid destination addresses (e.g. `0.0.0.0:0`) prior to explicit destinations being added.
5. In `SocketLifecycleTest.java`:
   - Repeated closing and reopening of publications and subscriptions with the same channel URI succeeds cleanly across IPC, unicast UDP, and MDC configurations without leaking resources or reporting false errors.
   - Subscriptions adding and removing receive destinations operate with proper isolation: removing a destination from one subscription frees its endpoint and allows subsequent or alternate subscriptions to reuse the destination without conflict.

In Rust:
1. `NetworkPublication` in `crates/driver/src/network_publication.rs`:
   - Destination list initialization skips port 0 endpoints (`ANY_IPV4`), ensuring manual MDC publications begin with an empty destination set rather than attempting datagram transmission to `0.0.0.0:0`.
   - `add_destination` resets `setup_sent = false` so that re-added destinations immediately receive an initial SETUP frame on the subsequent duty cycle.
   - `remove_destination` removes the target endpoint from the publication's active `destinations` slice.
2. `DriverConductor` in `crates/driver/src/conductor.rs`:
   - Manages publication and subscription destination additions, removals, and channel alias tracking.
   - Provides clean resource reclamation when publications and subscriptions close.
3. Unit and system test verification:
   - `crates/driver/src/network_publication.rs`: `should_initialize_empty_destinations_when_port_is_zero` and `should_add_and_remove_dynamic_destinations`.
   - `crates/driver/tests/system_tests.rs`:
     - `manual_mdc_destination_remove_stops_traffic_and_readd_resumes`: asserts live traffic delivery to two destinations, complete cessation of data to the removed destination while the surviving destination continues receiving, and successful resumption of delivery to both destinations once re-added.
     - `manual_mdc_publication_initializes_with_empty_destinations_when_port_is_zero`: verifies port 0 manual MDC initialization, dynamic destination addition, and data delivery.
     - `supports_closing_and_reopening_publication_with_same_channel`: verifies rapid close and reopen cycles across IPC, unicast UDP, and manual MDC channels.
     - `subscription_receive_destination_lifecycle_and_isolation`: verifies add, remove, and reuse lifecycles for subscription receive destinations.
4. Upstream map updates:
   - Line 228 (`MultiDestinationCastTest.java`): updated with manual MDC destination removal and re-addition verification.
   - Line 332 (`SocketLifecycleTest.java`): promoted to `verified-equivalent` (113/335 reviewed equivalents, 33.7%).
