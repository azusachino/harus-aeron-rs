# Aeron 1.54 Status Message position bounds

Compared against tag `1.54.0`, commit `454da1e4e6c7edf3dd6d34878512d3243384992a`.
In Aeron 1.54.0, `aeron-driver/src/main/java/io/aeron/driver/media/SendChannelEndpoint.java`
validates every incoming Status Message frame via `StatusMessageFlyweight.isValid`
before queueing: consumption term offset must be non-negative, strictly less than
the term length, and 32-byte frame aligned; receiver window length must be non-negative
and bounded by half the term length (`termLength >> 1`).

On `NetworkPublication.java`, `isValidStatusMessage` enforces sender-relative position
bounds:
```java
final long smPos = computePosition(
    msg.consumptionTermId(),
    msg.consumptionTermOffset(),
    positionBitsToShift,
    initialTermId);

final long sndPos = senderPosition.get();
final int halfTerm = termBufferLength >> 1;
return smPos >= (sndPos - halfTerm) && smPos <= (sndPos + termBufferLength + halfTerm);
```
An SM whose consumption position falls outside `[sndPos - halfTerm, sndPos + termLength + halfTerm]`
is dropped immediately and cannot advance or manipulate the publication's sender limit.
Messages carrying `SEND_SETUP_FLAG` are exempt from sender-relative position gating:
they elicit a re-transmitted SETUP frame from the sender to re-synchronize the receiver
without mutating the publication's flow-control limit.

In Rust:
1. `StatusMessageFlyweight::is_valid(term_length)` in `crates/client/src/protocol.rs`
   mirrors upstream frame-level geometric validity and is tested against all four
   upstream `StatusMessageFlyweightTest.java` parameter cases.
2. `SendChannelEndpoint::poll_control` rejects invalid frame geometry before queueing
   messages onto the duty cycle queue.
3. `NetworkPublication::is_valid_status_message` mirrors upstream position bounds,
   correctly handling 32-bit term ID rollover/wrap around `i32::MAX` -> `i32::MIN`.
4. `NetworkPublication::poll_control` drops out-of-bounds messages while preserving
   special handling for `SEND_SETUP_FLAG` setup elicitation and ensuring sender
   limits remain monotonic.
5. Integration test `status_message_position_bounds_protect_sender_limit_under_live_traffic`
   in `crates/driver/tests/system_tests.rs` verifies that raw UDP injection of out-of-bounds
   Status Messages cannot move the publication limit, valid in-bounds SMs advance the limit,
   and setup-eliciting SMs trigger setup retransmission without corrupting state.
