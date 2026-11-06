# Aeron 1.54 Client APIs & Driver End-to-End Verification

Compared against tag `1.54.0`, commit `454da1e4e6c7edf3dd6d34878512d3243384992a`.

## Upstream Aeron Client APIs & System Test Semantics

In Aeron 1.54:
1. **Buffer Claim Commit / Abort (`BufferClaimMessageTest.java`)**:
   - Zero-copy claims are reserved via `tryClaim(length, bufferClaim)`.
   - Calling `bufferClaim.abort()` marks the claimed region as padding (`HDR_TYPE_PAD`) and commits the frame length ordered, allowing the publication log to progress while subscribers skip the aborted space as padding.
   - Calling `bufferClaim.reservedValue(val)` writes a 64-bit value into the reserved space at the end of the data frame header (`DATA_RESERVED_VALUE_OFFSET`), which subscribers receive intact via `header.reservedValue()`.
   - Verified across both IPC and unicast UDP transport paths.
2. **Controlled Polling (`ControlledMessageTest.java`)**:
   - `controlledPoll(handler, limit)` allows the subscriber callback to direct consumption per-fragment:
     - `Abort`: aborts the current poll, leaves the fragment uncommitted at the current position, and yields 0 consumed fragments. Subsequent polls redeliver the aborted fragment.
     - `Break`: stops the current poll immediately while committing the current fragment.
     - `Continue`: consumes the fragment and continues to the next fragment up to the limit.
3. **Controlled Fragment Reassembly (`ControlledAssemblyTest.java`)**:
   - `ControlledFragmentAssembler` reassembles multi-fragment messages before passing them to a controlled handler.
   - When the delegate returns `Abort` on the final fragment of an assembled message, the assembler restores the buffer builder limit to its pre-fragment state and returns `Abort` to the poller.
   - On subsequent polls, the final fragment is redelivered and appended to the existing partial assembly without losing earlier fragments.
4. **Concurrent Publication Term Rotation Race (`ConcurrentPublicationTermRotationRaceTest.java`)**:
   - Multiple threads concurrently publishing via `offer()` and `tryClaim()` across term boundaries on a shared `ConcurrentPublication`.
   - Driver services IPC publication limit updates and cleans term buffers behind the subscriber (`cleanBufferTo`).
5. **Subscription Round-Robin Alternation (`SubscriptionTest.java`)**:
   - When polling across multiple images with an exhausted fragment limit (e.g., `fragmentLimit = 1`), the starting image index advances on each poll, alternating evenly between images.

## Rust Implementation

1. **`ControlledFragmentAssembler`**:
   - In `crates/client/src/fragment_assembler.rs`:
     - Added `ControlledFragmentAssembler` implementing `on_fragment` delegation with `ControlledPollAction`.
     - Direct unfragmented passthrough for complete messages.
     - Builder state tracking across session IDs, saving `limit_before` on the final fragment and restoring it on `Abort`.
     - Added comprehensive unit tests for unfragmented delegation, multi-fragment reassembly, abort retry restoration, and session buffer cleanup.
2. **`ClientConductor` Exclusive Publication Support**:
   - In `crates/client/src/conductor.rs`:
     - Added `exclusive_publications: HashMap<i64, ExclusivePublication>`.
     - Added `exclusive_publication(&self)` and `exclusive_publication_mut(&mut self)`.
     - Integrated `ExclusivePublication` into `remove_publication`, `publication_count`, and `close`.
     - Correctly routes `ON_EXCLUSIVE_PUBLICATION_READY` driver events to construct and register `ExclusivePublication`.
3. **Driver Default Frame Header Initialization Fix**:
   - In `crates/driver/src/conductor/mod.rs`:
     - Stamped `header.set_flags(BEGIN_AND_END_FLAGS)` (`0xC0` = `UNFRAGMENTED`) into the default data frame header stored in the log metadata buffer.
     - Previously flags was uninitialized (`0`), which caused `HeaderWriter` to stamp `0` on unfragmented messages, causing fragment assemblers to discard them as missing-begin fragments.
4. **Driver IPC Term Buffer Cleaning (`cleanBufferTo`)**:
   - In `crates/driver/src/conductor/mod.rs`:
     - Added `clean_position: i64` to `PublicationEntry`.
     - In `update_ipc_publication_limit`, ported Aeron's `IpcPublication.cleanBufferTo`: zeroes dirty term buffer regions behind advancing subscribers using `set_memory(term_offset + 8, length - 8, 0)` followed by ordered zeroing of the frame header length word `put_i64_ordered(term_offset, 0)`.
     - Eliminates stale frame reads when term buffers lap after 3 partitions.
5. **Subscription Image Alternation Unit Tests**:
   - In `crates/client/src/subscription.rs`:
     - Added unit tests `should_round_robin_starting_image_when_polling_with_exhausted_fragment_limit` and `should_round_robin_starting_image_when_controlled_polling_with_exhausted_fragment_limit`, verifying alternating image consumption with budget limit of 1.

## Verification

1. **Live Driver Integration Tests (`crates/driver/tests/system_tests.rs`)**:
   - `buffer_claim_should_receive_published_message_with_interleaved_abort_ipc`: message 1 offered, message 2 claimed, message 3 offered, claim 2 aborted; subscriber receives exactly 2 messages in order (1 and 3), aborted claim skipped as padding.
   - `buffer_claim_should_receive_published_message_with_interleaved_abort_udp`: same interleaved abort sequence verified over live UDP transport.
   - `buffer_claim_should_transfer_reserved_value_ipc`: claim sets 64-bit reserved value (`0x1234_5678_9ABC_DEF0`), commits, subscriber receives frame with identical reserved value.
   - `buffer_claim_should_transfer_reserved_value_udp`: reserved value transfer verified over live UDP transport.
   - `controlled_message_poll_with_abort_break_and_continue`: 4 messages polled with handler returning Abort on message 3 (first attempt), Continue on retry, and Break on message 4; all 4 delivered intact without loss.
   - `controlled_fragment_assembler_with_interleaved_abort_and_retry`: fragmented message (4 chunks) assembled via `ControlledFragmentAssembler`; handler aborts first assembled delivery, builder limit is restored, subsequent poll retries and completes delivery intact.
   - `concurrent_publication_term_rotation_race_with_offer_and_try_claim`: 3 threads concurrently publishing 450 messages across multiple 64KB term rotations (using both `offer` and `try_claim`), while consumer and driver service subscriber polling and term buffer cleaning concurrently. All 450 messages received in per-thread sequential order.
   - `exclusive_publication_try_claim_commit_and_abort`: single-writer `ExclusivePublication` with interleaved `offer`, `try_claim().commit()`, and `try_claim().abort()` across term boundaries.
2. **Gates**:
   - `cargo test -p aeron-client`: 167 unit tests + 8 property tests passed.
   - `cargo test -p aeron-driver --test system_tests`: 37 system tests passed.
   - `make test-parity`: 335 total upstream paths reconciled; 5 parity ledger entries updated to `verified-equivalent`.
