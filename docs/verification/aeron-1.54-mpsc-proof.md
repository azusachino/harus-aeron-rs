# Aeron 1.54 MPSC Ring Buffer Protocol & Concurrency Proof

Compared against tag `1.54.0`, commit `454da1e4e6c7edf3dd6d34878512d3243384992a`.

## Upstream Aeron & Agrona MPSC Ring Buffer Semantics

In Agrona and Aeron 1.54:
1. **Upstream PR #2156**:
   - `aeron_mpsc_rb.c`: `aeron_mpsc_rb_write` and `aeron_mpsc_rb_try_claim` fence the claim marker (negative length) before writing the payload record on weak-memory architectures (ARM64).
   - This ensures that if a writer is preempted or crashes during payload writing, `unblock()` can safely detect `length < 0` and convert the aborted claim into a `PADDING_MSG_TYPE_ID` record without mistaking partially written payload for valid record headers.
2. **Buffer Wrap, Padding & Memory Zeroing**:
   - Capacity reservations use CAS on `tail_position` to atomically reserve space for both required record capacity and any necessary end-of-buffer padding.
   - When readers consume records, memory is zeroed before `head_position` is published (`memset(buffer + head_index, 0, bytes_read)`). This guarantees that on subsequent buffer wraps, unwritten slots cannot be mistaken for completed records from the previous lap.
3. **Controlled Read & Claim Lifecycle**:
   - Zero-copy claims are provided via `try_claim`, `commit`, and `abort`.
   - Controlled polling is supported via `controlled_read` with `Abort`, `Break`, `Commit`, and `Continue` actions.

## Rust Implementation

1. **Negative Claim Marker & Publication Protocol**:
   - In `crates/agrona/src/ring_buffer/mpsc.rs`:
     - Added `claim_capacity`: atomically advances `tail_position` past both padding and record space in a single CAS.
     - Implemented negative claim marker publication in `write` and `try_claim`: `put_i32_ordered(record_index + LENGTH_OFFSET, -(record_length as i32))` precedes payload writes, followed by ordered positive length publication `record_length as i32`.
2. **Buffer Zeroing on Read**:
   - In `read_with_limit` and `controlled_read_with_limit`:
     - Bounded reads to the contiguous block (`capacity - head_index`).
     - Added `AtomicBuffer::set_memory(head_index, bytes_read, 0)` before publishing `head_position`, preventing stale lengths on second-lap reuse.
3. **Unblock Algorithm**:
   - Implemented `ManyToOneRingBuffer::unblock()` mirroring `aeron_mpsc_rb_unblock`:
     - If `length < 0`: rewrites header to `PADDING_MSG_TYPE_ID` with `length = -length`.
     - If `length == 0`: scans ahead for subsequent published records, verifies the gap is zeroed, and inserts a padding record covering the gap.
4. **Controlled Read & Zero-Copy API**:
   - Added `ControlledReadAction` (`Abort`, `Break`, `Commit`, `Continue`) and `controlled_read`/`controlled_read_with_limit`.
   - Added `try_claim`, `commit`, `abort`, `size`, `max_message_length`, and `next_correlation_id`.
   - Added `AgronaError::InvalidMessageType`, `AgronaError::MessageTooLarge`, and `AgronaError::InvalidClaimOffset`.

## Verification

1. **Wholeset Unit Tests**:
   - Comprehensive test suite covering all cases from `aeron_mpsc_rb_test.cpp`:
     - Buffer capacity validation, min capacity, power of two checks.
     - Message type validation (rejects negative and zero types).
     - Message size limits and max message length calculations.
     - Writing to empty buffer, full buffer rejection, insufficient space checks.
     - Buffer wrap with padding record insertion at buffer end and record write at index 0.
     - Reading from empty buffer, single message, two messages, and read limits.
     - Unblocking stalled writer with negative marker (`length < 0`) and zero gap (`length == 0`).
     - `try_claim`, `commit`, and `abort` lifecycle with bounds and error checks.
     - `controlled_read` with `Abort`, `Break`, `Commit`, and `Continue`.
     - Stale read prevention on second lap after buffer wrap.
     - Multi-threaded concurrent stress tests across producers and consumer (50,000 messages per producer for `write` and `try_claim`).
2. **Loom Concurrency Model Checking**:
   - Verified under `RUSTFLAGS="--cfg loom" cargo test -p agrona --lib`:
     - `test_mpsc_loom`: 2 concurrent writers with CAS retry loop + 1 consumer, verifying no lost messages, no torn reads, correct ordering.
     - `test_mpsc_loom_try_claim`: 2 concurrent writers using `try_claim` and `commit` + 1 consumer.
     - `test_mpsc_loom_unblock`: 1 writer stalled with negative claim marker + unblocker unblocking + consumer successfully proceeding past padding.
