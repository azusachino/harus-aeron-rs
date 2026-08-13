# Chapter 2: Client Wire Protocol & Log Buffer Layout

The Aeron Client (`aeron-client` in Rust, `io.aeron` in Java) manages the wire protocol frame codecs and high-performance, shared-memory log buffer descriptors. Before Publications and Subscriptions can send or receive streams, Aeron relies on standard binary header layouts and three-term rotated log buffers to achieve lock-free, zero-copy messaging.

In this chapter, you will learn the design principles behind Aeron's wire protocol and log buffer operations, and implement the learner stubs in `tutorial/client`.

Two tables carry this whole chapter, and they're both small enough to hold in your head at once.
The first is the 32-byte `DataHeaderFlyweight` that rides in front of every payload on the wire —
learn its field offsets and you've learned the wire protocol, because everything else (SM, NAK,
SETUP, RTTM) is a variation on the same 8-byte common header. The second is the log buffer's
metadata descriptor: four 64-bit counters that between them encode which of the three term
partitions is being written, which is being read, and which is sitting clean waiting for its turn.
Neither table needs a flowchart next to it — the rotation logic falls out of the layout once you
see it.

---

## 1. Wire Protocol Frame Layouts (`protocol.rs`)

All messages sent over UDP or shared memory IPC are framed with standard Aeron headers:
- **`HeaderFlyweight`**: Common 32-byte header base containing:
  - `frame_length` (32-bit integer, low 32 bits of first 64-bit word)
  - `version` (8-bit unsigned integer)
  - `flags` (8-bit bitfield: `BEGIN_FLAG`, `END_FLAG`, `EOS_FLAG`, `OBJECT_ALIGNMENT_FLAG`, etc.)
  - `type` (16-bit integer: `HDR_TYPE_DATA`, `HDR_TYPE_PAD`, `HDR_TYPE_SM`, `HDR_TYPE_NAK`, `HDR_TYPE_SETUP`, `HDR_TYPE_RTTM`)
- **`DataHeaderFlyweight`**: Header for data messages containing:
  - `term_offset` (32-bit integer at offset 8)
  - `session_id` (32-bit integer at offset 12)
  - `stream_id` (32-bit integer at offset 16)
  - `term_id` (32-bit integer at offset 20)
  - `reserved_value` (64-bit integer at offset 24)
- **`ErrorFlyweight`**: Driver error response containing:
  - `offending_command_correlation_id` (64-bit integer at offset 8)
  - `error_code` (32-bit integer at offset 16)
  - UTF-8 `error_message` with length prefix

---

## 2. Three-Term Log Buffer Architecture (`logbuffer.rs`)

Every `DataHeaderFlyweight` you just read carries a `term_id` and a `term_offset`, and those two
fields only make sense once you know where they land: a fixed-size ring of exactly three term
buffers. `crates/client/src/logbuffer.rs` is deliberately table-first about this — most of the
file is offset constants (`LOG_MTU_LENGTH_OFFSET` = 272, `LOG_TERM_LENGTH_OFFSET` = 276,
`LOG_PAGE_SIZE_OFFSET` = 280, `LOG_IS_CONNECTED_OFFSET` = 136, `LOG_END_OF_STREAM_POSITION_OFFSET`
= 128) into a 4096-byte metadata buffer that sits alongside the three term buffers on disk. The
three tail counters at `TERM_TAIL_COUNTERS_OFFSET` (one 64-bit counter per partition, packing
`term_id` in the high 32 bits and `term_offset` in the low 32 bits) plus the single
`LOG_ACTIVE_TERM_COUNT_OFFSET` counter are the entire rotation mechanism: `active_term_count %
PARTITION_COUNT` tells you which of the three partitions is currently being written, and bumping
that counter by one (paired with resetting the next partition's tail) is the entire "flowchart" of
term rotation. Everything downstream — `compute_position`, gap scanning, term rebuilding — is just
arithmetic over those four numbers.

Aeron streams use a 3-partition rotated log buffer layout. At any moment:
- Partition $i \bmod 3$ is active for append/write.
- Partition $(i + 1) \bmod 3$ is active for read/consume.
- Partition $(i + 2) \bmod 3$ is clean/ready for future term rotation.

### Metadata Descriptor Layout
The metadata buffer contains control counters for active term count and tails:
- `PARTITION_COUNT` = 3
- `LOG_META_DATA_LENGTH` = 4096 bytes
- `LOG_ACTIVE_TERM_COUNT_OFFSET` (offset 24): Atomic active term count.
- `TERM_TAIL_COUNTERS_OFFSET` (offset 0..24): Array of 64-bit tail counters for the 3 partitions (high 32 bits = `term_id`, low 32 bits = `term_offset`).

Rotation is performed atomically using Compare-And-Swap (`rotate_log`) without blocking readers or concurrent writers.

---

## 3. Log Buffer Helpers & Term Operations

### Header Mapper (`Header<'a>`)
During reading, a `Header` struct acts as a lightweight flyweight view over frames in the term buffer, computing global stream positions via `compute_position(term_id, term_offset, position_bits_to_shift, initial_term_id)`.

### Term Utility Suite
1. **`TermScanner`**: Scans available data fragments up to `max_length` or MTU boundaries, handling padding frames (`HDR_TYPE_PAD`).
2. **`TermGapScanner`**: Scans for gaps in lossy receiver streams and triggers `on_gap(term_id, gap_offset, gap_length)` callbacks.
3. **`TermReader`**: Reads available data fragments from a term, advancing `AtomicCounter` subscriber positions with release semantics.
4. **`TermRebuilder`**: Rebuilds out-of-order received frame packets into a term buffer safely (writing mid-header words first, and releasing the frame length word last).
5. **`TermGapFiller`**: Fills detected gaps with padding records when loss cannot be recovered.
6. **`TermUnblocker` & `LogBufferUnblocker`**: Detects and unblocks stalled or dead publisher claims to allow stream rotation to proceed.

---

## Hands-On Exercise

Implement the stubs in `tutorial/client/src/`:
1. `protocol.rs`
2. `logbuffer.rs`

Check your implementation by comparing with the reference code in `crates/client/src/` and running:
```bash
make check
```
