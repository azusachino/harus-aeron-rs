# Chapter 1: Agrona Concurrent Primitives

Agrona (`org.agrona` in Java) provides the core lock-free data structures, concurrent ring buffers, atomic byte buffers, and idle strategies used by the Aeron Media Driver and Client. Before Aeron can handle UDP packets or log buffers, it relies on Agrona to achieve low-latency IPC and concurrent state management.

In this chapter, you will learn the design principles behind Agrona's core primitives and implement the learner stubs in `tutorial/agrona`.

If you only remember one thing from this chapter, remember this: every "clever" concurrency
trick in Agrona is really a byte layout wearing a trench coat. There is no lock, no condvar, no
mutex anywhere in this crate — just two threads that have agreed, ahead of time, on exactly which
bytes at exactly which offsets mean what, and on the ordering rules (volatile load, release
store, CAS) that make reading and writing those bytes safe without a lock. Once you can draw the
buffer layout on a whiteboard, the "algorithm" is usually just "read this field, do the obvious
thing, write that field back."

---

## 1. Idle Strategies (`idle_strategy.rs`)

When an agent (such as the Aeron Receiver or Driver Conductor) runs in a duty cycle, it repeatedly polls for work. If no work is found (`work_count == 0`), the agent must idle to prevent burning CPU or to tune latency vs. power usage.

Agrona defines 5 standard idle strategies:

1. **`BusySpinIdleStrategy`**: Busy-spins using CPU hints (`std::hint::spin_loop()`). Lowest latency, 100% CPU core usage.
2. **`YieldingIdleStrategy`**: Yields the CPU thread (`std::thread::yield_now()`). Balances latency and CPU sharing.
3. **`SleepingIdleStrategy`**: Sleeps for a configured duration (`std::thread::sleep(...)`). Lowest power, highest latency.
4. **`NoOpIdleStrategy`**: Does nothing.
5. **`BackoffIdleStrategy`**: Phased backoff progression:
   - Phase 1: Spin for $N$ iterations.
   - Phase 2: Yield for $M$ iterations.
   - Phase 3: Park / sleep with exponentially increasing durations up to a max cap.

---

## 2. Atomic Buffer (`atomic_buffer.rs`)

`AtomicBuffer` is a zero-copy wrapper around a raw byte slice (`*mut u8` and `usize`). It provides safe, checked, little-endian atomic and volatile accessors:

- **Endianness**: All integer gets/puts are strictly little-endian (`from_le_bytes`, `to_le_bytes`).
- **Atomic Operations**: Volatile loads (`get_i64_volatile`), release stores (`put_i64_ordered`), atomic Compare-And-Swap (`compare_and_set_i64`), and atomic fetch-and-add (`get_and_add_i64`).
- **String & Bytes Copy**: UTF-8 string encoding/decoding with 32-bit length headers (`put_string_utf8`, `get_string_utf8`).

---

## 3. SPSC & MPSC Ring Buffers (`ring_buffer/`)

Here is the table that matters most in this whole chapter: the trailer at the tail end of the
ring buffer. A ring buffer is really just a data region plus this trailer — the trailer is the
entire coordination protocol between producer and consumer, and once you've internalized its five
fields you can predict most of what `spsc.rs`/`mpsc.rs` do without reading a line of control flow.

The current implementation (`crates/agrona/src/ring_buffer/mod.rs`) pads every counter to two
cache lines (`COUNTER_PADDING = CACHE_LINE_LENGTH * 2 = 128` bytes) so that a producer-written
counter and a consumer-written counter never sit on the same cache line — the classic false-sharing
fix. That gives a 768-byte trailer (`TRAILER_LENGTH = COUNTER_PADDING * 6`) laid out as:

| Offset | Field | Written by |
| --- | --- | --- |
| 128 | `TAIL_POSITION_OFFSET` — 64-bit tail counter | producer |
| 256 | `HEAD_CACHE_POSITION_OFFSET` — producer's cached copy of head | producer |
| 384 | `HEAD_POSITION_OFFSET` — 64-bit head counter | consumer |
| 512 | `CORRELATION_COUNTER_OFFSET` | either |
| 640 | `CONSUMER_HEARTBEAT_OFFSET` — liveness timestamp | consumer |

The producer only ever writes tail and its own cached head; the consumer only ever writes head and
the heartbeat. Neither thread waits on the other — a producer that outruns the consumer just
observes a stale `head_cache` and refreshes it from `HEAD_POSITION_OFFSET` before deciding whether
there's room. That's the whole "lock-free" story: no blocking primitive, just "which offset am I
allowed to write, and which one do I re-read when I'm not sure."

Agrona's lock-free ring buffers operate over a contiguous `AtomicBuffer`: a data region followed
by the 768-byte trailer tabulated above.

- **`OneToOneRingBuffer` (SPSC)**: Single-producer single-consumer ring buffer.
- **`ManyToOneRingBuffer` (MPSC)**: Many-producer single-consumer ring buffer using atomic CAS loops to reserve tail space concurrently.

### Memory Layout & Descriptor

```text
+------------------------------------------+-----------------------+
|              Data Buffer                 |        Trailer        |
|          (0 .. Data Capacity)            | (Data Cap .. +768)    |
+------------------------------------------+-----------------------+
```

Each message written into the data region carries its own small header, independent of the
trailer above:

- `HEADER_LENGTH` (8 bytes): 32-bit `length` at offset 0 + 32-bit `msg_type_id` at offset 4.
- `PADDING_MSG_TYPE_ID` (-1): Used when a message cannot fit at the end of the buffer and wraps to index 0.

---

## 4. Broadcast Transmitter & Receiver (`broadcast.rs`)

`BroadcastTransmitter` (single-writer) and `BroadcastReceiver` (multi-reader) allow one thread to broadcast messages to any number of independent receivers:

- **Lock-free Optimistic Validation**: Receivers check `tail_intent` before and after reading the message payload. If the transmitter overwrote memory mid-read (lapping), the receiver detects tearing and returns `AgronaError::Lapped`, catching up to the latest tail.

---

## 5. Counters Manager (`counters.rs`)

Counters are the other place Aeron leans on Agrona to avoid a database: every position, backlog,
and health signal you'd otherwise query through an API is instead a slot in one of two parallel,
fixed-stride arrays that any process can `mmap` and read cold. `CountersManager` is deliberately
thin — its whole job is allocating and freeing slots in that pair of tables, and `AtomicCounter`
is a small handle onto one slot.

In `crates/agrona/src/counters.rs`, the metadata array uses 512-byte records
(`METADATA_LENGTH`) — `state` (4 bytes: `RECORD_UNUSED` = 0, `RECORD_ALLOCATED` = 1,
`RECORD_RECLAIMED` = -1) at offset 0, `type_id` at offset 4, a `free_for_reuse_deadline_ms` at
offset 8, a 112-byte key region at offset 16, and a length-prefixed 380-byte label at offset 128.
The values array uses 128-byte records (`COUNTER_LENGTH`, two cache lines, so a counter you're
hammering with `increment()` never shares a line with its own metadata's registration/owner
fields) — the live 64-bit value at offset 0, `registration_id` at 8, `owner_id` at 16, and
`reference_id` at 24. `CountersManager::new` takes the smaller of `metadata_len /
METADATA_LENGTH` and `values_len / COUNTER_LENGTH` as `max_counters`, so the two arrays always
stay index-aligned: counter *N*'s metadata and counter *N*'s value are always the same *N* slots
into their respective buffers.

Aeron uses `CountersManager` to allocate and track 64-bit atomic performance counters (e.g. bytes received, position counters, heartbeats) across shared memory:

- **Metadata Buffer**: 512-byte records containing state (`RECORD_ALLOCATED`, `RECORD_RECLAIMED`), type ID, binary key data, and label string.
- **Values Buffer**: 128-byte padded 64-bit integer values (avoiding CPU cache-line false sharing).
- **`AtomicCounter`**: Convenient handle for atomic `increment()`, `add()`, `set()`, and `compare_and_set()`.

---

## Hands-On Exercise

Implement the stubs in `tutorial/agrona/src/`:

1. `idle_strategy.rs`
2. `atomic_buffer.rs`
3. `ring_buffer/spsc.rs` & `ring_buffer/mpsc.rs`
4. `broadcast.rs`
5. `counters.rs`

Check your implementation by comparing with the reference code in `crates/agrona/src/` and running:

```bash
make tutorial-check
make test
```
