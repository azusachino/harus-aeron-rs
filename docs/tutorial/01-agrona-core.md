# Chapter 1: Agrona Concurrent Primitives

Agrona (`org.agrona` in Java) provides the core lock-free data structures, concurrent ring buffers, atomic byte buffers, and idle strategies used by the Aeron Media Driver and Client. Before Aeron can handle UDP packets or log buffers, it relies on Agrona to achieve low-latency IPC and concurrent state management.

In this chapter, you will learn the design principles behind Agrona's core primitives and implement the learner stubs in `tutorial/agrona`.

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

Agrona's lock-free ring buffers operate over a contiguous `AtomicBuffer` with a 1024-byte descriptor trailer at the end of the buffer:
- **`OneToOneRingBuffer` (SPSC)**: Single-producer single-consumer ring buffer.
- **`ManyToOneRingBuffer` (MPSC)**: Many-producer single-consumer ring buffer using atomic CAS loops to reserve tail space concurrently.

### Memory Layout & Descriptor
```
+------------------------------------------+-----------------------+
|              Data Buffer                 |        Trailer        |
|          (0 .. Data Capacity)            |  (Data Cap .. +1024)  |
+------------------------------------------+-----------------------+
```
- `TAIL_POSITION_OFFSET` (0): 64-bit tail counter.
- `HEAD_CACHE_POSITION_OFFSET` (8): Producer's cached head position.
- `HEAD_POSITION_OFFSET` (16): Consumer's head counter.
- `HEADER_LENGTH` (8 bytes): 32-bit length + 32-bit msg_type_id.
- `PADDING_MSG_TYPE_ID` (-1): Used when a message cannot fit at the end of the buffer and wraps to index 0.

---

## 4. Broadcast Transmitter & Receiver (`broadcast.rs`)

`BroadcastTransmitter` (single-writer) and `BroadcastReceiver` (multi-reader) allow one thread to broadcast messages to any number of independent receivers:
- **Lock-free Optimistic Validation**: Receivers check `tail_intent` before and after reading the message payload. If the transmitter overwrote memory mid-read (lapping), the receiver detects tearing and returns `AgronaError::Lapped`, catching up to the latest tail.

---

## 5. Counters Manager (`counters.rs`)

Aeron uses `CountersManager` to allocate and track 64-bit atomic performance counters (e.g. bytes received, position counters, heartbeats) across shared memory:
- **Metadata Buffer**: 1024-byte records containing state (`RECORD_ALLOCATED`, `RECORD_RECLAIMED`), type ID, binary key data, and label string.
- **Values Buffer**: 64-byte padded 64-bit integer values (avoiding CPU cache-line false sharing).
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
