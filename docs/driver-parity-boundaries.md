# Driver parity boundaries

The driver parity register is a file-level ledger against Aeron 1.54.0 (commit `454da1e4e6c7edf3dd6d34878512d3243384992a`). The two broad
driver files below are marked `done` for the Rust runtime boundary they describe, with
the Java-only portions recorded explicitly here.

## Verified Aeron 1.54.0 Driver Capabilities

The driver runtime has been upgraded to match Aeron 1.54.0 specifications across the following milestones:
- **Packet Validation**: Every frame in a multi-frame UDP packet is validated before any log or driver state mutation; malformed trailing frames reject the entire packet without side effects (`docs/verification/aeron-1.54-udp-packet-validation.md`).
- **Status Message Bounds**: Implemented upstream 1.54 sender-relative bounds `[-0.5 term, +1.5 terms]` guarding against malformed or out-of-order SMs (`docs/verification/aeron-1.54-status-message-bounds.md`).
- **Min Flow Control Liveness**: Sender tracks active receiver heartbeats and times out inactive slow receivers, restoring progress (`docs/verification/aeron-1.54-min-flow-control-liveness.md`).
- **MDC Destination Lifecycle**: Removing a dynamic/manual MDC destination cleans up transport endpoints and allows clean re-addition without leaking state (`docs/verification/aeron-1.54-mdc-destination-lifecycle.md`).
- **Untethered Subscriber Draining**: Untethered subscribers in `LINGER` status transition cleanly to `DRAINING` and are closed when resting behind publication tails, preventing hung resources (`docs/verification/aeron-1.54-untethered-draining-cleanup.md`).
- **IPC Term Buffer Zeroing**: Implemented `cleanBufferTo` zeroing dirty term partitions behind subscribers, avoiding stale frame replays when term buffers lap (`docs/verification/aeron-1.54-client-apis-end-to-end.md`).
- **Channel URI Socket Options**: Implemented `so-tos` (IP TOS/DiffServ byte `0..255`) and `rcv-wnd` parsing with strict bounds validation (`docs/verification/aeron-1.54-bounded-feature-scope.md`).

## `DriverConductorTest`

The Rust equivalent covers command decoding and safety, publication and subscription
registration/removal, IPC and UDP linking, session and entity tags, dynamic destinations,
spy subscriptions, client timeouts, counters, image lifecycle, untethered IPC state, and
feedback/retransmission selection. The direct client-level spy test also proves that a
network spy polls bytes from the publication log without creating a second receive
transport.

The upstream cycle-threshold counter assertions and injectable async-executor shutdown
tests are Java implementation details. The Rust conductor is single-threaded within the
managed driver duty cycle, and `RunningMediaDriver` owns its standard thread directly.

## `MediaDriverContextTest`

The Rust equivalent covers startup validation for CnC ring geometry, counter and error
buffer bounds, term/MTU/window geometry, liveness, untethered timeouts, NAK timing,
resolver configuration, termination policy, storage policy, file page size, and shared
Cnc metadata. Driver command errors are counted in the standard CnC driver-exceptions
counter, surfaced as `ON_ERROR` events, and (2026-07-27) recorded into the CnC file's
error-log region via a real `DistinctErrorLog` writer (`aeron_client::error_log`, ported
from `vendor/aeron/aeron-client/src/main/c/concurrent/aeron_distinct_error_log.c`) —
readable directly from the CnC file by real Java `ErrorLogReader` tooling, not just by a
client subscribed to the broadcast at the time. Deduplicates repeat errors into one
record with a running `observation_count`, matching vendor semantics.

The upstream injectable `Executor` API and reflective assertions about Java transport
poller error-handler objects have no Rust equivalent. Rust uses the managed driver thread
and propagates duty-cycle failures through `RunningMediaDriver::stop`.

## Verification

The driver boundary is verified with:

- `make check`
- `make test-parity`
- `make interop-rust-smoke`
- `make interop-rust-soak`
- `make interop-smoke`
- `make interop-soak`

The Rust smoke and soak checks prove the multi-process Rust IPC/UDP paths; the Java
smoke and soak checks prove CnC/IPC/UDP mixing with the vendored Aeron `1.54.0` runtime,
with the Java soak validating ordered delivery of a bounded sequence-tagged UDP run.
These checks do not claim archive, cluster, or full system-test parity.
