# Driver parity boundaries

The driver parity register is a file-level ledger against Aeron 1.50.x. The two broad
driver files below are marked `done` for the Rust runtime boundary they describe, with
the Java-only portions recorded explicitly here.

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

The smoke and soak checks prove the Rust multi-process IPC/UDP paths; the soak validates
ordered delivery of a bounded sequence-tagged message run. `make interop-smoke` proves
the separate bidirectional Java CnC/IPC/UDP probes. These checks do not claim archive,
cluster, or full system-test parity.
