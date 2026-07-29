# Chapter 10: Aeron Archive Codecs, Catalog & Conductor

This chapter introduces the `aeron-archive` module architecture, protocol codecs, persistent recording catalog, and conductor sessions.

## Archive Control Protocol & Flyweights

- `crates/archive/src/codecs.rs`: bounded complete-message codecs for the v13 connect, start/stop recording, replay/stop replay, listing, recording-history position queries, find-last matching, and control-response SBE templates. Each frame uses the exact eight-byte message header, template-specific fixed block, and any required length-prefixed variable ASCII field.
- `crates/archive/src/control_session_adapter.rs`: validates the SBE header and dispatches complete request frames by their real template IDs.
- `crates/archive/src/recording_descriptor.rs`: complete template-22 `RecordingDescriptor` codec with the 80-byte fixed block and three bounded variable ASCII fields.
- `crates/archive/src/recording_subscription_descriptor.rs`: template-23 response codec for listing active recording subscription IDs, streams, and stripped channels.

## Persistent Catalog Indexing

- `crates/archive/src/catalog.rs`: `CatalogHeader` and `Catalog` manager handling sequential `recording_id` allocation and descriptor indexing.

## ArchiveConductor & Sessions

- `crates/archive/src/control_session.rs`: `ControlSession` state machine (`Init`, `Connected`, `Closed`).
- `crates/archive/src/conductor.rs`: `ArchiveConductor` for processing client connection requests (`on_connect`) and stream recording sessions (`on_start_recording`).
- `crates/archive/src/runtime.rs`: persistent recording of aligned frames from a real subscription, active and catalog-backed recording-position queries, and an incremental `ReplayCursor` that validates position/length bounds and advances only after successful publication offers.
- `crates/archive/src/service.rs`: `ArchiveContext`, the CnC-backed managed duty cycle, recording-subscription IDs and listing, stop-by-subscription, request correlation/deduplication, response publication, replay-session IDs, `StopReplay`, and final-fragment publication linger.
- `crates/archive/src/bin/aeron-archive.rs`: standalone process that attaches to an existing Media Driver and stops cleanly when standard input closes.

The integration test in `crates/archive/tests/control_service.rs` drives the real
control publication and response subscription from a separate client, then proves
record, stop, Archive restart, list-by-id, ranged and URI-filtered descriptor
responses, exhaustion signaling, bounded replay, invalid-position rejection, and
stopping replay before completion. It also verifies that start-recording returns the
subscription registration ID, template-23 listing exposes it, and stopping by that ID
removes the subscription while finalizing its catalog recording. Position queries
prove the catalog start, the active `NULL_POSITION` stop, the live writer maximum,
the finalized stop position before and after restart, and exact-session find-last
matching through the same external control client.

## Verification

Run all tests and workspace checks with:

```bash
make check
```
