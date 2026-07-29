# Chapter 10: Aeron Archive Codecs, Catalog & Conductor

This chapter introduces the `aeron-archive` module architecture, protocol codecs, persistent recording catalog, and conductor sessions.

## Archive Control Protocol & Flyweights

- `crates/archive/src/codecs.rs`: bounded complete-message codecs for the v13 connect, start/stop recording, replay/stop replay, listing, and control-response SBE templates. Each frame uses the exact eight-byte message header, template-specific fixed block, and any required length-prefixed variable ASCII field.
- `crates/archive/src/control_session_adapter.rs`: validates the SBE header and dispatches complete request frames by their real template IDs.
- `crates/archive/src/recording_descriptor.rs`: `RecordingDescriptor` struct encapsulating recording metadata (`recording_id`, `start_position`, `stop_position`, `stream_id`, `channel`).

## Persistent Catalog Indexing

- `crates/archive/src/catalog.rs`: `CatalogHeader` and `Catalog` manager handling sequential `recording_id` allocation and descriptor indexing.

## ArchiveConductor & Sessions

- `crates/archive/src/control_session.rs`: `ControlSession` state machine (`Init`, `Connected`, `Closed`).
- `crates/archive/src/conductor.rs`: `ArchiveConductor` for processing client connection requests (`on_connect`) and stream recording sessions (`on_start_recording`).
- `crates/archive/src/runtime.rs`: persistent recording of aligned frames from a real subscription and replay through a real publication after catalog restart.
- `crates/archive/src/service.rs`: `ArchiveContext`, the CnC-backed managed duty cycle, request correlation/deduplication, and response publication.
- `crates/archive/src/bin/aeron-archive.rs`: standalone process that attaches to an existing Media Driver and stops cleanly when standard input closes.

The integration test in `crates/archive/tests/control_service.rs` drives the real
control publication and response subscription from a separate client, then proves
record, stop, Archive restart, listing, and replay.

## Verification

Run all tests and workspace checks with:

```bash
make check
```
