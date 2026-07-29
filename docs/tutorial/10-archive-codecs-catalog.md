# Chapter 10: Aeron Archive Codecs, Catalog & Conductor

This chapter introduces the `aeron-archive` module architecture, protocol codecs, persistent recording catalog, and conductor sessions.

## Archive Control Protocol & Flyweights

- `crates/archive/src/codecs.rs`: bounded complete-message codecs for the v13 `ConnectRequest`, `StartRecordingRequest`, and `ControlResponse` SBE templates. Each frame uses the exact eight-byte message header, template-specific fixed block, and length-prefixed variable ASCII field.
- `crates/archive/src/recording_descriptor.rs`: `RecordingDescriptor` struct encapsulating recording metadata (`recording_id`, `start_position`, `stop_position`, `stream_id`, `channel`).

## Persistent Catalog Indexing

- `crates/archive/src/catalog.rs`: `CatalogHeader` and `Catalog` manager handling sequential `recording_id` allocation and descriptor indexing.

## ArchiveConductor & Sessions

- `crates/archive/src/control_session.rs`: `ControlSession` state machine (`Init`, `Connected`, `Closed`).
- `crates/archive/src/conductor.rs`: `ArchiveConductor` for processing client connection requests (`on_connect`) and stream recording sessions (`on_start_recording`).

## Verification

Run all tests and workspace checks with:

```bash
make check
```
