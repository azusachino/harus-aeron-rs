# Chapter 10: Aeron Archive Codecs, Catalog & Conductor

This chapter introduces the `aeron-archive` module architecture, protocol codecs, persistent recording catalog, and conductor sessions.

This is the chapter where the theaeronfiles philosophy pays off directly, because Archive's state
genuinely is a set of files you might have to read cold, at 3am, after a crash — not an
abstraction that happens to be backed by files. Two tables matter more than any control-flow
diagram here.

The first is the recording descriptor itself. `crates/archive/src/protocol/recording_descriptor.rs`
defines `RecordingDescriptor` as SBE template 22 with an 80-byte fixed block
(`RecordingDescriptor::BLOCK_LENGTH = 80`) — `control_session_id`, `correlation_id`,
`recording_id`, `start_position`/`stop_position`, `start_timestamp`/`stop_timestamp`,
`initial_term_id`, `segment_file_length`, `term_buffer_length`, `mtu_length`, `session_id`,
`stream_id` — followed by three length-prefixed ASCII fields (`stripped_channel`,
`original_channel`, `source_identity`). That fixed block is small and dense on purpose: it's the
part of a recording's identity that's cheap to scan across thousands of recordings without
touching the variable-length tail.

The second is the catalog that indexes those descriptors. `crates/archive/src/storage/catalog.rs`
wraps a `CatalogHeader` (`version`, `length`, `next_recording_id`) around a `HashMap<i64,
RecordingDescriptor>` plus an ordered `Vec` for range scans, and hands out recording IDs
sequentially from `next_recording_id`. `archive-mark.dat`
(`crates/archive/src/storage/archive_mark_file.rs`) is the liveness half of the same story: a
fixed header plus an embedded error buffer that a *separate* process — `aeron-archive-tool` — can
open and read without disturbing a running Archive, to answer "is this Archive alive, and what did
it last log," entirely from the file, no RPC required.

## Archive Control Protocol & Flyweights

- `crates/archive/src/protocol/archive_exception.rs`: upstream Archive error-code constants and stable string formatting for unknown codes.
- `crates/archive/src/storage/archive_counters.rs`: Archive-ID keyed counter allocation, lookup, and label formatting over `CountersManager`.
- `crates/archive/src/storage/recording_pos.rs`: recording-position counter key construction and lookup by recording/session and Archive ID.
- `crates/archive/src/service/delete_segments_session.rs`: incremental segment deletion, maximum position parsing, and ordered delete signaling.
- `crates/archive/src/storage/checksum.rs`: CRC-32 and CRC-32C implementations, aliases, and type-safe custom checksum construction.
- `crates/archive/src/protocol/codecs.rs`: bounded complete-message codecs for the v13 connect, start/stop/extend/truncate/purge recording, replay/stop replay, listing, recording-history position queries, find-last matching, and control-response SBE templates. Each frame uses the exact eight-byte message header, template-specific fixed block, and any required length-prefixed variable ASCII field; versioned replay and replication requests also decode the shorter v6 fixed blocks with Aeron null-value defaults.
- `crates/archive/src/service/control_session_adapter.rs`: validates the SBE header and dispatches complete request frames by their real template IDs, including ReplicateRequest2 and ReplayTokenRequest, while preserving v6 request compatibility.
- `crates/archive/src/protocol/recording_descriptor.rs`: complete template-22 `RecordingDescriptor` codec with the 80-byte fixed block and three bounded variable ASCII fields.
- `crates/archive/src/protocol/recording_subscription_descriptor.rs`: template-23 response codec for listing active recording subscription IDs, streams, and stripped channels.

## Persistent Catalog Indexing

- `crates/archive/src/storage/catalog.rs`: `CatalogHeader` and `Catalog` manager handling sequential `recording_id` allocation, crash-recovery validation of the next ID, and descriptor indexing.
- `crates/archive/tests/catalog_tests.rs`: catalog state transitions, stream/history lookup, stop/extend/remove index updates, and persisted descriptor behavior; legacy page-layout, checksum, growth, and segment-scan recovery remain outside the current format.
- `crates/archive/tests/catalog_jumbo_recordings.rs`: catalog and listing-session behavior across a large (192) recording population, proving index growth, ranged listing, and URI-filtered scans stay correct at jumbo scale.
- `crates/archive/tests/properties.rs`: `proptest` coverage for catalog recording-ID allocation and recording-descriptor round-trip properties.
- `crates/archive/tests/archive_tests.rs`: integration coverage for the catalog, SBE connect/start-recording/control-response codecs, and `ArchiveConductor` lifecycle in combination, independent of the full external-client `control_service.rs` test.
- `crates/archive/tests/fail_control_response_listener.rs`: callback-result equivalent of the upstream fail-on-unexpected-response/descriptor helper; a null-correlation response is allowed, while other responses and all descriptors produce full diagnostics because the Rust Archive client listener trait is not implemented yet.
- `crates/archive/tests/fail_recording_events_listener.rs`: callback-result equivalent of the upstream fail-on-recording-event helper, covering start, progress, and stop diagnostics without claiming a Rust client listener API that does not exist yet.
- `crates/archive/src/storage/catalog_index.rs`: ordered recording-id to catalog-offset index with monotonic insertion, binary lookup, growth, and removal semantics.
- `crates/archive/src/storage/catalog_view.rs`: read-only listing of all or one persisted recording descriptor.
- `crates/archive/src/admin/archive_tool.rs`: bounded catalog inspection and administration with `describe`, `describe-all`, state transitions, invalid-record compaction, unknown-segment cleanup, separate mark-file link resolution, and descriptor verification; frame-level repair/checksum, capacity mutation, and detached-range verification remain outside the current boundary.
- `crates/archive/src/bin/aeron-archive-tool.rs`: read-only Archive catalog inspection CLI entry point wrapping `archive_tool::run`.
- `crates/archive/tests/archive_tool_tests.rs`: unit-level coverage of `archive_tool`'s capacity, compaction, orphaned-segment deletion, and verify/mark-invalid functions against a real catalog.
- `crates/archive/tests/archive_tool_cli.rs`: end-to-end CLI coverage of `archive_tool::run`'s `describe`/`describe-all` output and state-transition commands.
- `crates/archive/tests/archive_tool_separate_mark_file.rs`: proves `resolve_mark_file` and `archive_tool::run` follow a separate mark-file directory through its `archive-mark.lnk` indirection.
- `crates/archive/tests/migration_utils.rs`: legacy v2 mark-file and catalog-frame fixtures for migration coverage.
- `crates/archive/tests/archive_migration.rs`: end-to-end v2.1.0-to-v3.0.0 migration coverage over the `migration_utils` fixtures, proving mark-file and catalog conversion together.
- `crates/archive/src/admin/migration.rs`: v2.1.0 mark/catalog conversion, migration timestamp guards, descriptor-gap preservation, and v3.0.0 mark-file versioning.
- `crates/archive/src/storage/archive_mark_file.rs`: SBE-compatible `archive-mark.dat` layout, aligned file sizing, metadata channels, liveness timestamps, ready/terminated signaling, and error-buffer access; upstream migration and process-ownership machinery remains outside the current boundary.
- `crates/archive/src/service/client_context.rs`: Archive control-channel session-ID/default injection and retry-attempt validation; connection ownership remains outside the current Rust client boundary.
- `crates/archive/src/service/list_recordings_session.rs`: incremental descriptor listing with recording-unknown and abort boundaries.
- `crates/archive/src/service/list_recording_by_id_session.rs`: retryable single-recording descriptor publication, catalog invalidation re-checks, and unknown responses.
- `crates/archive/src/service/list_recordings_for_uri_session.rs`: bounded stream/channel-filtered descriptor scanning with retry and unknown-response boundaries.

## ArchiveConductor & Sessions

- `crates/archive/src/service/control_session.rs`: `ControlSession` state machine (`Init`, `Connected`, `Closed`), abort-reason capture, active-listing cancellation, and warning classification.
- `crates/archive/src/service/conductor.rs`: `ArchiveConductor` for processing client connection requests (`on_connect`) and stream recording sessions (`on_start_recording`), plus upstream-compatible stripped channel identity construction.
- `crates/archive/src/service/runtime.rs`: persistent recording of aligned frames from a real subscription, validated extension from the exact stopped term position, truncation that clears/deletes the segment tail, purge of segment and catalog state, active and catalog-backed recording-position queries, and an incremental `ReplayCursor` that replays stopped recordings, follows active recordings with `length=-1`, takes a bounded active snapshot with `length=-2`, or obeys a counter-bounded replay limit, validates position/length bounds, and advances only after successful publication offers.
- `crates/archive/src/service/archive_service.rs`: `ArchiveContext` defaults and launch validation, configurable separate mark-file directories with `archive-mark.lnk`, the CnC-backed managed duty cycle, shared recording-subscription ownership with one recording session per joined image, recording-subscription IDs and listing, stop-by-subscription, template-63/64 auto-stop on publisher end-of-stream, request correlation/deduplication, response publication, replay-session IDs, `StopReplay`, and final-fragment publication linger.
- `crates/archive/src/bin/aeron-archive.rs`: standalone process that attaches to an existing Media Driver and stops cleanly when standard input closes.

The integration test in `crates/archive/tests/control_service.rs` drives the real
control publication and response subscription from a separate client, then proves
record, stop, Archive restart, list-by-id, ranged and URI-filtered descriptor
responses, exhaustion signaling, bounded replay, active replay follow after a later
publication, invalid-position rejection, and stopping replay before completion. It
also verifies that start-recording returns the
subscription registration ID, template-23 listing exposes it, and stopping by that ID
removes the subscription while finalizing its catalog recording. Position queries
prove the catalog start, the active `NULL_POSITION` stop, the live writer maximum,
the active-only recording position and its stopped `NULL_POSITION`, the finalized
stop position before and after restart, and exact-session find-last matching through
the same external control client.

The runtime lifecycle test also replays the range already written by an active
recording; it intentionally does not claim live follow behavior. The mutation
lifecycle test launches an extending publication from the stopped
recording's exact initial-term/term-id/term-offset triple, appends frames under the
same recording ID, truncates back to the prior aligned stop, restarts and replays only
the retained prefix, then purges and restarts again to prove both catalog invalidation
and segment deletion.

`crates/archive/tests/runtime_lifecycle.rs` also constructs two joined images under
one subscription and verifies that each image is polled into its own recording
session. The control-service multi-image test exercises the same behavior through
real MediaDriver channels.

## Verification

Run all tests and workspace checks with:

```bash
make check
```
