# Archive v13 SBE Control Framing

Issue #32 replaces the invented shared request header with complete bounded SBE messages. The pinned wire authority is `vendor/aeron/aeron-archive/src/main/resources/archive/aeron-archive-codecs.xml`: schema id 101, schema version 13, little-endian.

The foundation exposes complete codecs for the protocol boundary exercised by its current control session:

| Template | ID | Fixed block | Variable field |
| --- | ---: | ---: | --- |
| `ControlResponse` | 1 | 32 | `errorMessage` |
| `ConnectRequest` | 2 | 16 | `responseChannel` |
| `StartRecordingRequest` | 4 | 24 | `channel` |
| `RecordingPositionRequest` | 12 | 24 | none |
| `StopPositionRequest` | 15 | 24 | none |
| `FindLastMatchingRecordingRequest` | 16 | 32 | `channel` |
| `StartPositionRequest` | 52 | 24 | none |
| `MaxRecordedPositionRequest` | 67 | 24 | none |

Every complete frame starts with the eight-byte SBE message header. Variable ASCII fields use a four-byte unsigned little-endian length followed by US-ASCII bytes. Decoders validate the frame boundary, schema, template, minimum block length, enum discriminants, variable length, and ASCII encoding before reading data.

The managed boundary also implements `CloseSession=3`, `StopRecording=5`,
`Replay=6`, `StopReplay=7`, `ListRecordings=8`, `ListRecordingsForUri=9`,
`ListRecording=10`, `StopRecordingSubscription=14`, and
`ListRecordingSubscriptions=17`. Adding a request to the live Archive boundary
requires its own template-specific codec; there is deliberately no generic request
wire structure.

`test/archive_sbe/ArchiveSbeGoldenVectors.java` generates the three interoperability vectors from the pinned Java SBE-generated codecs. The Rust tests assert those exact bytes.
